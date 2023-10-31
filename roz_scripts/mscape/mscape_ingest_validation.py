#!/usr/bin/env python3

import csv
import os
from pathlib import Path
import json
import copy
import boto3
from botocore.exceptions import ClientError
import time
import logging
import argparse
import multiprocessing as mp
from collections import namedtuple

from roz_scripts.utils.utils import (
    pipeline,
    init_logger,
    get_credentials,
    onyx_unsuppress,
    onyx_submission,
    onyx_update,
)
from varys import varys

from onyx import OnyxClient


class worker_pool_handler:
    def __init__(self, workers, logger, varys_client):
        self._log = logger
        self.worker_pool = mp.Pool(processes=workers)
        self._varys_client = varys_client

        self._log.info(f"Successfully initialised worker pool with {workers} workers")

    def submit_job(self, message, args, ingest_pipe):
        self._log.info(
            f"Submitting job to the worker pool for UUID: {json.loads(message.body)['uuid']}"
        )

        self.worker_pool.apply_async(
            func=validate,
            kwds={"message": message, "args": args, "ingest_pipe": ingest_pipe},
            callback=self.callback,
            error_callback=self.error_callback,
        )

    def callback(self, validate_result):
        success, payload, message = validate_result
        if success:
            self._log.info(
                f"Successful validation for match UUID: {payload['uuid']}, sending result"
            )

            self._varys_client.acknowledge_message(message)

            new_artifact_payload = {
                "publish_timestamp": time.time_ns(),
                "cid": payload["cid"],
                "site": payload["site"],
                "platform": payload["platform"],
                "match_uuid": payload["uuid"],
            }

            self._varys_client.send(
                message=new_artifact_payload,
                exchange="inbound.new_artifact.mscape",
                queue_suffix="validator",
            )

            self._varys_client.send(
                message=payload,
                exchange=f"inbound.results.mscape.{payload['site']}",
                queue_suffix="validator",
            )

        else:
            self._log.info(
                f"Validation failed for match UUID: {payload['uuid']}, sending result"
            )

            if payload["rerun"]:
                if message.basic_deliver.redelivered:
                    self._log.error(
                        f"Message for UUID: {payload['uuid']} has been redelivered already, sending to dead letter queue"
                    )
                    payload["ingest_errors"].append(
                        f"Validation failed for UUID: {payload['uuid']} unrecoverably"
                    )

                    self._varys_client.send(
                        message=payload,
                        exchange="mscape.restricted.announce",
                        queue_suffix="dead_letter",
                    )

                    self._varys_client.send(
                        message=payload,
                        exchange=f"inbound.results.mscape.{payload['site']}",
                        queue_suffix="validator",
                    )

                    self._varys_client.nack_message(message, requeue=False)
                else:
                    self._log.info(
                        f"Rerun flag for UUID: {payload['uuid']} is set, re-queueing message"
                    )
                    self._varys_client.nack_message(message)

            else:
                self._varys_client.acknowledge_message(message)

                self._varys_client.send(
                    message=payload,
                    exchange=f"inbound.results.mscape.{payload['site']}",
                    queue_suffix="validator",
                )

    def error_callback(self, exception):
        self._log.error(f"Worker failed with unhandled exception: {exception}")
        self._varys_client.send(
            message=f"MScape ingest worker failed with unhandled exception: {exception}",
            exchange="mscape.restricted.announce",
            queue_suffix="dead_worker",
        )

    def close(self):
        self.worker_pool.close()
        self.worker_pool.join()


def execute_validation_pipeline(
    payload: dict,
    args: argparse.Namespace,
    ingest_pipe: pipeline,
) -> tuple[int, bool, str, str]:
    """Execute the validation pipeline for a given artifact

    Args:
        payload (dict): The payload dict for the current artifact
        args (argparse.Namespace): The command line arguments object
        log (logging.getLogger): The logger object
        ingest_pipe (pipeline): The instance of the ingest pipeline (see pipeline class)

    Returns:
        tuple[int, bool, str, str]: A tuple containing the return code, a bool indicating whether the pipeline timed out, stdout and stderr
    """

    parameters = {
        "outdir": args.result_dir,
        "unique_id": payload["uuid"],
        "climb": "",
        "max_human_reads_before_rejection": "10000",
        "k2_host": args.k2_host,  # Parameterise this and deal with DNS stuff
        "k2_port": "8080",
        "db": "/shared/public/db/kraken2/k2_pluspf",
    }

    if payload["platform"] == "ont":
        parameters["fastq"] = payload["files"][".fastq.gz"]["uri"]

    elif payload["platform"] == "illumina":
        parameters["fastq1"] = payload["files"][".1.fastq.gz"]["uri"]
        parameters["fastq2"] = payload["files"][".2.fastq.gz"]["uri"]
        parameters["paired"] = ""

    log_path = Path(args.result_dir, payload["uuid"])

    return ingest_pipe.execute(params=parameters, logdir=log_path)


def add_taxon_records(
    payload: dict, result_path: str, log: logging.getLogger, s3_client: boto3.client
) -> tuple[bool, dict]:
    """Function to add nested taxon records to an existing Onyx record from a Scylla reads_summary.json file

    Args:
        payload (dict): Dict containing the payload for the current artifact
        result_path (str): Result path for the current artifact
        log (logging.getLogger): Logger object
        s3_client (boto3.client): Boto3 client object for S3

    Returns:
        tuple[bool, dict]: Tuple containing a bool indicating whether the upload failed and the updated payload dict
    """
    nested_records = []
    binned_read_fail = False

    with open(
        os.path.join(result_path, "reads_by_taxa/reads_summary.json"), "rt"
    ) as read_summary_fh:
        summary = json.load(read_summary_fh)

        if not payload.get("ingest_errors"):
            payload["ingest_errors"] = []

        for taxa in summary:
            taxon_dict = {
                "taxon_id": taxa["taxon"],
                "human_readable": taxa["human_readable"],
                "n_reads": taxa["qc_metrics"]["num_reads"],
                "avg_quality": taxa["qc_metrics"]["avg_qual"],
                "mean_len": taxa["qc_metrics"]["mean_len"],
                "tax_level": taxa["tax_level"],
            }

            if payload["platform"] == "illumina":
                for i in (1, 2):
                    fastq_path = os.path.join(
                        result_path,
                        f"reads_by_taxa/{taxa['filenames'][i - 1]}.gz",
                    )

                    try:
                        s3_client.upload_file(
                            fastq_path,
                            "mscapetest-published-binned-reads",
                            f"{payload['cid']}/{taxa['taxon']}_{i}.fastq.gz",
                        )

                        taxon_dict[
                            f"fastq_{i}"
                        ] = f"s3://mscapetest-published-binned-reads/{payload['cid']}/{taxa['taxon']}_{i}.fastq.gz"

                    except Exception as add_taxon_record_exception:
                        log.error(
                            f"Failed to upload binned reads for taxon {taxa['taxon']} to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['cid']} due to client error: {add_taxon_record_exception}"
                        )
                        payload["ingest_errors"].append(
                            f"Failed to upload binned reads for taxon: {taxa['taxon']} to storage bucket"
                        )
                        binned_read_fail = True
                        continue

            elif payload["platform"] == "ont":
                fastq_path = os.path.join(
                    result_path, f"reads_by_taxa/{taxa['filenames'][0]}.gz"
                )

                try:
                    s3_client.upload_file(
                        fastq_path,
                        "mscapetest-published-binned-reads",
                        f"{payload['cid']}/{taxa['taxon']}.fastq.gz",
                    )

                    taxon_dict[
                        f"fastq_1"
                    ] = f"s3://mscapetest-published-binned-reads/{payload['cid']}/{taxa['taxon']}.fastq.gz"

                except Exception as add_taxon_record_exception:
                    log.error(
                        f"Failed to binned reads for taxon {taxa['taxon']} to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['cid']} due to client error: {add_taxon_record_exception}"
                    )
                    payload["ingest_errors"].append(
                        f"Failed to upload binned reads for taxon: {taxa['taxon']} to storage bucket"
                    )
                    binned_read_fail = True
                    continue

            else:
                log.error(f"Unknown platform: {payload['platform']}")
                payload["ingest_errors"].append(
                    f"Unknown platform: {payload['platform']}"
                )
                binned_read_fail = True
                continue

            nested_records.append(taxon_dict)

    if not binned_read_fail:
        update_fail, payload = onyx_update(
            payload=payload, fields={"taxa": nested_records}, log=log
        )

        if update_fail:
            binned_read_fail = True

    return (binned_read_fail, payload)


def push_taxon_reports(
    payload: dict, result_path: str, log: logging.getLogger, s3_client: boto3.client
) -> tuple[bool, dict]:
    """Push taxa reports to long-term storage bucket and update the Onyx record with the S3 directory URI

    Args:
        payload (dict): Payload dict for the current artifact
        result_path (str): Path to the results directory
        log (logging.getLogger): Logger object
        s3_client (boto3.client): S3 boto3 client object

    Returns:
        tuple[bool, dict]: Tuple containing a bool indicating whether the upload failed and the updated payload dict
    """

    taxon_report_fail = False

    taxon_report_path = os.path.join(result_path, f"classifications")
    try:
        reports = os.listdir(taxon_report_path)

        for report in reports:
            # Add handling for Db in name etc
            s3_client.upload_file(
                os.path.join(taxon_report_path, report),
                "mscapetest-published-taxon-reports",
                f"{payload['cid']}/{report}",
            )

    except Exception as push_taxon_report_exception:
        log.error(
            f"Failed to upload taxon classification to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['cid']} due to client error: {push_taxon_report_exception}"
        )
        payload["ingest_errors"].append(
            f"Failed to upload taxon classification to storage bucket"
        )
        taxon_report_fail = True

    if not taxon_report_fail:
        update_fail, payload = onyx_update(
            payload=payload,
            fields={
                "taxon_reports": f"s3://mscapetest-published-taxon-reports/{payload['cid']}/"
            },
            log=log,
        )

        if update_fail:
            taxon_report_fail = True

    return (taxon_report_fail, payload)


def push_report_file(
    payload: dict, result_path: str, log: logging.getLogger, s3_client: boto3.client
) -> tuple[bool, dict]:
    """Push report file to long-term storage bucket and update the Onyx record with the report URI

    Args:
        payload (dict): Payload dict for the current artifact
        result_path (str): Path to the results directory
        log (logging.getLogger): Logger object
        s3_client (boto3.client): Boto3 client object for S3

    Returns:
        tuple[bool, dict]: Tuple containing a bool indicating whether the upload failed and the updated payload dict
    """

    report_fail = False

    report_path = os.path.join(result_path, f"{payload['uuid']}_report.html")
    try:
        # Add handling for Db in name etc
        s3_client.upload_file(
            report_path,
            "mscapetest-published-reports",
            f"{payload['cid']}_scylla_report.html",
        )
    except ClientError as push_report_file_exception:
        log.error(
            f"Failed to upload scylla report to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['cid']} due to client error: {push_report_file_exception}"
        )
        payload["ingest_errors"].append(
            f"Failed to upload scylla report to storage bucket"
        )
        report_fail = True

    if not report_fail:
        update_fail, payload = onyx_update(
            payload=payload,
            fields={
                "validation_report": f"s3://mscapetest-published-reports/{payload['cid']}_validation_report.html"
            },
            log=log,
        )

        if update_fail:
            report_fail = True

    return (report_fail, payload)


def add_reads_record(
    payload: dict,
    s3_client: boto3.client,
    result_path: str,
    log: logging.getLogger,
) -> tuple[bool, dict]:
    """Function to upload raw reads to long-term storage bucket and add the fastq_1 and fastq_2 fields to the Onyx record

    Args:
        payload (dict): Payload dict for the record to update
        s3_client (boto3.client): Boto3 client object for S3
        result_path (str): Path to the results directory
        log (logging.getLogger): Logger object

    Returns:
        tuple[bool, dict]: Tuple containing a bool indicating whether the upload failed and the updated payload dict
    """

    raw_read_fail = False

    if payload["platform"] == "illumina":
        for i in (1, 2):
            fastq_path = os.path.join(
                result_path, f"preprocess/{payload['uuid']}_{i}.fastp.fastq.gz"
            )

            try:
                s3_client.upload_file(
                    fastq_path,
                    "mscapetest-published-reads",
                    f"{payload['cid']}_{i}.fastq.gz",
                )

            except ClientError as add_reads_record_exception:
                log.error(
                    f"Failed to upload reads to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['cid']} due to client error: {add_reads_record_exception}"
                )
                payload["ingest_errors"].append(
                    f"Failed to upload reads to storage bucket"
                )
                raw_read_fail = True
                continue

        if not raw_read_fail:
            update_fail, payload = onyx_update(
                payload=payload,
                fields={
                    "fastq_1": f"s3://mscapetest-published-reads/{payload['cid']}_1.fastq.gz",
                    "fastq_2": f"s3://mscapetest-published-reads/{payload['cid']}_2.fastq.gz",
                },
                log=log,
            )

            if update_fail:
                raw_read_fail = True

    else:
        fastq_path = os.path.join(
            result_path, f"preprocess/{payload['uuid']}.fastp.fastq.gz"
        )

        try:
            s3_client.upload_file(
                fastq_path,
                "mscapetest-published-reads",
                f"{payload['cid']}.fastq.gz",
            )

        except ClientError as add_reads_record_exception:
            log.error(
                f"Failed to upload reads to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['cid']} due to client error: {add_reads_record_exception}"
            )
            payload["ingest_errors"].append(f"Failed to upload reads to storage bucket")

            raw_read_fail = True

        if not raw_read_fail:
            update_fail, payload = onyx_update(
                payload=payload,
                fields={
                    "fastq_1": f"s3://mscapetest-published-reads/{payload['cid']}.fastq.gz"
                },
                log=log,
            )

            if update_fail:
                raw_read_fail = True

    return (raw_read_fail, payload)


def ret_0_parser(
    log: logging.getLogger,
    payload: dict,
    result_path: str,
    ingest_fail: bool = False,
) -> tuple[bool, dict]:
    """Function to parse the execution trace of a Nextflow pipeline run to determine whether any of the processes failed.

    Args:
        log (logging.getLogger): Logger object
        payload (dict): Payload dictionary
        result_path (str): Path to the results directory
        ingest_fail (bool): Boolean to indicate whether the ingest has failed up to this point (default: False)

    Returns:
        tuple[bool, dict]: Tuple containing the ingest fail boolean and the payload dictionary
    """

    try:
        with open(
            os.path.join(
                result_path,
                "pipeline_info",
                f"execution_trace_{payload['uuid']}.txt",
            )
        ) as trace_fh:
            reader = csv.DictReader(trace_fh, delimiter="\t")

            trace_dict = {}
            for process in reader:
                trace_dict[process["name"].split(":")[-1]] = process

    except Exception as pipeline_trace_exception:
        log.error(
            f"Could not open pipeline trace for UUID: {payload['uuid']} despite NXF exit code 0 due to error: {pipeline_trace_exception}"
        )
        payload["ingest_errors"].append("couldn't open nxf ingest pipeline trace")
        ingest_fail = True

    for process, trace in trace_dict.items():
        if trace["exit"] != "0":
            if (
                process.startswith("extract_paired_reads")
                or process.startswith("extract_reads")
            ) and trace["exit"] == "2":
                payload["ingest_errors"].append(
                    "Human reads detected above rejection threshold, please ensure pre-upload dehumanisation has been performed properly"
                )
                ingest_fail = True
            elif (
                process.startswith("extract_paired_reads")
                or process.startswith("extract_reads")
            ) and trace["exit"] == "3":
                continue
            else:
                payload["ingest_errors"].append(
                    f"MScape validation pipeline (Scylla) failed in process {process} with exit code {trace['exit']} and status {trace['status']}"
                )
                ingest_fail = True

    return (ingest_fail, payload)


def validate(
    message: namedtuple,
    args: argparse.Namespace,
    ingest_pipe: pipeline,
):
    s3_credentials = get_credentials()

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
        endpoint_url=s3_credentials.endpoint,
    )

    log = logging.getLogger("mscape.ingest")

    to_validate = json.loads(message.body)

    log.info(f"Started validation func for UUID: {to_validate['uuid']}")

    payload = copy.deepcopy(to_validate)

    payload["rerun"] = False

    # This client is purely for Mscape, ignore all other messages
    if to_validate["project"] != "mscapetest":
        log.info(
            f"Ignoring file set with UUID: {to_validate['uuid']} due non-mscape project ID"
        )
        return (False, payload, message)

    if not to_validate["onyx_test_create_status"] or not to_validate["validate"]:
        return (False, payload, message)

    log.info(f"Submitting ingest pipeline for UUID: {payload['uuid']}")

    rc, stdout, stderr = execute_validation_pipeline(
        payload=payload, args=args, ingest_pipe=ingest_pipe
    )

    if ingest_pipe.cmd:
        log.info(
            f"Execution of pipeline for UUID: {payload['uuid']} complete. Command was: {ingest_pipe.cmd}"
        )

    args.result_dir = Path(args.result_dir)

    result_path = os.path.join(args.result_dir.resolve(), payload["uuid"])

    if not os.path.exists(result_path):
        os.makedirs(result_path)

    with open(os.path.join(result_path, "nextflow.stdout"), "wt") as out_fh, open(
        os.path.join(result_path, "nextflow.stderr"), "wt"
    ) as err_fh:
        out_fh.write(stdout)
        err_fh.write(stderr)

    if rc != 0:
        log.error(
            f"Validation pipeline exited with non-0 exit code: {rc} for UUID: {payload['uuid']}"
        )
        payload["rerun"] = True
        ingest_pipe.cleanup(stdout=stdout)
        return (False, payload, message)

    ingest_fail, payload = ret_0_parser(
        log=log,
        payload=payload,
        result_path=result_path,
    )

    if ingest_fail:
        ingest_pipe.cleanup(stdout=stdout)
        return (False, payload, message)

    if payload["test_flag"]:
        log.info(
            f"Test ingest for artifact: {payload['artifact']} with UUID: {payload['uuid']} completed successfully"
        )
        payload["test_ingest_result"] = True
        ingest_pipe.cleanup(stdout=stdout)
        return (False, payload, message)

    submission_fail, payload = onyx_submission(
        log=log,
        payload=payload,
    )

    if submission_fail:
        log.info(f"Failed to submit to Onyx for UUID: {payload['uuid']}")
        ingest_pipe.cleanup(stdout=stdout)
        return (False, payload, message)

    log.info(
        f"Uploading files to long-term storage buckets for CID: {payload['cid']} after sucessful Onyx submission"
    )

    raw_read_fail, payload = add_reads_record(
        payload=payload,
        s3_client=s3_client,
        result_path=result_path,
        log=log,
    )

    binned_read_fail, payload = add_taxon_records(
        payload=payload, result_path=result_path, log=log, s3_client=s3_client
    )

    report_fail, payload = push_report_file(
        payload=payload, result_path=result_path, log=log, s3_client=s3_client
    )

    taxon_report_fail, payload = push_taxon_reports(
        payload=payload, result_path=result_path, log=log, s3_client=s3_client
    )

    if raw_read_fail or binned_read_fail or report_fail or taxon_report_fail:
        log.error(
            f"Failed to upload at least one file to long-term storage for CID: {payload['cid']}"
        )
        ingest_pipe.cleanup(stdout=stdout)
        return (False, payload, message)

    unsuppress_fail, payload = onyx_unsuppress(payload=payload, log=log)

    if unsuppress_fail:
        ingest_pipe.cleanup(stdout=stdout)
        return (False, payload, message)

    payload["published"] = True
    log.info(
        f"Sending successful ingest result for UUID: {payload['uuid']}, with CID: {payload['cid']}"
    )

    (
        cleanup_rc,
        cleanup_stdout,
        cleanup_stderr,
    ) = ingest_pipe.cleanup(stdout=stdout)

    if cleanup_rc != 0:
        log.error(
            f"Cleanup of pipeline for UUID: {payload['uuid']} failed with exit code: {cleanup_rc}. stdout: {cleanup_stdout}, stderr: {cleanup_stderr}"
        )

    return (True, payload, message)


def run(args):
    log = init_logger("mscape.ingest", args.logfile, args.log_level)

    varys_client = varys(
        profile="roz",
        logfile=args.logfile,
        log_level=args.log_level,
        auto_acknowledge=False,
    )

    validation_payload_template = {
        "uuid": "",
        "artifact": "",
        "project": "",
        "ingest_timestamp": "",
        "cid": False,
        "site": "",
        "created": False,
        "ingested": False,
        "onyx_test_status_code": False,
        "onyx_test_create_errors": {},  # Dict
        "onyx_test_create_status": False,
        "onyx_status_code": False,
        "onyx_errors": {},  # Dict
        "onyx_create_status": False,
        "ingest_errors": [],  # List,
        "test_flag": True,  # Add this throughout
        "test_ingest_result": False,
    }

    ingest_pipe = pipeline(
        pipe="snowy-leopard/scylla",
        profile="docker",
        config=args.nxf_config,
        nxf_executable=args.nxf_executable,
    )

    worker_pool = worker_pool_handler(
        workers=args.n_workers, logger=log, varys_client=varys_client
    )
    try:
        while True:
            message = varys_client.receive(
                exchange="inbound.to_validate.mscapetest", queue_suffix="validator"
            )

            worker_pool.submit_job(message=message, args=args, ingest_pipe=ingest_pipe)
    except:
        log.info("Shutting down worker pool")
        worker_pool.close()


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfile", type=Path)
    parser.add_argument("--log_level", type=str, default="DEBUG")
    parser.add_argument("--nxf_config")
    parser.add_argument("--nxf_executable", default="nextflow")
    parser.add_argument("--k2_host", type=str)
    parser.add_argument("--result_dir", type=Path)
    parser.add_argument("--n_workers", type=int, default=5)
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
