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
import sys
from itertools import batched

from roz_scripts.utils.utils import (
    pipeline,
    init_logger,
    get_s3_credentials,
    csv_create,
    onyx_update,
    ensure_file_unseen,
    onyx_reconcile,
    put_result_json,
)
from varys import Varys


class worker_pool_handler:
    def __init__(self, workers, logger, varys_client):
        self._log = logger
        self.worker_pool = mp.Pool(processes=workers)
        self._varys_client = varys_client

        self._log.info(f"Successfully initialised worker pool with {workers} workers")

        self._retry_log = {}

    def submit_job(self, message, args, ingest_pipe):
        self._log.info(
            f"Submitting job to the worker pool for UUID: {json.loads(message.body)['uuid']}"
        )

        self._retry_log.setdefault(json.loads(message.body)["uuid"], 0)

        self._retry_log[json.loads(message.body)["uuid"]] += 1

        self.worker_pool.apply_async(
            func=validate,
            kwds={"message": message, "args": args, "ingest_pipe": ingest_pipe},
            callback=self.callback,
            error_callback=self.error_callback,
        )

    def callback(self, validate_result):
        success, alert, hcid_alerts, payload, message = validate_result

        if alert:
            self._log.error(
                f"Alert flag set for UUID: {payload['uuid']}, manual intervention required"
            )
            self._varys_client.send(
                message=payload,
                exchange="restricted-mscape-announce",
                queue_suffix="alert",
            )

        if success:
            self._log.info(
                f"Successful validation for match UUID: {payload['uuid']}, sending result"
            )

            self._varys_client.acknowledge_message(message)

            self._varys_client.send(
                message=payload,
                exchange=f"inbound-results-{payload['project']}-{payload['site']}",
                queue_suffix="validator",
            )

            put_result_json(payload, self._log)

            if not payload["test_flag"]:
                new_artifact_payload = {
                    "publish_timestamp": time.time_ns(),
                    "climb_id": payload["climb_id"],
                    "run_id": payload["anonymised_run_id"],
                    "run_index": payload["anonymised_run_index"],
                    "biosample_id": payload["anonymised_biosample_id"],
                    "site": payload["site"],
                    "platform": payload["platform"],
                    "match_uuid": payload["uuid"],
                    "project": payload["project"],
                }

                if payload.get("anonymised_biosample_source_id"):
                    new_artifact_payload["biosample_source_id"] = payload[
                        "anonymised_biosample_source_id"
                    ]

                self._varys_client.send(
                    message=new_artifact_payload,
                    exchange="inbound-new_artifact-mscape",
                    queue_suffix="validator",
                )

        else:
            self._log.info(
                f"Validation failed for match UUID: {payload['uuid']}, sending result"
            )

            if payload["rerun"]:
                if self._retry_log[payload["uuid"]] >= 5:
                    self._log.error(
                        f"Message for UUID: {payload['uuid']} failed after {self._retry_log[payload['uuid']]} attempts, sending to dead letter queue"
                    )
                    payload.setdefault("ingest_errors", [])
                    payload["ingest_errors"].append(
                        f"Validation failed for UUID: {payload['uuid']} unrecoverably"
                    )

                    self._varys_client.send(
                        message=payload,
                        exchange="mscape-restricted-announce",
                        queue_suffix="dead_letter",
                    )

                    self._varys_client.send(
                        message=payload,
                        exchange=f"inbound-results-{payload['project']}-{payload['site']}",
                        queue_suffix="validator",
                    )

                    put_result_json(payload, self._log)

                    self._varys_client.nack_message(message)

                    raise ValueError(
                        "Validation failed after 5 attempts, shutting down worker pool"
                    )

                else:
                    self._log.info(
                        f"Rerun flag for UUID: {payload['uuid']} is set, re-queueing message"
                    )
                    self._varys_client.nack_message(message)

            else:
                self._varys_client.acknowledge_message(message)

                self._varys_client.send(
                    message=payload,
                    exchange=f"inbound-results-{payload['project']}-{payload['site']}",
                    queue_suffix="validator",
                )

                put_result_json(payload, self._log)

    def error_callback(self, exception):
        self._log.error(f"Worker failed with unhandled exception: {exception}")
        self._varys_client.send(
            message=f"MScape ingest worker failed with unhandled exception: {exception}",
            exchange="mscape-restricted-announce",
            queue_suffix="dead_worker",
        )

    def close(self):
        self.worker_pool.close()
        self.worker_pool.join()


def execute_validation_pipeline(
    payload: dict,
    args: argparse.Namespace,
    ingest_pipe: pipeline,
) -> tuple[int, str, str]:
    """Execute the validation pipeline for a given artifact

    Args:
        payload (dict): The payload dict for the current artifact
        args (argparse.Namespace): The command line arguments object
        log (logging.getLogger): The logger object
        ingest_pipe (pipeline): The instance of the ingest pipeline (see pipeline class)

    Returns:
        tuple[int, str, str]: Tuple containing the return code, stdout and stderr of the pipeline
    """

    k2_db_path = os.path.join(
        os.getenv("SCYLLA_K2_DB_PATH"), os.getenv("SCYLLA_K2_DB_DATE")
    )

    taxonomy_path = os.path.join(
        os.getenv("SCYLLA_TAXONOMY_PATH"), os.getenv("SCYLLA_TAXONOMY_DATE")
    )

    parameters = {
        "outdir": args.result_dir,
        "unique_id": payload["uuid"],
        "climb": "",
        "max_human_reads_before_rejection": "10000",
        "k2_host": args.k2_host,  # Parameterise this and deal with DNS stuff
        "k2_port": "8080",
        "database": k2_db_path,
        "taxonomy": taxonomy_path,
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
    alert = False

    try:
        with open(
            os.path.join(result_path, "reads_by_taxa/reads_summary_combined.json"), "rt"
        ) as read_summary_fh:
            summary = json.load(read_summary_fh)

    except FileNotFoundError:
        log.info(
            f"Could not find reads_summary_combined.json, this probably means that there are insufficient binned taxa produced by scylla for UUID: {payload['uuid']}"
        )
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append(
            "Could not find reads_summary_combined.json, this probably means that no taxa were present above binning thresholds by scylla"
        )
        return (binned_read_fail, alert, payload)

    for taxa in summary:
        taxon_dict = {
            "taxon_id": taxa["taxon"],
            "human_readable": taxa["human_readable"],
            "n_reads": taxa["qc_metrics"]["num_reads"],
            "avg_quality": taxa["qc_metrics"]["avg_qual"],
            "mean_len": taxa["qc_metrics"]["mean_len"],
            "rank": taxa["tax_level"],
        }

        if payload["platform"] == "illumina":
            for i in (1, 2):
                fastq_path = os.path.join(
                    result_path,
                    f"reads_by_taxa/{taxa['filenames'][i - 1]}.gz",
                )

                try:
                    s3_bucket = "mscape-published-binned-reads"
                    s3_key = f"{payload['climb_id']}/{payload['climb_id']}_{taxa['taxon']}_{i}.fastq.gz"
                    s3_uri = f"s3://{s3_bucket}/{s3_key}"

                    s3_client.upload_file(
                        fastq_path,
                        s3_bucket,
                        s3_key,
                    )

                    taxon_dict[f"fastq_{i}"] = s3_uri

                except Exception as add_taxon_record_exception:
                    log.error(
                        f"Failed to upload binned reads for taxon {taxa['taxon']} to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to client error: {add_taxon_record_exception}"
                    )
                    payload.setdefault("ingest_errors", [])
                    payload["ingest_errors"].append(
                        f"Failed to upload binned reads for taxon: {taxa['taxon']} to storage bucket"
                    )
                    binned_read_fail = True
                    alert = True
                    continue

        elif payload["platform"] == "ont":
            fastq_path = os.path.join(
                result_path, f"reads_by_taxa/{taxa['filenames'][0]}.gz"
            )

            try:
                s3_bucket = "mscape-published-binned-reads"
                s3_key = f"{payload['climb_id']}/{payload['climb_id']}_{taxa['taxon']}.fastq.gz"
                s3_uri = f"s3://{s3_bucket}/{s3_key}"

                s3_client.upload_file(
                    fastq_path,
                    s3_bucket,
                    s3_key,
                )

                taxon_dict["fastq_1"] = s3_uri

            except Exception as add_taxon_record_exception:
                log.error(
                    f"Failed to binned reads for taxon {taxa['taxon']} to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to client error: {add_taxon_record_exception}"
                )
                payload.setdefault("ingest_errors", [])
                payload["ingest_errors"].append(
                    f"Failed to upload binned reads for taxon: {taxa['taxon']} to storage bucket"
                )
                binned_read_fail = True
                alert = True
                continue

        else:
            log.error(f"Unknown platform: {payload['platform']}")
            payload.setdefault("ingest_errors", [])
            payload["ingest_errors"].append(f"Unknown platform: {payload['platform']}")
            binned_read_fail = True
            continue

        nested_records.append(taxon_dict)

    if not binned_read_fail:
        update_fail, update_alert, payload = onyx_update(
            payload=payload, fields={"taxa_files": nested_records}, log=log
        )

        if update_fail:
            binned_read_fail = True

        if update_alert:
            alert = True

    return (binned_read_fail, alert, payload)


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
    alert = False

    taxon_report_path = os.path.join(result_path, "classifications")

    try:
        reports = os.listdir(taxon_report_path)

        s3_bucket = "mscape-published-taxon-reports"

        for report in reports:
            # Skip directories and hidden files just incase
            if os.path.isdir(
                os.path.join(taxon_report_path, report)
            ) or report.startswith("."):
                continue

            s3_key = f"{payload['climb_id']}/{payload['climb_id']}_{report}"
            # Add handling for Db in name etc
            s3_client.upload_file(
                os.path.join(taxon_report_path, report),
                s3_bucket,
                s3_key,
            )

    except Exception as push_taxon_report_exception:
        log.error(
            f"Failed to upload taxon classification to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to client error: {push_taxon_report_exception}"
        )
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append(
            "Failed to upload taxon classification to storage bucket"
        )
        taxon_report_fail = True
        alert = True

    if not taxon_report_fail:
        update_fail, update_alert, payload = onyx_update(
            payload=payload,
            fields={"taxon_reports": f"s3://{s3_bucket}/{payload['climb_id']}/"},
            log=log,
        )

        if update_fail:
            taxon_report_fail = True

        if update_alert:
            alert = True

    return (taxon_report_fail, alert, payload)


def add_classifier_calls(
    payload: dict, result_path: str, log: logging.getLogger
) -> tuple[bool, bool, dict]:
    """Add classifier calls to the Onyx record from the Scylla kraken_report.json file

    Args:
        payload (dict): Payload dict for the current artifact
        result_path (str): Path to the results directory
        log (logging.getLogger): Logger object

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the upload failed, a bool indicating whether to squawk in the alert channel and the updated payload dict
    """
    classifier_calls_fail = False
    alert = False

    classifier_calls = []

    try:
        pipe_params_path = os.path.join(
            result_path, "pipeline_info", f"params_{payload['uuid']}.log"
        )

        with open(pipe_params_path, "rt") as pipe_params_fh:
            pipe_params = json.load(pipe_params_fh)

        classifier_calls_path = os.path.join(
            result_path,
            "classifications",
            f"{pipe_params['database_set']}.kraken_report.json",
        )

        with open(classifier_calls_path, "rt") as classifier_calls_fh:
            kraken_report_dict = json.load(classifier_calls_fh)

        for data in kraken_report_dict.values():
            data["taxon_id"] = data.pop("taxid")
            data["human_readable"] = data.pop("name")
            data["count_direct"] = data.pop("count")

            classifier_calls.append(data)

    except Exception as add_classifier_calls_exception:
        log.error(
            f"Failed to add classifier calls for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to error: {add_classifier_calls_exception}"
        )
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append("Failed to parse classifier calls dict")
        classifier_calls_fail = True
        alert = True

    if not classifier_calls_fail:
        for batch in batched(classifier_calls, 100):
            update_fail, update_alert, payload = onyx_update(
                payload=payload,
                fields={"classifier_calls": batch},
                log=log,
            )

        if update_fail:
            classifier_calls_fail = True
            alert = True

    return (classifier_calls_fail, alert, payload)


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
    alert = False

    report_path = os.path.join(result_path, f"{payload['uuid']}_report.html")

    s3_bucket = "mscape-published-reports"

    s3_key = f"{payload['climb_id']}_scylla_report.html"

    s3_uri = f"s3://{s3_bucket}/{s3_key}"

    try:
        # Add handling for Db in name etc
        s3_client.upload_file(
            report_path,
            s3_bucket,
            s3_key,
        )
    except (ClientError, FileNotFoundError) as push_report_file_exception:
        log.error(
            f"Failed to upload scylla report to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to client error: {push_report_file_exception}"
        )
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append(
            "Failed to upload scylla report to storage bucket"
        )
        report_fail = True
        alert = True

    if not report_fail:
        update_fail, update_alert, payload = onyx_update(
            payload=payload,
            fields={"ingest_report": s3_uri},
            log=log,
        )

        if update_fail:
            report_fail = True

        if update_alert:
            alert = True

    return (report_fail, alert, payload)


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
    alert = False

    s3_bucket = "mscape-published-reads"

    if payload["platform"] == "illumina":
        for i in (1, 2):
            fastq_path = os.path.join(
                result_path, f"preprocess/{payload['uuid']}_{i}.fastp.fastq.gz"
            )

            try:
                s3_key = f"{payload['climb_id']}_{i}.fastq.gz"

                s3_client.upload_file(
                    fastq_path,
                    s3_bucket,
                    s3_key,
                )

            except (ClientError, FileNotFoundError) as add_reads_record_exception:
                log.error(
                    f"Failed to upload reads to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to client error: {add_reads_record_exception}"
                )
                payload.setdefault("ingest_errors", [])
                payload["ingest_errors"].append(
                    "Failed to upload reads to storage bucket"
                )
                raw_read_fail = True
                alert = True
                continue

        if not raw_read_fail:
            update_fail, update_alert, payload = onyx_update(
                payload=payload,
                fields={
                    "fastq_1": f"s3://{s3_bucket}/{payload['climb_id']}_1.fastq.gz",
                    "fastq_2": f"s3://{s3_bucket}/{payload['climb_id']}_2.fastq.gz",
                },
                log=log,
            )

            if update_fail:
                raw_read_fail = True

            if update_alert:
                alert = True

    else:
        fastq_path = os.path.join(
            result_path, f"preprocess/{payload['uuid']}.fastp.fastq.gz"
        )

        s3_key = f"{payload['climb_id']}.fastq.gz"

        try:
            s3_client.upload_file(
                fastq_path,
                s3_bucket,
                s3_key,
            )

        except (ClientError, FileNotFoundError) as add_reads_record_exception:
            log.error(
                f"Failed to upload reads to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to client error: {add_reads_record_exception}"
            )
            payload.setdefault("ingest_errors", [])
            payload["ingest_errors"].append("Failed to upload reads to storage bucket")

            raw_read_fail = True
            alert = True

        if not raw_read_fail:
            update_fail, update_alert, payload = onyx_update(
                payload=payload,
                fields={"fastq_1": f"s3://{s3_bucket}/{s3_key}"},
                log=log,
            )

            if update_fail:
                raw_read_fail = True

            if update_alert:
                alert = True

    return (raw_read_fail, alert, payload)


def read_fraction_upload(
    payload: dict,
    s3_client: boto3.client,
    result_path: str,
    log: logging.getLogger,
    fraction_prefix: str,
) -> tuple[bool, bool, dict]:
    """Function to upload read fractions to long-term storage bucket and add the fastq_1 and fastq_2 fields to the Onyx record

    Args:
        payload (dict): Payload dict for the record to update
        s3_client (boto3.client): Boto3 client object for S3
        result_path (str): Path to the results directory
        log (logging.getLogger): Logger object
        fraction_prefix (str): Prefix for the read fraction

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the upload failed, a bool indicating whether to squawk in the alert channel and the updated payload dict
    """

    read_fraction_fail = False
    alert = False

    s3_bucket = "mscape-published-read-fractions"

    if payload["platform"] == "illumina":
        for i in (1, 2):
            fastq_path = os.path.join(
                result_path,
                "read_fractions",
                f"{fraction_prefix}_{i}.fastq.gz",
            )

            try:
                s3_key = f"{payload['climb_id']}/{payload['climb_id']}.{fraction_prefix}_{i}.fastq.gz"

                s3_client.upload_file(
                    fastq_path,
                    s3_bucket,
                    s3_key,
                )

            except ClientError as add_read_fraction_exception:
                log.error(
                    f"Failed to upload reads to long-term storage bucket for UUID: {payload['uuid']} with CLIMB-ID: {payload['climb_id']} due to client error: {add_read_fraction_exception}"
                )
                payload.setdefault("ingest_errors", [])
                payload["ingest_errors"].append(
                    f"Failed to upload read fraction: {fraction_prefix} to storage bucket"
                )
                read_fraction_fail = True
                alert = True
                continue

            except FileNotFoundError:
                log.info(
                    "Could not find read fraction file, probably because no reads were present in the fraction"
                )
                payload.setdefault("ingest_errors", [])
                payload["ingest_errors"].append(
                    f"Could not find read fraction file: {fraction_prefix}, probably because no reads were present in the fraction"
                )
                # This doesn't mean anything has actually failed, just that there were no reads in the fraction
                continue

        if not read_fraction_fail:
            update_fail, update_alert, payload = onyx_update(
                payload=payload,
                fields={
                    f"{fraction_prefix}_reads_1": f"s3://{s3_bucket}/{payload['climb_id']}/{payload['climb_id']}.{fraction_prefix}_1.fastq.gz",
                    f"{fraction_prefix}_reads_2": f"s3://{s3_bucket}/{payload['climb_id']}/{payload['climb_id']}.{fraction_prefix}_2.fastq.gz",
                },
                log=log,
            )

            if update_fail:
                read_fraction_fail = True

            if update_alert:
                alert = True

    else:
        fastq_path = os.path.join(
            result_path,
            "read_fractions",
            f"{fraction_prefix}.fastq.gz",
        )

        s3_key = (
            f"{payload['climb_id']}/{payload['climb_id']}.{fraction_prefix}.fastq.gz"
        )

        try:
            s3_client.upload_file(
                fastq_path,
                s3_bucket,
                s3_key,
            )

        except (ClientError, FileNotFoundError) as add_read_fraction_exception:
            log.error(
                f"Failed to upload reads to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to client error: {add_read_fraction_exception}"
            )
            payload.setdefault("ingest_errors", [])
            payload["ingest_errors"].append(
                f"Failed to upload read fraction: {fraction_prefix} to storage bucket"
            )

            read_fraction_fail = True
            alert = True

        if not read_fraction_fail:
            update_fail, update_alert, payload = onyx_update(
                payload=payload,
                fields={f"{fraction_prefix}_reads_1": f"s3://{s3_bucket}/{s3_key}"},
                log=log,
            )

            if update_fail:
                read_fraction_fail = True

            if update_alert:
                alert = True

    return (read_fraction_fail, alert, payload)


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

        with open(
            os.path.join(
                result_path,
                "pipeline_info",
                f"workflow_version_{payload['uuid']}.txt",
            ),
            "rt",
        ) as version_fh:
            version = version_fh.read().strip()

        payload["scylla_version"] = version

        for process, trace in trace_dict.items():
            if trace["exit"] != "0":
                if (
                    process.startswith("extract_taxa_reads")
                    or process.startswith("extract_taxa_paired_reads")
                ) and trace["exit"] == "2":
                    payload.setdefault("ingest_errors", [])
                    payload["ingest_errors"].append(
                        "Human reads detected above rejection threshold, please ensure pre-upload dehumanisation has been performed properly"
                    )
                    ingest_fail = True
                elif (
                    process.startswith("extract_taxa_reads")
                    or process.startswith("extract_taxa_paired_reads")
                ) and trace["exit"] == "3":
                    continue
                elif process.startswith("fastp") and trace["exit"] == "255":
                    payload.setdefault("ingest_errors", [])
                    payload["ingest_errors"].append(
                        "Submitted gzipped fastq file(s) appear to be corrupted or unreadable, please resubmit them or contact the mSCAPE admin team for assistance"
                    )
                    ingest_fail = True
                else:
                    payload.setdefault("ingest_errors", [])
                    payload["ingest_errors"].append(
                        f"MScape validation pipeline (Scylla) failed in process {process} with exit code {trace['exit']} and status {trace['status']}"
                    )
                    ingest_fail = True

    except Exception as pipeline_trace_exception:
        log.error(
            f"Could not open pipeline trace for UUID: {payload['uuid']} despite NXF exit code 0 due to error: {pipeline_trace_exception}"
        )
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append("Could not parse Scylla pipeline trace")
        payload["rerun"] = True
        ingest_fail = True
        time.sleep(args.retry_delay)

    return (ingest_fail, payload)


def handle_hcid(
    log: logging.getLogger, payload: dict, result_path: str
) -> tuple[bool, list, bool, dict]:
    """Function to handle the parsing of HCID warnings output by the Scylla pipeline

    Args:
        log (logging.getLogger): Logger object
        payload (dict): Payload dictionary
        result_path (str): Path to the results directory

    Returns:
        tuple[bool, list, bool, dict]: Tuple containing a bool indicating whether the ingest has failed, a list of HCID alerts, a bool indicating whether to squawk in the alert channel and the updated payload dictionary
    """

    hcid_fail = False
    alert = False

    hcid_alerts = []

    try:
        hcid_path = os.path.join(result_path, "qc")

        contents = os.listdir(hcid_path)

        if not any(x.endswith(".warning") for x in contents):
            return (hcid_fail, hcid_alerts, alert, payload)

        for path in contents:
            if path.endswith(".warning"):

                hcid_message = json.load(open(os.path.join(hcid_path, path), "rt"))

                hcid_alerts.append(hcid_message)

    except Exception as e:
        log.error(f"Unhandled exception in hcid warning parsing: {e}")
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append(
            f"Unhandled exception in hcid warning parsing: {e}"
        )
        hcid_fail = True
        alert = True
        return (hcid_fail, hcid_alerts, alert, payload)

    return (hcid_fail, hcid_alerts, alert, payload)


def validate(
    message: namedtuple,
    args: argparse.Namespace,
    ingest_pipe: pipeline,
) -> tuple[bool, bool, dict, namedtuple]:
    """Function to validate a single artifact and update the Onyx record accordingly

    Args:
        message (namedtuple): Varys message object for the current artifact
        args (argparse.Namespace): Command line arguments object
        ingest_pipe (pipeline): Instance of the ingest pipeline (see pipeline class)

    Returns:
        tuple[bool, bool, dict, namedtuple]: Tuple containing a bool indicating whether the validation was successful, a bool indicating whether to squawk in the alert channel, the updated payload dict and the Varys message object
    """
    s3_credentials = get_s3_credentials()

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
        endpoint_url=s3_credentials.endpoint,
    )

    log = logging.getLogger("mscape.ingest")

    to_validate = json.loads(message.body)

    payload = copy.deepcopy(to_validate)

    payload.setdefault("rerun", False)

    alert = False
    hcid_alerts = False

    # This client is purely for Mscape, ignore all other messages
    if to_validate["project"] != "mscape":
        log.info(
            f"Ignoring file set with UUID: {to_validate['uuid']} due non-mscape project ID"
        )
        return (False, alert, hcid_alerts, payload, message)

    if not to_validate["onyx_test_create_status"] or not to_validate["validate"]:
        return (False, alert, hcid_alerts, payload, message)

    if to_validate["platform"] == "ont":
        fastq_unseen, alert, payload = ensure_file_unseen(
            etag_field="fastq_1_etag",
            etag=to_validate["files"][".fastq.gz"]["etag"],
            log=log,
            payload=payload,
        )

        if not fastq_unseen:
            log.info(
                f"Fastq file for UUID: {payload['uuid']} has already been ingested into the {payload['project']} project, skipping validation"
            )
            payload.setdefault("ingest_errors", [])
            payload["ingest_errors"].append(
                "Fastq file appears identical to a previously ingested file, please ensure that the submission is not a duplicate. Please contact the mSCAPE admin team if you believe this to be in error."
            )
            return (False, alert, hcid_alerts, payload, message)

    elif to_validate["platform"] == "illumina":
        fastq_1_unseen, alert, payload = ensure_file_unseen(
            etag_field="fastq_1_etag",
            etag=to_validate["files"][".1.fastq.gz"]["etag"],
            log=log,
            payload=payload,
        )

        fastq_2_unseen, alert, payload = ensure_file_unseen(
            etag_field="fastq_2_etag",
            etag=to_validate["files"][".2.fastq.gz"]["etag"],
            log=log,
            payload=payload,
        )

        if not fastq_1_unseen or not fastq_2_unseen:
            log.info(
                f"Fastq file for UUID: {payload['uuid']} has already been ingested into the {payload['project']} project, skipping validation"
            )
            payload.setdefault("ingest_errors", [])
            payload["ingest_errors"].append(
                "At least one submitted fastq file appears identical to a previously ingested file, please ensure that the submission is not a duplicate. Please contact the mSCAPE admin team if you believe this to be in error."
            )
            return (False, alert, hcid_alerts, payload, message)

    rc, stdout, stderr = execute_validation_pipeline(
        payload=payload, args=args, ingest_pipe=ingest_pipe
    )

    if ingest_pipe.cmd:
        log.info(
            f"Execution of pipeline for UUID: {payload['uuid']} complete. Command was: {' '.join(str(x) for x in ingest_pipe.cmd)}"
        )

    args.result_dir = Path(args.result_dir)

    result_path = Path(args.result_dir.resolve(), payload["uuid"])

    if not os.path.exists(result_path):
        os.makedirs(result_path)

    with open(Path(result_path, "nextflow.stdout"), "wt") as out_fh, open(
        Path(result_path, "nextflow.stderr"), "wt"
    ) as err_fh:
        out_fh.write(stdout)
        err_fh.write(stderr)

    if rc != 0:
        log.error(
            f"Validation pipeline exited with non-0 exit code: {rc} for UUID: {payload['uuid']}"
        )
        payload["rerun"] = True
        time.sleep(args.retry_delay)
        return (False, alert, hcid_alerts, payload, message)

    ingest_fail, payload = ret_0_parser(
        log=log,
        payload=payload,
        result_path=result_path,
    )

    if ingest_fail:
        log.info(f"Validation pipeline failed for UUID: {payload['uuid']}")
        ingest_pipe.cleanup(stdout=stdout)
        return (False, alert, hcid_alerts, payload, message)

    if payload["test_flag"]:
        log.info(
            f"Test ingest for artifact: {payload['artifact']} with UUID: {payload['uuid']} completed successfully"
        )
        payload["test_ingest_result"] = True
        ingest_pipe.cleanup(stdout=stdout)
        return (True, alert, hcid_alerts, payload, message)

    # Spot if metadata disagrees anywhere, don't act on it yet though
    source_reconcile_success, alert, payload = onyx_reconcile(
        payload=payload,
        identifier="biosample_id",
        fields_to_reconcile=[
            "iso_country",
            "iso_region",
            "study_centre_id",
            "input_type",
            "specimen_type_details",
            "biosample_source_id",
            "is_approximate_date",
            "is_public_dataset",
            "received_date",
            "collection_date",
            "sample_source",
            "sample_type",
            "sequence_purpose",
        ],
        log=log,
    )

    run_reconcile_success, alert, payload = onyx_reconcile(
        payload=payload,
        identifier="run_id",
        fields_to_reconcile=[
            "batch_id",
            "bioinformatics_protocol",
            "dehumanisation_protocol",
            "extraction_enrichment_protocol",
            "library_protocol",
            "sequencing_protocol",
            "study_centre_id",
            "platform",
        ],
        log=log,
    )

    create_success, alert, payload = csv_create(
        payload=payload,
        log=log,
        test_submission=False,
    )

    if alert:
        log.error(
            f"Failed to create Onyx record for UUID: {payload['uuid']}, catastrophic error"
        )
        payload["rerun"] = True
        ingest_pipe.cleanup(stdout=stdout)
        time.sleep(args.retry_delay)
        return (False, alert, hcid_alerts, payload, message)

    if not create_success:
        log.info(f"Failed to submit to Onyx for UUID: {payload['uuid']}")
        ingest_pipe.cleanup(stdout=stdout)
        return (False, alert, hcid_alerts, payload, message)

    payload["onyx_create_status"] = True
    payload["created"] = True

    scylla_version_fail, alert, payload = onyx_update(
        payload=payload,
        fields={"scylla_version": payload["scylla_version"]},
        log=log,
    )

    if scylla_version_fail:
        log.error(f"Failed to update Onyx record for UUID: {payload['uuid']}")
        ingest_pipe.cleanup(stdout=stdout)
        return (False, alert, hcid_alerts, payload, message)

    if payload["platform"] == "illumina":
        etag_fail, alert, payload = onyx_update(
            payload=payload,
            log=log,
            fields={
                "fastq_1_etag": payload["files"][".1.fastq.gz"]["etag"],
                "fastq_2_etag": payload["files"][".2.fastq.gz"]["etag"],
            },
        )

    elif payload["platform"] == "ont":
        etag_fail, alert, payload = onyx_update(
            payload=payload,
            log=log,
            fields={"fastq_1_etag": payload["files"][".fastq.gz"]["etag"]},
        )

    if etag_fail:
        ingest_pipe.cleanup(stdout=stdout)
        return (False, alert, hcid_alerts, payload, message)

    log.info(
        f"Uploading files to long-term storage buckets for CID: {payload['climb_id']} after sucessful Onyx submission"
    )

    raw_read_fail, reads_alert, payload = add_reads_record(
        payload=payload,
        s3_client=s3_client,
        result_path=result_path,
        log=log,
    )

    binned_read_fail, taxa_alert, payload = add_taxon_records(
        payload=payload, result_path=result_path, log=log, s3_client=s3_client
    )

    classifier_calls_fail, classifier_alert, payload = add_classifier_calls(
        payload=payload, result_path=result_path, log=log
    )

    # Consider making this a little more versatile in future

    classifier_splits = os.getenv("SCYLLA_K2_DB_PATH").split("/")
    non_empty = [x for x in classifier_splits if x != ""]
    classifier_db = non_empty[-1]

    classifier_metadata_fail, classifier_metadata_alert, payload = onyx_update(
        payload=payload,
        fields={
            "classifier": "kraken2",
            "classifier_version": "2.1.2",
            "classifier_db": classifier_db,
            "classifier_db_date": os.getenv("SCYLLA_K2_DB_DATE"),
            "ncbi_taxonomy_date": os.getenv("SCYLLA_TAXONOMY_DATE"),
        },
        log=log,
    )

    fraction_fail_outer = False

    for fraction in (
        "human_filtered",
        "unclassified",
        "viral_and_unclassified",
        "viral",
    ):
        fraction_fail_inner, fraction_alert, payload = read_fraction_upload(
            payload=payload,
            s3_client=s3_client,
            result_path=result_path,
            log=log,
            fraction_prefix=fraction,
        )

        if fraction_alert:
            alert = True

        if fraction_fail_inner:
            fraction_fail_outer = True

    report_fail, report_alert, payload = push_report_file(
        payload=payload, result_path=result_path, log=log, s3_client=s3_client
    )

    taxon_report_fail, taxa_reports_alert, payload = push_taxon_reports(
        payload=payload, result_path=result_path, log=log, s3_client=s3_client
    )

    if (
        reads_alert
        or taxa_alert
        or report_alert
        or taxa_reports_alert
        or classifier_alert
        or classifier_metadata_alert
    ):
        alert = True

    if (
        raw_read_fail
        or binned_read_fail
        or report_fail
        or taxon_report_fail
        or fraction_fail_outer
        or classifier_calls_fail
        or classifier_metadata_fail
    ):
        log.error(
            f"Failed to upload files to S3 or update Onyx for CID: {payload['climb_id']} with match UUID: {payload['uuid']}"
        )
        payload["rerun"] = True
        ingest_pipe.cleanup(stdout=stdout)
        time.sleep(args.retry_delay)
        return (False, alert, hcid_alerts, payload, message)

    publish_fail, alert, payload = onyx_update(
        payload=payload, log=log, fields={"is_published": True}
    )

    if alert:
        log.error(
            f"Failed to update Onyx record for UUID: {payload['uuid']} with CID: {payload['climb_id']}"
        )
        payload["rerun"] = True
        ingest_pipe.cleanup(stdout=stdout)
        time.sleep(args.retry_delay)
        return (False, alert, hcid_alerts, payload, message)

    if publish_fail:
        ingest_pipe.cleanup(stdout=stdout)
        return (False, alert, hcid_alerts, payload, message)

    payload["published"] = True
    log.info(
        f"Sending successful ingest result for UUID: {payload['uuid']}, with CID: {payload['climb_id']}"
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

    return (True, alert, hcid_alerts, payload, message)


def run(args):
    log = init_logger("mscape.ingest", args.logfile, args.log_level)

    varys_client = Varys(
        profile="roz",
        logfile=args.logfile,
        log_level=args.log_level,
        auto_acknowledge=False,
    )

    ingest_pipe = pipeline(
        pipe=args.ingest_pipeline,
        branch=args.pipeline_branch,
        profile="docker",
        config=args.nxf_config,
        nxf_executable=args.nxf_executable,
        timeout=args.pipeline_timeout,
    )

    worker_pool = worker_pool_handler(
        workers=args.n_workers, logger=log, varys_client=varys_client
    )
    try:
        while True:
            message = varys_client.receive(
                exchange="inbound-to_validate-mscape",
                queue_suffix="validator",
                prefetch_count=args.n_workers,
            )

            worker_pool.submit_job(message=message, args=args, ingest_pipe=ingest_pipe)
    except BaseException as e:
        log.info(f"Shutting down worker pool due to exception: {e}")
        worker_pool.close()
        varys_client.close()
        time.sleep(1)
        sys.exit(1)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfile", type=Path)
    parser.add_argument("--log_level", type=str, default="DEBUG")
    parser.add_argument("--ingest_pipeline", type=str, default="artic-network/scylla")
    parser.add_argument("--pipeline_branch", type=str, default="main")
    parser.add_argument("--nxf_config")
    parser.add_argument("--nxf_executable", default="nextflow")
    parser.add_argument("--k2_host", type=str)
    parser.add_argument("--result_dir", type=Path)
    parser.add_argument("--n_workers", type=int, default=5)
    parser.add_argument("--retry-delay", type=int, default=180)
    parser.add_argument(
        "--pipeline_timeout", type=int, default=43200
    )  # 12 hours, might not even be enough for larger datasets (e.g. promethion / N*Seq)

    global args
    args = parser.parse_args()

    for i in (
        "ONYX_DOMAIN",
        "ONYX_TOKEN",
        "VARYS_CFG",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "SCYLLA_K2_DB_PATH",
        "SCYLLA_K2_DB_DATE",
        "SCYLLA_TAXONOMY_PATH",
        "SCYLLA_TAXONOMY_DATE",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    run(args)


if __name__ == "__main__":
    main()
