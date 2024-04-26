import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
import json
import copy
import argparse
import logging
import csv
import requests
import time
import multiprocessing as mp
import sys


from roz_scripts.utils.utils import (
    csv_create,
    onyx_update,
    pipeline,
    init_logger,
    get_s3_credentials,
    put_result_json,
    get_onyx_credentials,
    ensure_file_unseen,
)
from varys import Varys
from onyx import OnyxClient


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
        success, payload, message = validate_result

        # if alert:
        #     self._log.error(
        #         f"Alert flag set for UUID: {payload['uuid']}, manual intervention required"
        #     )
        #     self._varys_client.send(
        #         message=payload,
        #         exchange="restricted-mscape-announce",
        #         queue_suffix="alert",
        #     )

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

                self._varys_client.send(
                    message=new_artifact_payload,
                    exchange="inbound-new_artifact-pathsafe",
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
                        exchange="pathsafe-restricted-announce",
                        queue_suffix="dead_letter",
                    )

                    self._varys_client.send(
                        message=payload,
                        exchange=f"inbound-results-{payload['project']}-{payload['site']}",
                        queue_suffix="validator",
                    )

                    put_result_json(payload, self._log)

                    self._varys_client.nack_message(message)

                    raise ValueError("Validation failed after 5 attempts, shutting down worker pool")
                
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
            message=f"Pathsafe ingest worker failed with unhandled exception: {exception}",
            exchange="pathsafe-restricted-announce",
            queue_suffix="dead_worker",
        )


    def close(self):
        self.worker_pool.close()
        self.worker_pool.join()


def assembly_to_s3(
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

    s3_fail = False

    assembly_path = os.path.join(
        result_path, f"assembly/{payload['uuid']}.result.fasta"
    )

    try:
        s3_client.upload_file(
            assembly_path,
            "pathsafe-published-assembly",
            f"{payload['climb_id']}.assembly.fasta",
        )

        payload["assembly_presigned_url"] = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": "pathsafe-published-assembly",
                "Key": f"{payload['climb_id']}.assembly.fasta",
            },
            ExpiresIn=86400,
        )

    except ClientError as e:
        log.error(
            f"Failed to upload assembly to long-term storage bucket for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to client error: {e}"
        )
        payload["ingest_errors"].append("Failed to upload assembly to storage bucket")
        s3_fail = True

    if not s3_fail:
        update_fail, alert, payload = onyx_update(
            payload=payload,
            fields={
                "assembly": f"s3://pathsafe-published-assembly/{payload['climb_id']}.assembly.fasta",
            },
            log=log,
        )

        if update_fail:
            s3_fail = True

    return (s3_fail, payload)


def pathogenwatch_submission(
    payload: dict, log: logging.getLogger
) -> tuple[bool, dict]:
    """Function to submit a genome to pathogenwatch

    Args:
        payload (dict): Payload dict for the record to update
        log (logging.getLogger): Logger object
        s3_client (boto3.client): Boto3 client object for S3

    Returns:
        tuple[bool, dict]: Tuple containing a bool indicating whether the submission failed and the updated payload dict
    """
    pathogenwatch_fail = False

    onyx_config = get_onyx_credentials()

    log.info(f"Submitting to Pathogenwatch for UUID: {payload['uuid']}")
    with OnyxClient(config=onyx_config) as client:
        record = client.get(
            "pathsafe",
            payload["climb_id"],
        )

    ignore_fields = ["is_published", "published_date", "pathogenwatch_uuid"]

    headers = {
        "X-API-Key": os.getenv("PATHOGENWATCH_API_KEY"),
        "content-type": "application/json",
    }

    base_url = os.getenv("PATHOGENWATCH_ENDPOINT_URL")

    resp = requests.get(f"{base_url}/folders/list?user_owned=true", headers=headers)

    if resp.status_code != 200:
        log.error(f"Failed to retrieve Pathogenwatch folders due to error: {resp.text}")
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append(
            f"Failed to retrieve Pathogenwatch folders due to error: {resp.text}"
        )
        pathogenwatch_fail = True
        return (pathogenwatch_fail, payload)

    folders = resp.json()

    folder_id = False

    for folder in folders:
        if folder["name"].lower() == payload["site"]:
            folder_id = folder["id"]
            break

    if not folder_id:
        log.error(
            f"Failed to retrieve Pathogenwatch folder ID for site: {payload['site']}"
        )
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append(
            f"Failed to retrieve Pathogenwatch folder ID for site: {payload['site']}"
        )
        pathogenwatch_fail = True
        return (pathogenwatch_fail, payload)

    fields = {k: v for k, v in record.items() if v and k not in ignore_fields}

    # change site to submit_org for pathogenwatch benefit
    fields["submit_org"] = fields.pop("site")
    fields["sequencing_platform"] = fields.pop("platform")

    body = {
        "url": payload["assembly_presigned_url"],
        "folderId": folder_id,
        "metadata": fields,
    }

    r = requests.post(url=f"{base_url}/genomes/create", headers=headers, json=body)

    if r.status_code != 201:
        log.error(
            f"Pathogenwatch submission failed for UUID: {payload['uuid']} with CID: {payload['climb_id']} due to error: {r.text}"
        )
        payload.setdefault("ingest_errors", [])
        payload["ingest_errors"].append(
            f"Pathogenwatch submission failed with status code: {r.status_code}, due to error: {r.text}"
        )
        pathogenwatch_fail = True

    pathogenwatch_uuid = r.json()["uuid"]

    update_fail, alert, payload = onyx_update(
        payload=payload, fields={"pathogenwatch_uuid": pathogenwatch_uuid}, log=log
    )

    if update_fail:
        pathogenwatch_fail = True

    return (pathogenwatch_fail, payload)


def execute_assembly_pipeline(
    payload: dict,
    args: argparse.Namespace,
    log: logging.getLogger,
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
        "out_dir": args.result_dir,
        "sample_uuid": payload["uuid"],
        "fastq_1": payload["files"][".1.fastq.gz"]["uri"],
        "fastq_2": payload["files"][".2.fastq.gz"]["uri"],
    }

    log.info(f"Submitted ingest pipeline for UUID: {payload['uuid']}")

    log_path = Path(args.result_dir, payload["uuid"])

    return ingest_pipe.execute(params=parameters, logdir=log_path)


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

    except Exception as e:
        log.error(
            f"Could not open pipeline trace for UUID: {payload['uuid']} despite NXF exit code 0 due to error: {e}"
        )
        payload["ingest_errors"].append("couldn't open nxf ingest pipeline trace")
        ingest_fail = True

    for process, trace in trace_dict.items():
        if trace["exit"] != "0":
            if process.startswith("etoki_assemble") and trace["exit"] == "255":
                log.info(f"Etoki assembly failed for UUID: {payload['uuid']}, exit code: 255")
                payload.setdefault("ingest_errors", [])
                payload["ingest_errors"].append(
                    "Etoki assembly (spades) failed with exit code 255, most likely due to mangled quality strings, please check the fastq files and resubmit")
                ingest_fail = True
                continue
            
            payload.setdefault("ingest_errors", [])
            
            payload["ingest_errors"].append(
                f"MScape validation pipeline (Scylla) failed in process {process} with exit code {trace['exit']} and status {trace['status']}"
            )
            ingest_fail = True

    return (ingest_fail, payload)


def validate(
    message,
    args: argparse.Namespace,
    ingest_pipe: pipeline,
):
    s3_credentials = get_s3_credentials()

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
        endpoint_url=s3_credentials.endpoint,
    )

    log = logging.getLogger("pathsafe.validate")

    to_validate = json.loads(message.body)

    payload = copy.deepcopy(to_validate)

    payload["rerun"] = False

    # This client is purely for pathsafe, ignore all other messages
    if to_validate["project"] != "pathsafe":
        log.info(
            f"Ignoring file set with UUID: {to_validate['uuid']} due non-pathsafe project ID"
        )
        return (False, payload, message)

    if not to_validate["onyx_test_create_status"] or not to_validate["validate"]:
        return (False, payload, message)
    
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
            "At least one submitted fastq file appears identical to a previously ingested file, please ensure that the submission is not a duplicate. Please contact the pathsafe admin team if you believe this to be in error."
        )
        return (False, payload, message)

    rc, stdout, stderr = execute_assembly_pipeline(
        payload=payload, args=args, log=log, ingest_pipe=ingest_pipe
    )

    if ingest_pipe.cmd:
        log.info(
            f"Execution of pipeline for UUID: {payload['uuid']} complete. Command was: {" ".join(str(x) for x in ingest_pipe.cmd)}"
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
        time.sleep(args.retry_delay)
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

    submission_success, alert, payload = csv_create(
        log=log, payload=payload, test_submission=False
    )

    if not submission_success:
        log.error(
            f"Submission to Onyx failed for UUID: {payload['uuid']}"
        )
        payload["rerun"] = True
        ingest_pipe.cleanup(stdout=stdout)
        time.sleep(args.retry_delay)
        return (False, payload, message)

    payload["created"] = True

    s3_fail, payload = assembly_to_s3(
        payload=payload,
        s3_client=s3_client,
        result_path=result_path,
        log=log,
    )

    if s3_fail:
        log.error(
            f"Failed to upload assembly to long-term storage bucket for UUID: {payload['uuid']}"
        )
        ingest_pipe.cleanup(stdout=stdout)
        payload["rerun"] = True
        time.sleep(args.retry_delay)
        return (False, payload, message)

    pathogenwatch_fail, payload = pathogenwatch_submission(
        payload=payload,
        log=log,
    )

    if pathogenwatch_fail:
        log.error(
            f"Pathogenwatch submission failed for UUID: {payload['uuid']}"
        )
        payload["rerun"] = True
        ingest_pipe.cleanup(stdout=stdout)
        time.sleep(args.retry_delay)
        return (False, payload, message)
    
    log.info(f"Pathogenwatch submission successful for UUID: {payload['uuid']}")

    etag_fail, alert, payload = onyx_update(
    payload=payload,
    log=log,
    fields={
        "fastq_1_etag": payload["files"][".1.fastq.gz"]["etag"],
        "fastq_2_etag": payload["files"][".2.fastq.gz"]["etag"],
        },
    )

    if etag_fail:
        log.error(f"Failed to update etags for UUID: {payload['uuid']}")
        ingest_pipe.cleanup(stdout=stdout)
        payload["rerun"] = True
        time.sleep(args.retry_delay)
        return (False, payload, message)    

    unsuppress_fail, alert, payload = onyx_update(
        payload=payload, log=log, fields={"is_published": True}
    )

    if unsuppress_fail:
        log.error(
            f"Failed to unsuppress Onyx record for UUID: {payload['uuid']}, sending result"
        )
        payload["rerun"] = True
        ingest_pipe.cleanup(stdout=stdout)
        time.sleep(args.retry_delay)
        return (False, payload, message)

    payload["published"] = True

    (
        cleanup_status,
        cleanup_stdout,
        cleanup_stderr,
    ) = ingest_pipe.cleanup(stdout=stdout)

    if cleanup_status != 0:
        log.error(
            f"Cleanup of pipeline for UUID: {payload['uuid']} failed with exit code: {cleanup_status}, Stdout: {cleanup_stdout}, Stderr: {cleanup_stderr}"
        )

    return (True, payload, message)


def run(args):
    log = init_logger("pathsafe.validate", args.logfile, args.log_level)

    varys_client = Varys(
        profile="roz",
        logfile=args.logfile,
        log_level=args.log_level,
        auto_acknowledge=False,
    )

    ingest_pipe = pipeline(
        pipe="CLIMB-TRE/path-safe_assembler",
        branch="main",
        profile="docker",
        config=args.nxf_config,
        nxf_executable=args.nxf_executable,
        timeout=21600,
    )

    worker_pool = worker_pool_handler(
        workers=args.n_workers, logger=log, varys_client=varys_client
    )
    try:
        while True:
            message = varys_client.receive(
                exchange="inbound-to_validate-pathsafe",
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--logfile", type=Path, required=True, help="Path to logfile")
    parser.add_argument(
        "--log_level",
        type=str,
        help="Log level for logger object",
        choices=["NOTSET", "INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"],
        default="DEBUG",
    )
    parser.add_argument(
        "--result_dir", type=Path, required=True, help="Path to store results"
    )
    parser.add_argument(
        "--nxf_config", type=Path, required=False, help="Path to nxf config file"
    )
    parser.add_argument(
        "--nxf_executable", type=Path, required=False, default="nextflow"
    )
    parser.add_argument(
        "--n_workers",
        type=int,
        default=5,
        help="Number of workers to use for concurrent validation",
    )
    parser.add_argument("--retry-delay", type=int, default=180, help="Time to wait before re-queuing a failed message")
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
