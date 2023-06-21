import csv
import os
import subprocess
from pathlib import Path
from collections import namedtuple
import json
import copy

# from roz import varys
import utils
import varys

from onyx import Session as onyx_session


class pipeline:
    """
    Run a nxf pipeline as a subprocess, this is only advisable for use with cloud executors, specifically k8s.
    If local execution is needed then you should use something else.

    This needs to use a version of nextflow >= 23 (currently this is the edge version) due to a bug in the current
    stable version which prevents fusion from mounting the S3 work dir.
    """

    def __init__(
        self,
        pipe: str,
        config: Path,
        nxf_executable: Path,
        profile=None,
        timeout_seconds=3600,
    ):
        # if not pipe or not work_bucket or not nxf_executable:
        #     raise Exception("Necessary argument not provided to 'pipeline' class")

        self.pipe = pipe
        self.config = Path(config) if config else None
        self.nxf_executable = (
            Path(nxf_executable).resolve()
            if nxf_executable != "nextflow"
            else "nextflow"
        )
        self.profile = profile
        self.timeout_seconds = timeout_seconds

    def execute(self, params: dict):
        timeout = False

        cmd = [self.nxf_executable, "run", "-r", "main", "-latest", self.pipe]

        if self.config:
            cmd.extend(["-c", self.config.resolve()])

        if self.profile:
            cmd.extend(["-profile", self.profile])

        if params:
            for k, v in params.items():
                cmd.extend([f"--{k}", v])

        try:
            proc = subprocess.run(
                args=cmd,
                capture_output=True,
                universal_newlines=True,
                text=True,
                timeout=self.timeout_seconds,
            )

        except subprocess.TimeoutExpired:
            timeout = True

        return (proc.returncode, timeout, proc.stdout, proc.stderr)


def parse_nxf_report(report_path):
    pass


def run(args):
    # Setup producer / consumer
    log = varys.init_logger("mscape.ingest", args.logfile, args.log_level)

    varys_client = varys.varys(
        profile="roz",
        in_exchange="inbound.to_validate",
        out_exchange="inbound.validated.mscape",
        logfile=args.logfile,
        log_level=args.log_level,
        queue_suffix="validator",
        config_path="/home/jovyan/roz_profiles.json",
    )

    validation_payload_template = {
        "uuid": "",
        "artifact": "",
        "sample_id": "",
        "run_name": "",
        "project": "",
        "ingest_timestamp": "",
        "cid": False,
        "site": "",
        "created": False,
        "ingested": False,
        "files": {},  # Dict
        "local_paths": {},  # Dict
        "onyx_test_status_code": False,
        "onyx_test_create_errors": {},  # Dict
        "onyx_test_create_status": False,
        "onyx_status_code": False,
        "onyx_errors": {},  # Dict
        "onyx_create_status": False,
        "ingest_errors": [],  # List
    }

    ingest_pipe = pipeline(
        pipe="biowilko/scylla",
        profile="docker",
        config=args.nxf_config,
        nxf_executable=args.nxf_executable,
    )

    while True:
        message = varys_client.receive()

        to_validate = json.loads(message.body)

        payload = copy.deepcopy(to_validate)

        # This client is purely for Mscape, ignore all other messages
        if to_validate["project"] != "mscapetest":
            log.info(
                f"Ignoring file set with UUID: {message['uuid']} due non-mscape project ID"
            )
            continue

        if not to_validate["onyx_test_create_status"]:
            varys_client.send(payload)
            continue

        parameters = {
            "out_dir": args.result_dir,
            "unique_id": payload["uuid"],
            "climb": "",
            "k2_host": "10.1.185.58",  # Parameterise this and deal with DNS stuff
            "k2_port": "8080",
            "db": "/shared/public/db/kraken2/k2_pluspf/",
        }

        if payload["platform"] == "ont":
            parameters["fastq"] = payload["files"][".fastq.gz"]["uri"]

        elif to_validate["platform"] == "illumina":
            parameters["fastq1"] = payload["files"][".1.fastq.gz"]["uri"]
            parameters["fastq2"] = payload["files"][".2.fastq.gz"]["uri"]
            parameters["paired"] = ""
        else:
            log.error(
                f"Unrecognised platform: {payload['platform']} for UUID: {payload['uuid']}"
            )

        log.info(f"Submitted ingest pipeline for UUID: {payload['uuid']}")
        rc, timeout, stdout, stderr = ingest_pipe.execute(params=parameters)

        if not timeout:
            log.info(f"Pipeline execution for message id: {payload['uuid']}, complete.")
        else:
            log.error(f"Pipeline execution timed out for message id: {payload['uuid']}")
            payload["ingest_errors"].append("Validation pipeline timeout")
            log.info(f"Sending validation result for UUID: {payload['uuid']}")
            varys_client.send(payload)
            continue

        result_path = os.path.join(args.result_dir.resolve(), payload["uuid"])
        # tmp_path = os.path.join(args.temp_dir.resolve(), payload["uuid"])

        if not os.path.exists(result_path):
            os.makedirs(result_path)

        with open(os.path.join(result_path, "nextflow.stdout"), "wt") as out_fh, open(
            os.path.join(result_path, "nextflow.stderr"), "wt"
        ) as err_fh:
            out_fh.write(stdout)
            err_fh.write(stderr)

        if rc == 0:
            ingest_fail = False
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
                payload["ingest_errors"].append(
                    "couldn't open nxf ingest pipeline trace"
                )
                varys_client.send(payload)
                continue

            for process, trace in trace_dict.items():
                if trace["exit"] != 0:
                    if (
                        process == "extract_paired_reads"
                        or process == "extract_reads"
                        and trace["exit"] == 2
                    ):
                        payload["ingest_errors"].append(
                            "Human reads detected above rejection threshold, please ensure pre-upload dehumanisation has been performed properly"
                        )
                        ingest_fail = True
                    else:
                        payload["ingest_errors"].append(
                            f"MScape ingest (Scylla) pipeline failed in process {process} with exit code {trace['exit']} and status {trace['status']}"
                        )
                        ingest_fail = True

            if ingest_fail:
                varys_client.send(payload)
                continue

            payload["ingested"] = True

            with onyx_session() as client:
                log.info(
                    f"Received match for artifact: {payload['artifact']}, now attempting to test_create record in Onyx"
                )

                try:
                    with open(
                        utils.s3_to_fh(payload["files"][".csv"]), "rt"
                    ) as csv_s3_fh:
                        response_generator = client.csv_create(
                            payload["project"],
                            csv_file=csv_s3_fh,
                        )

                        response = response_generator.next()

                        payload["onyx_status_code"] = response.status_code

                        if response.status_code == 500:
                            log.error(
                                f"Onyx create for UUID: {payload['uuid']} lead to onyx internal server error"
                            )
                            ingest_fail = True
                            payload["onyx_errors"]["onyx_client_errors"] = [
                                "Onyx internal server error"
                            ]

                        elif response.status_code == 422:
                            log.error(
                                f"Onyx create for UUID: {payload['uuid']} failed, details in validated messages"
                            )
                            ingest_fail = True
                            if response.json().get("messages"):
                                for field, messages in response.json()[
                                    "messages"
                                ].items():
                                    if payload["onyx_errors"].get(field):
                                        payload["onyx_errors"][field].extend(messages)
                                    else:
                                        payload["onyx_errors"][field] = messages

                        elif response.status_code == 404:
                            log.error(
                                f"Onyx create for UUID: {payload['uuid']} failed because project: {payload['project']} does not exist"
                            )
                            ingest_fail = True
                            payload["onyx_errors"]["onyx_client_errors"] = [
                                f"Project {payload['project']} does not exist"
                            ]

                        elif response.status_code == 403:
                            log.error(
                                f"Onyx create for UUID: {payload['uuid']} failed due to a permission error"
                            )
                            ingest_fail = True
                            payload["onyx_errors"]["onyx_client_errors"] = [
                                "Bad fields in onyx create request"
                            ]

                        elif response.status_code == 400:
                            log.error(
                                f"Onyx create for UUID: {payload['uuid']} failed due to a malformed request (should not happen ever)"
                            )
                            ingest_fail = True
                            payload["onyx_errors"]["onyx_client_errors"] = [
                                "Malformed onyx create request"
                            ]

                        elif response.status == 200:
                            log.error(
                                f"Onyx responded with 200 on a create request for UUID: {payload['uuid']} (this should be 201)"
                            )
                            ingest_fail = True
                            payload["onyx_errors"]["onyx_client_errors"] = [
                                "200 response status on onyx create (should be 201)"
                            ]

                        if ingest_fail:
                            varys_client.send(payload)
                            continue

                        if response.status == 201:
                            log.info(
                                f"Successful create for UUID: {payload['uuid']} which has been assigned CID: {response.json()['data']['cid']}"
                            )
                            payload["onyx_create_status"] = True
                            payload["created"] = True
                            payload["cid"] = response.json()["data"]["cid"]

                except Exception as e:
                    log.error(
                        f"Onyx CSV create failed for UUID: {payload['uuid']} due to client error: {e}"
                    )
                    payload["onyx_errors"]["onyx_client_errors"] = [
                        f"Unhandled client error {e}"
                    ]
                    varys_client.send(payload)
                    continue

            # Move all the files around, delete old ones etc etc

            log.info(
                f"Sending successful validation result for UUID: {payload['uuid']}"
            )
            varys_client.send(payload)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--logfile", type=Path)
    parser.add_argument("--log_level", type=str, default="DEBUG")
    parser.add_argument("--nxf_config")
    parser.add_argument("--work_bucket")
    parser.add_argument("--nxf_executable", default="nextflow")
    parser.add_argument("--result_dir", type=Path)
    parser.add_argument("--temp_dir", type=Path)
    args = parser.parse_args()

    run(args)
