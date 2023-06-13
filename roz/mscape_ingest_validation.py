import sys
import os
import subprocess
from pathlib import Path
from collections import namedtuple
import json
import copy

from roz import varys
from metadbclient import Session, utils


class pipeline:
    """
    Run a nxf pipeline as a subprocess, this is only advisable for use with cloud executors, specifically k8s.
    If local execution is needed then you should use something else.

    This needs to use a version of nextflow >= 23 (currently this is the edge version) due to a bug in the current
    stable version which prevents fusion from mounting the S3 work dir.
    """

    def __init__(self, pipe: str, config: Path, work_bucket: str, nxf_executable: Path):
        if not pipe or not work_bucket or not nxf_executable:
            raise Exception("Necessary argument not provided to 'pipeline' class")

        self.pipe = pipe
        self.config = Path(config)
        self.nxf_executable = Path(nxf_executable)
        self.work_bucket = work_bucket

    def execute(self, params: dict, profile="k8s"):
        timeout = False

        cmd = [
            self.nxf_executable.resolve(),
            "run",
            self.pipe,
            "-work-dir",
            f"s3://{self.work_bucket}/scratch/",
        ]

        if self.config:
            cmd.extend(["-c", self.config.resolve()])

        if profile:
            cmd.extend(["-profile", profile])

        if params:
            for k, v in params.items():
                cmd.extend([f"--{k}", v])

        proc = subprocess.Popen(args=cmd)

        try:
            out, err = proc.communicate(timeout=7200)

        except subprocess.TimeoutExpired:
            timeout = True
            proc.kill()
            out, err = proc.communicate()

        rc = proc.poll()

        return (rc, timeout, out, err)


def parse_nxf_report(report_path):
    pass


def meta_create(metadata, pathogen_code):
    # Add metadata extraction and submission from report json here

    with Session(env_password=True) as session:
        response = session.create(pathogen_code=pathogen_code, fields=metadata)

    return response


def run(args):
    # Setup producer / consumer
    log = varys.init_logger("mscape.ingest", args.logfile, args.log_level)

    varys_client = varys.varys(
        profile="roz_admin",
        in_exchange="inbound.to_validate",
        out_exchange="inbound.validated",
        logfile=args.logfile,
        log_level=args.log_level,
        queue_suffix=".mscape",
    )

    validation_payload_template = {
        "mid": "",
        "artifact": "",
        "sample_id": "",
        "run_name": "",
        "project": "",
        "ingest_timestamp": "",
        "cid": False,
        "site": "",
        "created": False,
        "ingested": False,
        "files": False,  # Dict
        "local_paths": False,  # Dict
        "onyx_test_status_code": False,
        "onyx_test_errors": False,  # Dict
        "onyx_test_create_status": False,
        "onyx_status_code": False,
        "onyx_errors": False,  # Dict
        "onyx_create_status": False,
        "ingest_errors": False,  # List
    }

    ingest_pipe = pipeline("", args.nxf_config, args.work_bucket, args.nxf_executable)

    while True:
        message = varys_client.receive()

        to_validate = json.loads(message.body)

        payload = copy.deepcopy(to_validate)

        # This client is purely for Mscape, ignore all other messages
        if to_validate["project"] != "mscape_test":
            log.info(
                f"Ignoring message with mid: {message.basic_deliver.delivery_tag} due non-mscape project ID"
            )
            continue

        # TODO: account for platform differences here

        parameters = {}

        msg_id = message.basic_deliver.delivery_tag

        payload["mid"] = msg_id

        log.info(f"Submitted ingest pipeline for mid: {msg_id}")
        rc, timeout, stdout, stderr = ingest_pipe.execute(params=parameters)

        if not timeout:
            log.info(f"Pipeline execution for message id: {msg_id}, complete.")
        else:
            log.error(f"Pipeline execution timed out for message id: {msg_id}")
            payload["ingest_errors"] = ["pipe_timeout"]
            varys_client.send(payload)
            continue

        out_path = os.path.join(args.result_dir, msg_id, "stdout")
        err_path = os.path.join(args.temp_dir, msg_id, "stderr")

        with open(out_path, "wb") as out_fh, open(err_path, "wb") as err_fh:
            out_fh.write(stdout)
            err_fh.write(stderr)

        if rc == 0:
            ingest_report_path = f"{args.temp_dir}/{msg_id}/ingest_report.json"

            if os.path.exists(ingest_report_path):
                report = parse_nxf_report(ingest_report_path)

            else:
                log.error(
                    f"Pipeline report does not appear to exist in the expected path ({ingest_report_path}) for message id: {msg_id}"
                )
                # Insert fail rmq message here
                continue

            # Placeholder
            if report.passed_validation:
                meta_resp = meta_create()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--asdasdas")
    args = parser.parse_args()

    run(args)
