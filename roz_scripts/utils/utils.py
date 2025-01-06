import boto3
from botocore.exceptions import ClientError
from collections import namedtuple
import configparser
import os
import sys
from io import StringIO
import logging
import logging.handlers
from pathlib import Path
import time
import csv
import regex as re
import json
import random

from onyx import (
    OnyxClient,
    OnyxConfig,
)

from onyx.exceptions import (
    OnyxRequestError,
    OnyxConnectionError,
    OnyxServerError,
    OnyxConfigError,
    OnyxClientError,
)

from kubernetes.client import Configuration
from kubernetes.client.api import BatchV1Api

__s3_creds = namedtuple(
    "s3_credentials",
    ["access_key", "secret_key", "endpoint", "region", "profile_name"],
)


class EtagMismatchError(Exception):
    pass


class pipeline:
    def __init__(
        self,
        pipe: str,
        branch: str,
        config: Path,
        nxf_image: str,
        profile=None,
    ):
        """
        Run a nxf pipeline as a subprocess, this is only advisable for use with cloud executors, specifically k8s.
        If local execution is needed then you should use something else.

        Args:
            pipe (str): The pipeline to run as a github repo in the format 'user/repo'
            config (str): Path to a nextflow config file
            profile (str): The nextflow profile to use

        """

        self.pipe = pipe
        self.branch = branch
        self.config = Path(config) if config else None
        self.nxf_image = nxf_image
        # self.timeout = timeout
        self.profile = profile
        self.cmd = None

    def execute(
        self,
        params: dict,
        logdir: Path,
        timeout: int,
        env_vars: dict,
        namespace: str,
        job_id: str,
        stdout_path: str,
        stderr_path: str,
    ) -> int:
        """
        Execute the pipeline as a k8s job

        Args:
            params (dict): Parameters to pass to the pipeline
            logdir (Path): Path to the log directory
            timeout (int): Timeout for the job
            env_vars (dict): Environment variables to pass to the pod
            namespace (str): The namespace to run the job in
            job_id (str): The job id
            stdout_path (str): Path to the stdout file
            stderr_path (str): Path to the stderr file

        Returns:
            int: The (fake) return code of the job
        """

        cmd = ["nextflow"]

        if logdir:
            logfile_path = os.path.join(logdir.resolve(), "nextflow.log")
            cmd.extend(
                [
                    "-log",
                    logfile_path,
                ]
            )

        cmd.extend(["run", "-r", self.branch, "-latest", self.pipe])

        if self.config:
            cmd.extend(["-c", self.config.resolve()])

        if self.profile:
            cmd.extend(["-profile", self.profile])

        if params:
            for k, v in params.items():
                cmd.extend([f"--{k}", v])

        cmd_str = " ".join(str(x) for x in cmd)

        pod_env_vars = [{"name": k, "value": v} for k, v in env_vars.items()]

        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": f"roz-{job_id}"},
            "spec": {
                "ttlSecondsAfterFinished": 120,
                "backoffLimit": 5,
                "template": {
                    "spec": {
                        "hostname": f"roz-{job_id}",
                        "subdomain": namespace,
                        "restartPolicy": "Never",
                        "volumes": [
                            {
                                "name": "shared-public",
                                "persistentVolumeClaim": {
                                    "claimName": "cephfs-shared-ro-public"
                                },
                            },
                            {
                                "name": "shared-team",
                                "persistentVolumeClaim": {
                                    "claimName": "cephfs-shared-team"
                                },
                            },
                        ],
                        "nodeSelector": {"hub.jupyter.org/node-purpose": "user"},
                        "containers": [
                            {
                                "name": f"roz-{job_id}",
                                "image": str(self.nxf_image),
                                "resources": {"requests": {"cpu": "2", "memory": "4G"}},
                                "volumeMounts": [
                                    {
                                        "mountPath": "/shared/public/",
                                        "name": "shared-public",
                                        "readOnly": True,
                                    },
                                    {
                                        "mountPath": "/shared/team/",
                                        "name": "shared-team",
                                    },
                                ],
                                "workingDir": f"/shared/team/nxf_work/roz/nextflow.worker.{os.getpid()}/",
                                "env": pod_env_vars,
                                "args": [
                                    "/bin/sh",
                                    "-c",
                                    f"{cmd_str} > {stdout_path} 2> {stderr_path}",
                                ],
                            }
                        ],
                    },
                },
            },
        }

        try:
            self.cmd = cmd
            os.chdir(logdir)

            c = Configuration()

            with open(
                "/run/secrets/kubernetes.io/serviceaccount/token", "rt"
            ) as token_fh:
                token = token_fh.read()

            c.api_key["authorization"] = token
            c.api_key_prefix["authorization"] = "Bearer"
            c.host = f"https://{os.getenv('KUBERNETES_SERVICE_HOST')}"
            c.ssl_ca_cert = "/run/secrets/kubernetes.io/serviceaccount/ca.crt"

            Configuration.set_default(c)
            api_instance = BatchV1Api()

            try:
                resp = api_instance.read_namespaced_job_status(
                    name=f"roz-{job_id}", namespace=namespace
                )

            except Exception:
                api_instance.create_namespaced_job(
                    body=job_manifest, namespace=namespace
                )

            job_completed = False
            while not job_completed:
                resp = api_instance.read_namespaced_job_status(
                    name=f"roz-{job_id}", namespace=namespace
                )
                if resp.status.succeeded:
                    if resp.status.succeeded >= 1:
                        returncode = 0
                        job_completed = True
                        break

                if resp.status.failed:
                    if resp.status.failed >= 5:
                        api_instance.delete_namespaced_job(
                            name=f"roz-{job_id}", namespace=namespace
                        )
                        returncode = 1
                        job_completed = True
                        break

                time.sleep(random.uniform(2.0, 3.0))

        except BaseException as e:
            # proc = SimpleNamespace(returncode=1, stdout=str(k8s_exception), stderr="")
            print(f"Failed to execute pipeline due to exception: {e}")
            returncode = 1

        return returncode


def init_logger(name, log_path, log_level):
    log = logging.getLogger(name)
    log.propagate = False
    log.setLevel(log_level)
    if not (log.hasHandlers()):
        logging_fh = logging.handlers.TimedRotatingFileHandler(log_path, when="W0")
        logging_fh.setFormatter(
            logging.Formatter("%(name)s\t::%(levelname)s::%(asctime)s::\t%(message)s")
        )
        log.addHandler(logging_fh)
    return log


def put_result_json(payload: dict, log: logging.getLogger):
    """Send the result payload to S3

    Args:
        payload (dict): The payload to send to S3
        log (logging.getLogger): Logger object
    """

    s3_credentials = get_s3_credentials()

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        region_name=s3_credentials.region,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    try:
        s3_client.put_object(
            Bucket=f"{payload['project']}-{payload['raw_site']}-results",
            Key=f"{payload['project']}.{payload['run_index']}.{payload['run_id']}.result.json",
            Body=json.dumps(payload),
        )
    except ClientError as e:
        log.error(f"Failed to upload result JSON to S3: {e}")
        raise e


def put_linkage_json(payload: dict, log: logging.getLogger):
    """Send the linkage payload to S3

    Args:
        payload (dict): The payload dict to create the linkage dict from
        log (logging.getLogger): Logger object
    """

    s3_credentials = get_s3_credentials()

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        region_name=s3_credentials.region,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    linkage_dict = {
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
        linkage_dict["biosample_source_id"] = payload["anonymised_biosample_source_id"]

    try:
        s3_client.put_object(
            Bucket=f"{payload['project']}-{payload['raw_site']}-results",
            Key=f"{payload['project']}.{payload['run_index']}.{payload['run_id']}.linkage.json",
            Body=json.dumps(linkage_dict),
        )
    except ClientError as e:
        log.error(f"Failed to upload result JSON to S3: {e}")
        raise e


def csv_create(
    payload: dict,
    log: logging.getLogger,
    test_submission: bool = False,
) -> tuple[bool, bool, dict]:
    """Function to create a new record in onyx from a metadata CSV file, can be used for testing or for real submissions

    Args:
        payload (dict): Payload dict for the current artifact
        log (logging.getLogger): Logger object
        test_submission (bool, optional): Bool to indicate if submission is a test or not. Defaults to False.

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the create was successful, a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """
    # Not sure how to fully generalise this, the idea is to have a csv as the only file that will always exist, so I guess this is okay?
    # CSV file must always be called '.csv' though

    onyx_config = get_onyx_credentials()

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                # Test create from the metadata CSV
                response = client.csv_create(
                    payload["project"],
                    csv_file=s3_to_fh(
                        payload["files"][".csv"]["uri"],
                        payload["files"][".csv"]["etag"],
                    ),  # I don't like having a hardcoded metadata file name like this but hypothetically we should always have a metadata CSV
                    test=test_submission,
                    fields={
                        "site": payload["site"],
                        "platform": payload["platform"],
                        "is_published": False,
                    },
                    multiline=False,
                )

                if not test_submission:
                    payload["climb_id"] = response["climb_id"]
                    payload["anonymised_run_index"] = response["run_index"]
                    payload["anonymised_run_id"] = response["run_id"]
                    payload["anonymised_biosample_id"] = response["biosample_id"]
                    if response["biosample_source_id"]:
                        payload["anonymised_biosample_source_id"] = response[
                            "biosample_source_id"
                        ]

                return (True, False, payload)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
                    )
                    time.sleep(3)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )
                    if test_submission:
                        payload.setdefault("onyx_test_create_errors", {})
                        payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                        payload["onyx_test_create_errors"]["onyx_errors"].append(
                            f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                        )
                    else:
                        payload.setdefault("onyx_create_errors", {})
                        payload["onyx_create_errors"].setdefault("onyx_errors", [])
                        payload["onyx_create_errors"]["onyx_errors"].append(
                            f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                        )

                    return (False, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled csv_create Onyx error: {e}")
                if test_submission:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(
                        f"Unhandled csv_create Onyx error: {e}"
                    )
                else:
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(
                        f"Unhandled csv_create Onyx error: {e}"
                    )
                    payload["rerun"] = True

                return (False, True, payload)

            except OnyxClientError as e:
                log.info(
                    f"Onyx csv create failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}"
                )

                if test_submission:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(str(e))
                else:
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(str(e))

                return (False, False, payload)

            except OnyxRequestError as e:
                log.info(
                    f"Onyx test csv create failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}"
                )

                if test_submission:
                    # Handle the case where the record already exists but isn't published when field is added to onyx
                    payload.setdefault("onyx_test_create_errors", {})
                    for field, messages in e.response.json()["messages"].items():
                        payload["onyx_test_create_errors"].setdefault(field, [])
                        payload["onyx_test_create_errors"][field].extend(messages)

                    return (False, False, payload)

                else:
                    artifact_published, alert, payload = check_artifact_published(
                        payload=payload, log=log
                    )

                    if alert:
                        return (False, True, payload)

                    if artifact_published:
                        payload.setdefault("onyx_create_errors", {})
                        for field, messages in e.response.json()["messages"].items():
                            payload["onyx_create_errors"].setdefault(field, [])
                            payload["onyx_create_errors"][field].extend(messages)

                        return (False, alert, payload)

                    return (True, alert, payload)

            except EtagMismatchError:
                log.error(
                    f"CSV appears to have been modified after upload for artifact: {payload['artifact']}"
                )

                if test_submission:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(
                        f"CSV appears to have been modified after upload for artifact: {payload['artifact']}"
                    )
                else:
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(
                        f"CSV appears to have been modified after upload for artifact: {payload['artifact']}"
                    )

                return (False, False, payload)

            except Exception as e:
                if test_submission:
                    log.error(f"Unhandled csv_create error: {e}")
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(
                        f"Unhandled csv_create error: {e}"
                    )
                else:
                    log.error(f"Unhandled csv_create error: {e}")
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(
                        f"Unhandled csv_create error: {e}"
                    )

                return (False, True, payload)

        # This should never be reached
        if test_submission:
            payload.setdefault("onyx_test_create_errors", {})
            payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
            payload["onyx_test_create_errors"]["onyx_errors"].append(
                "End of csv_create func reached, this should never happen!"
            )
        else:
            payload.setdefault("onyx_create_errors", {})
            payload["onyx_create_errors"].setdefault("onyx_errors", [])
            payload["onyx_create_errors"]["onyx_errors"].append(
                "End of csv_create func reached, this should never happen!"
            )

        return (False, True, payload)


def csv_field_checks(payload: dict) -> tuple[bool, bool, dict]:
    """Function to check that the required fields are present in the metadata CSV and that they match the filename

    Args:
        payload (dict): Payload dict for the current artifact

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the field checks failed, a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """

    try:
        with s3_to_fh(
            payload["files"][".csv"]["uri"],
            payload["files"][".csv"]["etag"],
        ) as csv_fh:
            reader = csv.DictReader(csv_fh, delimiter=",")

            metadata = next(reader)

            name_matches = {
                x: metadata[x] == payload[x] for x in ("run_index", "run_id")
            }

            for k, v in name_matches.items():
                if not v:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault(k, [])
                    payload["onyx_test_create_errors"][k].append(
                        "Field does not match filename."
                    )

            if not all(name_matches.values()):
                return (False, False, payload)
            else:
                return (True, False, payload)

    except EtagMismatchError:
        payload.setdefault("onyx_test_create_errors", {})
        payload["onyx_test_create_errors"].setdefault("roz_errors", [])
        payload["onyx_test_create_errors"]["roz_errors"].append(
            f"CSV appears to have been modified after upload for artifact: {payload['artifact']}"
        )
        return (False, False, payload)

    except Exception as e:
        payload.setdefault("onyx_test_create_errors", {})
        payload["onyx_test_create_errors"].setdefault("roz_errors", [])
        payload["onyx_test_create_errors"]["roz_errors"].append(
            f"Unhandled csv field check error: {e}"
        )
        return (False, True, payload)


def valid_character_checks(payload: dict) -> tuple[bool, bool, dict]:
    """Function to check that the run_index and run_id contain only valid characters

    Args:
        payload (dict): Payload dict for the current artifact

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the character checks failed, a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """
    pattern = re.compile(r"^[A-Za-z0-9_-]*$")

    run_index_match = pattern.match(payload["run_index"])
    run_id_match = pattern.match(payload["run_id"])

    if not run_index_match:
        payload.setdefault("onyx_test_create_errors", {})
        payload["onyx_test_create_errors"].setdefault("run_index", [])
        payload["onyx_test_create_errors"]["run_index"].append(
            "run_index contains invalid characters, must be alphanumeric and contain only hyphens and underscores"
        )

    if not run_id_match:
        payload.setdefault("onyx_test_create_errors", {})
        payload["onyx_test_create_errors"].setdefault("run_id", [])
        payload["onyx_test_create_errors"]["run_id"].append(
            "run_id contains invalid characters, must be alphanumeric and contain only hyphens and underscores"
        )

    if not run_index_match or not run_id_match:
        return (False, False, payload)

    return (True, False, payload)


def onyx_identify(payload: dict, identity_field: str, log: logging.getLogger):
    if identity_field not in (
        "biosample_id",
        "run_id",
        "run_index",
        "biosample_source_id",
    ):
        log.error(
            f"Invalid identity field: {identity_field}. Must be one of 'biosample_id', 'run_id', 'run_index', or 'biosample_source_id'"
        )
        return (False, True, payload)

    onyx_config = get_onyx_credentials()

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                # Consider making this a bit more versatile (explicitly input the identifier)
                response = client.identify(
                    project=payload["project"],
                    field=identity_field,
                    value=payload[identity_field],
                    site=payload["site"],
                )

                payload[f"anonymised_{identity_field}"] = response["identifier"]

                return (True, False, payload)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
                    )
                    time.sleep(3)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )
                    payload.setdefault("onyx_errors", {})
                    payload["onyx_errors"].setdefault("onyx_errors", [])
                    payload["onyx_errors"]["onyx_errors"].append(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )

                    return (False, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx identify error: {e}")
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(
                    f"Unhandled Onyx identify error: {e}"
                )
                return (False, True, payload)

            except OnyxClientError as e:
                log.error(
                    f"Onyx identify failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(
                    f"Onyx identify failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                return (False, True, payload)

            except OnyxRequestError as e:
                if e.response.status_code == 404:
                    return (False, False, payload)

                log.error(
                    f"Onyx identify failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(
                    f"Onyx identify failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                return (False, True, payload)

            except Exception as e:
                log.error(f"Unhandled onyx_identify error: {e}")
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(
                    f"Unhandled onyx_identify error: {e}"
                )
                return (False, True, payload)


def onyx_reconcile(
    payload: dict, identifier: str, fields_to_reconcile: list, log: logging.getLogger
):
    identify_success, alert, payload = onyx_identify(payload, identifier, log)

    if not identify_success:
        log.info(f"Failed to identify {identifier} for artifact: {payload['artifact']}")
        return (True, alert, payload)

    if alert:
        return (False, True, payload)

    log.info(
        f"Successfully identified {identifier} for artifact: {payload['artifact']}"
    )

    with OnyxClient(config=get_onyx_credentials()) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                response = list(
                    client.filter(
                        payload["project"],
                        fields={identifier: payload[f"anonymised_{identifier}"]},
                    )
                )

                if len(response) == 0:
                    return (False, True, payload)

                fields_of_concern = []

                with s3_to_fh(
                    payload["files"][".csv"]["uri"],
                    payload["files"][".csv"]["etag"],
                ) as csv_fh:
                    reader = csv.DictReader(csv_fh, delimiter=",")

                    metadata = next(reader)

                for field in fields_to_reconcile:
                    to_reconcile = [x[field] for x in response]

                    if metadata.get(field):
                        if metadata[field].startswith("is_"):
                            metadata[field] = str(metadata[field]).lower().strip() in (
                                "t",
                                "y",
                                "yes",
                                "true",
                                "on",
                                "1",
                            )

                        to_reconcile.append(metadata[field])

                    if len(set(to_reconcile)) > 1:
                        fields_of_concern.append(field)

                if fields_of_concern:
                    payload.setdefault("onyx_errors", {})
                    payload["onyx_errors"].setdefault("reconcile_errors", [])
                    payload["onyx_errors"]["reconcile_errors"].append(
                        f"Onyx records for {identifier}: {payload[f'anonymised_{identifier}']} disagree for the following fields: {', '.join(fields_of_concern)}"
                    )
                    return (False, False, payload)

                return (True, False, payload)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
                    )
                    time.sleep(3)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )
                    payload.setdefault("onyx_errors", {})
                    payload["onyx_errors"].setdefault("onyx_errors", [])
                    payload["onyx_errors"]["onyx_errors"].append(str(e))

                    return (False, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                payload.setdefault("onyx_reconcile_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(e)
                return (False, True, payload)

            except OnyxClientError as e:
                log.error(
                    f"Onyx reconcile failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_reconcile_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(str(e))
                return (False, True, payload)

            except EtagMismatchError as e:
                log.error(
                    f"CSV appears to have been modified after upload for artifact: {payload['artifact']}"
                )
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(str(e))
                return (False, False, payload)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx reconcile failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_errors", {})
                for field, messages in e.response.json()["messages"].items():
                    payload["onyx_errors"].setdefault(field, [])
                    payload["onyx_errors"][field].extend(messages)
                return (False, True, payload)

            except Exception as e:
                log.error(f"Unhandled onyx_reconcile error: {e}")
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(
                    f"Unhandled onyx_reconcile error: {e}"
                )
                return (False, True, payload)

    # This should never be reached
    payload.setdefault("onyx_errors", {})
    payload["onyx_errors"].setdefault("reconcile_errors", [])
    payload["onyx_errors"]["reconcile_errors"].append(
        "End of onyx_reconcile func reached, this should never happen!"
    )
    return (False, True, payload)


def ensure_file_unseen(
    etag_field: str, etag: str, log: logging.getLogger, payload: dict
) -> tuple[bool, bool, bool, dict]:
    """Function to check that a file has not already been uploaded to Onyx

    Args:
        etag_field (str): The field in Onyx to check for the etag
        etag (str): The etag to check for
        log (logging.getLogger): Logger object
        payload (dict): Payload dict for the current artifact

    Returns:
        tuple[bool, bool, bool, dict]: Tuple containing a bool indicating whether the check failed, a bool indicating whether the file is unseen or not,  a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """
    onyx_config = get_onyx_credentials()

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                response = list(
                    client.filter(
                        project=payload["project"],
                        fields={f"{etag_field}__iexact": etag, "is_published": True},
                    )
                )

                if len(response) == 0:
                    return (False, True, False, payload)
                else:
                    return (False, False, False, payload)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
                    )
                    time.sleep(3)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )
                    payload.setdefault("onyx_errors", {})
                    payload["onyx_errors"].setdefault("onyx_errors", [])
                    payload["onyx_errors"]["onyx_errors"].append(str(e))

                    return (True, True, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(e)
                return (True, True, True, payload)

            except OnyxClientError as e:
                log.error(
                    f"Onyx filter failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(str(e))
                return (True, True, True, payload)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx filter failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_errors", {})
                for field, messages in e.response.json()["messages"].items():
                    payload["onyx_errors"].setdefault(field, [])
                    payload["onyx_errors"][field].extend(messages)
                return (True, True, True, payload)

            except Exception as e:
                log.error(f"Unhandled check_file_unseen error: {e}")
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(
                    f"Unhandled check_file_unseen error: {e}"
                )
                return (True, True, True, payload)


def check_artifact_published(
    payload: dict, log: logging.getLogger
) -> tuple[bool, bool, dict]:
    run_index_success, run_index_alert, payload = onyx_identify(
        payload=payload, identity_field="run_index", log=log
    )

    if not run_index_success:
        return (False, run_index_alert, payload)

    run_success, run_alert, payload = onyx_identify(
        payload=payload, identity_field="run_id", log=log
    )

    if not run_success:
        return (False, run_alert, payload)

    with OnyxClient(config=get_onyx_credentials()) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                response = list(
                    client.filter(
                        project=payload["project"],
                        fields={
                            "run_index": payload["anonymised_run_index"],
                            "run_id": payload["anonymised_run_id"],
                        },
                    )
                )

                if len(response) == 0:
                    log.error(
                        f"Failed to find records with Onyx for: {payload['artifact']} despite successful identification by Onyx"
                    )
                    payload.setdefault("onyx_errors", {})
                    payload["onyx_errors"].setdefault("onyx_errors", [])
                    payload["onyx_errors"]["onyx_errors"].append(
                        f"Failed to find records with Onyx for: {payload['artifact']} despite successful identification by Onyx"
                    )
                    return (True, True, payload)

                else:
                    if response[0]["is_published"]:
                        return (True, False, payload)

                    payload["climb_id"] = response[0]["climb_id"]
                    return (False, False, payload)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
                    )
                    time.sleep(3)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )
                    payload.setdefault("onyx_errors", {})
                    payload["onyx_errors"].setdefault("onyx_errors", [])
                    payload["onyx_errors"]["onyx_errors"].append(str(e))

                    return (False, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(e)
                return (False, True, payload)

            except OnyxClientError as e:
                log.error(
                    f"Onyx filter failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(str(e))
                return (False, True, payload)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx filter failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_errors", {})
                for field, messages in e.response.json()["messages"].items():
                    payload["onyx_errors"].setdefault(field, [])
                    payload["onyx_errors"][field].extend(messages)
                return (False, True, payload)

            except Exception as e:
                log.error(f"Unhandled check_published error: {e}")
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(
                    f"Unhandled check_published error: {e}"
                )
                return (False, True, payload)


def onyx_update(
    payload: dict, fields: dict, log: logging.getLogger
) -> tuple[bool, bool, dict]:
    """
    Update an existing Onyx record with the given fields

    Args:
        payload (dict): Payload dict for the current artifact
        fields (dict): Fields to update in the format {'field_name': 'field_value'}
        log (logging.getLogger): Logger object

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the update failed, a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """

    onyx_config = get_onyx_credentials()

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                client.update(
                    project=payload["project"],
                    climb_id=payload["climb_id"],
                    fields=fields,
                )

                return (False, False, payload)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 5 seconds"
                    )
                    time.sleep(5)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )

                    payload.setdefault("onyx_errors", {})
                    payload["onyx_errors"].setdefault("onyx_errors", [])
                    payload["onyx_errors"]["onyx_errors"].append(e)

                    return (True, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                payload.setdefault("onyx_update_errors", {})
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(e)

                return (True, True, payload)

            except OnyxClientError as e:
                log.error(
                    f"Onyx update failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_update_errors", {})
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(e)

                return (True, False, payload)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx update failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )

                payload.setdefault("onyx_update_errors", {})
                for field, messages in e.response.json()["messages"].items():
                    payload["onyx_update_errors"].setdefault(field, [])
                    payload["onyx_update_errors"][field].extend(messages)

                return (True, False, payload)

            except Exception as e:
                log.error(f"Unhandled onyx_update error: {e}")
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(
                    f"Unhandled onyx_update error: {e}"
                )

                return (True, True, payload)

    # This should never be reached
    payload.setdefault("onyx_update_errors", {})
    payload["onyx_update_errors"].setdefault("onyx_errors", [])
    payload["onyx_update_errors"]["onyx_errors"].append(
        "End of onyx_update func reached, this should never happen!"
    )
    return (True, True, payload)


def get_onyx_credentials():
    config = OnyxConfig(
        domain=os.environ["ONYX_DOMAIN"],
        token=os.environ["ONYX_TOKEN"],
    )
    return config


def get_s3_credentials(
    args=None,
) -> __s3_creds:
    """
    Get credentials for S3 from a config file, environment variables or command line arguments.

    Args:
        args (argparse.Namespace): Command line arguments

    Returns:
        namedtuple: Named tuple containing the access key, secret key, endpoint, region and profile name
    """

    credential_file = configparser.ConfigParser()

    credentials = {}

    if args:
        profile = "default" if not args.profile else args.profile
    else:
        profile = "default"

    try:
        credential_file.read_file(open(os.path.expanduser("~/.aws/credentials"), "rt"))
        credentials["access_key"] = credential_file[profile]["aws_access_key_id"]
        credentials["secret_key"] = credential_file[profile]["aws_secret_access_key"]
    except FileNotFoundError:
        pass

    if not os.getenv("UNIT_TESTING"):
        endpoint = "https://s3.climb.ac.uk"
    else:
        endpoint = "http://localhost:5000"

    region = "s3"

    if os.getenv("AWS_ACCESS_KEY_ID"):
        credentials["access_key"] = os.getenv("AWS_ACCESS_KEY_ID")

    if os.getenv("AWS_SECRET_ACCESS_KEY"):
        credentials["secret_key"] = os.getenv("AWS_SECRET_ACCESS_KEY")

    if args:
        if args.access_key:
            credentials["access_key"] = args.access_key

        if args.secret_key:
            credentials["secret_key"] = args.secret_key

    # Make this actually work
    if not credentials.get("access_key") or not credentials.get("secret_key"):
        error = """CLIMB S3 credentials could not be found, please provide valid credentials in one of the following ways:
            - In a correctly formatted config file (~/.aws/credentials)
            - As environmental variables 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY'
            - As a command line argument, see --help for more details
        """
        print(error, file=sys.stderr)
        sys.exit(1)

    s3_credentials = __s3_creds(
        access_key=credentials["access_key"],
        secret_key=credentials["secret_key"],
        endpoint=endpoint,
        region=region,
        profile_name=profile,
    )

    return s3_credentials


def s3_to_fh(s3_uri: str, eTag: str) -> StringIO:
    """
    Take file from S3 URI and return a file handle-like object using StringIO
    Requires an S3 URI and an ETag to confirm the file has not been modified since upload.

    Args:
        s3_uri (str): S3 URI of the file to be downloaded
        eTag (str): ETag of the file to be downloaded

    Returns:
        StringIO: File handle-like object of the downloaded file
    """

    s3_credentials = get_s3_credentials()

    bucket = s3_uri.replace("s3://", "").split("/")[0]

    key = s3_uri.replace("s3://", "").split("/", 1)[1]

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        region_name=s3_credentials.region,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    file_obj = s3_client.get_object(Bucket=bucket, Key=key)

    if file_obj["ETag"].replace('"', "") != eTag:
        raise EtagMismatchError(
            "ETag mismatch, CSV appears to have been modified between upload and parsing"
        )

    return StringIO(file_obj["Body"].read().decode("utf-8-sig"))
