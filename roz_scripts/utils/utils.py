import argparse
import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from collections import namedtuple
import configparser
from dataclasses import dataclass
import os
import sys
from io import StringIO
import logging
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

from kubernetes import config as k8s_config
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException
from kubernetes.client.api import BatchV1Api


def get_pod_namespace() -> str:
    sa_mount = Path(
        os.getenv("K8S_SECRETS_MOUNT", "/run/secrets/kubernetes.io/serviceaccount")
    )
    ns_file = sa_mount / "namespace"
    if ns_file.exists():
        return ns_file.read_text().strip()
    ns = os.getenv("POD_NAMESPACE")
    if ns:
        return ns
    raise RuntimeError(
        "Cannot determine k8s namespace: not running in a pod and POD_NAMESPACE is not set"
    )


__s3_creds = namedtuple(
    "s3_credentials",
    ["access_key", "secret_key", "endpoint", "region", "profile_name"],
)


class EtagMismatchError(Exception):
    pass


class NonPlaintextCSVError(Exception):
    pass


def send_admin_alert(
    varys_client, source: str, description: str, uuid: str | None = None
) -> None:
    """Send a stripped, off-prem-safe alert to inform admins that something needs attention.

    Only `source`, `description` and an optional `uuid` are ever included -
    never a full payload dict - since this exchange is consumed outside the
    restricted, per-project Slack channels.

    Args:
        varys_client: The Varys client instance to send the message with
        source (str): Name of the component raising the alert (e.g. "mscape", "s3_matcher")
        description (str): Human-readable description of the alert
        uuid (str | None): Opaque identifier for cross-referencing with the restricted system, if relevant
    """

    message = {"source": source, "description": description}

    if uuid:
        message["uuid"] = uuid

    varys_client.send(
        message=message,
        exchange="remote-announce",
        queue_suffix="alert",
    )


NO_LIMIT = "none"  # sentinel accepted on the CLI to drop a single resource dimension

_CPU_QUANTITY_RE = re.compile(r"^(\d+(?:\.\d+)?)(m)?$")
_MEMORY_QUANTITY_RE = re.compile(
    r"^(\d+(?:\.\d+)?)(E|P|T|G|M|K|Ei|Pi|Ti|Gi|Mi|Ki)?$"
)

_MEMORY_UNIT_MULTIPLIERS = {
    "": 1,
    "K": 1000,
    "M": 1000**2,
    "G": 1000**3,
    "T": 1000**4,
    "P": 1000**5,
    "E": 1000**6,
    "Ki": 1024,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
    "Ei": 1024**6,
}


class PodResourceError(ValueError):
    """Raised when a pod resource quantity or combination of settings is invalid"""


def parse_cpu_quantity(value: str) -> float:
    """Parse a k8s CPU quantity (e.g. "1", "0.5", "500m") into a number of cores

    Args:
        value (str): The k8s CPU quantity to parse

    Returns:
        float: The quantity in whole cores
    """
    match = _CPU_QUANTITY_RE.match(value.strip())
    if not match:
        raise PodResourceError(f"Invalid CPU quantity: {value!r}")

    number, suffix = match.groups()
    cores = float(number) / 1000 if suffix == "m" else float(number)

    if cores <= 0:
        raise PodResourceError(f"CPU quantity must be positive, got: {value!r}")

    return cores


def parse_memory_quantity(value: str) -> float:
    """Parse a k8s memory quantity (e.g. "8G", "512Mi") into a number of bytes

    Args:
        value (str): The k8s memory quantity to parse

    Returns:
        float: The quantity in bytes
    """
    match = _MEMORY_QUANTITY_RE.match(value.strip())
    if not match:
        raise PodResourceError(f"Invalid memory quantity: {value!r}")

    number, suffix = match.groups()
    multiplier = _MEMORY_UNIT_MULTIPLIERS[suffix or ""]
    quantity = float(number) * multiplier

    if quantity <= 0:
        raise PodResourceError(f"Memory quantity must be positive, got: {value!r}")

    return quantity


@dataclass(frozen=True)
class PodResources:
    """CPU/memory/ephemeral-storage requests and limits for the k8s pod that
    runs a nextflow pipeline as a Job (see `pipeline.execute`).

    A limit field left as None mirrors the corresponding request (matching
    k8s's own behaviour of a Guaranteed QoS pod when requests == limits). Set
    a limit field to the NO_LIMIT sentinel ("none") to omit just that
    dimension from the limits block, or set no_limits=True to omit the whole
    limits block and let the pod run Burstable/unbounded on that dimension.
    """

    cpu_request: str = "1"
    memory_request: str = "8G"
    cpu_limit: str | None = None
    memory_limit: str | None = None
    ephemeral_storage_request: str | None = None
    ephemeral_storage_limit: str | None = None
    no_limits: bool = False

    def validate(self) -> None:
        """Check that the requested quantities and combination of settings make sense

        Raises:
            PodResourceError: If a quantity is unparseable, or the settings are contradictory
        """
        request_cpu = parse_cpu_quantity(self.cpu_request)
        request_memory = parse_memory_quantity(self.memory_request)

        if self.ephemeral_storage_request is not None:
            parse_memory_quantity(self.ephemeral_storage_request)

        explicit_limits = {
            "cpu_limit": self.cpu_limit,
            "memory_limit": self.memory_limit,
            "ephemeral_storage_limit": self.ephemeral_storage_limit,
        }

        if self.no_limits:
            contradictions = [
                name
                for name, value in explicit_limits.items()
                if value is not None and value.strip().lower() != NO_LIMIT
            ]
            if contradictions:
                raise PodResourceError(
                    f"no_limits=True but explicit limit(s) were also set: {', '.join(contradictions)}"
                )
            return

        if (
            self.cpu_limit is not None
            and self.cpu_limit.strip().lower() != NO_LIMIT
        ):
            limit_cpu = parse_cpu_quantity(self.cpu_limit)
            if limit_cpu < request_cpu:
                raise PodResourceError(
                    f"cpu_limit ({self.cpu_limit}) is less than cpu_request ({self.cpu_request})"
                )

        if (
            self.memory_limit is not None
            and self.memory_limit.strip().lower() != NO_LIMIT
        ):
            limit_memory = parse_memory_quantity(self.memory_limit)
            if limit_memory < request_memory:
                raise PodResourceError(
                    f"memory_limit ({self.memory_limit}) is less than memory_request ({self.memory_request})"
                )

        if self.ephemeral_storage_limit is not None:
            if self.ephemeral_storage_limit.strip().lower() != NO_LIMIT:
                parse_memory_quantity(self.ephemeral_storage_limit)
            elif self.ephemeral_storage_request is None:
                raise PodResourceError(
                    "ephemeral_storage_limit set to 'none' without ephemeral_storage_request"
                )

    def to_manifest(self) -> dict:
        """Build the k8s "resources" dict for a pod container

        Returns:
            dict: A dict suitable for use as a container's "resources" field.
                The "limits" key is omitted entirely (not emitted as an empty
                dict) whenever no limit applies to any dimension.
        """
        requests = {"cpu": self.cpu_request, "memory": self.memory_request}
        if self.ephemeral_storage_request is not None:
            requests["ephemeral-storage"] = self.ephemeral_storage_request

        manifest = {"requests": requests}

        if self.no_limits:
            return manifest

        limits = {}

        if self.cpu_limit is None:
            limits["cpu"] = self.cpu_request
        elif self.cpu_limit.strip().lower() != NO_LIMIT:
            limits["cpu"] = self.cpu_limit

        if self.memory_limit is None:
            limits["memory"] = self.memory_request
        elif self.memory_limit.strip().lower() != NO_LIMIT:
            limits["memory"] = self.memory_limit

        if self.ephemeral_storage_limit is not None:
            if self.ephemeral_storage_limit.strip().lower() != NO_LIMIT:
                limits["ephemeral-storage"] = self.ephemeral_storage_limit
        elif self.ephemeral_storage_request is not None:
            limits["ephemeral-storage"] = self.ephemeral_storage_request

        if limits:
            manifest["limits"] = limits

        return manifest


def add_nxf_pod_resource_args(parser: argparse.ArgumentParser) -> None:
    """Add CLI flags controlling the nextflow k8s pod's resource requests/limits

    Each flag falls back to a ROZ_NXF_POD_* environment variable, then to a
    built-in default that reproduces the pipeline's historical hardcoded
    1 CPU / 8G resources with mirrored limits (Guaranteed QoS), so existing
    deployments are unaffected until these flags are explicitly set.

    Args:
        parser (argparse.ArgumentParser): The parser to add the arguments to
    """
    parser.add_argument(
        "--nxf_pod_cpu_request",
        default=os.getenv("ROZ_NXF_POD_CPU_REQUEST", "1"),
        help="CPU request for the nextflow k8s pod (default: 1)",
    )
    parser.add_argument(
        "--nxf_pod_memory_request",
        default=os.getenv("ROZ_NXF_POD_MEMORY_REQUEST", "8G"),
        help="Memory request for the nextflow k8s pod (default: 8G)",
    )
    parser.add_argument(
        "--nxf_pod_cpu_limit",
        default=os.getenv("ROZ_NXF_POD_CPU_LIMIT"),
        help="CPU limit for the nextflow k8s pod. Defaults to mirroring the "
        "request. Set to 'none' to omit a CPU limit while keeping other "
        "limits.",
    )
    parser.add_argument(
        "--nxf_pod_memory_limit",
        default=os.getenv("ROZ_NXF_POD_MEMORY_LIMIT"),
        help="Memory limit for the nextflow k8s pod. Defaults to mirroring "
        "the request. Set to 'none' to omit a memory limit while keeping "
        "other limits.",
    )
    parser.add_argument(
        "--nxf_pod_no_limits",
        action="store_true",
        default=_env_flag("ROZ_NXF_POD_NO_LIMITS"),
        help="Omit the whole resources.limits block for the nextflow k8s "
        "pod, leaving only the requests (Burstable QoS, unbounded on this "
        "node). Mutually exclusive with --nxf_pod_cpu_limit / "
        "--nxf_pod_memory_limit.",
    )
    parser.add_argument(
        "--nxf_pod_ephemeral_storage_request",
        default=os.getenv("ROZ_NXF_POD_EPHEMERAL_STORAGE_REQUEST"),
        help="Ephemeral storage request for the nextflow k8s pod. Omitted "
        "by default; the pipeline's own working directories live on a "
        "cephfs PVC, so this only accounts for container-local scratch "
        "space (e.g. /tmp).",
    )
    parser.add_argument(
        "--nxf_pod_ephemeral_storage_limit",
        default=os.getenv("ROZ_NXF_POD_EPHEMERAL_STORAGE_LIMIT"),
        help="Ephemeral storage limit for the nextflow k8s pod. Omitted by "
        "default. Unlike memory, exceeding an ephemeral-storage limit "
        "causes immediate pod eviction with no OOM-kill-and-retry grace.",
    )


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


def pod_resources_from_args(args: argparse.Namespace) -> PodResources:
    """Build and validate a PodResources from parsed CLI args

    Args:
        args (argparse.Namespace): Parsed args, from a parser that was passed
            through add_nxf_pod_resource_args()

    Returns:
        PodResources: The validated pod resource configuration

    Raises:
        PodResourceError: If the resulting configuration is invalid
    """
    pod_resources = PodResources(
        cpu_request=args.nxf_pod_cpu_request,
        memory_request=args.nxf_pod_memory_request,
        cpu_limit=args.nxf_pod_cpu_limit,
        memory_limit=args.nxf_pod_memory_limit,
        ephemeral_storage_request=args.nxf_pod_ephemeral_storage_request,
        ephemeral_storage_limit=args.nxf_pod_ephemeral_storage_limit,
        no_limits=args.nxf_pod_no_limits,
    )
    pod_resources.validate()
    return pod_resources


class pipeline:
    def __init__(
        self,
        pipe: str,
        branch: str,
        config: Path,
        nxf_image: str,
        job_prefix: str,
        profile=None,
        pod_resources: PodResources | None = None,
    ):
        """
        Run a nxf pipeline as a subprocess, this is only advisable for use with cloud executors, specifically k8s.
        If local execution is needed then you should use something else.

        Args:
            pipe (str): The pipeline to run as a github repo in the format 'user/repo'
            config (str): Path to a nextflow config file
            profile (str): The nextflow profile to use
            pod_resources (PodResources | None): CPU/memory/ephemeral-storage
                requests and limits for the k8s pod. Defaults to
                PodResources() (1 CPU / 8G, mirrored limits) if not given.

        """

        self.pipe = pipe
        self.branch = branch
        self.config = Path(config) if config else None
        self.nxf_image = nxf_image
        # self.timeout = timeout
        self.profile = profile
        self.job_prefix = job_prefix
        self.pod_resources = pod_resources or PodResources()
        self.cmd: list = []

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
        workingdir: Path,
        resume: bool = False,
        progress_cb=None,
        pod_resources: PodResources | None = None,
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
            resume (bool): Whether to resume the pipeline
            workingdir (Path): Path to the nextflow work directory
            progress_cb (Callable[[str], None] | None): Called on every poll
                iteration with a short stage description, so a caller can
                prove liveness while this method blocks for a long time
            pod_resources (PodResources | None): Overrides self.pod_resources
                for this call only, if given

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

        if resume:
            cmd.append("-resume")

        if self.config:
            cmd.extend(["-c", str(self.config.resolve())])

        if self.profile:
            cmd.extend(["-profile", self.profile])

        if params:
            for k, v in params.items():
                cmd.extend([f"--{k}", v])

        cmd_str = " ".join(str(x) for x in cmd)

        pod_env_vars = [{"name": k, "value": v} for k, v in env_vars.items()]

        job_name = f"roz-{self.job_prefix}-{job_id}"
        backoff_limit = 5

        job_manifest = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name},
            "spec": {
                "ttlSecondsAfterFinished": 120,
                "backoffLimit": backoff_limit,
                "template": {
                    "spec": {
                        "hostname": job_name,
                        "subdomain": namespace,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 1000,
                            "runAsGroup": 100,
                            "fsGroup": 100,
                        },
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
                        "nodeSelector": {
                            "hub.jupyter.org/node-purpose": "user-compute"
                        },
                        "containers": [
                            {
                                "name": job_name,
                                "image": str(self.nxf_image),
                                "resources": (
                                    pod_resources or self.pod_resources
                                ).to_manifest(),
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
                                "workingDir": str(workingdir),
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

        # (connect_timeout, read_timeout) for every k8s API call below, so a
        # dropped connection to the API server can't block this method forever.
        k8s_request_timeout = (10, 30)
        # Upper bound on how long to wait for a job deletion to be confirmed
        # before giving up - deletion is expected to be fast, so this is not
        # tied to the caller-supplied pipeline `timeout`.
        delete_confirm_timeout = 300

        try:
            self.cmd = cmd
            os.chdir(logdir)

            k8s_config.load_incluster_config()
            api_instance = BatchV1Api(ApiClient())

            try:
                resp = api_instance.read_namespaced_job_status(
                    name=job_name,
                    namespace=namespace,
                    _request_timeout=k8s_request_timeout,
                )

            except ApiException as e:
                if e.status != 404:
                    raise
                resp = None
                api_instance.create_namespaced_job(
                    body=job_manifest,
                    namespace=namespace,
                    _request_timeout=k8s_request_timeout,
                )

            if resp and resp.status.failed and resp.status.failed >= backoff_limit:  # type: ignore
                # A job with this name already reached a terminal failure in a
                # previous invocation (e.g. the worker process was restarted and
                # is reprocessing the same message) - delete it and start a fresh
                # attempt rather than immediately reporting the old failure.
                api_instance.delete_namespaced_job(
                    name=job_name,
                    namespace=namespace,
                    propagation_policy="Foreground",
                    _request_timeout=k8s_request_timeout,
                )

                delete_confirm_start = time.time()
                while True:
                    try:
                        api_instance.read_namespaced_job_status(
                            name=job_name,
                            namespace=namespace,
                            _request_timeout=k8s_request_timeout,
                        )
                    except ApiException as e:
                        if e.status != 404:
                            raise
                        break
                    if time.time() - delete_confirm_start > delete_confirm_timeout:
                        raise TimeoutError(
                            f"Timed out waiting for job {job_name} to be deleted"
                        )
                    if progress_cb:
                        progress_cb("awaiting_job_deletion")
                    time.sleep(random.uniform(2.0, 3.0))

                api_instance.create_namespaced_job(
                    body=job_manifest,
                    namespace=namespace,
                    _request_timeout=k8s_request_timeout,
                )

            job_loop_start = time.time()
            job_completed = False
            while not job_completed:
                resp = api_instance.read_namespaced_job_status(
                    name=job_name,
                    namespace=namespace,
                    _request_timeout=k8s_request_timeout,
                )
                if resp.status.succeeded:  # type: ignore
                    if resp.status.succeeded >= 1:  # type: ignore
                        returncode = 0
                        job_completed = True
                        break

                if resp.status.failed:  # type: ignore
                    if resp.status.failed >= backoff_limit:  # type: ignore
                        api_instance.delete_namespaced_job(
                            name=job_name,
                            namespace=namespace,
                            propagation_policy="Foreground",
                            _request_timeout=k8s_request_timeout,
                        )
                        returncode = 1
                        job_completed = True
                        break

                # Use the job's reported start_time where available, but fall
                # back to wall-clock time since we started polling - if the pod
                # never gets scheduled, start_time stays None forever and the
                # loop would otherwise never hit the timeout.
                if resp.status.start_time:  # type: ignore
                    job_age = time.time() - resp.status.start_time.timestamp()  # type: ignore
                else:
                    job_age = time.time() - job_loop_start

                if job_age > timeout:
                    api_instance.delete_namespaced_job(
                        name=job_name,
                        namespace=namespace,
                        propagation_policy="Foreground",
                        _request_timeout=k8s_request_timeout,
                    )
                    returncode = 124
                    job_completed = True
                    break

                if progress_cb:
                    progress_cb("awaiting_job_completion")
                time.sleep(random.uniform(2.0, 3.0))

        except Exception as e:
            # proc = SimpleNamespace(returncode=1, stdout=str(k8s_exception), stderr="")
            # print(f"Failed to execute pipeline due to exception: {e}")
            with open(stderr_path, "w") as stderr_fh:
                stderr_fh.write(f"Failed to execute pipeline due to exception: {e}")
            returncode = 1

        return returncode  # type: ignore


def init_logger(name, log_path, log_level):
    log = logging.getLogger(name)
    log.propagate = False
    log.setLevel(log_level)
    if not (log.hasHandlers()):
        logging_fh = logging.FileHandler(log_path, mode="a")
        logging_fh.setFormatter(
            logging.Formatter("%(name)s\t::%(levelname)s::%(asctime)s::\t%(message)s")
        )
        log.addHandler(logging_fh)
    return log


def put_result_json(payload: dict, log: logging.Logger):
    """Send the result payload to S3

    Args:
        payload (dict): The payload to send to S3
        log (logging.Logger): Logger object
    """

    s3_credentials = get_s3_credentials()

    s3_client = get_s3_client(s3_credentials)

    try:
        s3_client.put_object(
            Bucket=f"{payload['project']}-{payload['raw_site']}-results",
            Key=f"{payload['project']}.{payload['run_index']}.{payload['run_id']}.result.json",
            Body=json.dumps(payload),
        )

        log.info(
            f"Successfully uploaded result JSON for artifact: {payload['artifact']} to S3"
        )

    except ClientError as e:
        log.error(f"Failed to upload result JSON to S3: {e}")
        raise e


def put_linkage_json(payload: dict, log: logging.Logger):
    """Send the linkage payload to S3

    Args:
        payload (dict): The payload dict to create the linkage dict from
        log (logging.Logger): Logger object
    """

    s3_credentials = get_s3_credentials()

    s3_client = get_s3_client(s3_credentials)

    linkage_dict = {
        "publish_timestamp": time.time_ns(),
        "artifact": payload["artifact"],
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
        log.info(
            f"Successfully uploaded linkage JSON for artifact: {payload['artifact']} to S3"
        )

    except ClientError as e:
        log.error(f"Failed to upload result JSON to S3: {e}")
        raise e


def are_files_empty(*s3_uris: str) -> bool:
    """Check if the files at the given S3 URIs are empty

    Returns:
        bool: True if any files are empty or nonexistant, False otherwise
    """

    s3_credentials = get_s3_credentials()

    s3_client = get_s3_client(s3_credentials)

    try:
        for s3_uri in s3_uris:
            bucket, key = s3_uri.split("/", 3)[2:]
            obj = s3_client.head_object(Bucket=bucket, Key=key)
            if obj["ContentLength"] == 0:
                return True

    except ClientError:

        return True

    return False


def do_uris_exist(*s3_uris: str) -> bool:
    """Check if the files at the given S3 URIs exist

    Returns:
        bool: True if any files are non-existent, False otherwise
    """

    s3_credentials = get_s3_credentials()

    s3_client = get_s3_client(s3_credentials)

    try:
        for s3_uri in s3_uris:
            bucket, key = s3_uri.split("/", 3)[2:]
            s3_client.head_object(Bucket=bucket, Key=key)

    except ClientError:

        return False

    return True


def csv_create(
    payload: dict,
    log: logging.Logger,
    test_submission: bool = False,
) -> tuple[bool, bool, dict]:
    """Function to create a new record in onyx from a metadata CSV file, can be used for testing or for real submissions

    Args:
        payload (dict): Payload dict for the current artifact
        log (logging.Logger): Logger object
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
                    # multiline=False guarantees a single dict response, but
                    # onyx's declared return type is Dict | List[Dict]
                    payload["climb_id"] = response["climb_id"]  # type: ignore
                    payload["anonymised_run_index"] = response["run_index"]  # type: ignore
                    payload["anonymised_run_id"] = response["run_id"]  # type: ignore
                    payload["anonymised_biosample_id"] = response["biosample_id"]  # type: ignore
                    if response["biosample_source_id"]:  # type: ignore
                        payload["anonymised_biosample_source_id"] = response[  # type: ignore
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

            except OnyxServerError as e:
                log.error(f"Internal csv_create Onyx error: {e}")
                if test_submission:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(
                        f"Internal Onyx Server error during csv_create: {e}"
                    )
                else:
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(
                        f"Unhandled csv_create Onyx error: {e}"
                    )
                    payload["rerun"] = True

                return (False, False, payload)

            except OnyxConfigError as e:
                log.error(f"Local Onyx config error: {e}")
                if test_submission:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(
                        f"Local Onyx configuration error during csv_create: {e}"
                    )
                else:
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(
                        f"Local Onyx configuration error during csv_create: {e}"
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
                    f"Onyx csv create failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}"
                )

                error_messages = e.response.json()["messages"]

                if error_messages.get("non_field_errors"):
                    if (
                        "This combination of run_index, run_id already exists."
                        in error_messages["non_field_errors"]
                    ):

                        artifact_published, alert, payload = check_artifact_published(
                            payload=payload, log=log
                        )

                        if not artifact_published:
                            return (True, alert, payload)

                if test_submission:
                    payload.setdefault("onyx_test_create_errors", {})
                    for field, messages in e.response.json()["messages"].items():
                        payload["onyx_test_create_errors"].setdefault(field, [])
                        payload["onyx_test_create_errors"][field].extend(messages)

                    return (False, False, payload)

                else:
                    payload.setdefault("onyx_create_errors", {})
                    for field, messages in e.response.json()["messages"].items():
                        payload["onyx_create_errors"].setdefault(field, [])
                        payload["onyx_create_errors"][field].extend(messages)

                    return (False, False, payload)

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

            except NonPlaintextCSVError as e:
                log.info(
                    f"Non-plaintext CSV submitted for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
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

    except NonPlaintextCSVError as e:
        payload.setdefault("onyx_test_create_errors", {})
        payload["onyx_test_create_errors"].setdefault("roz_errors", [])
        payload["onyx_test_create_errors"]["roz_errors"].append(str(e))
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


def onyx_identify(payload: dict, identity_field: str, log: logging.Logger):
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

    # This should never be reached
    payload.setdefault("onyx_errors", {})
    payload["onyx_errors"].setdefault("onyx_errors", [])
    payload["onyx_errors"]["onyx_errors"].append(
        "End of onyx_identify func reached, this should never happen!"
    )
    return (False, True, payload)


def onyx_reconcile(
    payload: dict, identifier: str, fields_to_reconcile: list, log: logging.Logger
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
                    payload.setdefault("onyx_warnings", {})
                    payload["onyx_warnings"].setdefault("reconcile_errors", [])
                    payload["onyx_warnings"]["reconcile_errors"].append(
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
    etag_field: str, etag: str, log: logging.Logger, payload: dict
) -> tuple[bool, bool, bool, dict]:
    """Function to check that a file has not already been uploaded to Onyx

    Args:
        etag_field (str): The field in Onyx to check for the etag
        etag (str): The etag to check for
        log (logging.Logger): Logger object
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
                if e.response.status_code == 404:
                    # 404 means there's nothing, that's fine!
                    return (False, True, False, payload)

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

    # This should never be reached
    payload.setdefault("onyx_errors", {})
    payload["onyx_errors"].setdefault("onyx_errors", [])
    payload["onyx_errors"]["onyx_errors"].append(
        "End of ensure_file_unseen func reached, this should never happen!"
    )
    return (True, True, True, payload)


def check_artifact_published(
    payload: dict, log: logging.Logger
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

    # This should never be reached
    payload.setdefault("onyx_errors", {})
    payload["onyx_errors"].setdefault("onyx_errors", [])
    payload["onyx_errors"]["onyx_errors"].append(
        "End of check_artifact_published func reached, this should never happen!"
    )
    return (False, True, payload)


def onyx_update(
    payload: dict,
    fields: dict | None,
    log: logging.Logger,
    clear_fields: list | None = None,
) -> tuple[bool, bool, dict]:
    """
    Update an existing Onyx record with the given fields

    Args:
        payload (dict): Payload dict for the current artifact
        fields (dict | None): Fields to update in the format {'field_name': 'field_value'}
        log (logging.Logger): Logger object
        clear_fields (list | None): Fields to clear in the format ['field_name']

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the update failed, a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """

    onyx_config = get_onyx_credentials()

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                if fields:
                    client.update(
                        project=payload["project"],
                        climb_id=payload["climb_id"],
                        fields=fields,
                        clear=clear_fields,
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
                    f"Onyx update failed for artifact: {payload.get('artifact', 'NA')}, UUID: {payload.get('uuid') or payload.get('match_uuid', 'NA')}. Error: {e}"
                )
                payload.setdefault("onyx_update_errors", {})
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(e)

                return (True, False, payload)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx update failed for artifact: {payload.get('artifact', 'NA')}, UUID: {payload.get('uuid') or payload.get('match_uuid', 'NA')}. Error: {e}"
                )

                payload.setdefault("onyx_update_errors", {})
                for field, messages in e.response.json()["messages"].items():
                    payload["onyx_update_errors"].setdefault(field, [])
                    payload["onyx_update_errors"][field].extend(messages)

                return (True, False, payload)

            except Exception as e:
                log.error(f"Unhandled onyx_update error: {e}")
                payload.setdefault("onyx_update_errors", {})
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


S3_CLIENT_CONFIG = Config(
    connect_timeout=10,
    read_timeout=60,
    retries={"max_attempts": 3, "mode": "standard"},
)


def get_s3_client(s3_credentials: __s3_creds) -> BaseClient:
    """
    Build an S3 client with bounded connect/read timeouts and retries, so a
    stalled connection to the S3 endpoint cannot block a caller indefinitely.

    Args:
        s3_credentials (__s3_creds): Credentials as returned by get_s3_credentials()

    Returns:
        BaseClient: Configured S3 client
    """

    return boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        region_name=s3_credentials.region,
        aws_secret_access_key=s3_credentials.secret_key,
        config=S3_CLIENT_CONFIG,
    )


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

    s3_client = get_s3_client(s3_credentials)

    file_obj = s3_client.get_object(Bucket=bucket, Key=key)

    if file_obj["ETag"].replace('"', "") != eTag:
        raise EtagMismatchError(
            "ETag mismatch, CSV appears to have been modified between upload and parsing"
        )

    raw_bytes = file_obj["Body"].read()

    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise NonPlaintextCSVError(
            f"CSV file at {s3_uri} is not valid UTF-8 plaintext: {e}"
        )

    if "\x00" in text:
        raise NonPlaintextCSVError(
            f"CSV file at {s3_uri} contains NUL characters, it is likely not plaintext (e.g. UTF-16 or binary content)"
        )

    return StringIO(text)
