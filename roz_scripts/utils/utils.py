import argparse
import boto3
from boto3.s3.transfer import TransferConfig
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
import shutil
import threading
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

from varys import VarysPublishError

from roz_scripts.utils.config import site_bucket

from kubernetes import config as k8s_config
from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException
from kubernetes.client.api import BatchV1Api
from kubernetes.client.api import CoreV1Api


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


# Alert rate-limit priority, mirrored in slack_integrations/remote_alerts.py
# (that consumer cannot import from roz_scripts - it is deliberately kept
# dependency-free since it runs off-prem - so the values are duplicated
# rather than shared; keep both definitions in sync).
PRIORITY_ROUTINE = "routine"
PRIORITY_CRITICAL = "critical"


def send_admin_alert(
    varys_client,
    source: str,
    description: str,
    uuid: str | None = None,
    priority: str = PRIORITY_ROUTINE,
) -> None:
    """Send a stripped, off-prem-safe alert to inform admins that something needs attention.

    Only `source`, `description`, an optional `uuid`, and an optional
    `priority` are ever included - never a full payload dict - since this
    exchange is consumed outside the restricted, per-project Slack channels.

    Args:
        varys_client: The Varys client instance to send the message with
        source (str): Name of the component raising the alert (e.g. "mscape", "s3_matcher")
        description (str): Human-readable description of the alert
        uuid (str | None): Opaque identifier for cross-referencing with the restricted system, if relevant
        priority (str): PRIORITY_ROUTINE (default) or PRIORITY_CRITICAL. A
            critical alert bypasses the consumer's rate limit entirely and
            never updates its per-source cooldown, so it must be reserved for
            terminal, effectively-once-per-record events (a deadletter, or
            the first alert ever raised for a given record) - never for
            something that can repeat quickly, or it defeats the limiter.
            This is a control signal only: the consumer never renders its
            value into the alert text.
    """

    message = {"source": source, "description": description}

    if uuid:
        message["uuid"] = uuid

    if priority == PRIORITY_CRITICAL:
        # Assign the constant, not the caller's argument, so this can never
        # become a channel for a caller to smuggle arbitrary content through.
        message["priority"] = PRIORITY_CRITICAL

    varys_client.send(
        message=message,
        exchange="remote-announce",
        queue_suffix="alert",
    )


# How long to pause after a failed publish before returning to the receive loop,
# so a broken broker does not spin the loop at full tilt
PUBLISH_FAILURE_BACKOFF_S = 10


def persist_publish_ack(
    varys_client,
    message,
    log,
    sends,
    persists=(),
    source="",
    uuid=None,
    heartbeat=None,
) -> bool:
    """Persist side effects, publish downstream messages, then acknowledge - in that order.

    This is the only safe order. Acknowledging first (as roz did before varys 1.3.0)
    drops the artifact entirely if the publish then fails, because the broker has
    already been told the message is handled. Since 1.3.0, `varys_client.send`
    blocks until the broker confirms and raises otherwise, so reaching the ack here
    is a real guarantee that the downstream message is safe.

    On failure nothing is acknowledged: the delivery is nacked with requeue, so it
    comes back round. That accepts at-least-once delivery, whose duplicates
    `csv_create`'s "already exists" path already absorbs.

    Args:
        varys_client: The Varys client instance holding the inbound and outbound channels
        message: The inbound varys message to acknowledge or requeue
        log (logging.Logger): Logger object
        sends (iterable[dict]): kwargs for each `varys_client.send` call, published in
            the order given - callers order them cheapest/most-idempotent first and the
            expensive trigger last, so a partial failure re-runs the cheap work, and
            this function must not reorder them
        persists (iterable[callable]): zero-argument callables run before any publish
            (e.g. `functools.partial(put_result_json, payload=..., log=..., config=...)`).
            These write to deterministic S3 keys, so re-running one on redelivery is a
            harmless overwrite
        source (str): Component name for any admin alert raised from here
        uuid (str | None): Artifact UUID for cross-referencing an alert, if known
        heartbeat (callable | None): Zero-argument callable invoked around each persist
            and publish. Publishing can now block for up to ~110s per send while the
            broker is unreachable, which would otherwise count as no forward progress;
            pass `health.heartbeat` or a `JobHeartbeat.beat` partial so a slow publish
            cannot be mistaken for a wedged process

    Returns:
        bool: True if everything was persisted, published and acknowledged; False if the
        delivery was requeued instead, in which case the caller should move on to the
        next message rather than treating it as fatal
    """

    def _beat():
        if heartbeat is None:
            return
        try:
            heartbeat()
        except Exception:
            # A failed heartbeat is not worth losing the message over
            log.exception("Failed to record a heartbeat, continuing anyway")

    try:
        for persist in persists:
            _beat()
            persist()

        for send in sends:
            _beat()
            varys_client.send(**send)

    except (VarysPublishError, ClientError) as e:
        log.error(f"Failed to persist or publish, requeueing message: {e}")

        # The alert publishes through the same broker, so when the broker is the thing
        # that is broken this fails too. Log it and carry on; never fatal (Q10)
        try:
            send_admin_alert(
                varys_client,
                source=source,
                description=f"Failed to persist or publish, message requeued: {e}",
                uuid=uuid,
            )
        except Exception:
            log.exception("Failed to send admin alert about the publish failure")

        # The nack needs the same broker as well. It is best-effort because it does not
        # have to succeed: losing the connection makes RabbitMQ requeue every unacked
        # delivery anyway, which is what makes ack-after-publish safe by construction
        try:
            varys_client.nack_message(message, requeue=True)
        except Exception:
            log.exception(
                "Failed to nack the message; connection loss will requeue it anyway"
            )

        _beat()
        time.sleep(PUBLISH_FAILURE_BACKOFF_S)
        _beat()

        return False

    try:
        varys_client.acknowledge_message(message)
    except Exception:
        # The work is committed at this point, so a failed ack is not a failure of this
        # call - the broker will simply redeliver, and the redelivery deduplicates
        log.exception(
            "Failed to acknowledge the message after publishing; it may be redelivered"
        )

    return True


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


# Upper bound on how long to wait for the deletion of a *pre-existing* failed
# job to be confirmed before we recreate it. This one happens at the very start
# of a stage, with the caller's full heartbeat budget ahead of it, so it is not
# tied to the pipeline `timeout`.
JOB_DELETE_CONFIRM_TIMEOUT = int(os.getenv("ROZ_JOB_DELETE_CONFIRM_TIMEOUT", "300"))

# Bounds on how long the post-failure cleanup will wait for the job to actually
# disappear. The wait itself is sized as JOB_CLEANUP_CONFIRM_FRACTION * the
# pipeline timeout, clamped to [FLOOR, CAP].
#
# The fraction matters: this wait happens *after* the pipeline stage has already
# consumed its whole heartbeat budget, and health.DEADLINE_MULTIPLIER (1.1) only
# leaves 0.1 * budget of headroom before the liveness probe starts failing the
# worker. Staying well under that tenth is what stops a cleanup wait from
# getting the worker liveness-killed (which surfaces as exit 137, and is easily
# mistaken for an OOM). Keep FRACTION < 0.1 if DEADLINE_MULTIPLIER changes.
JOB_CLEANUP_CONFIRM_FRACTION = float(
    os.getenv("ROZ_JOB_CLEANUP_CONFIRM_FRACTION", "0.08")
)
JOB_CLEANUP_CONFIRM_CAP = int(os.getenv("ROZ_JOB_CLEANUP_CONFIRM_CAP", "180"))
JOB_CLEANUP_CONFIRM_FLOOR = int(os.getenv("ROZ_JOB_CLEANUP_CONFIRM_FLOOR", "30"))

# How many *consecutive* failures of the job-status poll to absorb before
# concluding the run has failed. Since a concluded failure now tears the job
# down, a single transient k8s API blip must not be allowed to kill a healthy
# in-flight run (a pathsafe assembly can be a 16-hour job).
JOB_POLL_MAX_CONSECUTIVE_ERRORS = int(
    os.getenv("ROZ_JOB_POLL_MAX_CONSECUTIVE_ERRORS", "5")
)

# Returned instead of 1 whenever a failure cannot be attributed to the
# pipeline itself - the job vanished before we could read its outcome, or an
# infrastructure/client error meant it may never have started. Keeping this
# distinct from a genuine job failure (rc 1) matters to callers that count
# pipeline failures: an unattributable failure must not be counted as one.
RC_INFRASTRUCTURE = 125


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
        backoff_limit = 0

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
                        # Give nextflow's shutdown hook time to close its
                        # LevelDB resume cache cleanly when the pod is
                        # deleted on timeout/backoff, rather than being
                        # SIGKILLed mid-write and corrupting it (see
                        # _clean_corrupt_cache below).
                        "terminationGracePeriodSeconds": 120,
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

        # How long the post-failure cleanup may spend waiting for the job to
        # actually be gone. See JOB_CLEANUP_CONFIRM_FRACTION for why this is a
        # fraction of the pipeline timeout rather than a flat value.
        cleanup_confirm_timeout = min(
            JOB_CLEANUP_CONFIRM_CAP,
            max(JOB_CLEANUP_CONFIRM_FLOOR, int(JOB_CLEANUP_CONFIRM_FRACTION * timeout)),
        )

        # Set as soon as we have a usable client - the finally block below has
        # to cope with load_incluster_config()/BatchV1Api() themselves raising.
        api_instance = None
        # None means "no outcome decided yet". Keeping this distinct from 1 is
        # what stops the catch-all overwriting a timeout's rc 124 (which callers
        # alert on separately) when it is the *cleanup* that raised.
        returncode = None
        # Whether this run ended in a state that leaves a job needing teardown.
        cleanup_required = False
        termination_logged = False

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

            if resp and (
                (resp.status.failed and resp.status.failed >= backoff_limit)  # type: ignore
                or resp.status.succeeded  # type: ignore
            ):
                # A job with this name already reached a terminal state -
                # failed or succeeded - in a previous invocation (e.g. the
                # worker process was restarted, or the message was redelivered
                # while a rerun's failure was still being written up, and is
                # reprocessing the same message). Delete it and start a fresh
                # attempt rather than attaching to (or silently re-reporting
                # the outcome of) the old one.
                self._delete_job(
                    api_instance, job_name, namespace, k8s_request_timeout
                )
                # Recreating over a job that is still terminating would collide
                # on the name, so a failure to confirm here stays fatal - but it
                # now also trips cleanup via the finally block below.
                self._await_job_deleted(
                    api_instance,
                    job_name,
                    namespace,
                    k8s_request_timeout,
                    JOB_DELETE_CONFIRM_TIMEOUT,
                    progress_cb=progress_cb,
                )

                api_instance.create_namespaced_job(
                    body=job_manifest,
                    namespace=namespace,
                    _request_timeout=k8s_request_timeout,
                )

            job_loop_start = time.time()
            job_completed = False
            while not job_completed:
                try:
                    resp = self._poll_job_status(
                        api_instance,
                        job_name,
                        namespace,
                        k8s_request_timeout,
                        progress_cb=progress_cb,
                    )
                except ApiException as e:
                    if e.status != 404:
                        raise
                    # The job vanished from under us - reaped by
                    # ttlSecondsAfterFinished, or deleted by something outside
                    # roz. There is no status left to interpret and nothing left
                    # to tear down, so report a generic failure without asking
                    # for a cleanup of something that is already gone.
                    self._append_stderr(
                        stderr_path,
                        f"Job {job_name} disappeared while being polled - "
                        "treating as unattributable",
                    )
                    returncode = RC_INFRASTRUCTURE
                    break

                if resp.status.succeeded:  # type: ignore
                    if resp.status.succeeded >= 1:  # type: ignore
                        returncode = 0
                        job_completed = True
                        break

                if resp.status.failed:  # type: ignore
                    if resp.status.failed >= backoff_limit:  # type: ignore
                        self._log_pod_termination(
                            job_name, namespace, k8s_request_timeout, stderr_path
                        )
                        termination_logged = True
                        # Decide the outcome *before* anything that can raise,
                        # so a cleanup failure can't rewrite why we failed.
                        returncode = 1
                        cleanup_required = True
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
                    self._log_pod_termination(
                        job_name, namespace, k8s_request_timeout, stderr_path
                    )
                    termination_logged = True
                    returncode = 124
                    cleanup_required = True
                    job_completed = True
                    break

                if progress_cb:
                    progress_cb("awaiting_job_completion")
                time.sleep(random.uniform(2.0, 3.0))

        except Exception as e:
            self._append_stderr(
                stderr_path, f"Failed to execute pipeline due to exception: {e}"
            )
            if returncode is None:
                returncode = RC_INFRASTRUCTURE
            # Any failure that isn't one of the two decided outcomes above -
            # a k8s API error, a progress_cb blowing up, a delete that wouldn't
            # confirm - leaves a job behind whose pod is very likely still
            # running and still writing the work directory. Tear it down.
            cleanup_required = True

        finally:
            if cleanup_required and api_instance is not None:
                if not termination_logged:
                    # Best-effort: on the exception path the pod is usually
                    # still running, so there will often be no terminated state
                    # to record. Absence of a block here no longer means we
                    # failed to look.
                    self._log_pod_termination(
                        job_name, namespace, k8s_request_timeout, stderr_path
                    )
                self._best_effort_cleanup(
                    api_instance,
                    job_name,
                    namespace,
                    k8s_request_timeout,
                    cleanup_confirm_timeout,
                    stderr_path,
                    progress_cb=progress_cb,
                )

        if returncode is None:
            returncode = RC_INFRASTRUCTURE

        if returncode != 0:
            # Deliberately outside the finally: this has to run after the
            # deletion has been confirmed, or a pod still inside its
            # terminationGracePeriod can re-dirty .nextflow behind the rmtree.
            self._clean_corrupt_cache(logdir, stdout_path, stderr_path)

        return returncode

    @staticmethod
    def _append_stderr(stderr_path: str, message: str) -> None:
        """Append a line to the job's captured stderr, never raising

        Always appends: the catch-all in `execute` used to open this with mode
        "w", which truncated whatever `_log_pod_termination` had just written
        and so destroyed the record of *why* the pod died.

        Args:
            stderr_path (str): Path to the job's captured stderr
            message (str): The message to append
        """
        try:
            with open(stderr_path, "a") as stderr_fh:
                stderr_fh.write(f"{message}\n")
        except OSError:
            pass

    @staticmethod
    def _poll_job_status(
        api_instance: BatchV1Api,
        job_name: str,
        namespace: str,
        k8s_request_timeout: tuple,
        progress_cb=None,
        max_consecutive_errors: int | None = None,
    ):
        """Read a job's status, absorbing a run of transient API failures

        Concluding that a run has failed now tears the job down, so a single
        blip talking to the API server must not be allowed to kill a healthy
        in-flight pipeline (a pathsafe assembly can run for the better part of
        a day). Only once `max_consecutive_errors` reads in a row have failed
        do we let the exception out and treat the run as failed.

        A 404 is never retried - it is a definitive answer that the job is gone,
        and the caller handles it separately.

        Args:
            api_instance (BatchV1Api): The batch API client
            job_name (str): Name of the job to read
            namespace (str): Namespace the job lives in
            k8s_request_timeout (tuple): (connect, read) timeout per API call
            progress_cb (Callable[[str], None] | None): Called between retries
                so the caller's liveness probe can see we're still working
            max_consecutive_errors (int | None): Overrides
                JOB_POLL_MAX_CONSECUTIVE_ERRORS for this call

        Returns:
            The job status response

        Raises:
            ApiException: On a 404, or once the error run is exhausted
            Exception: Whatever the last read raised, once the run is exhausted
        """
        if max_consecutive_errors is None:
            max_consecutive_errors = JOB_POLL_MAX_CONSECUTIVE_ERRORS

        attempt = 0
        while True:
            try:
                return api_instance.read_namespaced_job_status(
                    name=job_name,
                    namespace=namespace,
                    _request_timeout=k8s_request_timeout,
                )
            except Exception as e:
                if isinstance(e, ApiException) and e.status == 404:
                    raise
                attempt += 1
                if attempt >= max_consecutive_errors:
                    raise
                if progress_cb:
                    progress_cb("retrying_job_status_poll")
                time.sleep(random.uniform(2.0, 3.0))

    @staticmethod
    def _delete_job(
        api_instance: BatchV1Api,
        job_name: str,
        namespace: str,
        k8s_request_timeout: tuple,
    ) -> None:
        """Delete a job with Foreground propagation, so its pod goes with it

        A 404 means the job is already gone, which is the state we wanted, so
        it is treated as success - job names are deterministic and retries
        target the same name, so this has to be idempotent.

        Args:
            api_instance (BatchV1Api): The batch API client
            job_name (str): Name of the job to delete
            namespace (str): Namespace the job lives in
            k8s_request_timeout (tuple): (connect, read) timeout per API call
        """
        try:
            api_instance.delete_namespaced_job(
                name=job_name,
                namespace=namespace,
                propagation_policy="Foreground",
                _request_timeout=k8s_request_timeout,
            )
        except ApiException as e:
            if e.status != 404:
                raise

    @staticmethod
    def _await_job_deleted(
        api_instance: BatchV1Api,
        job_name: str,
        namespace: str,
        k8s_request_timeout: tuple,
        confirm_timeout: float,
        progress_cb=None,
        stage: str = "awaiting_job_deletion",
    ) -> None:
        """Block until a deleted job is actually gone

        Foreground deletion returns immediately and the job object lingers,
        held by its foregroundDeletion finalizer, until the pod is really gone
        - and the pod gets terminationGracePeriodSeconds to exit. Callers retry
        the same job id with `-resume` against the same work directory, so
        returning before the old pod has stopped is what puts two nextflow
        processes on one LevelDB cache.

        Args:
            api_instance (BatchV1Api): The batch API client
            job_name (str): Name of the job being deleted
            namespace (str): Namespace the job lives in
            k8s_request_timeout (tuple): (connect, read) timeout per API call
            confirm_timeout (float): Give up after this many seconds
            progress_cb (Callable[[str], None] | None): Called on every poll,
                so a long wait can't get the worker liveness-killed
            stage (str): Stage name reported to progress_cb

        Raises:
            TimeoutError: If the job is still there after confirm_timeout
        """
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
                return
            if time.time() - delete_confirm_start > confirm_timeout:
                raise TimeoutError(f"Timed out waiting for job {job_name} to be deleted")
            if progress_cb:
                progress_cb(stage)
            time.sleep(random.uniform(2.0, 3.0))

    @classmethod
    def _best_effort_cleanup(
        cls,
        api_instance: BatchV1Api,
        job_name: str,
        namespace: str,
        k8s_request_timeout: tuple,
        confirm_timeout: float,
        stderr_path: str,
        progress_cb=None,
    ) -> None:
        """Tear down a failed job, never raising and never changing the outcome

        This runs from `execute`'s finally block on every route that ends in
        failure, so it must not be able to turn a timeout (rc 124, which
        callers alert on separately) into a generic failure. Anything that goes
        wrong is recorded in the job's stderr and otherwise swallowed.

        Args:
            api_instance (BatchV1Api): The batch API client
            job_name (str): Name of the job to tear down
            namespace (str): Namespace the job lives in
            k8s_request_timeout (tuple): (connect, read) timeout per API call
            confirm_timeout (float): How long to wait for the job to be gone
            stderr_path (str): Path to the job's captured stderr
            progress_cb (Callable[[str], None] | None): Called while waiting
        """
        try:
            cls._delete_job(api_instance, job_name, namespace, k8s_request_timeout)
        except Exception as e:
            cls._append_stderr(
                stderr_path, f"Failed to delete job {job_name} after failure: {e}"
            )
            return

        try:
            cls._await_job_deleted(
                api_instance,
                job_name,
                namespace,
                k8s_request_timeout,
                confirm_timeout,
                progress_cb=progress_cb,
                stage="awaiting_job_cleanup",
            )
        except Exception as e:
            # Server-side foreground deletion carries on without us; the
            # pre-existing-job handling at the top of execute() is the backstop
            # for a retry that arrives before it finishes.
            cls._append_stderr(
                stderr_path,
                f"Deletion of job {job_name} was not confirmed: {e}",
            )

    @staticmethod
    def _log_pod_termination(
        job_name: str, namespace: str, k8s_request_timeout: tuple, stderr_path: str
    ) -> None:
        """
        Record why this job's pod(s) actually died before the Job is deleted.
        Deletion runs with `propagation_policy="Foreground"`, which removes the
        pod along with it - so without this, a real OOMKill of the nextflow pod
        is permanently invisible: roz only ever sees "job failed" (rc 1) or
        "job timed out" (rc 124), never the container's terminated reason.

        Also called from the cleanup path in `execute`'s finally block, where
        the pod is often still running and so has no terminated state at all -
        an absent block here therefore no longer implies we failed to look.

        Best-effort only: any failure here must not affect the returncode
        `execute` reports.
        """
        try:
            core_v1 = CoreV1Api(ApiClient())
            pods = core_v1.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"job-name={job_name}",
                _request_timeout=k8s_request_timeout,
            )
            lines = []
            for pod in pods.items:
                statuses = (pod.status.container_statuses or []) + (
                    pod.status.init_container_statuses or []
                )
                for status in statuses:
                    terminated = status.state.terminated
                    if terminated is None:
                        continue
                    lines.append(
                        f"pod {pod.metadata.name} container {status.name}: "
                        f"reason={terminated.reason} exit_code={terminated.exit_code} "
                        f"message={terminated.message}"
                    )

            if lines:
                with open(stderr_path, "a") as stderr_fh:
                    stderr_fh.write(
                        "\n--- pod termination status before job deletion ---\n"
                        + "\n".join(lines)
                        + "\n"
                    )
        except Exception:
            pass

    @staticmethod
    def _clean_corrupt_cache(logdir: Path, stdout_path: str, stderr_path: str) -> None:
        """
        If this run failed because nextflow's LevelDB resume cache was
        corrupt, remove it so the next -resume attempt for this job starts
        clean instead of hitting the same corruption forever.

        Corruption here is a known hazard of storing the cache on a network
        filesystem (e.g. CephFS) rather than local disk, and is most often
        triggered by the pod being hard-killed (job timeout/backoff) while
        the cache DB is mid-write.

        Args:
            logdir (Path): The nextflow launch directory, whose `.nextflow`
                subdirectory holds the resume cache
            stdout_path (str): Path to the job's captured stdout - the
                Launcher's fatal error banner can land here rather than
                stderr (k8s/`kubectl logs` merges the two streams, but our
                own shell redirect splits them, so both need checking)
            stderr_path (str): Path to the job's captured stderr
        """
        content = ""
        for path in (stdout_path, stderr_path):
            try:
                with open(path) as fh:
                    content += fh.read()
            except OSError:
                continue

        if "Can't open cache DB" in content or "Corruption:" in content:
            shutil.rmtree(Path(logdir) / ".nextflow", ignore_errors=True)


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


def put_result_json(payload: dict, log: logging.Logger, config: dict):
    """Send the result payload to S3

    Args:
        payload (dict): The payload to send to S3
        log (logging.Logger): Logger object
        config (dict): The loaded roz config, from load_config()
    """

    s3_credentials = get_s3_credentials()

    s3_client = get_s3_client(s3_credentials)

    results_bucket = site_bucket(
        config, payload["project"], payload["raw_site"], "results"
    )

    try:
        s3_client.put_object(
            Bucket=results_bucket,
            Key=f"{payload['project']}.{payload['run_index']}.{payload['run_id']}.result.json",
            Body=json.dumps(payload),
        )

        log.info(
            f"Successfully uploaded result JSON for artifact: {payload['artifact']} to S3"
        )

    except ClientError as e:
        log.error(f"Failed to upload result JSON to S3: {e}")
        raise e


def put_linkage_json(payload: dict, log: logging.Logger, config: dict):
    """Send the linkage payload to S3

    Args:
        payload (dict): The payload dict to create the linkage dict from
        log (logging.Logger): Logger object
        config (dict): The loaded roz config, from load_config()
    """

    s3_credentials = get_s3_credentials()

    s3_client = get_s3_client(s3_credentials)

    results_bucket = site_bucket(
        config, payload["project"], payload["raw_site"], "results"
    )

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
            Bucket=results_bucket,
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


ONYX_ITEM_IDENTIFIER_FIELDS = (
    "unique_accession",
    "gtdb_assembly_id",
    "reference_header",
    "taxon_id",
    "climb_id",
    "run_index",
    "run_id",
    "biosample_id",
)

MAX_ONYX_ERRORS_PER_FIELD = 25
_MAX_ONYX_ERROR_DEPTH = 5


def _describe_onyx_item(submitted, key: str) -> str:
    """Label one key of a per-item Onyx error dict, using the matching
    submitted row's identifier where the key is a numeric index and the
    originally submitted row data is available."""
    if not key.isdigit():
        return key

    try:
        if submitted is not None:
            item = submitted[int(key)]
            if isinstance(item, dict):
                for id_field in ONYX_ITEM_IDENTIFIER_FIELDS:
                    value = item.get(id_field)
                    if value:
                        return f"item {key} ({id_field}={value})"
    except (TypeError, IndexError, KeyError, ValueError):
        pass

    return f"item {key}"


def _flatten_onyx_messages(value, prefix: str = "", submitted=None, depth: int = 0) -> list[str]:
    """Recursively flatten one Onyx error value (string, list, or dict keyed
    by field name / item index / 'non_field_errors') into a flat list of
    human-readable strings."""
    if depth > _MAX_ONYX_ERROR_DEPTH:
        return [f"{prefix}{value}"]

    if isinstance(value, dict):
        flattened = []
        for key in sorted(
            value.keys(),
            key=lambda k: (0, int(k)) if str(k).isdigit() else (1, str(k)),
        ):
            label = _describe_onyx_item(submitted, str(key)) if depth == 0 else str(key)
            flattened.extend(
                _flatten_onyx_messages(
                    value[key],
                    prefix=f"{prefix}{label}: ",
                    submitted=None,
                    depth=depth + 1,
                )
            )
        return flattened

    if isinstance(value, (list, tuple)):
        flattened = []
        for item in value:
            flattened.extend(
                _flatten_onyx_messages(
                    item, prefix=prefix, submitted=submitted, depth=depth + 1
                )
            )
        return flattened

    return [f"{prefix}{value}"]


def merge_onyx_error_messages(
    errors: dict,
    messages,
    submitted_fields: dict | None = None,
    max_per_field: int = MAX_ONYX_ERRORS_PER_FIELD,
) -> dict:
    """Merge the 'messages' body of an Onyx error response into `errors` in
    place. Every field's value is left as a flat list of strings so the
    result stays JSON-serialisable and legible in logs.

    Onyx reports whole-field errors as a flat list of strings, but reports
    per-item errors for list/relation fields (e.g. alignment_results) as a
    dict keyed by stringified item index, e.g. {"0": {"sub_field": [...]}}.
    Naively extending a list with such a dict iterates its keys, discarding
    the real error content - this function flattens either shape correctly.

    `submitted_fields` is the `fields` dict that was sent to Onyx, used to
    label per-item errors with the failing row's identifier instead of a
    bare batch-local index.
    """
    if not isinstance(messages, dict):
        errors.setdefault("onyx_errors", []).append(str(messages))
        return errors

    for field, value in messages.items():
        submitted = (submitted_fields or {}).get(field)
        flattened = _flatten_onyx_messages(value, submitted=submitted)

        if max_per_field and len(flattened) > max_per_field:
            dropped = len(flattened) - max_per_field
            flattened = flattened[:max_per_field]
            flattened.append(f"... and {dropped} more error(s) for '{field}'")

        errors.setdefault(field, []).extend(flattened)

    return errors


def _get_onyx_error_messages(e: OnyxRequestError) -> dict:
    """Safely extract the 'messages' body from an OnyxRequestError's
    response, falling back to str(e) if the response body isn't the
    expected JSON shape (e.g. an upstream proxy/gateway error page)."""
    try:
        return e.response.json()["messages"]
    except (ValueError, KeyError):
        return {"onyx_errors": [str(e)]}


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

                error_messages = _get_onyx_error_messages(e)

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
                    merge_onyx_error_messages(
                        payload["onyx_test_create_errors"], error_messages
                    )

                    return (False, False, payload)

                else:
                    payload.setdefault("onyx_create_errors", {})
                    merge_onyx_error_messages(
                        payload["onyx_create_errors"], error_messages
                    )

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
                payload.setdefault("onyx_errors", {})
                payload["onyx_errors"].setdefault("onyx_errors", [])
                payload["onyx_errors"]["onyx_errors"].append(str(e))
                return (False, True, payload)

            except OnyxClientError as e:
                log.error(
                    f"Onyx reconcile failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
                )
                payload.setdefault("onyx_errors", {})
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
                merge_onyx_error_messages(
                    payload["onyx_errors"], _get_onyx_error_messages(e)
                )
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
                payload["onyx_errors"]["onyx_errors"].append(str(e))
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
                merge_onyx_error_messages(
                    payload["onyx_errors"], _get_onyx_error_messages(e)
                )
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
                payload["onyx_errors"]["onyx_errors"].append(str(e))
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
                merge_onyx_error_messages(
                    payload["onyx_errors"], _get_onyx_error_messages(e)
                )
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


def onyx_get_record(
    project: str, climb_id: str, log: logging.Logger
) -> tuple[bool, dict | None]:
    """Fetch a single Onyx record by climb_id, without needing a payload dict.

    Args:
        project (str): Onyx project name
        climb_id (str): CLIMB ID of the record to fetch
        log (logging.Logger): Logger object

    Returns:
        tuple[bool, dict | None]: A bool indicating whether a transient/
        unexpected Onyx failure occurred (the caller should treat this as
        retryable), and the record dict, or None if no such record exists.
    """
    with OnyxClient(config=get_onyx_credentials()) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                record = client.get(project=project, climb_id=climb_id)
                return (False, record)

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
                    return (True, None)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                return (True, None)

            except OnyxClientError as e:
                log.error(
                    f"Onyx get failed for project: {project}, climb_id: {climb_id}. Error: {e}"
                )
                return (True, None)

            except OnyxRequestError as e:
                if e.response.status_code == 404:
                    return (False, None)

                log.error(
                    f"Onyx get failed for project: {project}, climb_id: {climb_id}. Error: {e}"
                )
                return (True, None)

            except Exception as e:
                log.error(f"Unhandled onyx_get_record error: {e}")
                return (True, None)

    # This should never be reached
    log.error("End of onyx_get_record func reached, this should never happen!")
    return (True, None)


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
                    payload["onyx_errors"]["onyx_errors"].append(str(e))

                    return (True, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                payload.setdefault("onyx_update_errors", {})
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(str(e))

                return (True, True, payload)

            except OnyxClientError as e:
                log.error(
                    f"Onyx update failed for artifact: {payload.get('artifact', 'NA')}, UUID: {payload.get('uuid') or payload.get('match_uuid', 'NA')}. Error: {e}"
                )
                payload.setdefault("onyx_update_errors", {})
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(str(e))

                return (True, False, payload)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx update failed for artifact: {payload.get('artifact', 'NA')}, UUID: {payload.get('uuid') or payload.get('match_uuid', 'NA')}. Error: {e}"
                )

                payload.setdefault("onyx_update_errors", {})
                merge_onyx_error_messages(
                    payload["onyx_update_errors"],
                    _get_onyx_error_messages(e),
                    submitted_fields=fields,
                )

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

# Fixed multipart chunksize/concurrency so upload memory use stays bounded.
# Without this, s3transfer's default 8MB chunksize is auto-scaled up once a
# file needs more than 10,000 parts (~78GiB), which can balloon per-upload
# memory use for very large files.
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=4,
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


class throttled_progress:
    """
    Boto3 upload `Callback` that runs `callback_fn` at most once every
    `min_interval_s`, no matter how often boto3 reports progress.

    Uploading a single multi-GB file is one blocking call that can run for
    many minutes, during which nothing else in the worker gets to run. Any
    liveness heartbeat that isn't refreshed from inside the transfer will go
    stale and the pod gets killed mid-upload. Boto3 invokes the callback per
    transferred chunk and from several transfer threads at once, so this
    both rate-limits it (heartbeats land on shared storage) and serialises
    it behind a lock.
    """

    def __init__(self, callback_fn, min_interval_s: float = 30.0):
        self._callback_fn = callback_fn
        self._min_interval_s = min_interval_s
        self._lock = threading.Lock()
        self._last_run = 0.0

    def __call__(self, bytes_transferred: int) -> None:
        now = time.monotonic()

        with self._lock:
            if now - self._last_run < self._min_interval_s:
                return
            self._last_run = now

        self._callback_fn()


def s3_upload_file(
    s3_client: BaseClient,
    local_path: str,
    bucket: str,
    key: str,
    progress_cb=None,
) -> None:
    """
    Upload a file to S3 and drop its page cache afterwards.

    Reading a multi-GB FASTQ/BAM for upload can leave that data pinned in
    page cache long after the upload finishes, with no natural pressure to
    evict it. Dropping the cache for this file immediately after upload
    keeps a pod that streams >100GB per job from sitting at its cgroup
    memory limit indefinitely.

    Args:
        s3_client (BaseClient): Boto3 S3 client to upload with
        local_path (str): Path to the local file to upload
        bucket (str): Destination S3 bucket
        key (str): Destination S3 object key
        progress_cb: Optional callable invoked with the number of bytes
            transferred as the upload proceeds, for keeping a liveness
            heartbeat fresh across a long transfer. Wrap it in
            throttled_progress() unless it's already cheap enough to run
            on every chunk.
    """

    s3_client.upload_file(
        local_path, bucket, key, Config=S3_TRANSFER_CONFIG, Callback=progress_cb
    )

    if hasattr(os, "posix_fadvise"):
        try:
            fd = os.open(local_path, os.O_RDONLY)
        except OSError:
            return

        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        except OSError:
            pass
        finally:
            os.close(fd)


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
