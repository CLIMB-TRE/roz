import argparse
import functools
import logging
import json
import sys
import threading
import time
import multiprocessing as mp
from pathlib import Path
import os
import csv
from itertools import batched
import boto3
from glob import glob
from typing import NamedTuple

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

from roz_scripts.utils.utils import (
    pipeline,
    init_logger,
    onyx_update,
    get_pod_namespace,
    S3_CLIENT_CONFIG,
    send_admin_alert,
)
from roz_scripts.utils.health import HealthState, JobHeartbeat, get_health_dir
from varys import Varys


class chimera_worker_pool_handler:
    def __init__(self, workers, logger, varys_client, project, health: HealthState):
        self._log = logger
        # `pipeline.execute()` does os.chdir() and mutates self.cmd, and
        # JobHeartbeat/HealthState write per-pid files - both assume a
        # distinct process per job, so this must stay multiprocessing, never
        # threads. Pin the start method explicitly rather than relying on
        # whatever mp.Pool's platform default happens to be.
        self.worker_pool = mp.get_context("fork").Pool(processes=workers)
        self._varys_client = varys_client
        self._health = health
        self._project = project

        # Guards every varys ack/nack/send call made from callback()/
        # error_callback(), which run on the pool's result-handler thread -
        # without this, concurrent callbacks could race on lazy exchange/
        # producer creation inside varys.
        self._varys_lock = threading.Lock()
        self._in_flight_lock = threading.Lock()
        self._in_flight = 0

        # Failures here are never dead-lettered - every message at this
        # stage represents a record that must eventually be published, and
        # a poison message no longer blocks the whole queue once there are
        # multiple workers, so the only job of these counters is to alert a
        # human rather than to cap retries.
        self._failure_log = {}
        self._timeout_log = {}
        self._consecutive_worker_errors = 0

        self._log.info(
            f"Successfully initialised chimera worker pool with {workers} workers"
        )

    def _send_remote_alert(self, uuid: str, description: str) -> None:
        send_admin_alert(
            self._varys_client,
            source=self._project,
            description=description,
            uuid=uuid,
        )

    def in_flight(self) -> int:
        with self._in_flight_lock:
            return self._in_flight

    def _job_finished(self) -> None:
        with self._in_flight_lock:
            self._in_flight -= 1

    def submit_job(self, message, args, chimera_pipe, namespace, is_rerun=False):
        try:
            match_uuid = json.loads(message.body)["match_uuid"]
        except (json.JSONDecodeError, KeyError) as e:
            self._log.error(f"Malformed message body, re-queueing: {e}")
            with self._varys_lock:
                self._varys_client.nack_message(message)
            self._send_remote_alert(
                "unknown",
                f"Malformed chimera message body, requires manual intervention: {e}",
            )
            return

        self._log.info(
            f"Submitting chimera job to the worker pool for UUID: {match_uuid}"
        )

        with self._in_flight_lock:
            self._in_flight += 1

        self.worker_pool.apply_async(
            func=process_record,
            kwds={
                "message": message,
                "args": args,
                "chimera_pipe": chimera_pipe,
                "namespace": namespace,
                "is_rerun": is_rerun,
            },
            callback=self.callback,
            error_callback=functools.partial(self.error_callback, message),
        )

    def callback(self, result):
        success, timed_out, payload, match_uuid, is_rerun, message = result

        self._health.clear_job(match_uuid)
        self._consecutive_worker_errors = 0

        if success:
            self._failure_log.pop(match_uuid, None)
            self._timeout_log.pop(match_uuid, None)

            self._log.info(
                f"Successfully processed chimera record for UUID: {match_uuid}"
            )

            downstream_exchange = (
                f"downstream-chimera_rerun-{self._project}"
                if is_rerun
                else f"downstream-chimera-{self._project}"
            )

            with self._varys_lock:
                self._varys_client.acknowledge_message(message)
                self._varys_client.send(
                    message=payload,
                    exchange=downstream_exchange,
                    queue_suffix="chimera",
                )

            self._job_finished()
            return

        self._log.error(
            f"Chimera processing failed for UUID: {match_uuid}, re-queueing message"
        )

        self._failure_log[match_uuid] = self._failure_log.get(match_uuid, 0) + 1
        if self._failure_log[match_uuid] >= 5:
            self._log.error(
                f"UUID: {match_uuid} has failed {self._failure_log[match_uuid]} times, sending alert"
            )
            self._send_remote_alert(
                match_uuid,
                f"Repeated chimera processing failure ({self._failure_log[match_uuid]} attempts)",
            )

        if timed_out:
            self._timeout_log[match_uuid] = self._timeout_log.get(match_uuid, 0) + 1
            if self._timeout_log[match_uuid] >= 2:
                self._log.error(
                    f"UUID: {match_uuid} has timed out {self._timeout_log[match_uuid]} times, sending alert"
                )
                self._send_remote_alert(
                    match_uuid,
                    f"Chimera pipeline has timed out {self._timeout_log[match_uuid]} times",
                )

        # Never dead-letter: every message at this stage is vital, so always
        # requeue rather than dropping it after N attempts. Parallelism
        # means a poison message no longer blocks every other job - it just
        # keeps retrying (and alerting) until someone intervenes.
        with self._varys_lock:
            self._varys_client.nack_message(message)

        self._job_finished()

    def error_callback(self, message, exception):
        self._log.error(f"Chimera worker failed with unhandled exception: {exception}")

        try:
            match_uuid = json.loads(message.body).get("match_uuid", "unknown")
        except (json.JSONDecodeError, AttributeError):
            match_uuid = "unknown"

        self._health.clear_job(match_uuid)

        with self._varys_lock:
            self._varys_client.nack_message(message)

        self._varys_client.send(
            message=f"{self._project} chimera worker failed with unhandled exception: {exception}",
            exchange=f"{self._project}-restricted-announce",
            queue_suffix="dead_worker",
        )

        self._job_finished()

        # A single crashed job must not take the whole pod down - other
        # in-flight jobs would be killed with it, and the message would be
        # left neither acked nor nacked. Only escalate to a liveness-
        # triggered restart once failures look systemic (e.g. a broken
        # dependency) rather than one bad record.
        self._consecutive_worker_errors += 1
        if self._consecutive_worker_errors >= 3:
            reason = (
                f"chimera worker failed with {self._consecutive_worker_errors} "
                f"consecutive unhandled exceptions: {exception}"
            )
            self._health.mark_fatal(
                reason,
                alert_fn=lambda r: send_admin_alert(
                    self._varys_client, source=self._project, description=r
                ),
            )

    def close(self):
        self.worker_pool.close()
        self.worker_pool.join()


def onyx_get_metadata(
    args: argparse.Namespace, climb_id: str, log, uuid: str = "NA"
) -> dict | bool:
    """
    Get metadata from Onyx for a given climb_id
    Returns the metadata as a dictionary if successful, otherwise returns False
    and logs the error

    Args:
        args (argparse.Namespace): Command line arguments
        climb_id (str): Climb ID to get metadata for
        log (logging.Logger): Logger object
        uuid (str, optional): UUID of the record. Defaults to "NA".

    Returns:
        metadata(dict | bool): Metadata dictionary if successful, otherwise False
    """
    onyx_config = OnyxConfig(
        domain=os.environ["ONYX_DOMAIN"],
        token=os.environ["ONYX_TOKEN"],
    )

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                record = client.get(project=args.project, climb_id=climb_id)
                return record

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 20 seconds"
                    )
                    time.sleep(20)
                    continue

                else:
                    log.exception(
                        f"Failed to connect to Onyx {reconnect_count} times due to error:"
                    )
                    raise

            except (OnyxServerError, OnyxConfigError):
                log.exception("Unhandled Onyx error:")
                raise

            except OnyxClientError:
                log.exception(
                    f"Onyx get failed for climb_id: {climb_id}, UUID: {uuid}. Error:"
                )
                raise

            except OnyxRequestError:
                log.exception(
                    f"Onyx get failed for climb_id: {climb_id}, UUID: {uuid}. Error:"
                )
                raise

            except Exception:
                log.exception("Unhandled onyx_update error:")
                raise

    # This should never be reached
    return False


def ret_0_parser(
    log: logging.Logger,
    payload: dict,
    result_path: str,
    ingest_fail: bool = False,
) -> tuple[bool, dict]:
    """Function to parse the execution trace of a Nextflow pipeline run to determine whether any of the processes failed.

    Args:
        log (logging.Logger): Logger object
        payload (dict): Payload dictionary
        result_path (str): Path to the results directory
        ingest_fail (bool): Boolean to indicate whether the ingest has failed up to this point (default: False)

    Returns:
        tuple[bool, dict]: Tuple containing the ingest fail boolean and the payload dictionary
    """
    try:

        execution_traces = glob(
            os.path.join(
                result_path,
                "pipeline_info",
                "execution_trace*.txt",
            )
        )

        # Get the most recent trace file
        latest_trace = max(execution_traces, key=os.path.getmtime)

        with open(latest_trace) as trace_fh:
            reader = csv.DictReader(trace_fh, delimiter="\t")

            process_status = {}

            for trace in reader:
                process, tag = trace["name"].split()
                process_status[process] = trace["exit"]

            for process, exit_code in process_status.items():
                if exit_code != "0":
                    if process.endswith("SYLPH_TAXONOMY") and exit_code == "2":
                        log.info(
                            f"No Sylph hits found for {payload['match_uuid']}, skipping"
                        )
                        payload.setdefault("chimera_info", {})
                        payload["chimera_info"]["SYLPH_TAXONOMY"] = {
                            "status": "no_hits",
                            "message": "No Sylph hits found above 95% ANI",
                        }
                        continue

                    else:
                        log.error(
                            f"Process '{process}' failed with exit code '{trace['exit']}' for UUID: {payload['match_uuid']}"
                        )
                        raise Exception(
                            f"Process '{process}' failed with unexpected exit code '{trace['exit']}'"
                        )

    except Exception:
        log.exception(
            f"Could not open pipeline trace for UUID: {payload['match_uuid']} despite NXF exit code 0 due to error:"
        )
        raise

    return (ingest_fail, payload)


def create_samplesheet(metadata: list, out_path: Path):
    """Create a samplesheet CSV file from the given metadata

    Args:
        metadata (list): list of dicts representing the Onyx records for the data to be run
        out_path (Path): Path to the output samplesheet file
    """

    with open(out_path, "wt") as samplesheet_fh:
        writer = csv.DictWriter(
            samplesheet_fh, fieldnames=["sample", "platform", "fastq_1", "fastq_2"]
        )
        writer.writeheader()
        for row in metadata:
            out_row = {
                "sample": row["climb_id"],
                "platform": row["platform"],
                "fastq_1": row["human_filtered_reads_1"],
                "fastq_2": row["human_filtered_reads_2"],
            }
            writer.writerow(out_row)


def handle_alignment_report(
    alignment_report_path: str, payload: dict, log: logging.Logger
):

    with open(alignment_report_path) as report_fh:
        reader = csv.DictReader(report_fh, delimiter="\t")
        alignment_rows = [row for row in reader]

    clear_fail, clear_alert, payload = onyx_update(
        payload=payload, fields=None, log=log, clear_fields=["alignment_results"]
    )

    if clear_fail or clear_alert:
        log.error(
            f"Failed to clear old alignment results from Onyx for UUID: {payload['match_uuid']}, onyx errors: {payload.get('onyx_update_errors')}"
        )
        return False

    # Batch updates to Onyx in groups of 100
    for batch in batched(alignment_rows, 100):
        update_fail, update_alert, payload = onyx_update(
            payload=payload, fields={"alignment_results": batch}, log=log
        )

        if update_fail or update_alert:
            log.error(
                f"Failed to update Onyx with alignment results for UUID: {payload['match_uuid']}, onyx errors: {payload.get('onyx_update_errors')}"
            )
            return False

    return True


def handle_sylph_report(sylph_report_path: str, payload: dict, log: logging.Logger):
    with open(sylph_report_path) as report_fh:
        reader = csv.DictReader(report_fh, delimiter="\t")

        clear_fail, clear_alert, payload = onyx_update(
            payload=payload, fields=None, log=log, clear_fields=["sylph_results"]
        )

        if clear_fail or clear_alert:
            log.error(
                f"Failed to clear old Sylph results from Onyx for UUID: {payload['match_uuid']}, onyx errors: {payload.get('onyx_update_errors')}"
            )
            return False

        out_rows = []

        for row in reader:
            out_rows.append(
                {
                    "taxon_id": row["tax_id"],
                    "human_readable": row["human_readable"],
                    "gtdb_taxon_string": row["taxon_string"],
                    "gtdb_assembly_id": row["contig_id"],
                    "gtdb_contig_header": row["Contig_name"],
                    "taxonomic_abundance": row["Taxonomic_abundance"],
                    "sequence_abundance": row["Sequence_abundance"],
                    "adjusted_ani": row["Adjusted_ANI"],
                    "ani_confidence_interval": row["ANI_5-95_percentile"],
                    "effective_coverage": row["Eff_cov"],
                    "effective_coverage_confidence_interval": row[
                        "Lambda_5-95_percentile"
                    ],
                    "median_kmer_cov": row["Median_cov"],
                    "mean_kmer_cov": row["Mean_cov_geq1"],
                    "containment_index": row["Containment_ind"],
                    "naive_ani": row["Naive_ANI"],
                    "kmers_reassigned": row["kmers_reassigned"],
                }
            )

    # Batch updates to Onyx in groups of 100
    for batch in batched(out_rows, 100):
        update_fail, update_alert, payload = onyx_update(
            payload=payload, fields={"sylph_results": batch}, log=log
        )

        if update_fail or update_alert:
            log.error(
                f"Failed to update Onyx with sylph results for UUID: {payload['match_uuid']}, onyx errors: {payload.get('onyx_update_errors')}"
            )
            return False

    return True


def push_bam_file(bam_path: str, payload: dict, log: logging.Logger):

    s3_client = boto3.client(
        "s3",
        endpoint_url="https://s3.climb.ac.uk",
        config=S3_CLIENT_CONFIG,
    )

    s3_bucket = f"{payload['project']}-chimera-bams"

    s3_key = f"{payload['climb_id']}.chimera.bam"

    s3_uri = f"s3://{s3_bucket}/{s3_key}"

    try:
        # Add handling for Db in name etc
        s3_client.upload_file(
            bam_path,
            s3_bucket,
            s3_key,
        )
    except Exception:
        log.exception(
            f"Failed to upload BAM to S3 for UUID: {payload['match_uuid']}, error:"
        )
        raise

    return s3_uri


def push_chimera_report(
    report_path: str,
    report_suffix: str,
    db_version: str | None,
    payload: dict,
    log: logging.Logger,
) -> str:

    s3_client = boto3.client(
        "s3",
        endpoint_url="https://s3.climb.ac.uk",
    )

    s3_bucket = f"{payload['project']}-chimera-reports"

    s3_key = f"{payload['climb_id']}.{report_suffix}"

    s3_uri = f"s3://{s3_bucket}/{s3_key}"

    try:
        if not db_version:
            raise ValueError(
                f"No DB version supplied for {report_suffix}, cannot upload db-version-tagged report"
            )

        s3_client.upload_file(
            report_path,
            s3_bucket,
            s3_key,
        )

        s3_client.upload_file(
            report_path,
            s3_bucket,
            f"{payload['climb_id']}.{db_version}.{report_suffix}",
        )
    except Exception:
        log.exception(
            f"Failed to upload {report_suffix} to S3 for UUID: {payload['match_uuid']}, error:"
        )
        raise

    return s3_uri


def process_record(
    message,
    args: argparse.Namespace,
    chimera_pipe: pipeline,
    namespace: str,
    is_rerun: bool = False,
) -> tuple[bool, bool, dict, str, bool, NamedTuple]:
    """Run the chimera pipeline and process its reports for a single artifact.

    This function runs inside a worker process of the pool in run() - it
    never touches varys (ack/nack/publish) directly, since a multiprocessing
    worker can be killed independently of the main process and must not be
    the only thing holding a message's fate. Instead it always returns a
    result tuple describing the outcome, and the pool's callback (running in
    the main process) does all varys I/O.

    Args:
        message (namedtuple): Varys message object for the current artifact
        args (argparse.Namespace): Command line arguments object
        chimera_pipe (pipeline): Instance of the chimera pipeline (see pipeline class)
        namespace (str): The k8s namespace to run the pipeline job in
        is_rerun (bool): Whether this message came from the rerun exchange

    Returns:
        tuple[bool, bool, dict, str, bool, namedtuple]: success, whether this
        attempt timed out, the (possibly updated) payload dict, the
        match_uuid, is_rerun, and the original message object
    """
    log = logging.getLogger(f"{args.project}.chimera")

    payload = json.loads(message.body)
    match_uuid = payload["match_uuid"]

    job_heartbeat = JobHeartbeat(get_health_dir(), uuid=match_uuid, budget_s=600)
    job_heartbeat.beat(stage="pre_pipeline")

    def fail(reason: str, timed_out: bool = False):
        log.error(reason)
        return (False, timed_out, payload, match_uuid, is_rerun, message)

    try:
        metadata = onyx_get_metadata(
            args=args,
            climb_id=payload["climb_id"],
            log=log,
            uuid=match_uuid,
        )

        if not metadata:
            return fail(
                f"Failed to get metadata for climb_id: {payload['climb_id']}, UUID: {match_uuid}. This should never happen."
            )

        record_outdir = Path(os.path.join(args.outdir, match_uuid))

        record_outdir.mkdir(parents=True, exist_ok=True)
        log.info(f"Creating samplesheet for {match_uuid}")

        create_samplesheet(
            [metadata],
            Path(os.path.join(record_outdir, "samplesheet.csv")),
        )

        log.info(f"Running chimera pipeline for {match_uuid}")

        pipeline_params = {
            "input": os.path.join(record_outdir, "samplesheet.csv"),
            "mm2_index": args.mm2_index,
            "bwa_index_prefix": args.bwa_index_prefix,
            "sylph_db": args.sylph_db,
            "sylph_taxdb": args.sylph_taxdb,
            "database_metadata": args.database_metadata,
            "outdir": record_outdir,
        }

        nxf_home = Path(
            f"{os.environ['NXF_HOME'].rstrip('/')}/nextflow.worker.{os.getpid()}/"
        )
        nxf_home.mkdir(parents=True, exist_ok=True)
        nxf_home.chmod(0o775)

        env_vars = {
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "NXF_WORK": os.getenv("NXF_WORK"),
            "NXF_HOME": str(nxf_home),
        }

        job_heartbeat.beat(stage="running_pipeline", budget_s=args.chimera_timeout)

        rc = chimera_pipe.execute(
            params=pipeline_params,
            logdir=record_outdir,
            timeout=args.chimera_timeout,
            env_vars=env_vars,
            namespace=namespace,
            job_id=match_uuid,
            stdout_path=os.path.join(record_outdir, "chimera_stdout.log"),
            stderr_path=os.path.join(record_outdir, "chimera_stderr.log"),
            workingdir=record_outdir,
            progress_cb=lambda stage: job_heartbeat.beat(stage),
        )

        if rc != 0:
            # k8s Job execution reports a hard timeout as rc 124 (see
            # pipeline.execute) - track that distinctly so repeated timeouts
            # of the same record can be alerted on separately from other
            # failure modes.
            return fail(
                f"Chimera pipeline failed for {match_uuid} with return code {rc}",
                timed_out=(rc == 124),
            )

        log.info(f"Chimera pipeline completed with exit code {rc} for {match_uuid}")

        job_heartbeat.beat(stage="post_pipeline", budget_s=600)

        ingest_fail, payload = ret_0_parser(
            log=log,
            payload=payload,
            result_path=record_outdir,  # type: ignore
            ingest_fail=False,
        )

        if ingest_fail:
            return fail(
                f"Chimera pipeline failed for {match_uuid} due to process failure"
            )

        log.info(f"Processing record for {metadata['climb_id']}")  # type: ignore

        alignment_report_path = os.path.join(
            record_outdir,
            metadata["climb_id"],  # type: ignore
            f"{metadata['climb_id']}.alignment_report.tsv",  # type: ignore
        )

        if not os.path.exists(alignment_report_path):
            return fail(
                f"Alignment report not found for {match_uuid} at expected path {alignment_report_path}"
            )

        alignment_success = handle_alignment_report(
            alignment_report_path=alignment_report_path,
            payload=payload,
            log=log,
        )

        if not alignment_success:
            return fail(f"Failed to process alignment report for {match_uuid}")

        try:
            push_chimera_report(
                report_path=alignment_report_path,
                report_suffix="alignment_report.tsv",
                db_version=args.alignment_db_version,
                payload=payload,
                log=log,
            )
        except Exception:
            return fail(f"Failed to push alignment report for {match_uuid}")

        sylph_report_path = os.path.join(
            record_outdir,
            metadata["climb_id"],  # type: ignore
            f"{metadata['climb_id']}.sylph_taxonomy_report.tsv",  # type: ignore
        )
        if not os.path.exists(sylph_report_path):
            if not payload.get("chimera_info"):
                return fail(
                    f"Sylph report not found for {match_uuid} at expected path {sylph_report_path}"
                )

            else:
                sylph_taxonomy_info = payload["chimera_info"].get("SYLPH_TAXONOMY")
                if (
                    sylph_taxonomy_info
                    and sylph_taxonomy_info.get("status") == "no_hits"
                ):
                    log.info(
                        f"No Sylph report found for {match_uuid}, this just means that no hits were observed > 95% ANI"
                    )
                else:
                    return fail(
                        f"Sylph report not found for {match_uuid} at expected path {sylph_report_path}"
                    )

        else:
            sylph_success = handle_sylph_report(
                sylph_report_path=sylph_report_path,
                payload=payload,
                log=log,
            )

            if not sylph_success:
                return fail(f"Failed to process Sylph report for {match_uuid}")

            try:
                push_chimera_report(
                    report_path=sylph_report_path,
                    report_suffix="sylph_taxonomy_report.tsv",
                    db_version=args.sylph_db_version,
                    payload=payload,
                    log=log,
                )
            except Exception:
                return fail(f"Failed to push Sylph report for {match_uuid}")

            log.info(f"Successfully processed Sylph report for {match_uuid}")

        log.info(
            f"Successfully processed alignment / sylph reports for {metadata['climb_id']}"  # type: ignore
        )

        bam_path = os.path.join(
            record_outdir, metadata["climb_id"], f"{metadata['climb_id']}.bam"  # type: ignore
        )

        if not os.path.exists(bam_path):
            return fail(
                f"BAM file not found for {match_uuid} at expected path {bam_path}"
            )

        bam_uri = push_bam_file(
            bam_path=bam_path,
            payload=payload,
            log=log,
        )

        update_fail, update_alert, payload = onyx_update(
            payload=payload,
            fields={
                "chimera_bam": bam_uri,
                "alignment_db_version": args.alignment_db_version,
                "is_chimera_published": True,
                "sylph_db_version": args.sylph_db_version,
            },
            log=log,
        )
        if update_fail or update_alert:
            return fail(f"Failed to update Onyx with BAM URI for UUID: {match_uuid}")

        log.info(f"Successfully updated Onyx for {metadata['climb_id']}")  # type: ignore

        return (True, False, payload, match_uuid, is_rerun, message)

    except Exception as e:
        log.exception(f"Unhandled exception processing UUID {match_uuid}:")
        return fail(f"Unhandled exception processing UUID {match_uuid}: {e}")

    finally:
        job_heartbeat.clear()


def run(args):
    log = None
    health = None
    varys_client = None
    handler = None

    try:
        log = init_logger(f"{args.project}.chimera", args.logfile, args.log_level)

        varys_client = Varys(
            profile="roz",
            logfile=args.logfile,
            log_level=args.log_level,
            auto_acknowledge=False,
        )

        namespace = get_pod_namespace()

        chimera_pipe = pipeline(
            pipe="CLIMB-TRE/chimera",
            branch=args.chimera_release,
            config=args.nxf_config,
            nxf_image=args.nxf_image,
            job_prefix="chimera",
        )

        health = HealthState(get_health_dir())

        handler = chimera_worker_pool_handler(
            workers=args.n_workers,
            logger=log,
            varys_client=varys_client,
            project=args.project,
            health=health,
        )

        while True:
            if handler.in_flight() >= args.n_workers:
                health.heartbeat()
                time.sleep(5)
                continue

            priority_message = varys_client.receive(
                exchange=f"inbound-new_artifact-{args.project}",
                queue_suffix="chimera",
                prefetch_count=args.n_workers,
                timeout=10,
            )

            rerun_message = varys_client.receive(
                exchange=f"inbound-new_artifact_rerun-{args.project}",
                queue_suffix="chimera",
                prefetch_count=args.n_workers,
                timeout=10,
            )

            health.heartbeat()

            if not priority_message and not rerun_message:
                time.sleep(60)
                continue

            if priority_message:
                message = priority_message
                is_rerun = False
                if rerun_message:
                    varys_client.nack_message(rerun_message)
            elif rerun_message:
                message = rerun_message
                is_rerun = True
            else:
                log.error("This should never happen, no message received")
                continue

            handler.submit_job(
                message=message,
                args=args,
                chimera_pipe=chimera_pipe,
                namespace=namespace,
                is_rerun=is_rerun,
            )

    except BaseException as e:
        if health is not None:
            health.mark_fatal(
                f"chimera runner crashed: {e}",
                alert_fn=(
                    (
                        lambda r: send_admin_alert(
                            varys_client, source="chimera_runner", description=r
                        )
                    )
                    if varys_client is not None
                    else None
                ),
            )
        if handler is not None:
            handler.close()
        if varys_client is not None:
            varys_client.close()
        time.sleep(300)
        if log is not None:
            log.exception("Shutting down chimera runner due to exception:")
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project",
        type=str,
        required=True,
        help="Project name e.g. mscape, synthscape",
    )
    parser.add_argument("--outdir", type=Path, required=True, help="Output directory")
    parser.add_argument(
        "--chimera_release", type=str, required=True, help="Chimera release version"
    )
    parser.add_argument(
        "--mm2_index", type=Path, required=True, help="Path to mm2 index"
    )
    parser.add_argument(
        "--bwa_index_prefix",
        type=Path,
        required=True,
        help="Path to bwa index, e.g. /path/to/index_prefix (no suffixes like .bwt etc. needed)",
    )
    parser.add_argument(
        "--database_metadata",
        type=Path,
        help="Path to the alignment database metadata TSV",
    )
    parser.add_argument("--alignment_db_version", type=str, help="Alignment DB version")
    parser.add_argument(
        "--sylph_db", type=Path, required=True, help="Path to sylph database"
    )
    parser.add_argument(
        "--sylph_taxdb", type=Path, required=True, help="Path to sylph taxdb"
    )
    parser.add_argument("--sylph_db_version", type=str, help="Sylph DB version")
    parser.add_argument("--n_workers", type=int, default=3)
    parser.add_argument(
        "--chimera_timeout",
        type=int,
        default=3600,
        help="Timeout in seconds for a single chimera pipeline run",
    )
    parser.add_argument("--nxf_image", default="quay.io/climb-tre/nextflow:25.04.8")
    parser.add_argument("--logfile", type=Path, default=Path("chimera_runner.log"))
    parser.add_argument("--log_level", type=str, default="DEBUG")
    parser.add_argument(
        "--nxf_config",
        type=Path,
        help="Path to nextflow config file",
        required=True,
    )
    args = parser.parse_args()

    for i in (
        "ONYX_DOMAIN",
        "ONYX_TOKEN",
        "VARYS_CFG",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "NXF_WORK",
        "NXF_HOME",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    run(args)


if __name__ == "__main__":
    main()
