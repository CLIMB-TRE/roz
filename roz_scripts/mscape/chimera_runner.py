import argparse
import logging
import json
import sys
import time
from pathlib import Path
import os
import csv
from itertools import batched
import boto3

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
)
from varys import Varys


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

            except (OnyxServerError, OnyxConfigError):
                log.exception("Unhandled Onyx error:")

            except OnyxClientError:
                log.exception(
                    f"Onyx get failed for climb_id: {climb_id}, UUID: {uuid}. Error:"
                )

            except OnyxRequestError:
                log.exception(
                    f"Onyx get failed for climb_id: {climb_id}, UUID: {uuid}. Error:"
                )

            except Exception:
                log.exception("Unhandled onyx_update error:")

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
        with open(
            os.path.join(
                result_path,
                "pipeline_info",
                "execution_trace.txt",
            )
        ) as trace_fh:
            reader = csv.DictReader(trace_fh, delimiter="\t")

            trace_dict = {}
            for process in reader:
                trace_dict[process["name"].split(":")[-1]] = process

        for process, trace in trace_dict.items():
            if trace["exit"] != "0":
                if process.endswith("SYLPH_TAXONOMY") and trace["exit"] == "2":
                    log.info(
                        f"No Sylph hits found for {payload['match_uuid']}, skipping"
                    )
                    continue

                else:
                    payload.setdefault("ingest_errors", [])
                    payload["ingest_errors"].append(
                        f"{payload['project']} validation pipeline (Scylla) failed in process {process} with exit code {trace['exit']} and status {trace['status']}"
                    )
                    ingest_fail = True
                    payload["rerun"] = True

    except Exception:
        log.exception(
            f"Could not open pipeline trace for UUID: {payload['uuid']} despite NXF exit code 0 due to error:"
        )

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

    # Batch updates to Onyx in groups of 100
    for batch in batched(alignment_rows, 100):
        update_fail, update_alert, payload = onyx_update(
            payload=payload, fields={"alignment_results": batch}, log=log
        )

        if update_fail or update_alert:
            log.error(
                f"Failed to update Onyx with alignment results for UUID: {payload['match_uuid']}"
            )
            return False

    return True


def handle_sylph_report(sylph_report_path: str, payload: dict, log: logging.Logger):
    with open(sylph_report_path) as report_fh:
        reader = csv.DictReader(report_fh, delimiter="\t")

        out_rows = []

        for row in reader:
            out_rows.append(
                {
                    "taxon_id": int(row["tax_id"]),
                    "human_readable": row["human_readable"],
                    "gtdb_taxon_string": row["taxon_string"],
                    "gtdb_assembly_id": row["contig_id"],
                    "gtdb_contig_header": row["Contig_name"],
                    "taxonomic_abundance": float(row["Taxonomic_abundance"]),
                    "sequence_abundance": float(row["Sequence_abundance"]),
                    "adjusted_ani": float(row["Adjusted_ANI"]),
                    "ani_confidence_interval": row["ANI_5-95_percentile"],
                    "effective_coverage": float(row["Eff_cov"]),
                    "effective_coverage_confidence_interval": row[
                        "Lambda_5-95_percentile"
                    ],
                    "median_kmer_cov": int(row["Median_cov"]),
                    "mean_kmer_cov": float(row["Mean_cov_geq1"]),
                    "containment_index": row["Containment_ind"],
                    "naive_ani": float(row["Naive_ANI"]),
                    "kmers_reassigned": int(row["kmers_reassigned"]),
                }
            )

    # Batch updates to Onyx in groups of 100
    for batch in batched(out_rows, 100):
        update_fail, update_alert, payload = onyx_update(
            payload=payload, fields={"sylph_results": batch}, log=log
        )

        if update_fail or update_alert:
            log.error(
                f"Failed to update Onyx with sylph results for UUID: {payload['match_uuid']}"
            )
            return False


def push_bam_file(bam_path: str, payload: dict, log: logging.Logger):

    s3_client = boto3.client(
        "s3",
        endpoint_url="https://s3.climb.ac.uk",
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

    return s3_uri


def run(args):
    try:
        log = init_logger(f"{args.project}.chimera", args.logfile, args.log_level)

        varys_client = Varys(
            profile="roz",
            logfile=args.logfile,
            log_level=args.log_level,
            auto_acknowledge=False,
        )

        chimera_pipe = pipeline(
            pipe="CLIMB-TRE/chimera",
            branch=args.chimera_release,
            config=args.nxf_config,
            nxf_image=args.nxf_image,
        )

        while True:
            message = varys_client.receive(
                exchange=f"inbound-new_artifact-{args.project}",
                queue_suffix="chimera",
                prefetch_count=1,
            )
            log.info(f"Received message: {message.body}")

            payload = json.loads(message.body)

            metadata = onyx_get_metadata(
                args=args,
                climb_id=payload["climb_id"],
                log=log,
                uuid=payload["match_uuid"],
            )

            if not metadata:
                log.error(
                    f"Failed to get metadata for climb_id: {payload['climb_id']}, UUID: {payload['match_uuid']}. This should never happen."
                )
                sys.exit(1)

            record_outdir = Path(os.path.join(args.outdir, payload["match_uuid"]))

            metadata_list = [metadata]

            record_outdir.mkdir(parents=True, exist_ok=True)
            log.info(f"Creating samplesheet for {payload['match_uuid']}")

            create_samplesheet(
                metadata_list,
                Path(os.path.join(record_outdir, "samplesheet.csv")),
            )

            log.info(f"Running chimera pipeline for {payload['match_uuid']}")

            pipeline_params = {
                "input": os.path.join(record_outdir, "samplesheet.csv"),
                "mm2_index": args.mm2_index,
                "bwa_index_prefix": args.bwa_index_prefix,
                "sylph_db": args.sylph_db,
                "sylph_taxdb": args.sylph_taxdb,
                "database_metadata": args.database_metadata,
                "outdir": record_outdir,
                "samplesheet": os.path.join(record_outdir, "samplesheet.csv"),
            }

            env_vars = {
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "NXF_WORK": "/shared/team/nxf_work/roz/work/",
                "NXF_HOME": "/shared/team/nxf_work/roz_chimera/nextflow.worker/",
            }

            rc = chimera_pipe.execute(
                params=pipeline_params,
                logdir=record_outdir,
                timeout=21600,
                env_vars=env_vars,
                namespace=f"ns-{args.project}",
                job_id=payload["match_uuid"],
                stdout_path=os.path.join(record_outdir, "chimera_stdout.log"),
                stderr_path=os.path.join(record_outdir, "chimera_stderr.log"),
            )

            if rc != 0:
                log.error(
                    f"Chimera pipeline failed for {payload['match_uuid']} with return code {rc}"
                )
                # Do not acknowledge the message so it can be retried
                varys_client.nack_message(message)
                continue

            log.info(
                f"Chimera pipeline completed with exit code {rc} for {payload['match_uuid']}"
            )

            ingest_fail, payload = ret_0_parser(
                log=log,
                payload=payload,
                result_path=record_outdir,
                ingest_fail=False,
            )

            if ingest_fail:
                log.error(
                    f"Chimera pipeline failed for {payload['match_uuid']} due to process failure"
                )
                # Do not acknowledge the message so it can be retried
                varys_client.nack_message(message)
                continue

            # This is single sample currently but leaving loop in place in case of future multi-sample support
            for record in metadata_list:
                log.info(f"Processing record for {record['climb_id']}")

                alignment_report_path = os.path.join(
                    record_outdir, f"{record['climb_id']}.alignment_report.tsv"
                )

                if not os.path.exists(alignment_report_path):
                    varys_client.nack_message(message)
                    log.error(
                        f"Alignment report not found for {payload['match_uuid']} at expected path {alignment_report_path}"
                    )
                    continue

                alignment_success = handle_alignment_report(
                    alignment_report_path=alignment_report_path,
                    payload=payload,
                    log=log,
                )

                if not alignment_success:
                    log.error(
                        f"Failed to process alignment report for {payload['match_uuid']}"
                    )
                    varys_client.nack_message(message)
                    continue

                sylph_report_path = os.path.join(
                    record_outdir, f"{record['climb_id']}.sylph_report.tsv"
                )
                if not os.path.exists(sylph_report_path):
                    log.info(
                        f"No Sylph report found for {payload['match_uuid']}, this just means that no hits were observed > 95% ANI"
                    )
                else:
                    sylph_success = handle_sylph_report(
                        sylph_report_path=sylph_report_path,
                        payload=payload,
                        log=log,
                    )

                    if not sylph_success:
                        log.error(
                            f"Failed to process Sylph report for {payload['match_uuid']}"
                        )
                        varys_client.nack_message(message)
                        continue

                    log.info(
                        f"Successfully processed Sylph report for {payload['match_uuid']}"
                    )

                log.info(
                    f"Successfully processed alignment / sylph reports for {record['climb_id']}"
                )

                bam_path = os.path.join(record_outdir, f"{record['climb_id']}.bam")

                if not os.path.exists(bam_path):
                    log.error(
                        f"BAM file not found for {payload['match_uuid']} at expected path {bam_path}"
                    )
                    varys_client.nack_message(message)
                    continue

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
                    log.error(
                        f"Failed to update Onyx with BAM URI for UUID: {payload['match_uuid']}"
                    )
                    varys_client.nack_message(message)
                    continue

                log.info(f"Successfully updated Onyx for {record['climb_id']}")

            varys_client.acknowledge_message(message)

    except BaseException:
        varys_client.close()
        time.sleep(1)
        log.exception("Shutting down chimera runner due to exception:")


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
    parser.add_argument("--nxf_image", default="quay.io/climb-tre/nextflow:25.04.7")
    parser.add_argument("--logfile", type=Path, default=Path("chimera_runner.log"))
    parser.add_argument("--log_level", type=str, default="DEBUG")
    parser.add_argument(
        "--nxf_config",
        type=Path,
        help="Path to nextflow config file",
        required=True,
    )
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
