#!/usr/bin/env python3
"""Repair the nested `alignment_results` and `sylph_results` fields on
chimera Onyx records.

Background: `onyx_update()` gated its `client.update()` call on `fields`
being truthy, so the clear-only wipe that `handle_alignment_report()` /
`handle_sylph_report()` do before writing never reached Onyx. Nested rows
are matched on their identifier (`unique_accession` for alignment_results),
so a re-run updated the rows it still had and left everything else behind
as stale leftovers.

The two fields need different treatment, so they are separate subcommands
and can be run independently.

  alignment  Finds affected records by asking Onyx directly:

                 alignment_results__total_score__isnull=true

             `total_score` is an IntegerField(null=True) added by migration
             0038, so a NULL there marks a row that predates the scoring
             matrix and was never replaced. Rows are read back FROM ONYX,
             those with a null total_score are dropped, the field is wiped
             and the survivors rewritten. The report TSV is never consulted
             - the surviving rows in Onyx are already correct.

  sylph      Takes a CSV of records and rebuilds sylph_results in full from
             each record's sylph report TSV in S3, with no filtering.

Dry-run is the default for both; pass --commit to actually write.

Usage:
    python scripts/repair_chimera_fields.py alignment
    python scripts/repair_chimera_fields.py alignment --commit --failed-list failed.txt
    python scripts/repair_chimera_fields.py sylph records.csv --commit
"""

import argparse
import csv
import io
import logging
import os
import sys
import time
from itertools import batched
from math import ceil

from botocore.exceptions import ClientError
from onyx import OnyxClient
from onyx.exceptions import OnyxClientError, OnyxRequestError

from roz_scripts.utils.config import ConfigError, load_config, project_bucket
from roz_scripts.utils.utils import (
    get_onyx_credentials,
    get_s3_client,
    get_s3_credentials,
    init_logger,
    onyx_update,
)

# Matches the batch size chimera_runner uses for both fields.
BATCH_SIZE = 100

SYLPH_SUFFIX = "sylph_taxonomy_report.tsv"

# The Onyx filter that identifies affected records. The value must be the
# lowercase string: the server builds its accepted booleans with
# str(value).lower(), and requests serialises a Python True as "True",
# which would be rejected.
ALIGNMENT_STALE_FILTER = {"alignment_results__total_score__isnull": "true"}

ALIGNMENT_FILTER_SUBFIELD = "total_score"

# Subfields of alignment_results that may be written back, taken from the
# add/change permission set in onyx-projects/mscape/project.json. Rows read
# from Onyx are projected onto this list before being rewritten so that any
# server-managed key in the response cannot cause a 400.
ALIGNMENT_WRITABLE_SUBFIELDS = [
    "taxon_id",
    "human_readable",
    "segment",
    "unique_accession",
    "accession_description",
    "sequence_length",
    "evenness_value",
    "mean_depth",
    "coverage_1x",
    "coverage_10x",
    "mapped_reads",
    "uniquely_mapped_reads",
    "mapped_bases",
    "mean_read_identity",
    "read_duplication_rate",
    "forward_proportion",
    "mean_read_length",
    "mean_alignment_length",
    "mean_alignment_proportion",
    "mean_alignment_complexity",
    "total_score",
    "confidence",
]

# Onyx sylph_results field -> column in the sylph taxonomy report TSV.
# Kept in step with the mapping inlined in
# chimera_runner.handle_sylph_report() - this script is throwaway, so the
# duplication lives here rather than as a refactor of the pipeline.
SYLPH_FIELD_MAP = {
    "taxon_id": "tax_id",
    "human_readable": "human_readable",
    "gtdb_taxon_string": "taxon_string",
    "gtdb_assembly_id": "contig_id",
    "gtdb_contig_header": "Contig_name",
    "taxonomic_abundance": "Taxonomic_abundance",
    "sequence_abundance": "Sequence_abundance",
    "adjusted_ani": "Adjusted_ANI",
    "ani_confidence_interval": "ANI_5-95_percentile",
    "effective_coverage": "Eff_cov",
    "effective_coverage_confidence_interval": "Lambda_5-95_percentile",
    "median_kmer_cov": "Median_cov",
    "mean_kmer_cov": "Mean_cov_geq1",
    "containment_index": "Containment_ind",
    "naive_ani": "Naive_ANI",
    "kmers_reassigned": "kmers_reassigned",
}


# --------------------------------------------------------------------------
# Shared
# --------------------------------------------------------------------------


def write_nested_field(
    climb_id: str, project: str, field: str, rows: list, log
) -> bool:
    """Wipe `field` then rewrite it from `rows`, batched as chimera does."""

    payload = {"project": project, "climb_id": climb_id}

    clear_fail, _, payload = onyx_update(
        payload=payload,
        fields=None,
        log=log,
        clear_fields=[field],
    )

    if clear_fail:
        log.error(
            f"{climb_id}: failed to wipe {field}, not writing new rows. "
            f"Onyx errors: {payload.get('onyx_update_errors')}"
        )
        return False

    total_batches = ceil(len(rows) / BATCH_SIZE) if rows else 0

    for batch_no, batch in enumerate(batched(rows, BATCH_SIZE), start=1):
        log.debug(
            f"{climb_id}: writing {field} batch {batch_no}/{total_batches} "
            f"({len(batch)} rows)"
        )

        update_fail, _, payload = onyx_update(
            payload=payload,
            fields={field: batch},
            log=log,
        )

        if update_fail:
            # The wipe already landed, so bailing here leaves a partial set.
            log.error(
                f"{climb_id}: {field} batch {batch_no}/{total_batches} failed "
                f"after wipe - field now holds a PARTIAL set "
                f"({(batch_no - 1) * BATCH_SIZE} of {len(rows)} rows). "
                f"Onyx errors: {payload.get('onyx_update_errors')}"
            )
            return False

    return True


def _format_duration(seconds: float) -> str:
    """Render a duration as h/m/s, dropping the units that are zero."""

    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


class Progress:
    """Tracks per-record outcomes and reports rate/ETA as the run proceeds."""

    def __init__(self, total: int, log):
        self.total = total
        self.log = log
        self.done = 0
        self.succeeded = 0
        self.failed = 0
        self.rows_written = 0
        self.started = time.monotonic()

    def record(self, climb_id: str, ok: bool, rows: int) -> None:
        self.done += 1

        if ok:
            self.succeeded += 1
            self.rows_written += rows
        else:
            self.failed += 1

        elapsed = time.monotonic() - self.started
        rate = self.done / elapsed if elapsed > 0 else 0
        remaining = (self.total - self.done) / rate if rate > 0 else 0

        self.log.info(
            f"[{self.done}/{self.total}] {climb_id} "
            f"{'ok' if ok else 'FAILED'} | "
            f"{self.succeeded} ok, {self.failed} failed | "
            f"{rate * 60:.1f} rec/min | "
            f"elapsed {_format_duration(elapsed)}, "
            f"eta {_format_duration(remaining)}"
        )

    def summary(self) -> str:
        elapsed = time.monotonic() - self.started

        return (
            f"Done in {_format_duration(elapsed)}: "
            f"{self.succeeded} succeeded, {self.failed} failed, "
            f"{self.rows_written} nested rows written"
        )


# --------------------------------------------------------------------------
# alignment_results
# --------------------------------------------------------------------------


def is_blank_score(row: dict) -> bool:
    """A stale row is one whose total_score was never populated.

    Checked against None explicitly rather than by truthiness, so a
    legitimate total_score of 0 is not mistaken for a missing one.
    """

    value = row.get(ALIGNMENT_FILTER_SUBFIELD)

    return value is None or (isinstance(value, str) and not value.strip())


def find_affected_climb_ids(project: str, log) -> list[str] | None:
    """Ask Onyx which records still hold alignment_results rows with a null
    total_score."""

    log.info(
        f"Querying Onyx for records matching {ALIGNMENT_STALE_FILTER} "
        f"in project '{project}'"
    )

    try:
        with OnyxClient(config=get_onyx_credentials()) as client:
            records = list(
                client.filter(
                    project,
                    fields=dict(ALIGNMENT_STALE_FILTER),
                    include=["climb_id"],
                )
            )
    except (OnyxRequestError, OnyxClientError) as e:
        log.error(f"Onyx filter failed: {e}")
        return None
    except Exception as e:
        log.error(f"Unhandled error querying Onyx: {e}")
        return None

    # A record is returned once per matching nested row, so de-duplicate.
    seen = set()
    climb_ids = []

    for record in records:
        climb_id = record.get("climb_id")

        if climb_id and climb_id not in seen:
            seen.add(climb_id)
            climb_ids.append(climb_id)

    log.info(
        f"Onyx returned {len(records)} matching row(s) across "
        f"{len(climb_ids)} record(s)"
    )

    return climb_ids


def fetch_alignment_rows(project: str, climb_id: str, log) -> list[dict] | None:
    """Read a record's current alignment_results rows back from Onyx."""

    try:
        with OnyxClient(config=get_onyx_credentials()) as client:
            record = client.get(
                project, climb_id, include=["climb_id", "alignment_results"]
            )
    except OnyxRequestError as e:
        log.error(f"{climb_id}: Onyx get failed: {e}")
        return None
    except Exception as e:
        log.error(f"{climb_id}: unhandled error reading record: {e}")
        return None

    rows = record.get("alignment_results")

    if rows is None:
        log.error(
            f"{climb_id}: response carried no alignment_results field - "
            "cannot tell an empty field from one that was not returned, "
            "so not touching it"
        )
        return None

    return list(rows)


def repair_alignment_results(
    climb_id: str, project: str, commit: bool, allow_empty: bool, log
) -> tuple[bool, int]:
    """Drop alignment_results rows with a null total_score, keep the rest."""

    rows = fetch_alignment_rows(project, climb_id, log)

    if rows is None:
        return (False, 0)

    kept = [row for row in rows if not is_blank_score(row)]
    dropped = len(rows) - len(kept)

    if not dropped:
        log.info(
            f"{climb_id}: alignment_results - no rows with a blank "
            f"{ALIGNMENT_FILTER_SUBFIELD}, nothing to do"
        )
        return (True, 0)

    if not kept and not allow_empty:
        log.error(
            f"{climb_id}: all {len(rows)} alignment_results rows have a blank "
            f"{ALIGNMENT_FILTER_SUBFIELD}, so the repair would leave the field "
            "empty. Pass --allow-empty if that is intended."
        )
        return (False, 0)

    projected = [
        {k: row[k] for k in ALIGNMENT_WRITABLE_SUBFIELDS if k in row} for row in kept
    ]

    log.info(
        f"{climb_id}: alignment_results - {len(rows)} rows in Onyx, "
        f"{dropped} dropped for a blank {ALIGNMENT_FILTER_SUBFIELD}, "
        f"{len(projected)} to rewrite"
    )

    if not commit:
        return (True, len(projected))

    ok = write_nested_field(climb_id, project, "alignment_results", projected, log)

    return (ok, len(projected) if ok else 0)


def run_alignment(args, log) -> list[str]:
    """alignment subcommand. Returns the CLIMB IDs that failed."""

    if args.climb_ids_from:
        with open(args.climb_ids_from, "rt") as fh:
            climb_ids = [line.strip() for line in fh if line.strip()]

        log.info(f"Read {len(climb_ids)} CLIMB IDs from {args.climb_ids_from}")
    else:
        climb_ids = find_affected_climb_ids(args.project, log)

        if climb_ids is None:
            sys.exit(1)

    if not climb_ids:
        log.info("No affected records found, nothing to do")
        return []

    if args.limit:
        climb_ids = climb_ids[: args.limit]
        log.info(f"Limited to the first {len(climb_ids)} record(s)")

    log.info(f"Repairing alignment_results for {len(climb_ids)} record(s)")

    progress = Progress(len(climb_ids), log)
    failed = []

    for climb_id in climb_ids:
        ok, rows = repair_alignment_results(
            climb_id, args.project, args.commit, args.allow_empty, log
        )

        if not ok:
            failed.append(climb_id)

        progress.record(climb_id, ok, rows)

    log.info(progress.summary())

    return failed


# --------------------------------------------------------------------------
# sylph_results
# --------------------------------------------------------------------------


def sylph_row_to_onyx(row: dict) -> dict:
    """Reshape one sylph taxonomy report row into an Onyx sylph_results row."""

    return {field: row[column] for field, column in SYLPH_FIELD_MAP.items()}


def read_csv_records(
    csv_path: str, id_column: str, uri_column: str | None, log
) -> list[tuple[str, str | None]]:
    """Read (climb_id, report URI or None) pairs from the CSV."""

    with open(csv_path, "rt", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []

        if id_column not in fieldnames:
            log.error(
                f"CSV {csv_path} has no '{id_column}' column "
                f"(found: {fieldnames}). Use --climb-id-column to override."
            )
            sys.exit(1)

        if uri_column and uri_column not in fieldnames:
            log.error(
                f"CSV {csv_path} has no '{uri_column}' column "
                f"(found: {fieldnames})"
            )
            sys.exit(1)

        seen = set()
        records = []

        for row in reader:
            climb_id = (row.get(id_column) or "").strip()

            if not climb_id or climb_id in seen:
                continue

            uri = (row.get(uri_column) or "").strip() if uri_column else None

            if uri_column and not uri:
                log.error(f"{climb_id}: blank '{uri_column}', skipping")
                continue

            seen.add(climb_id)
            records.append((climb_id, uri or None))

    return records


def fetch_tsv_rows(s3_client, s3_uri: str, log) -> tuple[list[dict], list[str]] | None:
    """Fetch a TSV from S3 and return its rows plus its header.

    Returns None if the object is missing or unreadable; an empty row list
    with a populated header is a legitimate result.
    """

    bucket, key = s3_uri.removeprefix("s3://").split("/", 1)

    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            log.info(f"No object at {s3_uri}")
        else:
            log.error(f"Failed to read {s3_uri}: {e}")
        return None
    except UnicodeDecodeError as e:
        log.error(f"{s3_uri} is not valid UTF-8: {e}")
        return None

    reader = csv.DictReader(io.StringIO(body), delimiter="\t")

    return ([row for row in reader], list(reader.fieldnames or []))


def repair_sylph_results(
    s3_client, climb_id: str, s3_uri: str, project: str, commit: bool, log
) -> tuple[bool, int]:
    """Rebuild sylph_results in full from the report TSV."""

    fetched = fetch_tsv_rows(s3_client, s3_uri, log)

    if fetched is None:
        # chimera legitimately produces no sylph report when there were no
        # hits above 95% ANI, and with no report there is nothing to rebuild
        # from - so this is a failure to repair, not a reason to wipe.
        log.error(
            f"{climb_id}: no readable sylph report at {s3_uri} - leaving "
            "sylph_results untouched"
        )
        return (False, 0)

    rows, header = fetched

    missing = [c for c in SYLPH_FIELD_MAP.values() if c not in header]

    if missing:
        log.error(
            f"{climb_id}: sylph report {s3_uri} is missing expected "
            f"column(s): {missing} (columns: {header})"
        )
        return (False, 0)

    try:
        out_rows = [sylph_row_to_onyx(row) for row in rows]
    except KeyError as e:
        log.error(f"{climb_id}: malformed sylph report {s3_uri}, missing {e}")
        return (False, 0)

    log.info(f"{climb_id}: sylph_results - {len(out_rows)} rows to write")

    if not commit:
        return (True, len(out_rows))

    ok = write_nested_field(climb_id, project, "sylph_results", out_rows, log)

    return (ok, len(out_rows) if ok else 0)


def run_sylph(args, log) -> list[str]:
    """sylph subcommand. Returns the CLIMB IDs that failed."""

    bucket = args.bucket

    if not bucket:
        try:
            bucket = project_bucket(
                load_config(args.config), args.project, "chimera_reports"
            )
        except ConfigError as e:
            log.error(f"Could not resolve the chimera reports bucket: {e}")
            log.error("Pass --bucket explicitly, or set --config / $ROZ_CONFIG_JSON")
            sys.exit(1)

    s3_client = get_s3_client(get_s3_credentials(args))

    records = read_csv_records(args.csv, args.climb_id_column, args.uri_column, log)

    if not records:
        log.error(f"No usable rows in {args.csv}")
        sys.exit(1)

    if args.limit:
        records = records[: args.limit]
        log.info(f"Limited to the first {len(records)} record(s)")

    log.info(
        f"Rebuilding sylph_results for {len(records)} record(s), "
        f"reports from s3://{bucket}/"
    )

    progress = Progress(len(records), log)
    failed = []

    for climb_id, uri in records:
        s3_uri = uri or f"s3://{bucket}/{climb_id}.{SYLPH_SUFFIX}"

        ok, rows = repair_sylph_results(
            s3_client, climb_id, s3_uri, args.project, args.commit, log
        )

        if not ok:
            failed.append(climb_id)

        progress.record(climb_id, ok, rows)

    log.info(progress.summary())

    return failed


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--project", default="mscape", help="Onyx project (default: mscape)"
    )
    common.add_argument(
        "--commit",
        action="store_true",
        help="Actually write to Onyx. Without this, nothing is modified.",
    )
    common.add_argument(
        "--limit", type=int, default=None, help="Process at most this many records"
    )
    common.add_argument(
        "--failed-list",
        help="Write the CLIMB IDs that failed to this file, one per line",
    )
    common.add_argument(
        "--logfile",
        default="/tmp/repair_chimera_fields.log",
        help="Log file (default: /tmp/repair_chimera_fields.log)",
    )
    common.add_argument("--log-level", default="INFO")
    common.add_argument(
        "--quiet",
        action="store_true",
        help="Don't mirror progress to the terminal, only to --logfile",
    )

    s3_opts = argparse.ArgumentParser(add_help=False)
    s3_opts.add_argument("--profile", default=None, help="AWS credentials profile")
    s3_opts.add_argument("--access-key", default=None)
    s3_opts.add_argument("--secret-key", default=None)

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    alignment = sub.add_parser(
        "alignment",
        parents=[common],
        help="Drop alignment_results rows with a blank total_score",
        description=(
            "Finds affected records via the Onyx filter "
            f"{ALIGNMENT_STALE_FILTER}, reads their alignment_results rows "
            "back from Onyx, drops the ones with a blank total_score and "
            "rewrites the survivors. Does not touch S3."
        ),
    )
    alignment.add_argument(
        "--climb-ids-from",
        default=None,
        help=(
            "Read CLIMB IDs (one per line) from this file instead of querying "
            "Onyx. For retrying a previous run's --failed-list."
        ),
    )
    alignment.add_argument(
        "--allow-empty",
        action="store_true",
        help=(
            "Permit wiping alignment_results to empty when every row has a "
            f"blank {ALIGNMENT_FILTER_SUBFIELD}"
        ),
    )
    alignment.set_defaults(func=run_alignment)

    sylph = sub.add_parser(
        "sylph",
        parents=[common, s3_opts],
        help="Rebuild sylph_results in full from the report TSVs in S3",
        description=(
            "Takes a CSV of records and rebuilds each one's sylph_results "
            "from its sylph report TSV in S3, with no filtering."
        ),
    )
    sylph.add_argument("csv", help="CSV of records to rebuild")
    sylph.add_argument(
        "--climb-id-column",
        default="climb_id",
        help="CSV column holding the CLIMB ID (default: climb_id)",
    )
    sylph.add_argument(
        "--uri-column",
        default=None,
        help=(
            "CSV column holding the sylph report S3 URI. By default the URI "
            "is derived as s3://<bucket>/<climb_id>." + SYLPH_SUFFIX
        ),
    )
    sylph.add_argument(
        "--bucket",
        default=None,
        help=(
            "chimera reports bucket. Defaults to the project's "
            "'chimera_reports' bucket from the roz config."
        ),
    )
    sylph.add_argument(
        "--config",
        default=None,
        help="Path to the roz config JSON (default: $ROZ_CONFIG_JSON)",
    )
    sylph.set_defaults(func=run_sylph)

    return parser


def main():
    args = build_parser().parse_args()

    log = init_logger("repair_chimera_fields", args.logfile, args.log_level)

    # init_logger only attaches a file handler, so without this a long run
    # looks like it has hung. Guarded because init_logger caches its logger
    # by name - a second call in the same process would otherwise stack up
    # another handler and double every line.
    # logging.FileHandler subclasses StreamHandler, so the file handler
    # init_logger attached must be excluded from this check.
    already_streaming = any(
        isinstance(h, logging.StreamHandler)
        and not isinstance(h, logging.FileHandler)
        for h in log.handlers
    )

    if not args.quiet and not already_streaming:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )
        log.addHandler(stream_handler)

    # The alignment subcommand reads Onyx even on a dry run, so both
    # subcommands need credentials regardless of --commit.
    for var in ("ONYX_DOMAIN", "ONYX_TOKEN"):
        if not os.getenv(var):
            log.error(f"{var} is not set")
            sys.exit(1)

    if not args.commit:
        log.info(f"DRY RUN ({args.command}) - nothing will be written. Pass --commit to apply.")

    failed = args.func(args, log)

    if failed and args.failed_list:
        with open(args.failed_list, "wt") as fh:
            fh.write("\n".join(failed) + "\n")

        retry = (
            f" (retry with: {args.command} --climb-ids-from {args.failed_list})"
            if args.command == "alignment"
            else ""
        )
        log.info(f"Wrote {len(failed)} failed CLIMB IDs to {args.failed_list}{retry}")
    elif failed:
        log.info("Rerun with --failed-list to capture the failed records for a retry")

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
