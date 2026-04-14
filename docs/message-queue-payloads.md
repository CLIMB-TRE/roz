# Message Queue Payload Structures

This document describes the RabbitMQ (AMQP) message payload structures used across the roz ingest pipeline for each project. Messages are JSON-serialised dictionaries exchanged via the **Varys** client library.

**Queue addressing**: each queue is identified by an exchange name and a queue suffix (worker pool name). Varys routes messages to `{exchange}.{queue_suffix}`.

---

## Contents

1. [S3 Matcher](#1-s3-matcher)
2. [Generic Ingest (pre-project validator)](#2-generic-ingest-pre-project-validator)
3. [mscape](#3-mscape)
4. [synthscape / openmgs](#4-synthscape--openmgs)
5. [pathsafe](#5-pathsafe)
6. [Shared Payload Reference](#6-shared-payload-reference)

---

## 1. S3 Matcher

`roz_scripts/general/s3_matcher.py` — listens for S3 object-creation events and accumulates them until a complete set of files for an artifact has been seen, then emits a single matched-artifact payload.

### 1.1 Input — `inbound-s3` / `s3_matcher`

Raw S3 event notifications (see [S3 Notification Format](#s3-notification-format)). A new file is registered in an in-memory dict keyed by `(artifact, project, site, platform, test_flag)`. No output is produced until the artifact is complete.

### 1.2 Output (complete artifact) — `inbound-matched` / `s3_matcher`

Emitted once all required files for an artifact have been seen.

| Field | Type | Description |
|-------|------|-------------|
| `uuid` | string (UUID4) | Unique identifier for this match event |
| `site` | string | Submitting site name (short form, e.g. `"bham"`) |
| `raw_site` | string | Submitting site as parsed from the bucket name (may include domain prefix) |
| `uploaders` | array[string] | Deduplicated list of uploader IDs that contributed files |
| `match_timestamp` | integer | Unix timestamp in nanoseconds when the match was made |
| `artifact` | string | Composite artifact identifier: `{project}.{run_index}.{run_id}` |
| `run_index` | string | Run index identifier (plain, not anonymised) |
| `run_id` | string | Run identifier (plain, not anonymised) |
| `project` | string | Project name: `mscape`, `synthscape`, `openmgs`, or `pathsafe` |
| `platform` | string | Sequencing platform: `ont`, `illumina`, or `illumina.se` |
| `test_flag` | boolean | `true` if this is a test submission |
| `files` | object | Map of file extension → file descriptor (see [File Descriptor](#file-descriptor)) |

**`files` keys by platform:**

| Platform | Keys present |
|----------|-------------|
| `ont` / `illumina.se` | `.csv`, `.fastq.gz` |
| `illumina` | `.csv`, `.1.fastq.gz`, `.2.fastq.gz` |

### 1.3 Output (parse failure) — `inbound-results-{project}-{site}` / `s3_matcher`

Sent as a **plain string** (not JSON) when the object key cannot be parsed or the project name in the key does not match the bucket. No payload dict is constructed; the message is a human-readable error string describing the problem.

---

## 2. Generic Ingest (pre-project validator)

`roz_scripts/general/ingest.py` — receives matched artifacts and performs three sequential pre-checks before forwarding to the appropriate project validator: (1) Onyx test-create, (2) character validation on `run_index`/`run_id`, (3) CSV field checks.

### 2.1 Input — `inbound-matched` / `ingest`

The matched artifact payload from §1.2 (all fields unchanged).

### 2.2 Output (all checks passed) — `inbound-to_validate-{project}` / `ingest`

The §1.2 payload with three fields appended:

| Field | Type | Description |
|-------|------|-------------|
| `validate` | boolean | `true` — signals downstream that all pre-checks passed |
| `onyx_test_create_status` | boolean | `true` — Onyx test-create succeeded |
| `biosample_id` | string | `biosample_id` value read directly from the submitted CSV |

### 2.3 Output (pre-check failure) — `inbound-results-{project}-{site}` / `s3_matcher`

Sent when the Onyx test-create fails, `run_index`/`run_id` contain invalid characters, or CSV field checks fail. The message is the accumulated payload dict with `validate: false`. A result JSON is also written to S3.

### 2.4 Output (alert) — `restricted-{project}-alert` / `ingest`

Sent on any unexpected/unrecoverable error (e.g. Onyx connection failure). Contains the accumulated payload dict at the point of failure. The message is nack'd and not requeued automatically.

---

## 3. mscape

The mscape validator (`roz_scripts/mscape/mscape_ingest_validation.py`) runs the Scylla metagenomics pipeline and updates Onyx with results.

### 3.1 Input — `inbound-to_validate-mscape` / `validator`

The payload from §2.2 (all fields from §1.2 plus `validate`, `onyx_test_create_status`, `biosample_id`). Messages for other projects are silently ignored.

### 3.2 Output (result) — `inbound-results-mscape-{site}` / `validator`

Sent on both success and non-rerunnable failure. Contains the full accumulated payload. Additional fields set during validation:

| Field | Type | Description |
|-------|------|-------------|
| `low_priority` | boolean | `true` if the job was submitted as a rerun/low-priority task |
| `rerun` | boolean | `true` if the failure is considered transient and eligible for retry |
| `ingest_errors` | array[string] | Human-readable validation error messages (absent if no errors) |
| `ingest_warnings` | array[string] | Human-readable warning messages (absent if no warnings) |
| `onyx_warnings` | object | Warnings from Onyx reconciliation. Shape: `{"reconcile_errors": [string]}` |
| `test_ingest_result` | boolean | Present only when `test_flag` is `true`; `true` on pipeline success |
| `onyx_create_status` | boolean | `true` if the Onyx record was created (non-test submissions only) |
| `created` | boolean | Mirrors `onyx_create_status`; `true` after successful Onyx creation |
| `published` | boolean | `true` after the Onyx record has been published (`is_published: true`) |
| `climb_id` | string | CLIMB ID assigned by Onyx (set on successful submission) |
| `anonymised_run_id` | string | Onyx-anonymised run ID |
| `anonymised_run_index` | string | Onyx-anonymised run index |
| `anonymised_biosample_id` | string | Onyx-anonymised biosample ID |
| `anonymised_biosample_source_id` | string | Onyx-anonymised biosample source ID (only present if the CSV includes `biosample_source_id`) |
| `scylla_version` | string | Version of the Scylla pipeline that processed this artifact |

### 3.3 Output (new artifact) — `inbound-new_artifact-mscape` / `validator`

Sent on successful validation of a non-test, non-rerun artifact. This is the downstream trigger for further analysis (e.g. chimera detection). Contains only a small subset of fields:

| Field | Type | Description |
|-------|------|-------------|
| `publish_timestamp` | integer | Unix timestamp in nanoseconds at time of publish |
| `climb_id` | string | CLIMB ID |
| `run_id` | string | Anonymised run ID |
| `run_index` | string | Anonymised run index |
| `biosample_id` | string | Anonymised biosample ID |
| `biosample_source_id` | string | Anonymised biosample source ID (only if present) |
| `site` | string | Submitting site |
| `platform` | string | Sequencing platform |
| `match_uuid` | string | UUID from the original matched artifact payload |
| `project` | string | Project name (`mscape`) |

### 3.4 Output (rerun artifact) — `inbound-new_artifact_rerun-mscape` / `validator`

Identical structure to §3.3. Sent instead of §3.3 when `low_priority` is `true` (i.e. the artifact was submitted as a rerun).

### 3.5 Output (alert) — `mscape-restricted-announce` / `alert`

The full payload dict forwarded on unexpected errors requiring manual intervention.

### 3.6 Output (HCID alert) — `mscape-restricted-hcid` / `alert`

Sent once per `.warning.json` file found in the Scylla pipeline QC output. The message body is the parsed content of the warning JSON file with one field appended:

| Field | Type | Description |
|-------|------|-------------|
| `climb_id` | string | CLIMB ID of the record that triggered the HCID alert |

The remaining fields are defined by the Scylla pipeline's HCID warning schema.

### 3.7 Output (dead letter) — `mscape-restricted-announce` / `dead_letter`

Sent when a rerunnable artifact has failed 5 or more consecutive validation attempts. Contains the full payload with an additional error entry in `ingest_errors`.

### 3.8 Output (dead worker) — `mscape-restricted-announce` / `dead_worker`

Sent as a plain string (not a JSON payload) when a worker process crashes with an unhandled exception. Format: `"mscape ingest worker failed with unhandled exception: {exception}"`.

---

## 4. synthscape / openmgs

synthscape and openmgs use **the same validator code** as mscape (`roz_scripts/mscape/mscape_ingest_validation.py`), invoked with `--project synthscape` or `--project openmgs`. All payload structures and queue names are identical to mscape with the project name substituted.

### Queue mapping

| mscape queue | synthscape equivalent | openmgs equivalent |
|---|---|---|
| `inbound-to_validate-mscape` / `validator` | `inbound-to_validate-synthscape` / `validator` | `inbound-to_validate-openmgs` / `validator` |
| `inbound-results-mscape-{site}` / `validator` | `inbound-results-synthscape-{site}` / `validator` | `inbound-results-openmgs-{site}` / `validator` |
| `inbound-new_artifact-mscape` / `validator` | `inbound-new_artifact-synthscape` / `validator` | `inbound-new_artifact-openmgs` / `validator` |
| `inbound-new_artifact_rerun-mscape` / `validator` | `inbound-new_artifact_rerun-synthscape` / `validator` | `inbound-new_artifact_rerun-openmgs` / `validator` |
| `mscape-restricted-announce` / `alert` | `synthscape-restricted-announce` / `alert` | `openmgs-restricted-announce` / `alert` |
| `mscape-restricted-hcid` / `alert` | `synthscape-restricted-hcid` / `alert` | `openmgs-restricted-hcid` / `alert` |
| `mscape-restricted-announce` / `dead_letter` | `synthscape-restricted-announce` / `dead_letter` | `openmgs-restricted-announce` / `dead_letter` |
| `mscape-restricted-announce` / `dead_worker` | `synthscape-restricted-announce` / `dead_worker` | `openmgs-restricted-announce` / `dead_worker` |

All payload field definitions from §3.2 – §3.8 apply unchanged; only `project` value differs.

---

## 5. pathsafe

The pathsafe validator (`roz_scripts/pathsafe/pathsafe_validation.py`) runs an assembly pipeline (etoki/SPAdes via Nextflow), submits the assembly to Pathogenwatch, and uploads the result to S3.

### 5.1 Input — `inbound-to_validate-pathsafe` / `validator`

The payload from §2.2. Messages with `project != "pathsafe"` are silently ignored. pathsafe is **Illumina only** and always expects `.1.fastq.gz` and `.2.fastq.gz` in `files`.

### 5.2 Output (result) — `inbound-results-pathsafe-{site}` / `validator`

Sent on both success and non-rerunnable failure. Additional fields set during validation:

| Field | Type | Description |
|-------|------|-------------|
| `rerun` | boolean | `true` if the failure is transient and eligible for retry |
| `ingest_errors` | array[string] | Human-readable error messages (absent if no errors) |
| `test_ingest_result` | boolean | Present only when `test_flag` is `true`; `true` on pipeline success |
| `created` | boolean | `true` after successful Onyx record creation |
| `published` | boolean | `true` after the Onyx record has been published |
| `climb_id` | string | CLIMB ID assigned by Onyx |
| `anonymised_run_id` | string | Onyx-anonymised run ID |
| `anonymised_run_index` | string | Onyx-anonymised run index |
| `anonymised_biosample_id` | string | Onyx-anonymised biosample ID |
| `assembly_presigned_url` | string | Pre-signed S3 URL (24 h TTL) for the uploaded assembly FASTA (present on success) |

### 5.3 Output (new artifact) — `inbound-new_artifact-pathsafe` / `validator`

Sent on successful validation of a non-test artifact. Same structure as the mscape new artifact payload (§3.3) but without `biosample_source_id` and with `project: "pathsafe"`. pathsafe does not distinguish rerun vs. standard new artifact queues.

| Field | Type | Description |
|-------|------|-------------|
| `publish_timestamp` | integer | Unix timestamp in nanoseconds |
| `climb_id` | string | CLIMB ID |
| `run_id` | string | Anonymised run ID |
| `run_index` | string | Anonymised run index |
| `biosample_id` | string | Anonymised biosample ID |
| `site` | string | Submitting site |
| `platform` | string | Sequencing platform (always `illumina` for pathsafe) |
| `match_uuid` | string | UUID from the original matched artifact payload |
| `project` | string | `"pathsafe"` |

### 5.4 Output (dead letter) — `pathsafe-restricted-announce` / `dead_letter`

Sent after 5 consecutive rerunnable failures. Full payload with an error appended to `ingest_errors`.

### 5.5 Output (dead worker) — `pathsafe-restricted-announce` / `dead_worker`

Plain string message (not JSON) sent on unhandled worker exception. Format: `"Pathsafe ingest worker failed with unhandled exception: {exception}"`.

### 5.6 Onyx CSV update input — `inbound-onyx-updates-pathsafe` / `pathsafe_updater`

Produced by `s3_onyx_updates.py` (see §5.2) when a CSV update is received for a pathsafe record. The payload is a slim dict built from the S3 notification:

| Field | Type | Description |
|-------|------|-------------|
| `climb_id` | string | CLIMB ID identified from Onyx via `run_index` + `run_id` |
| `project` | string | `"pathsafe"` |
| `run_index` | string | Plain run index |
| `run_id` | string | Plain run ID |
| `site` | string | Short site name |
| `site_str` | string | Site string as parsed from bucket name |
| `artifact` | string | `{project}.{run_index}.{run_id}` |
| `uuid` | string | `"fake_uuid"` (placeholder; no match event for update messages) |
| `files` | object | Single entry: `{".csv": {uri, etag, key}}` |
| `update_status` | string | `"success"` or `"failed"` |
| `update_errors` | array[string] | Error messages (present only on failure) |

### 5.7 Pathogenwatch update output — `inbound-onyx-updates` / `onyx_updates`

Produced by `pathsafe_updates.py` after a successful Pathogenwatch metadata sync. Same shape as §4.6 but with `climb_id` removed before the S3 result write, and `update_status: "success"` guaranteed.

---

## 6. Shared Payload Reference

### File Descriptor

Each entry in the `files` map uses this structure:

```json
{
  "uri": "s3://{bucket}/{key}",
  "etag": "{etag-without-quotes}",
  "key": "{object-key-in-bucket}",
  "submitter": "{uploader-id}",
  "parsed_fname": {
    "project": "string",
    "run_index": "string",
    "run_id": "string",
    "direction": "1 or 2  (illumina paired only)",
    "ftype": "csv | fastq"
  }
}
```

The `submitter` and `parsed_fname` fields are only present on entries within the matched artifact payload (§1.1). The slim CSV-update payload (§4.6) omits them.

### S3 Notification Format

The `inbound-s3` exchange carries raw S3 event notifications forwarded from Ceph. Both `s3_matcher.py` and `s3_onyx_updates.py` consume this exchange (different queue suffixes).

```json
{
  "Records": [
    {
      "eventVersion": "2.2",
      "eventSource": "ceph:s3",
      "eventTime": "ISO-8601 timestamp",
      "eventName": "ObjectCreated:Put",
      "userIdentity": { "principalId": "string" },
      "s3": {
        "bucket": {
          "name": "string",
          "ownerIdentity": { "principalId": "string" },
          "arn": "arn:aws:s3:::{bucket}"
        },
        "object": {
          "key": "string",
          "size": "integer",
          "eTag": "string",
          "versionId": "string",
          "sequencer": "string",
          "metadata": [
            { "key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD" },
            { "key": "x-amz-date", "val": "ISO-8601" }
          ],
          "tags": []
        }
      }
    }
  ]
}
```

### S3 CSV Update Payload (`inbound-onyx-updates-{project}` / `onyx_updates`)

Produced by `s3_onyx_updates.py` for any project with `csv_updates` enabled in `config.json`. Consumed by per-project update handlers.

| Field | Type | Description |
|-------|------|-------------|
| `climb_id` | string | CLIMB ID identified via Onyx |
| `project` | string | Project name |
| `run_index` | string | Plain run index |
| `run_id` | string | Plain run ID |
| `site` | string | Short site name |
| `site_str` | string | Site string as parsed from bucket name |
| `artifact` | string | `{project}.{run_index}.{run_id}` |
| `update_status` | string | `"success"` or `"failed"` |

### Queue Suffix Summary

| Queue suffix | Consumer | Receives from | Purpose |
|---|---|---|---|
| `s3_matcher` | `s3_matcher.py` | `inbound-s3` | Match files into complete artifacts |
| `ingest` | `ingest.py` | `inbound-matched` | Pre-project validation (test-create, field checks) |
| `validator` | `{project}_validation.py` | `inbound-to_validate-{project}` | Full project-specific pipeline validation |
| `onyx_updater` | `s3_onyx_updates.py` | `inbound-s3` | Handle CSV metadata update notifications |
| `onyx_updates` | Onyx update handler | `inbound-onyx-updates-{project}` / `inbound-onyx-updates` | Apply metadata updates to Onyx |
| `pathsafe_updater` | `pathsafe_updates.py` | `inbound-onyx-updates-pathsafe` | Sync updated metadata to Pathogenwatch |
| `alert` | Alert handler | `{project}-restricted-announce`, `{project}-restricted-hcid`, `restricted-{project}-alert` | Deliver alerts for manual review |
| `dead_letter` | Dead letter handler | `{project}-restricted-announce` | Receive permanently failed messages |
| `dead_worker` | Dead worker handler | `{project}-restricted-announce` | Receive crashed worker notifications |
