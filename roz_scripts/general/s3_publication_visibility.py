#!/usr/bin/env python3
"""Reconcile published-bucket object visibility against Onyx publication state.

Published-bucket objects are readable by sites only while they carry
roz-visibility=visible (enforced by a Condition on the bucket policy - see
s3_controller._bucket_is_tag_gated). This service keeps that tag in step with
each record's is_published flag, so that suppressing a record in Onyx withdraws
access to its files, and republishing restores it.

Nothing here deletes an object, and nothing here ever will: the module contains
no delete_object call of any kind, and a test asserts that. The only destructive
-sounding call it makes is delete_object_tagging, which removes access and is
fully reversible by re-tagging.

Design notes worth knowing before changing this:

* The pass is STATELESS. Every decision is recomputed from Onyx and the bucket
  listing each time. There is no state file to corrupt, no previous-pass set to
  disagree with, and a wrong decision self-corrects on the next pass.

* "No opinion" is a first-class verdict. An object whose key does not parse to a
  climb_id, or whose climb_id Onyx has never heard of, is LEFT EXACTLY AS IT IS
  by default. Those are the classes most likely to indicate that something else
  is wrong, and acting on them is how you turn a mapping bug into an outage.

* Any Onyx failure aborts the whole pass before a single tag is written. A
  partial or failed Onyx view makes every record look unpublished, which would
  mass-hide published data. build_publication_map reports check-failure
  separately from "no records" precisely so this code can tell them apart.
"""

import argparse
import json
import os
import re
import sys

from botocore.exceptions import ClientError

from roz_scripts.utils.utils import (
    PUBLICATION_VISIBILITY_TAG_KEY,
    PUBLICATION_VISIBLE,
    build_publication_map,
    get_onyx_credentials,
    get_s3_client,
    get_s3_credentials,
    init_logger,
    onyx_get_record,
    send_admin_alert,
    PRIORITY_CRITICAL,
    PRIORITY_ROUTINE,
)
from roz_scripts.utils.config import load_config, ConfigError
from roz_scripts.general.s3_controller import _bucket_is_tag_gated, create_config_map

# climb_id is "C-" followed by alphanumerics, max length 12 overall. The "-" is
# INSIDE the identifier and is not a delimiter, so it must not be split on.
CLIMB_ID_PATTERN = re.compile(r"^C-[A-Za-z0-9]{1,10}$")

# Every published key is built from the climb_id and then a separator: either a
# path separator for the prefixed layouts ({climb_id}/{climb_id}_...), or "." or
# "_" for the flat ones. None of those three can occur inside a climb_id.
KEY_DELIMITERS = "/._"

# Written by the permission canary in s3_controller; never a real artifact.
CANARY_KEY = "test"


def climb_id_from_key(key: str) -> str | None:
    """Extract the climb_id a published-bucket key belongs to, or None

    Returns None rather than guessing. A None here means "no opinion", which
    keeps the object exactly as it is - deliberately, because the dangerous
    failure is not a key that fails to parse, it is a key that parses to the
    WRONG climb_id and so inherits another record's publication state. A
    suppressed record's file left visible is a data exposure; a published file
    left as-is is not.
    """

    if not key or key == CANARY_KEY:
        return None

    candidate = key

    for index, char in enumerate(key):
        if char in KEY_DELIMITERS:
            candidate = key[:index]
            break

    if CLIMB_ID_PATTERN.match(candidate):
        return candidate

    return None


def desired_visibility(key: str, publication_map: dict) -> tuple[bool | None, str]:
    """Decide whether an object should be visible

    Returns (verdict, reason). A verdict of None means "no opinion, leave the
    object's tag exactly as it is".
    """

    climb_id = climb_id_from_key(key)

    if climb_id is None:
        return (None, "unparseable")

    if climb_id not in publication_map:
        return (None, "no_record")

    if publication_map[climb_id]:
        return (True, "published")

    return (False, "unpublished")


def plan_tag_changes(
    objects: list,
    publication_map: dict,
    hide_unmappable: bool = False,
    hide_orphans: bool = False,
) -> tuple[list, list, dict]:
    """Work out which objects must be shown and which hidden

    Pure: no I/O, no clock. Takes [(key, currently_visible)] and returns
    (to_show, to_hide, counts_by_reason).
    """

    to_show = []
    to_hide = []
    counts: dict = {}

    for key, currently_visible in objects:
        verdict, reason = desired_visibility(key, publication_map)

        counts[reason] = counts.get(reason, 0) + 1

        if verdict is None:
            if reason == "unparseable" and hide_unmappable and currently_visible:
                to_hide.append(key)
            elif reason == "no_record" and hide_orphans and currently_visible:
                to_hide.append(key)
            continue

        if verdict and not currently_visible:
            to_show.append(key)
        elif not verdict and currently_visible:
            to_hide.append(key)

    return (to_show, to_hide, counts)


def cross_check_publication_map(
    project: str, publication_map: dict, canary_climb_id: str | None, log
) -> bool:
    """Verify the bulk Onyx scan actually returned a complete, cross-site view

    The whole design assumes one bulk filter sees every site's records. A token
    silently scoped to one site would return a plausible, non-empty map in which
    every other site's records are simply absent - and absent reads as "no
    record", which with --hide-orphans would hide another site's published data.
    An empty-result guard cannot catch that, because the result is not empty.

    So: assert that a record known to belong to another site is present. This is
    the only check that distinguishes a complete map from a confidently
    incomplete one.
    """

    if not publication_map:
        log.error(f"Onyx returned no records at all for project {project}")
        return False

    if not canary_climb_id:
        log.warning(
            f"No cross-check climb_id configured for project {project}; cannot "
            "verify the Onyx view is complete across sites. Set "
            "visibility_canary_climb_id in config."
        )
        return True

    if canary_climb_id in publication_map:
        return True

    # It is absent from the bulk map. Either the map is incomplete, or the record
    # genuinely went away - distinguish, rather than assume.
    check_failed, record = onyx_get_record(project, canary_climb_id, log)

    if check_failed:
        log.error(
            f"Could not verify cross-check record {canary_climb_id} for {project}"
        )
        return False

    if record is None:
        log.error(
            f"Cross-check record {canary_climb_id} for {project} no longer exists; "
            "update visibility_canary_climb_id in config"
        )
        return False

    log.error(
        f"Cross-check FAILED for {project}: record {canary_climb_id} exists in Onyx "
        "but is missing from the bulk scan. The Onyx view is incomplete - most "
        "likely a site-scoped token. Refusing to touch any tags."
    )
    return False


def list_bucket_objects(s3_client, bucket: str, log) -> list:
    """List a bucket, returning [(key, currently_visible)]

    Reads each object's tags. That is one request per object, which dominates the
    pass; it is accepted here rather than cached because a stale cache would
    produce wrong visibility decisions, and correctness beats a cheaper pass on a
    control that gates access to data.
    """

    objects = []

    try:
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if key == CANARY_KEY:
                    continue

                try:
                    tagging = s3_client.get_object_tagging(Bucket=bucket, Key=key)
                    tags = {t["Key"]: t["Value"] for t in tagging.get("TagSet", [])}
                    visible = (
                        tags.get(PUBLICATION_VISIBILITY_TAG_KEY) == PUBLICATION_VISIBLE
                    )
                except ClientError as e:
                    log.error(f"Could not read tags for s3://{bucket}/{key}: {e}")
                    continue

                objects.append((key, visible))

    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchBucket":
            log.warning(f"Bucket {bucket} does not exist, skipping")
            return []
        raise

    return objects


def apply_tag_changes(
    s3_client, bucket: str, to_show: list, to_hide: list, log, commit: bool
) -> dict:
    """Apply the planned visibility changes. Never raises."""

    from roz_scripts.utils.utils import set_object_visibility

    applied = {"shown": 0, "hidden": 0, "failed": 0}

    if not commit:
        return applied

    for key in to_show:
        if set_object_visibility(s3_client, bucket, key, True, log):
            applied["shown"] += 1
        else:
            applied["failed"] += 1

    for key in to_hide:
        if set_object_visibility(s3_client, bucket, key, False, log):
            applied["hidden"] += 1
            log.info(f"HID s3://{bucket}/{key}")
        else:
            applied["failed"] += 1

    return applied


def gated_buckets(config_map: dict, config_dict: dict) -> list:
    """Every tag-gated project bucket, as [(project, bucket, bucket_arn)]

    Driven solely by the config opt-in, the same source of truth the policy
    generator uses - so a bucket cannot be gated in the policy but invisible to
    the reconciler, or vice versa.
    """

    gated = []

    for project, project_config in config_map.items():
        for bucket, bucket_arn in sorted(project_config["project_buckets"]):
            if _bucket_is_tag_gated(config_dict, project, bucket):
                gated.append((project, bucket, bucket_arn))

    return gated


def run_pass(args, log) -> int:
    """Run one reconciliation pass. Returns a process exit code."""

    try:
        config_dict = load_config(args.config)
    except ConfigError as e:
        log.error(f"Failed to load config: {e}")
        return 1

    config_map = create_config_map(config_dict)

    targets = gated_buckets(config_map, config_dict)

    if not targets:
        log.info("No tag-gated buckets configured, nothing to do")
        return 0

    s3_client = get_s3_client(get_s3_credentials())

    publication_maps = {}

    for project in sorted({project for project, _, _ in targets}):
        check_failed, publication_map = build_publication_map(project, log)

        if check_failed:
            log.error(
                f"Onyx lookup failed for project {project}; aborting the pass "
                "without writing any tags"
            )
            send_admin_alert(
                None,
                source="s3-publication-visibility",
                description=(
                    f"Onyx lookup failed for {project}; visibility pass aborted "
                    "with no changes"
                ),
                priority=PRIORITY_CRITICAL,
            )
            return 1

        canary = (
            config_dict["configs"][project].get("visibility_canary_climb_id")
        )

        if not cross_check_publication_map(project, publication_map, canary, log):
            send_admin_alert(
                None,
                source="s3-publication-visibility",
                description=(
                    f"Onyx cross-check failed for {project}; visibility pass "
                    "aborted with no changes"
                ),
                priority=PRIORITY_CRITICAL,
            )
            return 1

        publication_maps[project] = publication_map

    summary = {}

    for project, bucket, bucket_arn in targets:
        objects = list_bucket_objects(s3_client, bucket_arn, log)

        to_show, to_hide, counts = plan_tag_changes(
            objects,
            publication_maps[project],
            hide_unmappable=args.hide_unmappable,
            hide_orphans=args.hide_orphans,
        )

        applied = apply_tag_changes(
            s3_client, bucket_arn, to_show, to_hide, log, args.commit
        )

        missing_tag = sum(
            1
            for key, visible in objects
            if not visible and desired_visibility(key, publication_maps[project])[0]
        )

        summary[bucket_arn] = {
            "project": project,
            "objects": len(objects),
            "classes": counts,
            "to_show": len(to_show),
            "to_hide": len(to_hide),
            "applied": applied,
            "published_objects_missing_tag": missing_tag,
        }

        log.info(
            f"{bucket_arn}: {len(objects)} objects, classes={counts}, "
            f"to_show={len(to_show)}, to_hide={len(to_hide)}, applied={applied}, "
            f"published_objects_missing_tag={missing_tag}"
        )

    print(json.dumps(summary, indent=2), file=sys.stdout)

    if args.report:
        with open(args.report, "w") as f:
            json.dump(summary, f, indent=2)

    # Ready to apply the policy only once every published object already carries
    # its tag - otherwise applying it takes live data dark.
    not_ready = {b: s for b, s in summary.items() if s["published_objects_missing_tag"]}

    if not_ready:
        print(
            "NOT READY to apply the visibility policy: these buckets still have "
            "published objects without the visibility tag:",
            file=sys.stdout,
        )
        for bucket_arn, stats in sorted(not_ready.items()):
            print(
                f"  {bucket_arn}: {stats['published_objects_missing_tag']}",
                file=sys.stdout,
            )
    else:
        print(
            "All published objects in every gated bucket carry the visibility tag.",
            file=sys.stdout,
        )

    if args.commit:
        send_admin_alert(
            None,
            source="s3-publication-visibility",
            description=f"Visibility pass complete: {json.dumps(summary)}",
            priority=PRIORITY_ROUTINE,
        )

    return 0


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile published-bucket object visibility against Onyx "
            "publication state. Read-only unless --commit is given. Never "
            "deletes an object."
        )
    )
    parser.add_argument("--config", default=os.getenv("ROZ_CONFIG_JSON"))
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually write tag changes. Without this the pass is read-only and only reports.",
    )
    parser.add_argument(
        "--hide-unmappable",
        action="store_true",
        help="Also hide objects whose key does not parse to a climb_id. Off by default: these are usually a sign something else is wrong, and hiding them turns a mapping bug into an outage.",
    )
    parser.add_argument(
        "--hide-orphans",
        action="store_true",
        help="Also hide objects whose climb_id has no Onyx record. Off by default: 'no record' is also what a partial Onyx view looks like.",
    )
    parser.add_argument("--report", metavar="PATH", help="Write the summary JSON here")
    parser.add_argument("--logfile", default=None)
    parser.add_argument("--log_level", default="INFO")

    args = parser.parse_args()

    if not args.config:
        print(
            "ROZ_CONFIG_JSON is not set and --config was not given", file=sys.stderr
        )
        sys.exit(1)

    log = init_logger("s3_publication_visibility", args.logfile, args.log_level)

    sys.exit(run_pass(args, log))


if __name__ == "__main__":
    main()
