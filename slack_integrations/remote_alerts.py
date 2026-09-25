"""Slack integration for the admin alerts channel.

Consumes stripped alert messages ({"source", "description", "uuid"?,
"priority"?}) from the single `remote-announce` exchange and posts them to a
Slack webhook. Messages on this channel are restricted but safe for off-prem
infrastructure - producers must only ever put `source`/`description`/`uuid`/
`priority` in the body (see roz_scripts.utils.utils.send_admin_alert), and
this consumer additionally allow-lists those same fields before formatting,
so a producer bug can't leak anything else through to Slack. `priority` is a
control signal only - its value is never rendered into the Slack text, it
only decides whether this consumer's rate limit applies (see `should_post`).

Webhook URL is read from REMOTE_ALERT_WEBHOOK at runtime.
"""

from varys import Varys
import os
import requests
import json
import sys
import time

# Mirrored in roz_scripts.utils.utils - this module is deliberately kept
# dependency-free since it runs off-prem, so it cannot import that module.
# Keep both definitions in sync.
PRIORITY_CRITICAL = "critical"

MIN_SECONDS_BETWEEN_ALERTS = 1800

# Synthetic "source" used to rate-limit reports about alert bodies this
# consumer couldn't parse at all - kept separate from real component names so
# a run of bad bodies can't borrow (or exhaust) a real component's cooldown
# window, and so it can't collide with an actual `source` value either.
MALFORMED_BODY_SOURCE = "malformed-alert-body"
MALFORMED_BODY_EXCERPT_CHARS = 200


def should_post(body: dict, last_sent_at_by_source: dict, now: float) -> bool:
    """Decide whether an alert should be posted, applying the per-source rate limit.

    A critical alert always posts, and deliberately neither reads nor writes
    `last_sent_at_by_source`: writing would start a fresh cooldown window over
    every unrelated routine alert sharing that source, for what is meant to
    be a rare, effectively-once-per-record event (see
    roz_scripts.utils.utils.send_admin_alert).

    Args:
        body (dict): The parsed alert body
        last_sent_at_by_source (dict): Mutated in place - records `now` under
            this alert's source whenever a routine alert is allowed through
        now (float): Current `time.monotonic()` reading

    Returns:
        bool: True if the alert should be posted
    """
    if body.get("priority") == PRIORITY_CRITICAL:
        return True

    source = body.get("source", "unknown")
    last_sent_at = last_sent_at_by_source.get(source)
    if last_sent_at is not None and now - last_sent_at < MIN_SECONDS_BETWEEN_ALERTS:
        return False

    last_sent_at_by_source[source] = now
    return True


def format_alert(body: dict) -> str:
    source = body.get("source", "unknown")
    description = body.get("description", "(no description)")
    uuid = body.get("uuid")

    lines = [
        "<!channel>",
        f"*ROZ Alert — {source}*",
        "```",
        f"description: {description}",
    ]

    if uuid:
        lines.append(f"uuid: {uuid}")

    lines.append("```")

    return "\n".join(lines)


def format_malformed_body_alert(raw_body: str, reason: str, body_type: str) -> str:
    """Report an alert message body this consumer could not treat as a JSON object.

    No `<!channel>` - this reports a producer bug, not something needing
    immediate paging. The excerpt is truncated and has its backticks
    neutralised so a malformed body can't break out of the code fence or
    dump an unbounded amount of content into Slack. This is a narrower
    exposure than it might look: a full payload dict is exactly what the
    allow-listed `format_alert` path above already handles safely, so all
    this can ever carry is a misrouted plain string or a JSON list/number -
    comparable to `description`, which already ships unrestricted - bounded
    to `MALFORMED_BODY_EXCERPT_CHARS`.

    Args:
        raw_body (str): The raw, undecoded message body
        reason (str): Why this body was rejected
        body_type (str): Python type name of whatever this body actually is
            (e.g. "str" for a body that failed to parse as JSON at all, or
            "list"/"int" for JSON that parsed but wasn't an object)
    """
    excerpt = raw_body[:MALFORMED_BODY_EXCERPT_CHARS].replace("`", "'")
    if len(raw_body) > MALFORMED_BODY_EXCERPT_CHARS:
        excerpt += "..."

    lines = [
        "*ROZ Alert — malformed alert body*",
        "```",
        f"reason: {reason}",
        f"type: {body_type}",
        f"length: {len(raw_body.encode('utf-8', errors='replace'))} bytes",
        f"excerpt: {excerpt}",
        "```",
    ]

    return "\n".join(lines)


def post_to_slack(webhook_url: str, text: str) -> None:
    success = False
    while not success:
        try:
            r = requests.post(webhook_url, json={"text": text}, timeout=10)
            success = True
        except Exception:
            time.sleep(1)

    if not r.ok:  # type: ignore[reportPossiblyUnboundVariable]
        print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")  # type: ignore[reportPossiblyUnboundVariable]
        sys.exit(1)


def main():
    webhook_url = os.getenv("REMOTE_ALERT_WEBHOOK")

    if not webhook_url:
        print("REMOTE_ALERT_WEBHOOK is not set", file=sys.stderr)
        sys.exit(1)

    varys_client = Varys(
        profile="roz",
        logfile=os.devnull,
        log_level="CRITICAL",
        auto_acknowledge=False,
    )

    last_sent_at_by_source = {}

    while True:
        message = varys_client.receive(
            "remote-announce",
            queue_suffix="slack_integration",
            timeout=1,
        )

        if not message:
            continue

        # A body that can't even be read as a JSON object used to kill this
        # loop outright (AttributeError from `.get` on a non-dict, or an
        # uncaught JSONDecodeError) - report it instead, routed through the
        # same rate limiter under a synthetic source so a producer stuck in
        # a requeue loop can't spam Slack either.
        try:
            parsed_body = json.loads(message.body)
        except json.JSONDecodeError as e:
            report = format_malformed_body_alert(
                message.body,
                reason=f"could not parse as JSON: {type(e).__name__}",
                body_type=type(message.body).__name__,
            )
            now = time.monotonic()
            if should_post({"source": MALFORMED_BODY_SOURCE}, last_sent_at_by_source, now):
                post_to_slack(webhook_url, report)
            else:
                print("Rate limited, dropping malformed-alert-body report", file=sys.stderr)
            varys_client.acknowledge_message(message)
            continue

        if not isinstance(parsed_body, dict):
            report = format_malformed_body_alert(
                message.body,
                reason="parsed JSON is not an object",
                body_type=type(parsed_body).__name__,
            )
            now = time.monotonic()
            if should_post({"source": MALFORMED_BODY_SOURCE}, last_sent_at_by_source, now):
                post_to_slack(webhook_url, report)
            else:
                print("Rate limited, dropping malformed-alert-body report", file=sys.stderr)
            varys_client.acknowledge_message(message)
            continue

        body = parsed_body
        now = time.monotonic()
        if not should_post(body, last_sent_at_by_source, now):
            print(
                f"Rate limited, dropping routine alert from {body.get('source', 'unknown')}",
                file=sys.stderr,
            )
            varys_client.acknowledge_message(message)
            continue

        text = format_alert(body)
        post_to_slack(webhook_url, text)
        varys_client.acknowledge_message(message)


if __name__ == "__main__":
    main()
