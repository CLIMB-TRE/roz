"""Slack integration for the admin alerts channel.

Consumes stripped alert messages ({"source", "description", "uuid"?}) from
the single `remote-announce` exchange and posts them to a Slack webhook.
Messages on this channel are restricted but safe for off-prem infrastructure -
producers must only ever put `source`/`description`/`uuid` in the body (see
roz_scripts.utils.utils.send_admin_alert), and this consumer additionally
allow-lists those same fields before formatting, so a producer bug can't leak
anything else through to Slack.

Webhook URL is read from REMOTE_ALERT_WEBHOOK at runtime.
"""

from varys import Varys
import os
import requests
import json
import sys
import time


def format_alert(body: dict) -> str:
    source = body.get("source", "unknown")
    description = body.get("description", "(no description)")
    uuid = body.get("uuid")

    lines = ["<!channel>", f"*ROZ Alert — {source}*", "```", f"description: {description}"]

    if uuid:
        lines.append(f"uuid: {uuid}")

    lines.append("```")

    return "\n".join(lines)


def post_to_slack(webhook_url: str, text: str) -> None:
    success = False
    while not success:
        try:
            r = requests.post(webhook_url, json={"text": text}, timeout=10)
            success = True
        except Exception:
            time.sleep(1)

    if not r.ok:
        print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
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

    while True:
        message = varys_client.receive(
            "remote-announce",
            queue_suffix="slack_integration",
            timeout=1,
        )

        if not message:
            continue

        body = json.loads(message.body)
        text = format_alert(body)
        post_to_slack(webhook_url, text)
        varys_client.acknowledge_message(message)


if __name__ == "__main__":
    main()
