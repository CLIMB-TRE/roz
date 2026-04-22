"""Slack integration for the remote alerts channel.

Consumes stripped alert messages (UUID + description only, no identifying fields)
from {project}-remote-announce exchanges and posts them to a single Slack webhook.
Messages on this channel are still restricted but safe for off-prem infrastructure.

Webhook URL is read from REMOTE_ALERT_WEBHOOK at runtime.

To add a project, append a call to remote_alert_channel() in CHANNELS:
    remote_alert_channel("pathsafe", label="PATH-SAFE"),
"""

from dataclasses import dataclass
from typing import Callable
from varys import Varys
import os
import requests
import json
import sys
import time


@dataclass
class AlertChannel:
    exchange: str
    queue_suffix: str
    format_message: Callable[[dict], str]


def remote_alert_channel(project: str, label: str | None = None) -> AlertChannel:
    display = label or project

    def formatter(body: dict) -> str:
        uuid = body.get("uuid", "unknown")
        description = body.get("description", "(no description)")
        return f"<!channel>\n*{display} Alert*\n```UUID: {uuid}\n{description}```"

    return AlertChannel(
        exchange=f"{project}-remote-announce",
        queue_suffix="slack_integration",
        format_message=formatter,
    )


CHANNELS: list[AlertChannel] = [
    remote_alert_channel("mscape", label="mSCAPE"),
]


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
    for channel in CHANNELS:
        message = varys_client.receive(
            channel.exchange,
            queue_suffix=channel.queue_suffix,
            timeout=1,
        )

        if not message:
            continue

        body = json.loads(message.body)
        text = channel.format_message(body)
        post_to_slack(webhook_url, text)
        varys_client.acknowledge_message(message)
