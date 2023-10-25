from varys import varys
import os
import requests

varys_client = varys(
    profile="roz",
    logfile=os.devnull,
    log_level="CRITICAL",
    auto_acknowledge=True,
)

new_artifact_url = os.getenv("NEW_ARTIFACT_WEBHOOK")

while True:
    message = varys_client.receive(
        "inbound.new_artifact.mscape", queue_suffix="slack_integration"
    )

    r = requests.post(new_artifact_url, json=message.body)

    if not r.ok:
        print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
