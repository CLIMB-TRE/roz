from varys import varys
import os
import requests
import json

varys_client = varys(
    profile="roz",
    logfile=os.devnull,
    log_level="CRITICAL",
    auto_acknowledge=True,
)

new_artifact_url = os.getenv("NEW_ARTIFACT_WEBHOOK")

while True:
    in_message = varys_client.receive(
        "inbound.new_artifact.mscape", queue_suffix="slack_integration"
    )

    out_text = f"""*New MScape Artifact Published*
                   Message:
                   ```
                   {json.dumps(in_message.body, indent=2)}
                   ```
                   """

    out_message = {"text": out_text}

    r = requests.post(new_artifact_url, json=out_message)

    if not r.ok:
        print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
