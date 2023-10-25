from varys import varys
import os
import requests
import json
import sys

varys_client = varys(
    profile="roz",
    logfile=os.devnull,
    log_level="CRITICAL",
    auto_acknowledge=False,
)

new_artifact_url = os.getenv("NEW_ARTIFACT_WEBHOOK")

message_template = """*New MScape Artifact Published*
```
{}
```
"""


while True:
    in_message = varys_client.receive(
        "inbound.new_artifact.mscape", queue_suffix="slack_integration"
    )

    in_dict = json.loads(in_message.body)

    out_text = message_template.format(json.dumps(in_dict, indent=2))

    out_message = {"text": out_text}

    r = requests.post(new_artifact_url, json=out_message)

    if not r.ok:
        print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
        sys.exit(1)

    varys_client.acknowledge_message(in_message)
