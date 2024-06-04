from varys import Varys
import os
import requests
import json
import sys

varys_client = Varys(
    profile="roz",
    logfile=os.devnull,
    log_level="CRITICAL",
    auto_acknowledge=False,
)

new_artifact_url = os.getenv("NEW_ARTIFACT_WEBHOOK")
public_result_url = os.getenv("PUBLIC_RESULT_WEBHOOK")
alert_url = os.getenv("MSCAPE_ALERT_WEBHOOK")
hcid_url = os.getenv("HCID_WEBHOOK")

new_artifact_message_template = """*New MScape Artifact Published*
```
{}
```
"""

public_result_message_template = """*New MScape Public Dataset Result*
Outcome - *{}*
```
{}
```
"""

mscape_alert_template = """<!channel>
*MScape Alert*
```
{}
```
"""


while True:
    new_artifact_message = varys_client.receive(
        "inbound-new_artifact-mscape", queue_suffix="slack_integration", timeout=1
    )
    if new_artifact_message:
        in_dict = json.loads(new_artifact_message.body)

        out_text = new_artifact_message_template.format(json.dumps(in_dict, indent=2))

        out_message = {"text": out_text}

        r = requests.post(new_artifact_url, json=out_message)

        if not r.ok:
            print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
            sys.exit(1)

        varys_client.acknowledge_message(new_artifact_message)

    public_result_message = varys_client.receive(
        "inbound-results-mscape-public", queue_suffix="slack_integration", timeout=1
    )

    if public_result_message:
        in_dict = json.loads(public_result_message.body)

        outcome = ""

        if isinstance(in_dict, dict):

            outcome = "Success" if in_dict.get("published") else "Failure"

        out_text = public_result_message_template.format(
            outcome, json.dumps(in_dict, indent=2)
        )

        out_message = {"text": out_text}

        r = requests.post(public_result_url, json=out_message)

        if not r.ok:
            print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
            sys.exit(1)

        varys_client.acknowledge_message(public_result_message)

    alert_message = varys_client.receive(
        "mscape-restricted-announce", queue_suffix="slack_integration", timeout=1
    )

    if alert_message:
        in_dict = json.loads(alert_message.body)

        out_text = mscape_alert_template.format(json.dumps(in_dict, indent=2))

        out_message = {"text": out_text}

        r = requests.post(alert_url, json=out_message)

        if not r.ok:
            print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
            sys.exit(1)

        varys_client.acknowledge_message(alert_message)

    hcid_message = varys_client.receive(
        "mscape-restricted-hcid", queue_suffix="slack_integration", timeout=1
    )

    if hcid_message:
        in_dict = json.loads(hcid_message.body)

        out_text = mscape_alert_template.format(json.dumps(in_dict, indent=2))

        out_message = {"text": out_text}

        r = requests.post(hcid_url, json=out_message)

        if not r.ok:
            print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
            sys.exit(1)

        varys_client.acknowledge_message(hcid_message)
