from varys import Varys
import os
import requests
import json
import sys

varys_client = Varys(
    profile="roz",
    logfile=sys.stderr,
    log_level="CRITICAL",
    auto_acknowledge=False,
)

new_artifact_url = os.getenv("NEW_ARTIFACT_WEBHOOK")
# public_result_url = os.getenv("PUBLIC_RESULT_WEBHOOK")
alert_url = os.getenv("PATHSAFE_ALERT_WEBHOOK")

new_artifact_message_template = """*New PATH-SAFE Artifact Published*
```
{}
```
"""

public_result_message_template = """*New PATH-SAFE Public Dataset Result*
Outcome - *{}*
```
{}
```
"""

mscape_alert_template = """<!channel>
*PATH-SAFE Alert*
```
{}
```
"""


while True:
    new_artifact_message = varys_client.receive(
        "inbound-new_artifact-pathsafe", queue_suffix="slack_integration", timeout=1
    )
    if new_artifact_message:
        in_dict = json.loads(new_artifact_message.body)

        out_text = new_artifact_message_template.format(json.dumps(in_dict, indent=2))

        out_message = {"text": out_text}

        success = False

        while not success:
            try:
                r = requests.post(new_artifact_url, json=out_message)
                success = True

            except Exception:
                continue

        if not r.ok:
            print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
            sys.exit(1)

        varys_client.acknowledge_message(new_artifact_message)

    alert_message = varys_client.receive(
        "pathsafe-restricted-announce", queue_suffix="slack_integration", timeout=1
    )

    if alert_message:
        in_dict = json.loads(alert_message.body)

        out_text = mscape_alert_template.format(json.dumps(in_dict, indent=2))

        out_message = {"text": out_text}

        success = False

        while not success:
            try:
                r = requests.post(alert_url, json=out_message)
                success = True

            except Exception:
                continue

        if not r.ok:
            print(f"Error posting to Slack webhook: {r.status_code} - {r.reason}")
            sys.exit(1)

        varys_client.acknowledge_message(alert_message)
