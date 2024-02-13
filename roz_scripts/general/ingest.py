import os
import sys
import json
import time

import varys

from onyx import OnyxClient
from onyx.exceptions import (
    OnyxRequestError,
    OnyxConnectionError,
    OnyxServerError,
    OnyxConfigError,
    OnyxClientError,
)

from roz_scripts.utils.utils import (
    init_logger,
    csv_create,
    csv_field_checks,
    get_onyx_credentials,
)


# def identify_run_disagreements(payload, log):
#     onyx_config = get_onyx_credentials()

#     with OnyxClient(config=onyx_config) as client:
#         while reconnect_count <= 3:
#             try:
#                 run = client.identify(
#                     project=payload["project"], field="run_id", value=payload["run_id"]
#                 )

#             except OnyxConnectionError as e:
#                 if reconnect_count < 3:
#                     reconnect_count += 1
#                     log.error(
#                         f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
#                     )
#                     time.sleep(3)
#                     continue

#                 else:
#                     log.error(
#                         f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
#                     )
#                     payload.setdefault("onyx_errors", {})
#                     payload["onyx_errors"].setdefault("onyx_errors", [])
#                     payload["onyx_errors"]["onyx_errors"].append(str(e))

#                     return (False, True, payload)

#             except (OnyxServerError, OnyxConfigError) as e:
#                 log.error(f"Unhandled Onyx error: {e}")
#                 payload.setdefault("onyx_errors", {})
#                 payload["onyx_errors"].setdefault("onyx_errors", [])
#                 payload["onyx_errors"]["onyx_errors"].append(e)
#                 return (False, True, payload)

#             except OnyxClientError as e:
#                 log.error(
#                     f"Onyx filter failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
#                 )
#                 payload.setdefault("onyx_errors", {})
#                 payload["onyx_errors"].setdefault("onyx_errors", [])
#                 payload["onyx_errors"]["onyx_errors"].append(str(e))
#                 return (False, True, payload)

#             except OnyxRequestError as e:
#                 log.error(
#                     f"Onyx filter failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}. Error: {e}"
#                 )
#                 payload.setdefault("onyx_errors", {})
#                 for field, messages in e.response.json()["messages"].items():
#                     payload["onyx_errors"].setdefault(field, [])
#                     payload["onyx_errors"][field].extend(messages)
#                 return (False, True, payload)

#             except Exception as e:
#                 log.error(f"Unhandled error: {e}")
#                 return (False, True, payload)


def main():
    for i in (
        "ONYX_DOMAIN",
        "ONYX_TOKEN",
        "ROZ_INGEST_LOG",
        "INGEST_LOG_LEVEL",
        "VARYS_CFG",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    # Setup producer / consumer
    log = init_logger(
        "roz_ingest", os.getenv("ROZ_INGEST_LOG"), os.getenv("INGEST_LOG_LEVEL")
    )

    varys_client = varys.varys(
        profile="roz",
        logfile=os.getenv("ROZ_INGEST_LOG"),
        log_level=os.getenv("INGEST_LOG_LEVEL"),
        auto_acknowledge=False,
    )

    while True:
        message = varys_client.receive(
            exchange="inbound.matched", queue_suffix="ingest"
        )

        payload = json.loads(message.body)
        payload["validate"] = False

        log.info(
            f"Attempting to test create metadata record in onyx for match with UUID: {payload['uuid']}"
        )
        test_create_status, alert, payload = csv_create(
            payload=payload, log=log, test_submission=True
        )

        if alert:
            log.error(
                "Something went wrong with the test create, more details available in the alert channel"
            )
            varys_client.send(
                message=payload,
                exchange=f"restricted.{payload['project']}.alert",
                queue_suffix="ingest",
            )
            varys_client.nack_message(message)
            continue

        if not test_create_status:
            log.info(f"Test create failed for UUID: {payload['uuid']}")
            varys_client.acknowledge_message(message)
            varys_client.send(
                message=payload,
                exchange=f"inbound.results.{payload['project']}.{payload['site']}",
                queue_suffix="s3_matcher",
            )
            continue

        payload["onyx_test_create_status"] = True
        payload["validate"] = True

        field_check_status, alert, payload = csv_field_checks(payload=payload)

        if alert:
            varys_client.send(
                message=payload,
                exchange=f"restricted.{payload['project']}.alert",
                queue_suffix="ingest",
            )
            varys_client.nack_message(message)
            continue

        if not field_check_status:
            payload["validate"] = False
            log.info(f"Field checks failed for UUID: {payload['uuid']}")
            varys_client.acknowledge_message(message)
            varys_client.send(
                message=payload,
                exchange=f"inbound.results.{payload['project']}.{payload['site']}",
                queue_suffix="s3_matcher",
            )
            continue

        varys_client.acknowledge_message(message)

        varys_client.send(
            message=payload,
            exchange=f"inbound.to_validate.{payload['project']}",
            queue_suffix="ingest",
        )


if __name__ == "__main__":
    main()
