import functools
import os
import sys
import json
import csv
import time

import varys
from varys import VarysPublishError

from roz_scripts.utils.utils import (
    init_logger,
    csv_create,
    csv_field_checks,
    valid_character_checks,
    put_result_json,
    s3_to_fh,
    EtagMismatchError,
    NonPlaintextCSVError,
    send_admin_alert,
    persist_publish_ack,
    PUBLISH_FAILURE_BACKOFF_S,
)
from roz_scripts.utils.health import HealthState, get_health_dir
from roz_scripts.utils.config import load_config, ConfigError


def main():
    for i in (
        "ONYX_DOMAIN",
        "ONYX_TOKEN",
        "ROZ_INGEST_LOG",
        "INGEST_LOG_LEVEL",
        "VARYS_CFG",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "ROZ_CONFIG_JSON",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    try:
        config = load_config()
    except ConfigError as e:
        print(f"Invalid roz config: {e}", file=sys.stderr)
        sys.exit(3)

    # Setup producer / consumer
    log = init_logger(
        "roz_ingest", os.getenv("ROZ_INGEST_LOG"), os.getenv("INGEST_LOG_LEVEL")
    )

    varys_client = varys.Varys(
        profile="roz",
        logfile=os.getenv("ROZ_INGEST_LOG"),
        log_level=os.environ["INGEST_LOG_LEVEL"],
        auto_acknowledge=False,
    )

    health = HealthState(get_health_dir())

    def requeue_with_alert(message, payload):
        """Announce a payload on its project alert channel and requeue the delivery.

        Neither step may be fatal: if the broker is the broken thing then both fail,
        and losing the connection requeues the delivery regardless.
        """
        try:
            varys_client.send(
                message=payload,
                exchange=f"restricted-{payload['project']}-alert",
                queue_suffix="ingest",
            )
        except VarysPublishError:
            log.exception("Failed to publish the alert message")

        try:
            varys_client.nack_message(message)
        except Exception:
            log.exception(
                "Failed to nack the message; connection loss will requeue it anyway"
            )

    def send_result(message, payload):
        """File the result JSON and publish the result, then ack - in that order."""
        return persist_publish_ack(
            varys_client,
            message,
            log,
            sends=[
                {
                    "message": payload,
                    "exchange": f"inbound-results-{payload['project']}-{payload['site']}",
                    "queue_suffix": "s3_matcher",
                }
            ],
            persists=[
                functools.partial(put_result_json, payload=payload, log=log, config=config)
            ],
            source="onyx-checks",
            uuid=payload.get("uuid"),
            heartbeat=health.heartbeat,
        )

    while True:
        message = None
        try:
            message = varys_client.receive(
                exchange="inbound-matched", queue_suffix="ingest", timeout=60
            )

            health.heartbeat()

            if not message:
                continue

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
                requeue_with_alert(message, payload)
                continue

            if not test_create_status:
                log.info(f"Test create failed for UUID: {payload['uuid']}")
                send_result(message, payload)
                continue

            log.info(
                f"Checking that run_index and run_id do not contain invalid characters for match UUID: {payload['uuid']}"
            )

            valid_character_status, alert, payload = valid_character_checks(
                payload=payload
            )

            if alert:
                requeue_with_alert(message, payload)
                continue

            if not valid_character_status:
                payload["validate"] = False
                log.info(f"Invalid characters found for UUID: {payload['uuid']}")
                send_result(message, payload)
                continue

            log.info(
                f"Checking that run_index and run_id match provided CSV for match UUID: {payload['uuid']}"
            )

            field_check_status, alert, payload = csv_field_checks(payload=payload)

            if alert:
                requeue_with_alert(message, payload)
                continue

            if not field_check_status:
                payload["validate"] = False
                log.info(f"Field checks failed for UUID: {payload['uuid']}")
                send_result(message, payload)
                continue

            payload["onyx_test_create_status"] = True
            payload["validate"] = True

            try:
                with s3_to_fh(
                    payload["files"][".csv"]["uri"],
                    payload["files"][".csv"]["etag"],
                ) as csv_fh:
                    reader = csv.DictReader(csv_fh, delimiter=",")

                    metadata = next(reader)
            except (NonPlaintextCSVError, EtagMismatchError) as e:
                log.info(f"Rejecting CSV for UUID: {payload['uuid']}. Error: {e}")
                payload["validate"] = False
                payload.setdefault("onyx_test_create_errors", {})
                payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                payload["onyx_test_create_errors"]["onyx_errors"].append(str(e))
                send_result(message, payload)
                continue

            payload["biosample_id"] = metadata["biosample_id"]

            persist_publish_ack(
                varys_client,
                message,
                log,
                sends=[
                    {
                        "message": payload,
                        "exchange": f"inbound-to_validate-{payload['project']}",
                        "queue_suffix": "ingest",
                    }
                ],
                source="onyx-checks",
                uuid=payload.get("uuid"),
                heartbeat=health.heartbeat,
            )

        except VarysPublishError as e:
            # The broker being unreachable is transient and recoverable, so it must
            # not kill the pod: alert if the alert itself can get out, requeue
            # best-effort, and carry on. Losing the connection requeues any unacked
            # delivery anyway, which is what makes ack-after-publish safe here.
            log.error(f"Failed to reach the broker, requeueing and backing off: {e}")

            try:
                send_admin_alert(
                    varys_client,
                    source="onyx-checks",
                    description=f"Failed to reach the broker, message requeued: {e}",
                )
            except Exception:
                log.exception("Failed to send admin alert about the broker failure")

            if message:
                try:
                    varys_client.nack_message(message)
                except Exception:
                    log.exception(
                        "Failed to nack the message; connection loss will requeue it anyway"
                    )

            # Heartbeat around the backoff as well as at the top of the loop, so a
            # long outage cannot be mistaken for a wedged process
            health.heartbeat()
            time.sleep(PUBLISH_FAILURE_BACKOFF_S)
            health.heartbeat()
            continue

        except Exception as e:
            log.error(f"An unhandled exception occurred: {str(e)}")
            reason = f"failed with unhandled exception: {e}"
            health.mark_fatal(
                reason,
                alert_fn=lambda r: send_admin_alert(
                    varys_client, source="onyx-checks", description=r
                ),
            )
            if message:
                varys_client.nack_message(message)
            sys.exit(1)


if __name__ == "__main__":
    main()
