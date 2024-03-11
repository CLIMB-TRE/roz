import os
import sys
import json
import csv

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
    valid_character_checks,
    put_result_json,
    s3_to_fh,
)


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

    varys_client = varys.Varys(
        profile="roz",
        logfile=os.getenv("ROZ_INGEST_LOG"),
        log_level=os.getenv("INGEST_LOG_LEVEL"),
        auto_acknowledge=False,
    )

    while True:
        message = varys_client.receive(
            exchange="inbound-matched", queue_suffix="ingest"
        )

        payload = json.loads(message.body)
        payload["validate"] = False

        log.info(
            f"Checking that run_index and run_id do not contain invalid characters for match UUID: {payload['uuid']}"
        )

        valid_character_status, alert, payload = valid_character_checks(payload=payload)

        if alert:
            varys_client.send(
                message=payload,
                exchange=f"restricted-{payload['project']}-alert",
                queue_suffix="ingest",
            )
            varys_client.nack_message(message)
            continue

        if not valid_character_status:
            payload["validate"] = False
            log.info(f"Invalid characters found for UUID: {payload['uuid']}")
            varys_client.acknowledge_message(message)
            varys_client.send(
                message=payload,
                exchange=f"inbound-results-{payload['project']}-{payload['site']}",
                queue_suffix="s3_matcher",
            )
            put_result_json(payload=payload, log=log)
            continue

        log.info(
            f"Checking that run_index and run_id match provided CSV for match UUID: {payload['uuid']}"
        )

        field_check_status, alert, payload = csv_field_checks(payload=payload)

        if alert:
            varys_client.send(
                message=payload,
                exchange=f"restricted-{payload['project']}-alert",
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
                exchange=f"inbound-results-{payload['project']}-{payload['site']}",
                queue_suffix="s3_matcher",
            )
            put_result_json(payload=payload, log=log)
            continue

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
                exchange=f"restricted-{payload['project']}-alert",
                queue_suffix="ingest",
            )
            varys_client.nack_message(message)
            continue

        if not test_create_status:
            log.info(f"Test create failed for UUID: {payload['uuid']}")
            varys_client.acknowledge_message(message)
            varys_client.send(
                message=payload,
                exchange=f"inbound-results-{payload['project']}-{payload['site']}",
                queue_suffix="s3_matcher",
            )
            put_result_json(payload=payload, log=log)
            continue

        payload["onyx_test_create_status"] = True
        payload["validate"] = True

        with s3_to_fh(
            payload["files"][".csv"]["uri"],
            payload["files"][".csv"]["etag"],
        ) as csv_fh:
            reader = csv.DictReader(csv_fh, delimiter=",")

            metadata = next(reader)

        payload["biosample_id"] = metadata["biosample_id"]

        varys_client.acknowledge_message(message)

        varys_client.send(
            message=payload,
            exchange=f"inbound-to_validate-{payload['project']}",
            queue_suffix="ingest",
        )


if __name__ == "__main__":
    main()
