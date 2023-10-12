import csv
import os
import sys
import time
import json
import copy

from onyx import OnyxClient

import varys

from roz_scripts.utils.utils import init_logger, s3_to_fh


def handle_status_code(status_code):
    if status_code == 422:
        return (False, "validation_failure")
    elif status_code == 403:
        return (False, "perm_failure")
    elif status_code == 400:
        return (False, "bad_request")
    elif status_code == 201:
        return (True, "success")
    else:
        return (False, "unknown")


def parse_match_message(matched_message, payload):
    payload["artifact"] = matched_message["artifact"]
    payload["sample_id"] = matched_message["sample_id"]
    payload["run_name"] = matched_message["run_name"]
    payload["project"] = matched_message["project"]
    payload["uploaders"] = matched_message["uploaders"]
    payload["platform"] = matched_message["platform"]
    payload["ingest_timestamp"] = time.time_ns()
    payload["site"] = matched_message["site"]
    payload["files"] = matched_message["files"]
    payload["test_flag"] = matched_message["test_flag"]

    return payload


def main():
    for i in (
        "ONYX_ROZ_PASSWORD",
        "ROZ_INGEST_LOG",
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
    )

    ingest_payload_template = {
        "uuid": "",
        "artifact": "",
        "sample_id": "",
        "run_name": "",
        "project": "",
        "uploaders": [],
        "platform": "",
        "ingest_timestamp": "",
        "cid": False,
        "validate": True,
        "site": "",
        "created": False,
        "ingested": False,
        "files": {},  # Dict
        "onyx_test_status_code": False,
        "onyx_test_create_errors": {},  # Dict
        "onyx_test_create_status": False,
        "onyx_status_code": False,
        "onyx_errors": {},  # Dict
        "onyx_create_status": False,
        "ingest_errors": [],
        "test_flag": True,
        "test_ingest_result": False,
    }

    while True:
        payload = copy.deepcopy(ingest_payload_template)

        message = varys_client.receive(
            exchange="inbound.matched", queue_suffix="ingest"
        )

        matched_message = json.loads(message.body)

        payload["uuid"] = matched_message["uuid"]

        # Not sure how to fully generalise this, the idea is to have a csv as the only file that will always exist, so I guess this is okay?
        # CSV file must always be called '.csv' though
        try:
            with OnyxClient(env_password=True) as client:
                log.info(
                    f"Received match for artifact: {matched_message['artifact']}, UUID: {payload['uuid']} now attempting to test_create record in Onyx"
                )
                try:
                    # Test create from the metadata CSV
                    response_generator = client._csv_create(
                        matched_message["project"],
                        csv_file=s3_to_fh(
                            matched_message["files"][".csv"]["uri"],
                            matched_message["files"][".csv"]["etag"],
                        ),  # I don't like having a hardcoded metadata file name like this but hypothetically we should always have a metadata CSV
                        test=True,
                        fields={"site": matched_message["site"]},
                    )
                except Exception as e:
                    log.error(
                        f"Onxy test csv create failed for artifact: {matched_message['artifact']}, UUID: {matched_message['uuid']} due to client error: {e}"
                    )
                    continue

                multiline_csv = False

                responses = [response for response in response_generator]

                if len(responses) == 0:
                    log.error("Onyx test csv create failed to return any responses")
                    continue

                if len(responses) > 1:
                    log.info(
                        f"Metadata CSV for artifact {matched_message['artifact']}, UUID: {matched_message['uuid']} contains more than one record, metadata CSVs should only ever contain a single record"
                    )
                    multiline_csv = True

                else:
                    to_test = responses[0]

        except Exception as e:
            log.error(f"Failed to connect to Onyx due to error: {e}")

        if multiline_csv:
            payload["onyx_test_create_errors"]["metadata_csv"] = [
                "Multiline metadata CSVs are not permitted"
            ]

            payload["validate"] = False

            to_send = None

            to_send = parse_match_message(
                matched_message=matched_message, payload=payload
            )

            varys_client.send(
                message=to_send,
                exchange=f"inbound.to_validate.{matched_message['project']}",
                queue_suffix="ingest",
            )
            continue

        log.info(
            f"Received Onyx test create response for artifact: {matched_message['artifact']}"
        )

        payload["onyx_test_create_status"] = to_test.ok
        payload["onyx_test_status_code"] = to_test.status_code

        if not payload["onyx_test_create_status"]:
            payload["validate"] = False

        if to_test.json().get("messages"):
            for field, messages in to_test.json()["messages"].items():
                if payload["onyx_test_create_errors"].get(field):
                    payload["onyx_test_create_errors"][field].extend(messages)
                else:
                    payload["onyx_test_create_errors"][field] = messages

        with s3_to_fh(
            matched_message["files"][".csv"]["uri"],
            matched_message["files"][".csv"]["etag"],
        ) as csv_fh:
            reader = csv.DictReader(csv_fh, delimiter=",")

            metadata = next(reader)

            fields_present = {"sample_id": False, "run_name": False}

            for field in ("sample_id", "run_name"):
                if metadata.get(field):
                    fields_present[field] = True

            for k, v in fields_present.items():
                if not v:
                    if payload["onyx_test_create_errors"].get(k):
                        payload["onyx_test_create_errors"][k].append(
                            "Required field is not present"
                        )
                    else:
                        payload["onyx_test_create_errors"][k] = [
                            "Required field is not present"
                        ]

            if not all(fields_present.values()):
                to_send = None

                to_send = parse_match_message(
                    matched_message=matched_message, payload=payload
                )

                varys_client.send(
                    message=to_send,
                    exchange=f"inbound.to_validate.{matched_message['project']}",
                    queue_suffix="ingest",
                )
                continue

            name_matches = {
                x: metadata[x] == matched_message[x] for x in ("sample_id", "run_name")
            }

            for k, v in name_matches.items():
                if not v:
                    if payload["onyx_test_create_errors"].get(k):
                        payload["onyx_test_create_errors"][k].append(
                            "Field does not match filename"
                        )
                    else:
                        payload["onyx_test_create_errors"][k] = [
                            "Field does not match filename"
                        ]

            if not all(name_matches.values()):
                payload["validate"] = False

                to_send = None

                to_send = parse_match_message(
                    matched_message=matched_message, payload=payload
                )

                varys_client.send(
                    message=to_send,
                    exchange=f"inbound.to_validate.{matched_message['project']}",
                    queue_suffix="ingest",
                )
                continue

        to_send = None

        to_send = parse_match_message(matched_message=matched_message, payload=payload)

        varys_client.send(
            message=to_send,
            exchange=f"inbound.to_validate.{matched_message['project']}",
            queue_suffix="ingest",
        )


if __name__ == "__main__":
    main()
