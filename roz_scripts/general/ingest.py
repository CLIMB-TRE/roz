import csv
import os
import sys
import time
import json
import copy

from onyx import OnyxClient

import varys

from roz_scripts.utils import utils


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
    log = varys.utils.init_logger(
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
                        csv_file=utils.s3_to_fh(
                            matched_message["files"][".csv"]["uri"],
                            matched_message["files"][".csv"]["etag"],
                        ),  # I don't like having a hardcoded metadata file name like this but hypothetically
                        test=True,  # we should always have a metadata CSV
                        fields={"site": matched_message["site"]},
                    )
                except Exception as e:
                    log.error(
                        f"Onxy test csv create failed for artifact: {matched_message['artifact']}, UUID: {matched_message['uuid']} due to client error: {e}"
                    )
                    continue

                to_test = False
                multiline_csv = False

                for response in response_generator:
                    if to_test:
                        log.info(
                            f"Metadata CSV for artifact {matched_message['artifact']}, UUID: {matched_message['uuid']} contains more than one record, metadata CSVs should only ever contain a single record"
                        )
                        multiline_csv = True
                        break
                    else:
                        to_test = response
        except Exception as e:
            log.error(f"Failed to connect to Onyx due to error: {e}")

        if not multiline_csv:
            with utils.s3_to_fh(
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
                        exchange="inbound.to_validate",
                        queue_suffix="ingest",
                    )
                    continue

                name_matches = {
                    x: metadata[x] == matched_message[x]
                    for x in ("sample_id", "run_name")
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

                if not all(name_matches.keys()):
                    to_send = None

                    to_send = parse_match_message(
                        matched_message=matched_message, payload=payload
                    )

                    varys_client.send(
                        message=to_send,
                        exchange="inbound.to_validate",
                        queue_suffix="ingest",
                    )
                    continue

        if multiline_csv:
            if payload["onyx_test_create_errors"].get("metadata_csv"):
                payload["onyx_test_create_errors"]["metadata_csv"].append(
                    "Multiline metadata CSVs are not permitted"
                )
            else:
                payload["onyx_test_create_errors"]["metadata_csv"] = [
                    "Multiline metadata CSVs are not permitted"
                ]

        else:
            # status, reason = handle_status_code(to_test.status_code)

            log.info(
                f"Received Onyx test create response for artifact: {matched_message['artifact']}"
            )

            payload["onyx_test_create_status"] = to_test.ok
            payload["onyx_test_status_code"] = to_test.status_code

            if to_test.json().get("messages"):
                for field, messages in to_test.json()["messages"].items():
                    if payload["onyx_test_create_errors"].get(field):
                        payload["onyx_test_create_errors"][field].extend(messages)
                    else:
                        payload["onyx_test_create_errors"][field] = messages

            # if not status:
            #     if reason == "unknown":
            #         log.error(
            #             f"Onyx test create returned an unknown status code: {to_test.status_code} for artifact: {matched_message['artifact']}, UUID: {payload['uuid']}"
            #         )
            #         continue

            #     elif reason == "perm_failure":
            #         log.error(
            #             f"Onyx test create for artifact: {matched_message['artifact']}, UUID: {payload['uuid']} due to Onyx permissions failure"
            #         )
            #         continue

            # elif reason == "success":
            #     log.info(
            #         f"Onyx test create success for artifact: {matched_message['artifact']}, UUID: {payload['uuid']}"
            #     )
            #     if to_test.json()["data"]["cid"]:
            #         log.error(
            #             f"Onyx appears to have assigned a CID ({response['data']['cid']}) to artifact: {matched_message['artifact']}. This should NOT happen in any circumstance."
            #         )
            #         continue

        to_send = None

        to_send = parse_match_message(matched_message=matched_message, payload=payload)

        varys_client.send(
            message=to_send,
            exchange="inbound.to_validate",
            queue_suffix="ingest",
        )


if __name__ == "__main__":
    main()
