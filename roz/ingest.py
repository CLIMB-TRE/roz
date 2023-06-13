import roz
import os
import sys
import time
import json

from onyxclient import Session as onyx_session

from roz import varys


def handle_status_code(status_code):
    if status_code == 422:
        return (False, "validation_failure")
    elif status_code == 403:
        return (False, "perm_failure")
    elif status_code == 201:
        return (True, "success")
    else:
        return (False, "unknown")


def main():
    for i in (
        "onyx_ROZ_PASSWORD",
        "ROZ_INGEST_LOG",
        "ROZ_PROFILE_CFG",
        "AWS_ENDPOINT",
        "ROZ_AWS_ACCESS",
        "ROZ_AWS_SECRET",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    # Setup producer / consumer
    log = varys.init_logger(
        "roz_ingest", os.getenv("ROZ_INGEST_LOG"), os.getenv("ROZ_LOG_LEVEL")
    )

    varys_client = varys.varys(
        profile="roz_admin",
        in_exchange="inbound.matched",
        out_exchange="inbound.to_validate",
        logfile=os.getenv("ROZ_INGEST_LOG"),
        log_level=os.getenv("ROZ_LOG_LEVEL"),
        queue_suffix="roz_ingest",
    )

    ingest_payload_template = {
        "mid": "",
        "artifact": "",
        "sample_id": "",
        "run_name": "",
        "project": "",
        "platform": "",
        "ingest_timestamp": "",
        "cid": False,
        "site": "",
        "created": False,
        "ingested": False,
        "files": False,  # Dict
        "local_paths": False,  # Dict
        "onyx_test_status_code": False,
        "onyx_test_errors": False,  # Dict
        "onyx_test_create_status": False,
        "onyx_status_code": False,
        "onyx_errors": False,  # Dict
        "onyx_create_status": False,
        "ingest_errors": False,
    }

    while True:
        # I think shallow copy is appropriate here?
        payload = ingest_payload_template.copy()

        message = varys_client.receive()

        matched_message = json.loads(message.body)

        payload["mid"] = message.basic_deliver.delivery_tag

        # Not sure how to fully generalise this, the idea is to have a csv as the only file that will always exist, so I guess this is okay?
        # CSV file must always be called '.csv' though
        with onyx_session() as client:
            log.info(
                f"Received match for artifact: {matched_message['artifact']}, now attempting to test_create record in Onyx"
            )

            try:
                # Test create from the metadata CSV
                response = client.csv_create(
                    matched_message["project"],
                    csv_path=matched_message["local_paths"][
                        ".csv"
                    ],  # I don't like having a hardcoded metadata file name like this but hypothetically
                    test=True,  # we should always have a metadata CSV
                )

                status, reason = handle_status_code(response.status_code)

                payload["onyx_test_create_status"] = status
                payload["onyx_test_status_code"] = response.status_code

                test_create_errors = (
                    response.json()["messages"]
                    if response.json().get("messages")
                    else False
                )

                payload["onyx_test_create_errors"] = test_create_errors

                log.info(
                    f"Received Onyx response for artifact: {matched_message['artifact']}"
                )

            except Exception as e:
                log.error(
                    f"Onxy test csv create failed for artifact: {matched_message['artifact']} due to client error: {e}"
                )
                continue

            if not status:
                if reason == "unknown":
                    log.error(
                        f"Onyx test create returned an unknown status code: {response.status_code} for artifact: {matched_message['artifact']}"
                    )
                    continue

                elif reason == "perm_failure":
                    log.error(
                        f"Onyx test create for artifact: {matched_message['artifact']} due to Onyx permissions failure"
                    )
                    continue

                if reason == "validation_failure":
                    log.info(
                        f"Onyx test create failed for artifact: {matched_message['artifact']} due to errors: {test_create_errors} (if this 'False' something has gone awry)"
                    )

            elif reason == "success":
                log.info(
                    f"Onyx test create success for artifact: {matched_message['artifact']}"
                )
                if response.json()["data"]["cid"]:
                    log.error(
                        f"Onyx appears to have assigned a CID ({response['data']['cid']}) to artifact: {matched_message['artifact']}. This should NOT happen in any circumstance."
                    )
                    continue

            payload["artifact"] = matched_message["artifact"]
            payload["sample_id"] = matched_message["sample_id"]
            payload["run_name"] = matched_message["run_name"]
            payload["project"] = matched_message["project"]
            payload["platform"] = matched_message["platform"]
            payload["ingest_timestamp"] = time.time_ns()
            payload["site"] = matched_message["site"]
            payload["files"] = matched_message["files"]
            payload["local_paths"] = matched_message["local_paths"]

            varys_client.send(payload)


if __name__ == "__main__":
    main()
