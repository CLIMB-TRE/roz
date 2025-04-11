import os
import time
import sys
import json
import csv

import boto3

from roz_scripts.utils.utils import (
    init_logger,
    get_s3_credentials,
    onyx_update,
    s3_to_fh,
    csv_field_checks,
)
from roz_scripts.general.s3_matcher import parse_object_key
from varys import Varys

from onyx import (
    OnyxClient,
    OnyxConfig,
)

from onyx.exceptions import (
    OnyxRequestError,
    OnyxConnectionError,
    OnyxServerError,
    OnyxConfigError,
    OnyxClientError,
)


def onyx_climb_identify(run_index: str, run_id: str, project: str, log):

    onyx_config = OnyxConfig(
        domain=os.getenv("ONYX_DOMAIN"), token=os.getenv("ONYX_ROZ_TOKEN")
    )

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                response = list(
                    client.filter(
                        project=project,
                        fields={
                            "run_index": run_index,
                            "run_id": run_id,
                        },
                    )
                )

                if len(response) == 0:
                    log.info(
                        f"Failed to find records with Onyx for: {project}, {run_index}, {run_id} despite successful identification by Onyx"
                    )
                    return (False, False)

                else:
                    if response[0]["is_published"]:
                        log.info(
                            f"Successfully identified climb_id: {response[0]['climb_id']} for {project}, {run_index}, {run_id}"
                        )
                        return (False, response[0]["climb_id"])

                    log.info(
                        f"Found record with Onyx: {project}, {run_index}, {run_id} but not published therefore skipping update"
                    )
                    return (False, False)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
                    )
                    time.sleep(3)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )

                    return (True, False)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                return (True, False)

            except OnyxClientError as e:
                log.error(
                    f"Onyx filter failed for artifact: {project}, {run_index}, {run_id}. Error: {e}"
                )
                return (True, False)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx filter failed for artifact: {project}, {run_index}, {run_id}. Error: {e}"
                )
                return (True, False)

            except Exception as e:
                log.error(f"Unhandled onyx_climb_identify error: {e}")
                return (True, False)


def onyx_identify_simple(
    identifier: str, identity_field: str, project: str, site: str, log
):
    onyx_config = OnyxConfig(
        domain=os.getenv("ONYX_DOMAIN"), token=os.getenv("ONYX_ROZ_TOKEN")
    )

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                # Consider making this a bit more versatile (explicitly input the identifier)
                response = client.identify(
                    project=project,
                    field=identity_field,
                    value=identifier,
                    site=site,
                )

                log.info(
                    f"Successfully identified {identity_field}: {identifier} as {response['identifier']}"
                )

                return (False, response["identifier"])

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
                    )
                    time.sleep(3)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )

                    return (True, False)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx identify error: {e}")
                return (True, False)

            except OnyxClientError as e:
                log.error(
                    f"Onyx identify failed for {identity_field}: {identifier} . Error: {e}"
                )
                return (True, False)

            except OnyxRequestError as e:
                if e.response.status_code in [400, 404]:
                    return (False, False)

                log.error(
                    f"Onyx identify failed for {identity_field}: {identifier}. Error: {e}"
                )
                return (True, False)

            except Exception as e:
                log.error(f"Unhandled onyx_identify error: {e}")
                return (True, False)


def csv_update(parsed_message, config_dict, log):

    record = parsed_message["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]

    parsed_bucket_name = {
        x: y
        for x, y in zip(
            ("project", "site_str", "platform", "test_flag"),
            bucket_name.split("-"),
        )
    }

    if "." in parsed_bucket_name["site_str"]:
        site = parsed_bucket_name["site_str"].split(".")[-2]
    else:
        site = parsed_bucket_name["site_str"]

    # ignore files from test buckets
    if parsed_bucket_name["test_flag"] == "test":
        log.info(
            f"Ignoring file: {record["s3"]["object"]["key"]} from test bucket: {bucket_name}"
        )
        return (True, False)

    # Skip new files from projects which do not have csv_updates enabled
    if not config_dict["configs"][parsed_bucket_name["project"]]["csv_updates"]:
        log.info(
            f"Skipping file: {record['s3']['object']['key']} from project: {parsed_bucket_name['project']}"
        )
        return (True, False)

    extension, parsed_object_key = parse_object_key(
        object_key=record["s3"]["object"]["key"],
        config_dict=config_dict,
        project=parsed_bucket_name["project"],
        platform=parsed_bucket_name["platform"],
    )

    if not extension or not parsed_object_key:
        log.info(f"Failed to parse object key: {record['s3']['object']['key']}")
        return (True, False)

    if extension != ".csv":
        log.info(f"File: {record['s3']['object']['key']} is not a CSV file. Ignoring")
        return (True, False)

    run_index_fail, run_index = onyx_identify_simple(
        identifier=parsed_object_key["run_index"],
        identity_field="run_index",
        project=parsed_bucket_name["project"],
        site=site,
        log=log,
    )

    if run_index_fail:
        log.error(
            f"Failed to identify run_index: {parsed_object_key['run_index']} for project: {parsed_bucket_name['project']}"
        )
        return (False, False)

    run_id_fail, run_id = onyx_identify_simple(
        identifier=parsed_object_key["run_id"],
        identity_field="run_id",
        project=parsed_bucket_name["project"],
        site=site,
        log=log,
    )

    if run_id_fail:
        log.error(
            f"Failed to identify run_id: {parsed_object_key['run_id']} for project: {parsed_bucket_name['project']}"
        )
        return (False, False)

    if not all([run_id, run_index]):
        log.info(
            f"Either run_id or run_index is unknown to onyx for {record['s3']['object']['key']}"
        )
        return (True, False)

    climb_id_fail, climb_id = onyx_climb_identify(
        run_index=run_index,
        run_id=run_id,
        project=parsed_bucket_name["project"],
        log=log,
    )

    if climb_id_fail:
        log.error(
            f"Failed to identify climb_id for {parsed_bucket_name['project']}.{parsed_object_key['run_index']}.{parsed_object_key['run_id']}"
        )
        return (False, False)

    if not climb_id:
        log.info(
            f"Climb_id not found for {parsed_bucket_name['project']}.{parsed_object_key['run_index']}.{parsed_object_key['run_id']}"
        )
        return (True, False)

    payload = {
        "climb_id": climb_id,
        "project": parsed_bucket_name["project"],
        "run_index": parsed_object_key["run_index"],
        "run_id": parsed_object_key["run_id"],
        "site": site,
        "site_str": parsed_bucket_name["site_str"],
        "files": {
            ".csv": {
                "uri": f"s3://{bucket_name}/{record['s3']['object']['key']}",
                "etag": record["s3"]["object"]["eTag"],
                "key": record["s3"]["object"]["key"],
            },
        },
        "artifact": f"{parsed_bucket_name['project']}.{parsed_object_key["run_index"]}.{parsed_object_key["run_id"]}",
        "uuid": "fake_uuid",
    }

    field_check_status, alert, payload = csv_field_checks(payload=payload)

    if alert:
        log.error(f"Field check failed for {payload['artifact']}.")
        return (False, payload)

    if not field_check_status:
        log.info(f"Field check failed: {payload}")
        payload.pop("climb_id")
        payload.pop("files")
        payload.pop("uuid")

        s3_credentials = get_s3_credentials()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_credentials.endpoint,
            aws_access_key_id=s3_credentials.access_key,
            aws_secret_access_key=s3_credentials.secret_key,
        )

        payload["update_status"] = "failed"

        s3_client.put_object(
            Bucket=f"{parsed_bucket_name['project']}-{parsed_bucket_name['site_str']}-results",
            Key=f"{payload['artifact']}.update.json",
            Body=json.dumps(payload),
        )

        return (True, payload)

    csv_s3_uri = f"s3://{bucket_name}/{record['s3']['object']['key']}"

    with s3_to_fh(csv_s3_uri, record["s3"]["object"]["eTag"]) as fh:
        reader = csv.DictReader(fh)

        records = [record for record in reader]

        if len(records) > 1:
            log.info(f"Multiple records found in CSV for {csv_s3_uri}. Skipping update")
            return (True, payload)

        record = records[0]

    field_blacklist = ["run_index", "run_id", "biosample_id"]

    update_fields = {}

    for k, v in record.items():
        if k in field_blacklist:
            continue

        update_fields[k] = v

    update_failure, update_alert, payload = onyx_update(
        payload=payload, fields=update_fields, log=log
    )

    payload.pop("files")
    payload.pop("uuid")

    s3_credentials = get_s3_credentials()

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    payload["update_status"] = "failed" if update_failure else "success"

    s3_client.put_object(
        Bucket=f"{parsed_bucket_name['project']}-{parsed_bucket_name['site_str']}-results",
        Key=f"{payload['artifact']}.update.json",
        Body=json.dumps(payload),
    )

    log.info(f"Update status for {payload['artifact']}: {payload['update_status']}")
    return (True, payload)


def run(args):
    try:
        log = init_logger("s3_onyx_updater", args.logfile, args.log_level)

        varys_client = Varys(
            profile="roz",
            logfile=args.logfile,
            log_level=args.log_level,
            auto_acknowledge=False,
        )

        with open(os.getenv("ROZ_CONFIG_JSON"), "r") as f:
            config_dict = json.load(f)

        while True:

            message = varys_client.receive(
                exchange="inbound-s3",
                queue_suffix="onyx_updater",
                timeout=60,
            )

            with open("/tmp/healthy", "w") as fh:
                fh.write(str(time.time_ns()))

            if message:

                parsed_message = json.loads(message.body)

                success, payload = csv_update(
                    parsed_message=parsed_message,
                    config_dict=config_dict,
                    log=log,
                )

                if success:
                    varys_client.acknowledge_message(message)

                    if not payload:
                        log.info(
                            f"No work to do for message: {json.loads(message.body)}"
                        )
                        continue

                    if payload["update_status"] == "success":
                        varys_client.send(
                            message=payload,
                            exchange=f"inbound-onyx-updates-{payload['project']}",
                            queue_suffix="onyx_updates",
                        )

                else:

                    log.error(f"Failed to process message: {json.loads(message.body)}")
                    varys_client.nack_message(message)

    except BaseException:
        log.exception("Unhandled error: ")
        os.remove("/tmp/healthy")
        varys_client.close()
        time.sleep(1)
        sys.exit(1)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--logfile",
        type=str,
        default="/tmp/s3_onyx_updater.log",
        help="Path to the log file",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        help="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
