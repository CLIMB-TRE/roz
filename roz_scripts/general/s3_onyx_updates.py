import os
import time
import sys
import json
import csv

import boto3

from roz_scripts.utils.utils import (
    init_logger,
    get_s3_credentials,
    get_onyx_credentials,
    onyx_update,
    s3_to_fh,
    csv_field_checks,
    EtagMismatchError,
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


def csv_onyx_update(message, roz_config_dict, log):
    pass


def onyx_climb_identify(run_index: str, run_id: str, project: str, log):

    with OnyxClient(config=get_onyx_credentials()) as client:
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
                    log.error(
                        f"Failed to find records with Onyx for: {project}.{run_index}.{run_id} despite successful identification by Onyx"
                    )
                    return (False, False)

                else:
                    if response[0]["is_published"]:
                        return (False, response[0]["climb_id"])

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
                    f"Onyx filter failed for artifact: {project}.{run_index}.{run_id}. Error: {e}"
                )
                return (True, False)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx filter failed for artifact: {project}.{run_index}.{run_id}. Error: {e}"
                )
                return (True, False)

            except Exception as e:
                log.error(f"Unhandled check_published error: {e}")
                return (True, False)


def onyx_identify_simple(
    identifier: str, identity_field: str, project: str, site: str, log
):
    onyx_config = get_onyx_credentials()

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

                return (True, response["identifier"])

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

                    return (False, False)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx identify error: {e}")
                return (False, False)

            except OnyxClientError as e:
                log.error(
                    f"Onyx identify failed for {identity_field}: {identifier} . Error: {e}"
                )
                return (False, False)

            except OnyxRequestError as e:
                if e.response.status_code == 404:
                    return (True, False)

                log.error(
                    f"Onyx identify failed for {identity_field}: {identifier}. Error: {e}"
                )
                return (False, False)

            except Exception as e:
                log.error(f"Unhandled onyx_identify error: {e}")
                return (False, False)


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

        s3_credentials = get_s3_credentials()

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_credentials.endpoint,
            aws_access_key_id=s3_credentials.access_key,
            aws_secret_access_key=s3_credentials.secret_key,
        )

        while True:

            message = varys_client.receive(
                exchange="inbound-s3",
                queue_suffix="onyx_updater",
                timeout=60,
            )

            with open("/tmp/healthy", "w") as fh:
                fh.write(str(time.time_ns()))

            if message:
                record = message["Records"][0]

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
                    varys_client.acknowledge_message(message)
                    continue

                # Skip new files from projects which do not have csv_updates enabled
                if not config_dict["configs"][parsed_bucket_name["project"]][
                    "csv_updates"
                ]:
                    varys_client.acknowledge_message(message)
                    continue

                extension, parsed_object_key = parse_object_key(
                    object_key=record["s3"]["object"]["key"],
                    config_dict=config_dict,
                    project=parsed_bucket_name["project"],
                    platform=parsed_bucket_name["platform"],
                    site=site,
                )

                if not extension or not parsed_object_key:
                    varys_client.acknowledge_message(message)
                    continue

                if extension != ".csv":
                    varys_client.acknowledge_message(message)
                    continue

                run_index_fail, run_index = onyx_identify_simple(
                    identifier=parsed_object_key["run_index"],
                    identity_field="run_index",
                    project=parsed_bucket_name["project"],
                    site=site,
                    log=log,
                )

                if run_index_fail:
                    varys_client.nack_message(message)
                    continue

                run_id_fail, run_id = onyx_identify_simple(
                    identifier=parsed_object_key["run_id"],
                    identity_field="run_id",
                    project=parsed_bucket_name["project"],
                    site=site,
                    log=log,
                )

                if run_id_fail:
                    varys_client.nack_message(message)
                    continue

                if not all([run_id, run_index]):
                    varys_client.acknowledge_message(message)
                    continue

                climb_id_fail, climb_id = onyx_climb_identify(
                    run_index=run_index,
                    run_id=run_id,
                    project=parsed_bucket_name["project"],
                    log=log,
                )

                if climb_id_fail:
                    varys_client.nack_message(message)
                    continue

                if not climb_id:
                    varys_client.acknowledge_message(message)
                    continue

                payload = {
                    "climb_id": climb_id,
                    "project": parsed_bucket_name["project"],
                    "run_index": run_index,
                    "run_id": run_id,
                    "site": site,
                }

                field_check_status, alert, payload = csv_field_checks(payload=payload)

                if alert:
                    varys_client.nack_message(message)
                    continue

                if not field_check_status:
                    varys_client.acknowledge_message(message)
                    continue

                csv_s3_uri = f"s3://{bucket_name}/{record['s3']['object']['key']}"

                with open(s3_to_fh(csv_s3_uri, record["s3"]["object"]["eTag"])) as fh:
                    reader = csv.DictReader(fh)

                    records = [record for record in reader]

                    if len(records) > 1:
                        log.error(
                            f"Multiple records found in CSV for {csv_s3_uri}. Skipping update"
                        )
                        varys_client.acknowledge_message(message)
                        continue

                    record = records[0]

                field_blacklist = ["climb_id", "run_index", "run_id", "biosample_id"]

                update_fields = {}

                for k, v in record.items():
                    if k in field_blacklist:
                        continue

                    update_fields[k] = v

                update_success, update_alert, payload = onyx_update(
                    payload=payload, fields=update_fields, log=log
                )

                if update_success:
                    payload["update_status"] = "success"
                    payload.pop("climb_id")

                    s3_client.put_object(
                        Bucket=f"{parsed_bucket_name['project']}-{parsed_bucket_name['site_str']}-results",
                        Key=f"{parsed_bucket_name['project']}.{run_index}.{run_id}.update.json",
                        Body=json.dumps(payload),
                    )

                else:
                    payload["update_status"] = "failed"

                    s3_client.put_object(
                        Bucket=f"{parsed_bucket_name['project']}-{parsed_bucket_name['site_str']}-results",
                        Key=f"{parsed_bucket_name['project']}.{run_index}.{run_id}.update.json",
                        Body=json.dumps(payload),
                    )

                varys_client.acknowledge_message(message)

    except BaseException:
        os.remove("/tmp/healthy")
        varys_client.close()
        time.sleep(1)
        sys.exit(1)
