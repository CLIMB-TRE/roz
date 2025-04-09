from varys import Varys

from roz_scripts.utils.utils import init_logger, get_onyx_credentials

from onyx import (
    OnyxClient,
)

import os
import json
import time
import sys
import requests

import boto3


def pathogenwatch_update(climb_id: str, payload: dict, log) -> tuple[bool, dict]:
    """Function to submit a genome to pathogenwatch

    Args:
        climb_id (str): CLIMB-ID of the genome
        payload (dict): Update payload dict
        log: Logger object

    Returns:
        tuple[bool, dict]: Tuple containing a bool indicating whether the submission succeeded and the updated payload dict
    """
    onyx_config = get_onyx_credentials()

    log.info(f"Updating Pathogenwatch record for CLIMB-ID: {climb_id}")
    with OnyxClient(config=onyx_config) as client:
        record = client.get(
            "pathsafe",
            climb_id=climb_id,
        )

    if not record.get("pathogenwatch_uuid"):
        log.info(
            f"Pathogenwatch UUID not found for CLIMB-ID: {climb_id}, cannot update record"
        )
        payload.setdefault("update_errors", [])
        payload["update_errors"].append(
            "Pathogenwatch UUID not found for record, cannot update"
        )

        payload["update_status"] = "failed"
        return (False, payload)

    ignore_fields = ["is_published", "published_date", "pathogenwatch_uuid"]

    headers = {
        "X-API-Key": os.getenv("PATHOGENWATCH_API_KEY"),
        "content-type": "application/json",
    }

    base_url = os.getenv("PATHOGENWATCH_ENDPOINT_URL")

    try:
        body = {"id": record["pathogenwatch_uuid"]}

        resp = requests.get(f"{base_url}/genomes/details", headers=headers, params=body)

        if resp.status_code != 200:
            log.error(
                f"Failed to retrieve Pathogenwatch genome details due to error: {resp.text}"
            )
            payload.setdefault("update_errors", [])
            payload["update_errors"].append(
                f"Failed to retrieve Pathogenwatch genome details with status code: {resp.status_code}, due to error: {resp.text}"
            )
            payload["update_status"] = "failed"
            return (False, payload)

        genome_id = resp.json()["id"]

    except requests.exceptions.RequestException as e:
        log.error(
            f"Failed to retrieve Pathogenwatch genome details due to error: {e}, sending result"
        )
        payload.setdefault("update_errors", [])
        payload["update_errors"].append(
            f"Failed to retrieve Pathogenwatch genome details due to error: {e}"
        )
        payload["update_status"] = "failed"
        return (False, payload)

    fields = {k: v for k, v in record.items() if k not in ignore_fields}

    # change site to submit_org for pathogenwatch benefit
    fields["submit_org"] = fields.pop("site")
    fields["sequencing_platform"] = fields.pop("platform")

    body = {
        "genomeId": genome_id,
        "metadata": fields,
    }

    try:
        r = requests.post(
            url=f"{base_url}/climb/genomes/upsert", headers=headers, data=body
        )

        if r.status_code != 200:
            log.error(
                f"Pathogenwatch update failed for CLIMB_ID: {climb_id} due to error: {r.text}"
            )
            payload.setdefault("update_errors", [])
            payload["update_errors"].append(
                f"Pathogenwatch update failed with status code: {r.status_code}, due to error: {r.json()}"
            )
            payload["update_status"] = "failed"
            return (False, payload)

    except requests.exceptions.RequestException as e:
        log.error(
            f"Pathogenwatch update for CID: {payload['climb_id']} due to error: {e}"
        )
        payload.setdefault("update_errors", [])
        payload["update_errors"].append(
            f"Pathogenwatch update failed due to error: {e}"
        )
        payload["update_status"] = "failed"
        return (False, payload)

    payload["update_status"] = "success"
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

        s3_client = boto3.client("s3", endpoint_url="https://s3.climb.ac.uk")

        while True:

            message = varys_client.receive(
                exchange="inbound-onyx-updates-pathsafe",
                queue_suffix="pathsafe_updater",
                timeout=60,
            )

            with open("/tmp/healthy", "w") as fh:
                fh.write(str(time.time_ns()))

            if message:

                parsed_message = json.loads(message.body)

                success, payload = pathogenwatch_update(
                    parsed_message["climb_id"], parsed_message, log
                )

                if success:
                    if payload["update_status"] == "success":
                        varys_client.publish(
                            exchange="inbound-onyx-updates",
                            routing_key="onyx_updates",
                            body=json.dumps(payload),
                        )

                        log.info(
                            f"Pathogenwatch update for CLIMB-ID: {payload['climb_id']} succeeded"
                        )

                    # Upload the updated payload to S3
                    payload.pop("climb_id")
                    s3_client.put_object(
                        Bucket=f"pathsafe-{payload['site_str']}-results",
                        Key=f"pathsafe.{payload['run_index']}.{payload['run_id']}.update.json",
                        Body=json.dumps(payload),
                    )
                    varys_client.acknowledge_message(message)

                else:

                    log.error(f"Failed to process message: {json.loads(message.body)}")
                    varys_client.nack_message(message)

    except BaseException:
        log.exception("Exception in main loop:")
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
        default="pathsafe_updater.log",
        help="Log file name",
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="info",
        help="Log level",
    )

    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
