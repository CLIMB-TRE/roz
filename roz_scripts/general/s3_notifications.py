from roz_scripts.general.s3_controller import create_config_map
from roz_scripts.utils.utils import get_s3_credentials, init_logger
from varys import varys
import datetime
import copy
import os
import json
import boto3
from botocore.exceptions import ClientError
import sys
import time

message_template = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "ceph:s3",
            "awsRegion": "",
            "eventTime": "",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": ""},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {
                "x-amz-request-id": "",
                "x-amz-id-2": "",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "inbound.s3",
                "bucket": {
                    "name": "",
                    "ownerIdentity": {"principalId": ""},
                    "arn": "arn:aws:s3:::",
                    "id": "",
                },
                "object": {
                    "key": "",
                    "size": 0,
                    "eTag": "",
                    "versionId": "",
                    "sequencer": "",
                    "metadata": [
                        {"key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD"},
                        {"key": "x-amz-date", "val": "testdata"},
                    ],
                    "tags": [],
                },
            },
            "eventId": "",
            "opaqueData": "",
        }
    ]
}


def get_bucket_objects(s3_client: boto3.client, bucket_arn: str) -> list:
    """Get all objects in a bucket

    Args:
        s3_client (boto3.client): s3 client
        bucket_arn (str): bucket arn

    Returns:
        list: list of objects
    """
    objects = []

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        response_iterator = paginator.paginate(Bucket=bucket_arn, FetchOwner=True)

        for response in response_iterator:
            if "Contents" in response:
                for obj in response["Contents"]:
                    obj["Bucket"] = bucket_arn
                    obj["BucketArn"] = f"arn:aws:s3:::{bucket_arn}"
                    objects.append(obj)

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            return []

        raise e

    return objects


def obj_to_message(obj):
    message = copy.deepcopy(message_template)
    message["Records"][0]["eventTime"] = obj["LastModified"].strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    message["Records"][0]["userIdentity"]["principalId"] = obj["Owner"]["ID"]
    message["Records"][0]["requestParameters"]["sourceIPAddress"] = obj["Owner"][
        "DisplayName"
    ]
    message["Records"][0]["responseElements"]["x-amz-request-id"] = obj["Owner"]["ID"]
    message["Records"][0]["responseElements"]["x-amz-id-2"] = obj["Owner"]["ID"]
    message["Records"][0]["s3"]["bucket"]["name"] = obj["Bucket"]
    message["Records"][0]["s3"]["bucket"]["arn"] = obj["BucketArn"]
    message["Records"][0]["s3"]["bucket"]["id"] = obj["BucketArn"]
    message["Records"][0]["s3"]["object"]["key"] = obj["Key"]
    message["Records"][0]["s3"]["object"]["size"] = obj["Size"]
    message["Records"][0]["s3"]["object"]["eTag"] = obj["ETag"].replace('"', "")
    message["Records"][0]["s3"]["object"]["sequencer"] = obj["Owner"]["ID"]
    message["Records"][0]["eventId"] = obj["Owner"]["ID"]
    message["Records"][0]["opaqueData"] = obj["Owner"]["ID"]

    return message


def run(args):
    for i in (
        "S3_NOTIFICATIONS_LOG",
        "INGEST_LOG_LEVEL",
        "VARYS_CFG",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "ROZ_CONFIG_JSON",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    varys_client = varys(profile="roz", logfile=os.getenv("S3_NOTIFICATIONS_LOG"))

    s3_credentials = get_s3_credentials()

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    config_dict = json.load(open(os.getenv("ROZ_CONFIG_JSON"), "r"))

    config_map = create_config_map(config_dict)

    sent_etags = {}

    log = init_logger(
        name="s3_notifications",
        log_path=os.getenv("S3_NOTIFICATIONS_LOG"),
        log_level=os.getenv("INGEST_LOG_LEVEL"),
    )

    start_timestamp = datetime.datetime.now(datetime.timezone.utc).replace(
        microsecond=0
    )

    while True:
        log.info(f"Sleeping for {args.sleep_interval} seconds")
        time.sleep(args.sleep_interval)
        end_timestamp = datetime.datetime.now(datetime.timezone.utc).replace(
            microsecond=0
        )
        log.info(
            f"Checking for new objects between {start_timestamp} and {end_timestamp}"
        )
        for project, project_config in config_map.items():
            for bucket, bucket_arn in project_config["project_buckets"]:
                if bucket == "ingest":
                    objects = get_bucket_objects(
                        s3_client=s3_client, bucket_arn=bucket_arn
                    )
                    sent_etags.setdefault(bucket_arn, set())

                    for obj in objects:
                        last_modified = obj["LastModified"]
                        if start_timestamp <= last_modified <= end_timestamp:
                            if (obj["Key"], obj["ETag"]) not in sent_etags[bucket_arn]:
                                log.info(f"New object: {obj['Key']}")

                                message = obj_to_message(obj)
                                log.info(
                                    f"Sending message for new object: {obj['Key']}: {message}"
                                )
                                varys_client.send(
                                    message=message,
                                    exchange="inbound.s3",
                                    queue_suffix="ingest",
                                )
                                sent_etags[bucket_arn].add((obj["Key"], obj["ETag"]))
                            else:
                                log.info(f"Object already sent: {obj['Key']}")

            for site, site_config in project_config["sites"].items():
                for bucket, bucket_arn in site_config["site_buckets"]:
                    if bucket == "ingest":
                        objects = get_bucket_objects(
                            s3_client=s3_client, bucket_arn=bucket_arn
                        )
                        sent_etags.setdefault(bucket_arn, set())

                        for obj in objects:
                            last_modified = obj["LastModified"]
                            if start_timestamp <= last_modified <= end_timestamp:
                                if (obj["Key"], obj["ETag"]) not in sent_etags[
                                    bucket_arn
                                ]:
                                    log.info(f"New object: {obj['Key']}")
                                    message = obj_to_message(obj)
                                    varys_client.send(
                                        message=message,
                                        exchange="inbound.s3",
                                        queue_suffix="s3_matcher",
                                    )
                                    sent_etags[bucket_arn].add(
                                        (obj["Key"], obj["ETag"])
                                    )
                                else:
                                    log.info(f"Object already sent: {obj['Key']}")

        start_timestamp = end_timestamp


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Bucket notifications emulation")
    parser.add_argument(
        "--sleep-interval",
        help="Sleep interval between checks in seconds",
        type=int,
        default=600,
    )
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
