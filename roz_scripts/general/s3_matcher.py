from roz_scripts.utils.utils import get_s3_credentials, init_logger
from roz_scripts.general.s3_controller import create_config_map
from varys import varys

import boto3
from botocore.exceptions import ClientError

import uuid
import time
import json
import os


def get_existing_objects(s3_client: boto3.client, to_check: list) -> dict:
    """Fetches existing object keys from S3.

    Args:
        s3_client (boto3.client): s3 client
        to_check (list): list of bucket names to check

    Returns:
        dict: dictionary of bucket names and existing keys within them
    """
    existing_objects = {}

    for bucket_name in to_check:
        existing_objects[bucket_name] = []

        paginator = s3_client.get_paginator("list_objects_v2")

        try:
            response_iterator = paginator.paginate(Bucket=bucket_name, FetchOwner=True)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucket":
                continue

            raise e

        for response in response_iterator:
            if "Contents" in response:
                for obj in response["Contents"]:
                    existing_objects[bucket_name].append(obj)

    return existing_objects


def parse_object_key(
    object_key: str, config_dict: dict, project: str, platform: str
) -> tuple:
    """Parses an object key into a dict containing the fields specified in the config file.

    Args:
        object_key (str): Key of the S3 object to be parsed
        config_dict (dict): Dictionary containing the config file
        project (str): Project name as it appears in the config file
        platform (str): Platform name as it appears in the config file

    Returns:
        tuple: Tuple containing the extension of the object key and the parsed object key or False if the object key can't be parsed or doesn't match the spec
    """
    spec = False
    for extension in config_dict["configs"][project]["file_specs"][platform].keys():
        if object_key.endswith(extension):
            spec = config_dict["configs"][project]["file_specs"][platform][extension]
            break

    if not spec:
        return (False, False)

    key_split = object_key.split(".")

    spec_split = spec["layout"].split(".")

    if len(key_split) != len(spec_split):
        return (False, False)

    return (
        extension,
        {field: content for field, content in zip(spec_split, key_split)},
    )


def generate_artifact(parsed_object_key: dict, artifact_layout: str) -> str | bool:
    """Generates an artifact name from a parsed object key.

    Args:
        parsed_object_key (dict): Object key parsed into a dict with func parse_object_key
        artifact_layout (str): Layout of the artifact name from config json

    Returns:
        str | bool: Artifact name, or False if the artifact name can't be generated
    """

    layout = artifact_layout.split(".")

    try:
        artifact = ".".join(str(parsed_object_key[x]) for x in layout)
    except KeyError:
        return False

    return artifact


def gen_s3_uri(bucket_name: str, key: str) -> str:
    """Generates an S3 URI from a bucket name and key.

    Args:
        bucket_name (str): Name of the bucket
        key (str): Key of the object

    Returns:
        str: S3 URI
    """
    return f"s3://{bucket_name}/{key}"


def parse_existing_objects(existing_objects: dict, config_dict: dict) -> dict:
    """Parses existing objects into a dictionary of artifacts.

    Args:
        existing_objects (dict): Dictionary of existing objects from func get_existing_objects
        config_dict (dict): Dictionary containing the config file

    Returns:
        dict: Dictionary of artifacts
    """

    parsed_objects = {}

    for bucket_name, objs in existing_objects.items():
        project, site, platform, test_flag = bucket_name.split("-")

        for obj in objs:
            extension, parsed_object_key = parse_object_key(
                object_key=obj["Key"],
                config_dict=config_dict,
                project=project,
                platform=platform,
            )

            if not extension:
                continue

            artifact = generate_artifact(
                parsed_object_key=parsed_object_key,
                artifact_layout=config_dict["configs"][project]["artifact_layout"],
            )

            if not artifact:
                continue

            parsed_objects.setdefault(
                (artifact, project, site, platform, test_flag),
                {"files": {}, "objects": {}},
            )

            parsed_objects[(artifact, project, site, platform, test_flag)]["files"][
                extension
            ] = {
                "uri": gen_s3_uri(bucket_name, obj["Key"]),
                "etag": obj["ETag"],
                "key": obj["Key"],
                "submitter": obj["Owner"]["DisplayName"],
                "parsed_fname": parsed_object_key,
            }

            parsed_objects[(artifact, project, site, platform, test_flag)]["objects"][
                extension
            ] = obj

    return parsed_objects


def is_artifact_dict_complete(
    index_tuple: tuple, existing_object_dict: dict, config_dict: dict
) -> bool:
    """Checks if an artifact dict is complete.

    Args:
        index_tuple (tuple): Tuple containing artifact name, project name, site name, platform name, and test flag
        existing_object_dict (dict): Dictionary of artifacts
        config_dict (dict): Dictionary containing the config file

    Returns:
        bool: True if the artifact dict is complete, False otherwise
    """

    artifact, project, site, platform, test_flag = index_tuple

    artifact_dict = existing_object_dict[index_tuple]

    file_spec = config_dict["configs"][project]["file_specs"][platform]

    if artifact_dict["files"].keys() != file_spec.keys():
        return False

    return True


def parse_new_object_message(
    existing_object_dict: dict, new_object_message: dict, config_dict: dict
) -> tuple[bool, dict, tuple]:
    """Parses a new object message, adds it to the existing object dict, and checks if the artifact dict is complete according to the config file.

    Args:
        existing_object_dict (dict): Dictionary of artifacts
        new_object_message (dict): Dictionary containing the new object message

    Returns:
        tuple[bool, dict, tuple]: Tuple containing True if the artifact dict is complete, the existing object dict, and the index tuple
    """

    # There should only ever be one record here
    record = new_object_message["Records"][0]

    bucket_name = record["s3"]["bucket"]["name"]

    project, site, platform, test_flag = bucket_name.split("-")

    object_key = record["s3"]["object"]["key"]

    extension, parsed_object_key = parse_object_key(
        object_key=object_key,
        config_dict=config_dict,
        project=project,
        platform=platform,
    )

    if not extension:
        return (False, False)

    artifact = generate_artifact(
        parsed_object_key=parsed_object_key,
        artifact_layout=config_dict["configs"][project]["artifact_layout"],
    )

    if not artifact:
        return (False, False, False)

    index_tuple = (artifact, project, site, platform, test_flag)

    existing_object_dict.setdefault(index_tuple, {"files": {}, "objects": {}})

    existing_object_dict[index_tuple]["files"][extension] = {
        "uri": gen_s3_uri(bucket_name, object_key),
        "etag": record["s3"]["object"]["eTag"],
        "key": object_key,
        "submitter": record["userIdentity"]["principalId"],
        "parsed_fname": parsed_object_key,
    }

    existing_object_dict[index_tuple]["objects"][extension] = record

    return (
        is_artifact_dict_complete(
            (artifact, project, site, platform, test_flag),
            existing_object_dict,
            config_dict,
        ),
        existing_object_dict,
        index_tuple,
    )


def generate_payload(index_tuple: tuple, existing_object_dict: dict) -> dict:
    """Generates a payload for the matched artifact.

    Args:
        index_tuple (tuple): Tuple containing artifact name, project name, site name, platform name, and test flag
        existing_object_dict (dict): Dictionary of artifacts

    Returns:
        dict: Dictionary containing the payload
    """

    artifact, project, site, platform, test_flag = index_tuple

    artifact_dict = existing_object_dict[index_tuple]

    unique = str(uuid.uuid4())

    ts = time.time_ns()

    # Raise error if there's more than one sample id or run name (unpack the set like a tuple)
    (sample_id,) = set(
        x["parsed_fname"]["sample_id"] for x in artifact_dict["files"].values()
    )

    (run_name,) = set(
        x["parsed_fname"]["run_name"] for x in artifact_dict["files"].values()
    )

    payload = {
        "uuid": unique,
        "site": site,
        "uploaders": list(set(x["submitter"] for x in artifact_dict["files"].values())),
        "match_timestamp": ts,
        "artifact": artifact,
        "sample_id": sample_id,
        "run_name": run_name,
        "project": project,
        "platform": platform,
        "files": artifact_dict["files"],
        "test_flag": test_flag == "test",
    }

    return payload


def main():
    log = init_logger(
        "roz_client", os.getenv("S3_MATCHER_LOG"), os.getenv("INGEST_LOG_LEVEL")
    )

    s3_credentials = get_s3_credentials()

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    varys_client = varys(
        profile="roz",
        logfile=os.getenv("S3_MATCHER_LOG"),
        log_level=os.getenv("INGEST_LOG_LEVEL"),
    )

    with open(os.getenv("ROZ_CONFIG_JSON"), "r") as f:
        config_dict = json.load(f)

    config_map = create_config_map(config_dict=config_dict)
    print(config_map)

    buckets = []

    # Get all site buckets (might need changing to check the bucket name label later)
    for site, site_config in config_map["sites"].items():
        for bucket, bucket_arn in site_config["site_buckets"]:
            buckets.extend(bucket_arn)

    objects = get_existing_objects(s3_client=s3_client, to_check=buckets)

    existing_object_dict = parse_existing_objects(
        existing_objects=objects, config_dict=config_dict
    )

    while True:
        message = varys_client.receive(
            exchange="inbound.s3",
            queue_suffix="s3_matcher",
        )

        message_dict = json.loads(message.body)

        artifact_complete, existing_object_dict, index_tuple = parse_new_object_message(
            existing_object_dict=existing_object_dict,
            new_object_message=message_dict,
            config_dict=config_dict,
        )

        artifact, project, site, platform, test_flag = index_tuple

        if not any(artifact_complete, existing_object_dict, index_tuple):
            failure_message = f"Problem parsing object with key: {message_dict['Records'][0]['s3']['object']['key']}, probable cause - key does not match file spec for this bucket or is malformed"
            log.info(failure_message)
            varys_client.send(
                message=failure_message,
                exchange=f"inbound.results.{project}.{site}",
                queue_suffix="s3_matcher",
            )
            continue

        if not artifact_complete:
            continue

        payload = generate_payload(
            index_tuple=index_tuple, existing_object_dict=existing_object_dict
        )

        log.info(f"Successful match for artifact: {artifact}. Sending payload.")
        varys_client.send(
            message=payload, exchange="inbound.matched", queue_suffix="s3_matcher"
        )


if __name__ == "__main__":
    main()
