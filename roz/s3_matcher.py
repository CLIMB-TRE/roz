import sys
import json
import os
import boto3
import time
from collections import defaultdict, namedtuple
import uuid
import copy

import varys
import utils

from onyx import Session as onyx_session

from sqlmodel import Field, Session, SQLModel, create_engine, select
from snoop_db import db
from snoop_db.models import inbound_matched_table


def generate_file_uri(record):
    return f"s3://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}"


def get_already_matched_submissions():
    nested_ddict = lambda: defaultdict(nested_ddict)

    engine = db.make_engine()

    with Session(engine) as session:
        statement = select(inbound_matched_table)
        matched_submissions = session.exec(statement).all()

        out_dict = nested_ddict()

        for submission in matched_submissions:
            out_dict[submission.project][submission.site_code][submission.platform][
                submission.test_code
            ][submission.artifact] = json.loads(submission.payload)

        return out_dict


def handle_artifact_messages(
    artifact_messages,
    validation_config,
    log,
    varys_client,
):
    """
    Iterate through the artifact messages and do basic validation of whether the files provided match the specification
    artifact_messages should be structured as follows:
        {project_code:
            {site_code :
                {platform:
                    {artifact:
                        {file_type:
                            s3 on create RabbitMQ message JSON}}}}}

    It will return a list of named tuples with the following attributes:
        success: True if the artifact matched the specification and was sent to the validator
        previously_matched: Records to add to the previously matched dict
        project: Project code
        site_code: Site code
        platform: Platform
        artifact: Artifact name

    """

    _result = namedtuple(
        "result",
        [
            "success",
            "records",
            "project",
            "site_code",
            "platform",
            "test_flag",
            "artifact",
        ],
    )

    results = []

    for project, sites in artifact_messages.items():
        for site_code, platforms in sites.items():
            for platform, test_flags in platforms.items():
                for test_flag, artifacts in test_flags.items():
                    for artifact, records in artifacts.items():
                        if test_flag == "test":
                            test_bool = True
                        elif test_flag == "prod":
                            test_bool = False

                        parsed_fname = False
                        if len(records) != len(
                            validation_config["configs"][project]["file_specs"][
                                platform
                            ]["files"]
                        ):
                            log.info(
                                f"Skipping artifact: {artifact} for this loop since files provided do not appear to match the specification for this project and platform"
                            )
                            results.append(
                                _result(
                                    success=False,
                                    records=False,
                                    project=project,
                                    site_code=site_code,
                                    platform=platform,
                                    test_flag=test_flag,
                                    artifact=artifact,
                                )
                            )
                            continue

                        ftype_matches = {
                            x: False
                            for x in validation_config["configs"][project][
                                "file_specs"
                            ][platform]["files"]
                        }

                        for ftype in validation_config["configs"][project][
                            "file_specs"
                        ][platform]["files"]:
                            if records.get(ftype):
                                ftype_matches[ftype] = True

                        if all(ftype_matches.values()):
                            # Slightly gross but it means that we can keep it project/platform general by parsing the filename of the last file iterated over
                            parsed_fname = parse_fname(
                                fname=records[ftype]["s3"]["object"]["key"],
                                fname_layout=validation_config["configs"][project][
                                    "file_specs"
                                ][platform][ftype]["layout"],
                            )

                            log.info(
                                f"Submission matched for artifact: {artifact}, attempting to send submission payload"
                            )
                            try:
                                to_send = generate_payload(
                                    artifact=artifact,
                                    parsed_fname=parsed_fname,
                                    file_submission=records,
                                    project=project,
                                    site_code=site_code,
                                    upload_config=validation_config,
                                    platform=platform,
                                    test_bool=test_bool,
                                )

                                varys_client.send(
                                    message=to_send,
                                    exchange="inbound.matched",
                                    queue_suffix="s3_matcher",
                                )

                                results.append(
                                    _result(
                                        success=True,
                                        records=records,
                                        project=project,
                                        site_code=site_code,
                                        platform=platform,
                                        test_flag=test_flag,
                                        artifact=artifact,
                                    )
                                )
                            except Exception as e:
                                log.error(
                                    f"Failed to send payload for artifact: {artifact} with error: {e}"
                                )
                                results.append(
                                    _result(
                                        success=False,
                                        records=False,
                                        project=project,
                                        site_code=site_code,
                                        platform=platform,
                                        test_flag=test_flag,
                                        artifact=artifact,
                                    )
                                )

                        else:
                            log.info(
                                f"Provided files for artifact: {artifact} do not currently match the specification for this project and platform"
                            )
                            results.append(
                                _result(
                                    success=False,
                                    records=False,
                                    project=project,
                                    site_code=site_code,
                                    platform=platform,
                                    test_flag=test_flag,
                                    artifact=artifact,
                                )
                            )
    return results


def handle_update_messages(
    update_messages,
    validation_config,
    log,
    varys_client,
):
    """
    Iterate through the artifact messages and do basic validation of whether the files provided match the specification
    artifact_messages should be structured as follows:
        {project_code:
            {site_code :
                {platform:
                    {test_flag:
                        {artifact:
                            {file_type:
                                s3 on create RabbitMQ message JSON}}}}}

    It will return a list of named tuples with the following attributes:
        success: True if the artifact matched the specification and was sent to the validator
        previously_matched: Records to add to the previously matched dict
        project: Project code
        site_code: Site code
        platform: Platform
        artifact: Artifact name

    """

    _result = namedtuple(
        "result",
        [
            "success",
            "records",
            "project",
            "site_code",
            "platform",
            "test_flag",
            "artifact",
        ],
    )

    results = []

    for project, sites in update_messages.items():
        for site_code, platforms in sites.items():
            for platform, test_flags in platforms.items():
                for test_flag, artifacts in test_flags.items():
                    for artifact, records in artifacts.items():
                        if test_flag == "test":
                            test_bool = True
                        elif test_flag == "prod":
                            test_bool = False

                        modified_records = copy.deepcopy(records)

                        # Slightly gross but it means that we can keep it project/platform general by parsing the filename of the first file described in the spec
                        first_ftype = validation_config["configs"][project][
                            "file_specs"
                        ][platform]["files"][0]
                        parsed_fname = parse_fname(
                            fname=records[first_ftype]["s3"]["object"]["key"],
                            fname_layout=validation_config["configs"][project][
                                "file_specs"
                            ][platform][first_ftype]["layout"],
                        )

                        log.info(
                            f"Submission matched for previously rejected artifact: {artifact}, attempting to send submission payload"
                        )

                        try:
                            to_send = generate_payload(
                                artifact=artifact,
                                parsed_fname=parsed_fname,
                                file_submission=records,
                                project=project,
                                site_code=site_code,
                                upload_config=validation_config,
                                platform=platform,
                                test_bool=test_bool,
                            )

                            varys_client.send(
                                message=to_send,
                                exchange="inbound.matched",
                                queue_suffix="s3_matcher",
                            )
                            results.append(
                                _result(
                                    success=True,
                                    records=records,
                                    project=project,
                                    site_code=site_code,
                                    platform=platform,
                                    test_flag=test_flag,
                                    artifact=artifact,
                                )
                            )
                        except Exception as e:
                            log.error(
                                f"Failed to send payload for artifact: {artifact} with error: {e}"
                            )
                            results.append(
                                _result(
                                    success=False,
                                    records=False,
                                    project=project,
                                    site_code=site_code,
                                    platform=platform,
                                    test_flag=test_flag,
                                    artifact=artifact,
                                )
                            )

    return results


def query_onyx(
    project,
    artifact,
    parsed_fname,
    log,
    fname,
):
    try:
        with onyx_session(env_password=True) as session:
            response = next(
                session.filter(
                    project,
                    fields={
                        "sample_id": parsed_fname["sample_id"],
                        "run_name": parsed_fname["run_name"],
                    },
                )
            )

            if response.status_code == 500:
                log.error(
                    f"Onyx query for artifact: {artifact} lead to onyx internal server error"
                )
                return False

            elif response.status_code == 422:
                log.error(
                    f"Onyx query for artifact: {artifact} failed due to bad fields in request (should not happen ever)"
                )
                return False

            elif response.status_code == 404:
                log.error(
                    f"Onyx query for artifact: {artifact} failed because project: {project} does not exist"
                )
                return False

            elif response.status_code == 403:
                log.error(
                    f"Onyx query for artifact: {artifact} failed due to a permission error"
                )
                return False

            elif response.status_code == 400:
                log.error(
                    f"Onyx query for artifact: {artifact} failed due to a malformed request (should not happen ever)"
                )
                return False

            elif response.status_code == 200:
                if len(response.json()["data"]["records"]) == 1:
                    log.info(
                        f"Artifact: {artifact} has been sucessfully ingested previously with CID: {response.json()['data']['records'][0]['cid']} and as such cannot be modified by re-submission"
                    )
                    return False

                elif len(response.json()["data"]["records"]) > 1:
                    log.error(
                        f"onyx query returned more than one response for artifact: {artifact}"
                    )

                    return False

                else:
                    return True

            else:
                log.error(
                    f"Onyx query for artifact: {artifact} failed due to unhandled error code: {response.status_code}"
                )
                return False

    except Exception as e:
        log.error(f"Submitted file: {fname} lead to onyx-client exception: {e}")
        return False


def generate_payload(
    artifact,
    parsed_fname,
    file_submission,
    project,
    site_code,
    upload_config,
    platform,
    test_bool,
):
    unique = str(uuid.uuid4())

    ts = time.time_ns()

    files = {
        x: record_parser(file_submission[x])
        for x in upload_config["configs"][project]["file_specs"][platform]["files"]
    }

    s3_msgs = {
        x: file_submission[x]
        for x in upload_config["configs"][project]["file_specs"][platform]["files"]
    }

    uploaders = set(msg["userIdentity"]["principalId"] for msg in s3_msgs.values())

    payload = {
        "uuid": unique,
        "payload_version": 1,
        "site": site_code,
        "uploaders": list(uploaders),
        "match_timestamp": ts,
        "artifact": artifact,
        "sample_id": parsed_fname["sample_id"],
        "run_name": parsed_fname["run_name"],
        "project": project,
        "platform": platform,
        "files": files,
        "test_flag": test_bool,
    }

    return payload


def parse_fname(fname, fname_layout):
    fname_split = fname.split(".")
    spec_split = fname_layout.split(".")

    return {field: content for field, content in zip(spec_split, fname_split)}


def generate_artifact(parsed_fname, artifact_layout):
    layout = artifact_layout.split(".")

    return ".".join(str(parsed_fname[x]) for x in layout)


def record_parser(record):
    return {
        "uri": generate_file_uri(record),
        "etag": record["s3"]["object"]["eTag"],
        "key": record["s3"]["object"]["key"],
    }


def run(args):
    # TODO Standardise all of the ebvironmental variables -> parameterise them instead?

    for i in (
        "ONYX_ROZ_PASSWORD",
        "ROZ_CONFIG_JSON",
        "S3_MATCHER_LOG",
        "INGEST_LOG_LEVEL",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    log = varys.utils.init_logger(
        "roz_client", os.getenv("S3_MATCHER_LOG"), os.getenv("INGEST_LOG_LEVEL")
    )

    try:
        with open(os.getenv("ROZ_CONFIG_JSON"), "rt") as validation_cfg_fh:
            validation_config = json.load(validation_cfg_fh)
    except:
        log.error(
            "ROZ configuration JSON could not be parsed, ensure it is valid JSON and restart"
        )
        sys.exit(2)

    nested_ddict = lambda: defaultdict(nested_ddict)

    s3_credentials = utils.get_credentials()

    # Init S3 client
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

    previously_matched = get_already_matched_submissions()

    artifact_messages = nested_ddict()

    while True:
        messages = varys_client.receive_batch(
            exchange="inbound.s3",
            queue_suffix="s3_matcher",
        )

        update_messages = nested_ddict()

        for message in messages:
            ftype = None
            fname = None
            ftype = None

            payload = json.loads(message.body)

            for record in payload["Records"]:
                # Bucket names should follow the format "project-site_code-platform-test_status"
                project = record["s3"]["bucket"]["name"].split("-")[0]
                site_code = record["s3"]["bucket"]["name"].split("-")[1]
                platform = record["s3"]["bucket"]["name"].split("-")[2]
                test = record["s3"]["bucket"]["name"].split("-")[3]

                fname = record["s3"]["object"]["key"]

                if test != "prod" and test != "test":
                    log.error(
                        f"Test flag in bucket name is not either 'test' or 'prod' ignoring message"
                    )
                    continue

                if "/" in fname or "\\" in fname:
                    log.info(
                        f"Submitted object: {fname} in bucket: {record['s3']['bucket']['name']} appears to be within a bucket subdirectory, ignoring"
                    )
                    continue

                log.info(f"Attempting to process object with key: {fname}")

                for ext, file_spec in validation_config["configs"][project][
                    "file_specs"
                ][platform].items():
                    if fname.endswith(ext):
                        ftype = ext
                        break

                if not ftype:
                    log.error(
                        f"File {fname} doesn't appear to have a valid extension (accepted extensions are: {', '.join(str(x) for x in validation_config['configs'][project]['file_specs'][platform]['files'])}), ignoring"
                    )
                    continue

                if (
                    len(fname.split("."))
                    != validation_config["configs"][project]["file_specs"][platform][
                        ftype
                    ]["sections"]
                ):
                    log.error(
                        f"File {fname} does not appear to conform to filename specification, ignoring"
                    )
                    continue

                parsed_fname = parse_fname(
                    fname,
                    validation_config["configs"][project]["file_specs"][platform][
                        ftype
                    ]["layout"],
                )

                # If the filename spec contains "project" or "platform" ensure that the project in the filename matches the bucket name
                if parsed_fname.get("project"):
                    if parsed_fname["project"] != project:
                        log.info(
                            f"Submitted file: {fname} appears to be in a bucket for the wrong project, only files for the project {project} should be submitted to bucket: {record['s3']['bucket']['name']} ignoring"
                        )
                        continue

                if parsed_fname.get("platform"):
                    if parsed_fname["platform"] != platform:
                        log.info(
                            f"Submitted file: {fname} appears to be in a bucket for the wrong platform, only files for the platform {platform} should be submitted to bucket: {record['s3']['bucket']['name']} ignoring"
                        )
                        continue

                artifact = generate_artifact(
                    parsed_fname,
                    validation_config["configs"][project]["artifact_layout"],
                )

                if previously_matched[project][site_code][platform][test].get(artifact):
                    previous_etag = previously_matched[project][site_code][platform][
                        test
                    ][artifact][ftype]["s3"]["object"]["eTag"]

                    if previous_etag == record["s3"]["object"]["eTag"]:
                        log.info(
                            f"Previously ingested file: {fname} has been previously matched and appears identical to previously matched version, ignoring"
                        )
                        continue

                    else:
                        onyx_resp = query_onyx(
                            project=project,
                            artifact=artifact,
                            parsed_fname=parsed_fname,
                            log=log,
                            fname=fname,
                        )

                        if onyx_resp:
                            log.info(
                                f"Resubmitting previously rejected submission with for artifact: {artifact} due to update of submission {ftype}"
                            )
                            if update_messages[project][site_code][platform][test].get(
                                artifact
                            ):
                                update_messages[project][site_code][platform][test][
                                    artifact
                                ][ftype] = record
                            else:
                                previous_matched_copy = copy.deepcopy(
                                    previously_matched[project][site_code][platform][
                                        test
                                    ][artifact]
                                )
                                update_messages[project][site_code][platform][test][
                                    artifact
                                ] = previous_matched_copy

                                update_messages[project][site_code][platform][test][
                                    artifact
                                ][ftype] = record
                        else:
                            continue

                else:
                    artifact_messages[project][site_code][platform][test][artifact][
                        ftype
                    ] = record

        artifact_results = handle_artifact_messages(
            artifact_messages=artifact_messages,
            validation_config=validation_config,
            log=log,
            varys_client=varys_client,
        )

        for result in artifact_results:
            if result.success:
                previously_matched[result.project][result.site_code][result.platform][
                    result.test_flag
                ][result.artifact] = result.records
                del artifact_messages[result.project][result.site_code][
                    result.platform
                ][result.test_flag][result.artifact]

        update_results = handle_update_messages(
            update_messages=update_messages,
            validation_config=validation_config,
            log=log,
            varys_client=varys_client,
        )

        for result in update_results:
            if result.success:
                previously_matched[result.project][result.site_code][result.platform][
                    result.test_flag
                ][result.artifact] = result.records

        time.sleep(args.sleep_time)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep-time", default=30)
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
