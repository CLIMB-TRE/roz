import sys
import json
import os
import boto3
import time
from collections import defaultdict
from types import SimpleNamespace

from onyx import Session as onyx_session

import roz.varys

from sqlmodel import Field, Session, SQLModel, create_engine, select
from snoop_db import db
from snoop_db.models import inbound_matched_table


def generate_file_url(record):
    return f"https://{record['s3']['bucket']['name']}.s3.climb.ac.uk/{record['s3']['object']['key']}"


def get_already_matched_submissions():
    nested_ddict = lambda: defaultdict(nested_ddict)

    engine = db.make_engine()

    with Session(engine) as session:
        statement = select(inbound_matched_table)
        matched_submissions = session.exec(statement).all()

        out_dict = nested_ddict()

        for submission in matched_submissions:
            out_dict[submission.project][submission.site_code][submission.platform][
                submission.artifact
            ] = json.loads(submission.payload)

        return out_dict


def pull_submission_files(s3_client, records, local_scratch_path, log):
    if not os.path.exists(local_scratch_path):
        raise Exception(
            f"The local scratch path: {local_scratch_path} provided with environmental variable 'ROZ_SCRATCH_PATH' does not appear to exist"
        )

    resps = []

    for k, v in records.items():
        local_path = os.path.join(local_scratch_path, v["s3"]["object"]["key"])

        try:
            s3_client.download_file(
                v["s3"]["bucket"]["name"], v["s3"]["object"]["key"], local_path
            )
            resps.append(local_path)
        except Exception as e:
            log.error(
                f"Unable to pull file: {v['s3']['object']['key']} for artifact: {k}, due to error: {e}"
            )
            resps.append(False)

    return resps


def generate_payload(
    artifact,
    parsed_fname,
    file_submission,
    project,
    site_code,
    upload_config,
    local_scratch_path,
    platform,
):
    ts = time.time_ns()

    files = {
        x: record_parser(file_submission[x])
        for x in upload_config["configs"][project]["file_specs"][platform]["files"]
    }

    s3_msgs = {
        x: file_submission[x]
        for x in upload_config["configs"][project]["file_specs"][platform]["files"]
    }

    local_paths = {
        x: os.path.join(local_scratch_path, s3_msgs[x]["s3"]["object"]["key"])
        for x in upload_config["configs"][project]["file_specs"][platform]["files"]
    }

    payload = {
        "payload_version": 1,
        "site": site_code,
        "match_timestamp": ts,
        "artifact": artifact,
        "sample_id": parsed_fname["sample_id"],
        "run_name": parsed_fname["run_name"],
        "project": project,
        "platform": platform,
        "files": files,
        "local_paths": local_paths,
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
        "url": generate_file_url(record),
        "etag": record["s3"]["object"]["eTag"],
        "key": record["s3"]["object"]["key"],
    }


def run(args):
    for i in (
        "ONYX_ROZ_PASSWORD",
        "ROZ_CONFIG_JSON",
        "S3_MATCHER_LOG",
        "S3_LOG_LEVEL",
        "ROZ_SCRATCH_PATH",
        "AWS_ENDPOINT",
        "ROZ_AWS_ACCESS",
        "ROZ_AWS_SECRET",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    log = roz.varys.init_logger(
        "roz_client", os.getenv("S3_MATCHER_LOG"), os.getenv("S3_LOG_LEVEL")
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

    # Init S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT"),
        aws_access_key_id=os.getenv("ROZ_AWS_ACCESS"),
        aws_secret_access_key=os.getenv("ROZ_AWS_SECRET"),
    )

    varys_client = roz.varys.varys(
        profile="roz",
        in_exchange="inbound.s3",
        out_exchange="inbound.matched",
        logfile=os.getenv("S3_MATCHER_LOG"),
        log_level=os.getenv("S3_LOG_LEVEL"),
        queue_suffix="s3_matcher",
    )

    previously_matched = get_already_matched_submissions()

    artifact_messages = nested_ddict()

    while True:
        messages = varys_client.receive_batch()

        update_messages = nested_ddict()

        for message in messages:
            ftype = None

            payload = json.loads(message.body)

            for record in payload["Records"]:
                # mscapetest-birm-ont-prod
                # Bucket names should follow the format "project-site_code-platform-test_status"
                project = record["s3"]["bucket"]["name"].split("-")[0]
                site_code = record["s3"]["bucket"]["name"].split("-")[1]
                platform = record["s3"]["bucket"]["name"].split("-")[2]
                test = record["s3"]["bucket"]["name"].split("-")[3]

                fname = record["s3"]["object"]["key"]

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

                artifact = generate_artifact(
                    parsed_fname,
                    validation_config["configs"][project]["artifact_layout"],
                )

                if previously_matched[project][site_code][platform].get(artifact):
                    if (
                        previously_matched[project][site_code][platform][artifact][
                            ftype
                        ]["s3"]["object"]["eTag"]
                        == record["s3"]["object"]["eTag"]
                    ):
                        log.info(
                            f"Previously ingested file: {fname} has been previously matched and appears identical to previously matched version, ignoring"
                        )
                        continue

                    else:
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
                                    continue

                                elif response.status_code == 422:
                                    log.error(
                                        f"Onyx query for artifact: {artifact} failed due to bad fields in request (should not happen ever)"
                                    )
                                    continue

                                elif response.status_code == 404:
                                    log.error(
                                        f"Onyx query for artifact: {artifact} failed because project: {project} does not exist"
                                    )
                                    continue

                                elif response.status_code == 403:
                                    log.error(
                                        f"Onyx query for artifact: {artifact} failed due to a permission error"
                                    )
                                    continue

                                elif response.status_code == 400:
                                    log.error(
                                        f"Onyx query for artifact: {artifact} failed due to a malformed request (should not happen ever)"
                                    )
                                    continue

                                elif response.status_code == 200:
                                    if len(response.json()["data"]["records"]) == 1:
                                        log.info(
                                            f"Artifact: {artifact} has been sucessfully ingested previously and as such cannot be modified by re-submission"
                                        )
                                        continue

                                    elif len(response.json()["data"]["records"]) > 1:
                                        log.error(
                                            f"onyx query returned more than one response for artifact: {artifact}"
                                        )

                                        continue
                                    else:
                                        log.info(
                                            f"Resubmitting previously rejected submission for artifact: {artifact} due to update of submission {ftype}"
                                        )
                                        if update_messages[project][site_code][
                                            platform
                                        ].get(artifact):
                                            update_messages[project][site_code][
                                                platform
                                            ][artifact][ftype] = record
                                        else:
                                            update_messages[project][site_code][
                                                platform
                                            ][artifact] = previously_matched[project][
                                                site_code
                                            ][
                                                platform
                                            ][
                                                artifact
                                            ].copy()
                                            update_messages[project][site_code][
                                                platform
                                            ][artifact][ftype] = record

                        except Exception as e:
                            log.error(
                                f"Submitted file: {fname} lead to onyx-client exception: {e}"
                            )
                            continue

                else:
                    artifact_messages[project][site_code][platform][artifact][
                        ftype
                    ] = record

        new_artifacts_to_delete = []

        for project, sites in artifact_messages.items():
            for site_code, platforms in sites.items():
                for platform, artifacts in platforms.items():
                    for artifact, records in artifacts.items():
                        if len(records) != len(
                            validation_config["configs"][project]["file_specs"][
                                platform
                            ]["files"]
                        ):
                            log.info(
                                f"Skipping artifact: {artifact} for this loop since not enough files have currently been provided based on the spec"
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
                            log.info(
                                f"Submission matched for artifact: {artifact}, attempting to download files then sending submission payload"
                            )
                            try:
                                to_send = False

                                s3_resp = pull_submission_files(
                                    s3_client,
                                    records,
                                    os.getenv("ROZ_SCRATCH_PATH"),
                                    log,
                                )
                                if all(s3_resp):
                                    to_send = generate_payload(
                                        artifact=artifact,
                                        parsed_fname=parsed_fname,
                                        file_submission=records,
                                        project=project,
                                        site_code=site_code,
                                        upload_config=validation_config,
                                        local_scratch_path=os.getenv(
                                            "ROZ_SCRATCH_PATH"
                                        ),
                                        platform=platform,
                                    )

                                    varys_client.send(to_send)

                                    previously_matched[project][site_code][platform][
                                        artifact
                                    ] = records
                                    new_artifacts_to_delete.append(
                                        (project, site_code, platform, artifact)
                                    )

                                else:
                                    log.error(
                                        f"Unable to pull {len([x for x in s3_resp if x])} files for artifact: {artifact}"
                                    )
                                    new_artifacts_to_delete.append(
                                        (project, site_code, platform, artifact)
                                    )
                                    previously_matched[project][site_code][platform][
                                        artifact
                                    ] = {
                                        x: record_parser(records[x])
                                        for x in validation_config["configs"][project][
                                            "file_specs"
                                        ][platform]["files"]
                                    }

                            except Exception as e:
                                log.error(
                                    f"Failed to download files to local scratch then send submission payload for artifact {artifact} with error: {e}"
                                )
                                new_artifacts_to_delete.append(
                                    (project, site_code, platform, artifact)
                                )
                        else:
                            log.info(
                                f"Provided files for artifact: {artifact} do not match the specification provided"
                            )
                            continue

        for project, sites in update_messages.items():
            for site_code, platforms in sites.items():
                for platform, artifacts in platforms.items():
                    for artifact, records in artifacts.items():
                        log.info(
                            f"Submission matched for previously rejected artifact: {artifact}, attempting to download files then sending submission payload"
                        )
                        try:
                            to_send = False

                            s3_resp = pull_submission_files(
                                s3_client,
                                records,
                                os.getenv("ROZ_SCRATCH_PATH"),
                                log,
                            )
                            if all(s3_resp):
                                to_send = generate_payload(
                                    artifact=artifact,
                                    parsed_fname=parsed_fname,
                                    file_submission=records,
                                    project=project,
                                    site_code=site_code,
                                    upload_config=validation_config,
                                    local_scratch_path=os.getenv("ROZ_SCRATCH_PATH"),
                                    platform=platform,
                                )

                                varys_client.send(to_send)
                                previously_matched[project][site_code][platform][
                                    artifact
                                ] = records

                            else:
                                log.error(
                                    f"Unable to pull {len([x for x in s3_resp if x])} files for previously matched artifact: {artifact}"
                                )
                                continue

                        except Exception as e:
                            log.error(
                                f"Failed to download files to local scratch then send submission payload for artifact {artifact} with error: {e}"
                            )

        for new_artifact in new_artifacts_to_delete:
            del artifact_messages[new_artifact[0]][new_artifact[1]][new_artifact[2]][
                new_artifact[3]
            ]

        time.sleep(args.sleep_time)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep-time", default=30)
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
