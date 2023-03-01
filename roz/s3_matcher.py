import sys
import json
import queue
import os
import boto3
import hashlib
import time
from collections import defaultdict

from metadbclient import Session, utils

import roz.varys
from roz.util import get_env_variables

from sqlmodel import Field, Session, SQLModel, create_engine, select
from snoop_db import db
from snoop_db.models import inbound_matched_table


def generate_file_url(record):
    return f"https://{record['s3']['bucket']['name']}.s3.climb.ac.uk/{record['s3']['object']['key']}"


def metadb_search(pathogen_code, sample_id, run_name):
    """
    Check metadb for the existence of a record, based on the Db constraints this query should never return more than one result, if a return tuple evaluates to (False, True) something has gone severely wrong.
    """
    with Session(env_password=True) as session:
        response = next(
            session.get(
                pathogen_code=pathogen_code, sample_id=sample_id, run_name=run_name
            )
        )

        if len(response.json()["results"]) == 1:
            return (True, response.json()["results"])
        elif len(response.json()["results"]) > 1:
            return (False, response.json()["results"])
        else:
            return (False, False)


def get_already_matched_triplets():

    nested_ddict = lambda: defaultdict(nested_ddict)

    engine = db.make_engine()

    with Session(engine) as session:

        statement = select(inbound_matched_table)
        matched_triplets = session.exec(statement).all()

        out_dict = nested_ddict

        for triplet in matched_triplets:
            out_dict[triplet.pathogen_code][triplet.site_code][
                triplet.artifact
            ] = json.loads(triplet.payload)

        return out_dict


def pull_triplet_files(s3_client, records, local_scratch_path, log):
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


def generate_payload(artifact, file_triplet, pathogen_code, site_code, spec_version=1):
    if spec_version == 1:
        ts = time.time_ns()
        payload = {
            "payload_version": 1,
            "site": site_code,
            "match_timestamp": ts,
            "artifact": artifact,
            "pathogen_code": pathogen_code,
            "files": {
                "csv": record_parser(file_triplet["csv"]),
                "fasta": record_parser(file_triplet["fasta"]),
                "bam": record_parser(file_triplet["bam"]),
            },
            "s3_msgs": {
                "csv": file_triplet["csv"],
                "fasta": file_triplet["fasta"],
                "bam": file_triplet["bam"],
            },
        }
    else:
        # TODO HANDLE IT
        pass

    return payload


def record_parser(record):
    return {"url": generate_file_url(record), "etag": record["s3"]["object"]["eTag"]}


def run(args):
    nested_ddict = lambda: defaultdict(nested_ddict)

    env_vars = get_env_variables()

    for i in (
        "METADB_ROZ_PASSWORD",
        "ROZ_INGEST_LOG",
        "ROZ_SCRATCH_PATH",
        "AWS_ENDPOINT",
        "ROZ_AWS_ACCESS",
        "ROZ_AWS_SECRET",
    ):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    # Init S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT"),
        aws_access_key_id=os.getenv("ROZ_AWS_ACCESS"),
        aws_secret_access_key=os.getenv("ROZ_AWS_SECRET"),
    )

    varys_client = roz.varys.varys(
        profile="roz_admin",
        in_exchange="inbound.s3",
        out_exchange="inbound.matched",
        logfile=env_vars.logfile,
        log_level=env_vars.log_level,
        queue_suffix="s3_matcher",
    )

    log = roz.varys.init_logger("roz_client", env_vars.logfile, env_vars.log_level)

    previously_matched = get_already_matched_triplets()

    artifact_messages = nested_ddict

    while True:

        messages = varys_client.receive_batch()

        update_messages = nested_ddict

        for message in messages:

            payload = json.loads(message.body)

            for record in payload["Records"]:

                pathogen_code = record["s3"]["bucket"]["name"].split("_")[0]
                site_code = record["s3"]["bucket"]["name"].split("_")[1]

                fname = record["s3"]["object"]["key"]
                if len(fname.split(".")) != 3:
                    log.error(
                        f"File {fname} does not appear to conform to filename specification, ignoring"
                    )
                    continue

                ftype = fname.split(".")[2]
                if ftype not in ("fasta", "csv", "bam"):
                    log.error(
                        f"File {fname} has an invalid extension (accepted extensions are: .fasta, .csv, .bam), ignoring"
                    )
                    continue

                artifact = ".".join(fname.split(".")[:2])

                if previously_matched[pathogen_code][site_code].get(artifact):
                    if (
                        previously_matched[artifact][f"{ftype}_etag"]
                        == payload["object"]["eTag"]
                    ):
                        log.info(
                            f"Previously ingested file: {fname} has been previously matched and appears identical to previously matched version, ignoring"
                        )
                        continue

                    else:
                        metadb_resp = metadb_search(
                            pathogen_code,
                            artifact.split(".")[0],
                            artifact.split(".")[1],
                        )

                        if not metadb_resp[0] and metadb_resp[1]:
                            log.error(
                                f"Metadb query returned more than one response; pathogen_code: {pathogen_code}\tsample_id: {artifact.split('.')[0]}\trun_name: {artifact.split('.')[1]}"
                            )
                            continue

                        elif metadb_resp[0] and metadb_resp[1]:
                            log.info(
                                f"Artifact: {artifact} has been ingested previously and as such cannot be modified by re-submission"
                            )
                            continue

                        elif not metadb_resp[0] and not metadb_resp[1]:
                            log.info(
                                f"Resubmitting previously rejected triplet for artifact: {artifact} due to update of triplet {ftype}"
                            )
                            if update_messages[pathogen_code][site_code].get(artifact):
                                update_messages[pathogen_code][site_code][artifact][
                                    ftype
                                ] = record
                            else:
                                update_messages[pathogen_code][site_code][
                                    artifact
                                ] = previously_matched[pathogen_code][site_code][
                                    artifact
                                ].copy()
                                update_messages[pathogen_code][site_code][artifact][
                                    ftype
                                ] = record

                        else:
                            log.error(
                                f"Submitted file: {fname} was handled improperly, this should never happen!"
                            )

                else:
                    artifact_messages[pathogen_code][site_code][artifact][
                        ftype
                    ] = record

        new_artifacts_to_delete = []

        for pathogen_code, sites in artifact_messages.items():
            for site_code, artifacts in sites.items():
                for artifact, records in artifacts.items():
                    if len(records) != 3:
                        continue

                    ftype_matches = {"fasta": False, "csv": False, "bam": False}
                    for ftype in ("fasta", "csv", "bam"):
                        if records.get(ftype):
                            ftype_matches[ftype] = True

                    if all(ftype_matches.values()):
                        log.info(
                            f"Triplet matched for artifact: {artifact}, attempting to download files then sending triplet payload"
                        )
                        try:
                            s3_resp = pull_triplet_files(
                                s3_client, records, os.getenv("ROZ_SCRATCH_PATH"), log
                            )
                            if all(s3_resp):
                                varys_client.send(
                                    generate_payload(
                                        artifact,
                                        records,
                                        pathogen_code,
                                        site_code,
                                        spec_version=1,
                                    )
                                )
                                previously_matched[pathogen_code][site_code][
                                    artifact
                                ] = records
                                new_artifacts_to_delete.append(
                                    (pathogen_code, site_code, artifact)
                                )

                            else:
                                log.error(
                                    f"Unable to pull {len([x for x in s3_resp if x])} files for artifact: {artifact}"
                                )
                                new_artifacts_to_delete.append(
                                    (pathogen_code, site_code, artifact)
                                )
                                previously_matched[pathogen_code][site_code][
                                    artifact
                                ] = records

                        except Exception as e:
                            log.error(
                                f"Failed to download files to local scratch then send triplet payload for artifact {artifact} with error: {e}"
                            )
                            new_artifacts_to_delete.append(
                                (pathogen_code, site_code, artifact)
                            )

        for pathogen_code, sites in update_messages.items():
            for site_code, artifacts in sites.items():
                for artifact, records in artifacts.items():
                    log.info(
                        f"Triplet matched for previously rejected artifact: {artifact}, attempting to download files then sending triplet payload"
                    )
                    try:
                        s3_resp = pull_triplet_files(
                            s3_client, records, os.getenv("ROZ_SCRATCH_PATH"), log
                        )
                        if all(s3_resp):
                            varys_client.send(
                                generate_payload(
                                    artifact,
                                    records,
                                    pathogen_code,
                                    site_code,
                                    spec_version=1,
                                )
                            )
                            previously_matched[pathogen_code][site_code][artifact] = records
                    except Exception as e:
                        log.error(
                            f"Failed to download files to local scratch then send triplet payload for artifact {artifact} with error: {e}"
                        )

        for new_artifact in new_artifacts_to_delete:
            del artifact_messages[new_artifact[0]][new_artifact[1]][new_artifact[2]]

        time.sleep(args.sleep_time)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep-time", default=30)
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
