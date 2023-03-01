from metadbclient import Session, utils
from csv import DictReader
import hashlib
from collections import namedtuple
import os
import sys
import roz.varys
import queue
import json
import time
import shutil
import boto3


# def hash_file(filepath, blocksize=2**20):
#     m = hashlib.etag()
#     with open(filepath, "rb") as f:
#         while True:
#             buf = f.read(blocksize)
#             if not buf:
#                 break
#             m.update(buf)
#     return m.hexdigest()

# TODO Put this all in s3 buckets (wait for rads)
def fmove(src, dest):
    """
    Move file from source to dest.  dest can include an absolute or relative path
    If the path doesn't exist, it gets created
    """
    dest_dir = os.path.dirname(dest)
    try:
        os.makedirs(dest_dir)
    except os.error as e:
        pass  # Assume it exists.  This could fail if you don't have permissions, etc...
    shutil.move(src, dest)

def add_to_bucket():



def meta_csv_parser(csv_path):
    reader = DictReader(open(csv_path, "rt"))

    metadata = next(reader)

    return metadata


def meta_create(metadata, pathogen_code):

    with Session(env_password=True) as session:
        response = session.create(pathogen_code=pathogen_code, fields=metadata)

    return response


def main():

    for i in ("METADB_ROZ_PASSWORD", "ROZ_INGEST_LOG", "ROZ_PROFILE_CFG", "AWS_ENDPOINT", "ROZ_AWS_ACCESS", "ROZ_AWS_SECRET"):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    # Setup producer / consumer
    log = roz.varys.init_logger(
        "roz_ingest", os.getenv("ROZ_INGEST_LOG"), os.getenv("ROZ_LOG_LEVEL")
    )

    #Init S3 client
    s3_client = boto3.client("s3",
        endpoint_url=os.getenv("AWS_ENDPOINT"),
        aws_access_key_id=os.getenv("ROZ_AWS_ACCESS"),
        aws_secret_access_key=os.getenv("ROZ_AWS_SECRET"),
    )

    varys_client = roz.varys.varys(
        profile="roz_admin",
        in_exchange="inbound.validated",
        out_exchange="inbound.artifacts",
        logfile=os.getenv("ROZ_INGEST_LOG"),
        log_level=os.getenv("ROZ_LOG_LEVEL"),
        queue_suffix="roz_ingest",
    )

    ingest_payload_template = {
        "artifact": "",
        "sample_id": "",
        "run_name": "",
        "pathogen_code": "",
        "ingest_timestamp": "",
        "cid": "",
        "site": "",
        "created": False,
        "ingested": False,
        "fasta_path": "",
        "bam_path": "",
        "metadb_status_code": "",
        "metadb_errors": {},
        "ingest_errors": [],
    }

    while True:
        try:
            validated_triplet = varys_client.receive()

            payload = json.loads(validated_triplet.body)

            ts = time.time_ns()

            out_payload = {
                "artifact": "",
                "sample_id": "",
                "run_name": "",
                "pathogen_code": "",
                "ingest_timestamp": "",
                "cid": "",
                "site": "",
                "created": False,
                "ingested": False,
                "fasta_path": "",
                "bam_path": "",
                "metadb_status_code": "",
                "metadb_errors": {},
                "ingest_errors": [],
            }
            out_payload["artifact"] = payload["artifact"]
            out_payload["pathogen_code"] = payload["pathogen_code"]
            out_payload["ingest_timestamp"] = ts
            out_payload["site"] = payload["site"]

            if payload["triplet_result"]:
                triplet_metadata = meta_csv_parser(
                    payload["files"]["csv"]["path"], payload["files"]["csv"]["etag"]
                )

                if not triplet_metadata:
                    log.error(
                        f"Metadata CSV for artifact: {payload['artifact']} checksum mismatch"
                    )
                    out_payload["ingest_errors"].append(
                        f"Metadata CSV for artifact: {payload['artifact']} checksum mismatch"
                    )
                else:
                    out_payload["sample_id"] = triplet_metadata["sample_id"]
                    out_payload["run_name"] = triplet_metadata["run_name"]

                    fasta_path = f"{os.getenv('MPX_ARTIFACTS')}/consensus/{payload['site']}/{triplet_metadata['run_name']}/{triplet_metadata['sample_id']}.{triplet_metadata['run_name']}.fasta"
                    bam_path = f"{os.getenv('MPX_ARTIFACTS')}/mapped_reads/{payload['site']}/{triplet_metadata['run_name']}/{triplet_metadata['sample_id']}.{triplet_metadata['run_name']}.bam"

                    out_payload["fasta_path"] = fasta_path
                    triplet_metadata["fasta_path"] = fasta_path

                    out_payload["bam_path"] = bam_path
                    triplet_metadata["bam_path"] = bam_path

                    try:
                        metadb_response = meta_create(
                            triplet_metadata, payload["pathogen_code"]
                        )
                    except Exception as e:
                        log.error(f"Metadb ingest failed due to exception: {e}")

                    out_payload["metadb_status_code"] = metadb_response.status_code

                    out_payload["metadb_errors"] = metadb_response.json()["errors"]

                    if metadb_response.ok:
                        out_payload["created"] = True

                        out_payload["cid"] = metadb_response.json()["results"][0]["cid"]

                        try:
                            fmove(payload["files"]["fasta"]["path"], fasta_path)
                            fasta_ingested = True
                        except Exception as e:
                            log.error(
                                f"Ingest of consensus for artifact '{payload['artifact']}' failed due to exception: {e}"
                            )
                            out_payload["ingest_errors"].append(e)

                        try:
                            fmove(payload["files"]["bam"]["path"], bam_path)
                            bam_ingested = True
                        except Exception as e:
                            log.error(
                                f"Ingest of aligned_reads for artifact '{payload['artifact']}' failed due to exception: {e}"
                            )
                            out_payload["ingest_errors"].append(e)

                        if fasta_ingested and bam_ingested:
                            out_payload["ingested"] = True

                        varys_client.send(out_payload)

                    else:

                        error_string = "\n".join(
                            f"Field: {k}\tError(s): {v[0]}"
                            for k, v in metadb_response.json()["errors"].items()
                        )
                        log.info(
                            f"Failed to create artifact on metadb due to the following errors:\n{error_string}"
                        )
                        out_payload["metadb_errors"] = metadb_response.json()["errors"]
                        varys_client.send(out_payload)

        except Exception as e:
            log.error(f"Ingest failed due to unhandled error: {e}")


if __name__ == "__main__":
    main()
