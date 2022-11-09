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


def hash_file(filepath, blocksize=2**20):
    m = hashlib.md5()
    with open(filepath, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


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


def meta_csv_parser(csv_path, csv_md5):
    if hash_file(csv_path) == csv_md5:
        reader = DictReader(open(csv_path, "rt"))

        metadata = next(reader)

        return metadata
    else:
        return False


def meta_create(metadata, pathogen_code):

    with Session(env_password=True) as session:
        response = session.create(pathogen_code=pathogen_code, fields=metadata)

    return response


def main():

    for i in ("METADB_ROZ_PASSWORD", "ROZ_INGEST_LOG", "ROZ_PROFILE_CFG"):
        if not os.getenv(i):
            print(f"The environmental variable '{i}' has not been set", file=sys.stderr)
            sys.exit(3)

    # Setup producer / consumer
    log = roz.varys.init_logger(
        "roz_client", os.getenv("ROZ_INGEST_LOG"), os.getenv("ROZ_LOG_LEVEL")
    )

    inbound_cfg = roz.varys.configurator(
        "validated_triplets", os.getenv("ROZ_PROFILE_CFG")
    )
    outbound_cfg = roz.varys.configurator("new_artifacts", os.getenv("ROZ_PROFILE_CFG"))

    inbound_queue = queue.Queue()
    outbound_queue = queue.Queue()

    ingest_consumer = roz.varys.consumer(
        received_messages=inbound_queue,
        configuration=inbound_cfg,
        log_file=os.getenv("ROZ_INGEST_LOG"),
        log_level=os.getenv("ROZ_LOG_LEVEL"),
    ).start()

    ingest_producer = roz.varys.producer(
        to_send=outbound_queue,
        configuration=outbound_cfg,
        log_file=os.getenv("ROZ_INGEST_LOG"),
        log_level=os.getenv("ROZ_LOG_LEVEL"),
    ).start()

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
            validated_triplet = inbound_queue.get()

            payload = json.loads(validated_triplet.body)

            log.info(payload)
            print(payload, file=sys.stderr)

            ts = time.time_ns()

            out_payload = ingest_payload_template.copy()
            out_payload["artifact"] = payload["artifact"]
            out_payload["pathogen_code"] = payload["pathogen_code"]
            out_payload["ingest_timestamp"] = ts
            out_payload["site"] = payload["site"]

            if payload["triplet_result"]:
                triplet_metadata = meta_csv_parser(
                    payload["files"]["csv"]["path"], payload["files"]["csv"]["md5"]
                )
                print(triplet_metadata, file=sys.stderr)

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

                    log.info(triplet_metadata)

                    print(triplet_metadata, file=sys.stderr)

                    try:
                        metadb_response = meta_create(
                            triplet_metadata, payload["pathogen_code"]
                        )
                    except Exception as e:
                        log.error(f"Metadb ingest failed due to exception: {e}")

                    out_payload["metadb_status_code"] = metadb_response.status_code

                    out_payload["metadb_errors"] = metadb_response.json()["errors"]

                    log.info(out_payload)
                    log.info(metadb_response)

                    if metadb_response.ok:
                        out_payload["created"] = True

                        out_payload["cid"] = metadb_response.json()["results"]["cid"]

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

                        outbound_queue.put(out_payload)

                    else:

                        error_string = "\n".join(
                            f"Field: {k}\tError(s): {v}"
                            for k, v in metadb_response["errors"].items()
                        )
                        log.info(
                            f"Failed to create artifact on metadb due to the following errors:\n{error_string}"
                        )
                        out_payload["metadb_errors"] = metadb_response["errors"]
                        outbound_queue.put(out_payload)

        except Exception as e:
            log.error(f"Ingest failed due to unhandled error: {e}")


if __name__ == "__main__":
    main()
