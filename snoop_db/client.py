from sqlmodel import Field, Session, SQLModel, create_engine, select
from snoop_db.models import (
    inbound_s3_table,
    inbound_matched_table,
    inbound_validated_table,
    inbound_artifacts_table,
)
import snoop_db.db

import roz.varys

import os
import json
import time


def main():

    snooper_log_path = os.getenv("SNOOPER_LOG_PATH")

    log = roz.varys.init_logger(
        "snoop_db", snooper_log_path, os.getenv("ROZ_LOG_LEVEL")
    )

    inbound_s3 = roz.varys.varys(
        profile="roz_admin",
        in_exchange="inbound.s3",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    inbound_matched = roz.varys.varys(
        profile="roz_admin",
        in_exchange="inbound.matched",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    inbound_validated = roz.varys.varys(
        profile="roz_admin",
        in_exchange="inbound.validated",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    inbound_artifacts = roz.varys.varys(
        profile="roz_admin",
        in_exchange="inbound.artifacts",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    engine = snoop_db.db.make_engine()

    while True:
        inbound_s3_messages = inbound_s3.receive_batch()

        inbound_matched_messages = inbound_matched.receive_batch()

        inbound_validated_messages = inbound_validated.receive_batch()

        inbound_artifacts_messages = inbound_artifacts.receive_batch()

        if inbound_s3_messages:
            with Session(engine) as session:
                for message in inbound_s3_messages:
                    payload = json.loads(message.body)
                    log.info(
                        f"Submitting matched_triplet message #{message.basic_deliver.delivery_tag} to snoop_db"
                    )
                    try:
                        session.add(
                            inbound_s3_table(
                                payload=message.body,
                            )
                        )
                    except Exception as e:
                        log.error(
                            f"Unable to submit message #{message.basic_deliver.delivery_tag} to snoop_db session with error: {e}"
                        )
                try:
                    session.commit()
                except Exception as e:
                    log.error(f"Unable to commit session to snoop_db with error: {e}")

        if inbound_matched_messages:
            with Session(engine) as session:
                for message in inbound_matched_messages:
                    payload = json.loads(message.body)
                    log.info(
                        f"Submitting matched_triplet message #{message.basic_deliver.delivery_tag} to snoop_db"
                    )
                    try:
                        session.add(
                            inbound_matched_table(
                                timestamp=payload["match_timestamp"],
                                site_code=payload["site"],
                                pathogen_code=payload["pathogen_code"],
                                artifact=payload["artifact"],
                                csv_url=payload["files"]["csv"]["path"],
                                csv_etag=payload["files"]["csv"]["etag"],
                                fasta_url=payload["files"]["fasta"]["path"],
                                fasta_etag=payload["files"]["fasta"]["etag"],
                                bam_url=payload["files"]["bam"]["path"],
                                bam_etag=payload["files"]["bam"]["etag"],
                                payload=message.body,
                            )
                        )
                    except Exception as e:
                        log.error(
                            f"Unable to submit message #{message.basic_deliver.delivery_tag} to snoop_db session with error: {e}"
                        )
                try:
                    session.commit()
                except Exception as e:
                    log.error(f"Unable to commit session to snoop_db with error: {e}")

        if inbound_validated_messages:
            with Session(engine) as session:
                for message in inbound_validated_messages:
                    payload = json.loads(message.body)
                    log.info(
                        f"Submitting validation_result message #{message.basic_deliver.delivery_tag} to snoop_db"
                    )
                    try:
                        session.add(
                            inbound_validated_table(
                                timestamp=payload["match_timestamp"],
                                site_code=payload["site"],
                                pathogen_code=payload["pathogen_code"],
                                artifact=payload["artifact"],
                                triplet_result=payload["triplet_result"],
                                csv_result=payload["validation"]["csv"]["result"],
                                csv_messages=json.dumps(
                                    payload["validation"]["csv"]["errors"]
                                ),
                                fasta_result=payload["validation"]["fasta"]["result"],
                                fasta_messages=json.dumps(
                                    payload["validation"]["fasta"]["errors"]
                                ),
                                bam_result=payload["validation"]["bam"]["result"],
                                bam_messages=json.dumps(
                                    payload["validation"]["bam"]["errors"]
                                ),
                                payload=message.body,
                            )
                        )
                    except Exception as e:
                        log.error(
                            f"Unable to submit message #{message.basic_deliver.delivery_tag} to snoop_db session with error: {e}"
                        )
                try:
                    session.commit()
                except Exception as e:
                    log.error(f"Unable to commit session to snoop_db with error: {e}")

        if inbound_artifacts_messages:
            with Session(engine) as session:
                for message in inbound_artifacts_messages:
                    payload = json.loads(message.body)
                    log.info(
                        f"Submitting new_artifact message #{message.basic_deliver.delivery_tag} to snoop_db"
                    )
                    try:
                        session.add(
                            inbound_artifacts_table(
                                cid=payload["cid"],
                                timestamp=payload["ingest_timestamp"],
                                created=payload["created"],
                                ingested=payload["ingested"],
                                site_code=payload["site"],
                                pathogen_code=payload["pathogen_code"],
                                artifact=payload["artifact"],
                                fasta_url=payload["fasta_path"],
                                bam_url=payload["bam_path"],
                                payload=message.body,
                            )
                        )
                    except Exception as e:
                        log.error(
                            f"Unable to submit message #{message.basic_deliver.delivery_tag} to snoop_db session with error: {e}"
                        )
                try:
                    session.commit()
                except Exception as e:
                    log.error(f"Unable to commit session to snoop_db with error: {e}")

            time.sleep(5)


if __name__ == "__main__":
    main()
