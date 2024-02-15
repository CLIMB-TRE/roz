from sqlmodel import Field, Session, SQLModel, create_engine, select
from snoop_db.models import (
    inbound_s3_table,
    inbound_matched_table,
    inbound_to_validate_table,
    inbound_validated_table,
    inbound_artifacts_table,
)
import snoop_db.db

# from roz import Varys

import os
import json
import time


def main():
    snooper_log_path = os.getenv("SNOOPER_LOG_PATH")

    log = varys.init_logger("snoop_db", snooper_log_path, os.getenv("INGEST_LOG_LEVEL"))

    inbound_s3 = varys.Varys(
        profile="roz",
        in_exchange="inbound.s3",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    inbound_matched = varys.Varys(
        profile="roz",
        in_exchange="inbound.matched",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    inbound_to_validate = varys.Varys(
        profile="roz",
        in_exchange="inbound.to_validate",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    inbound_validated = varys.Varys(
        profile="roz",
        in_exchange="inbound.validated",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    inbound_artifacts = varys.Varys(
        profile="roz",
        in_exchange="inbound.artifacts",
        logfile=snooper_log_path,
        log_level="DEBUG",
        queue_suffix="snoop_db",
    )

    engine = snoop_db.db.make_engine()

    while True:
        inbound_s3_messages = inbound_s3.receive_batch()

        inbound_matched_messages = inbound_matched.receive_batch()

        inbound_to_validate_messages = inbound_to_validate.receive_batch()

        inbound_validated_messages = inbound_validated.receive_batch()

        inbound_artifacts_messages = inbound_artifacts.receive_batch()

        if inbound_s3_messages:
            with Session(engine) as session:
                for message in inbound_s3_messages:
                    payload = json.loads(message.body)
                    for record in payload["Records"]:
                        log.info(
                            f"Submitting on_create message from bucket: {record['s3']['bucket']['name']} to snoop_db"
                        )
                        try:
                            session.add(
                                inbound_s3_table(
                                    bucket=record["s3"]["bucket"]["name"],
                                    uploader=record["userIdentity"]["principalId"],
                                    payload=record,
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
                                uuid=payload["uuid"],
                                site=payload["site"],
                                project=payload["project"],
                                platform=payload["platform"],
                                uploaders=payload["uploaders"],
                                artifact=payload["artifact"],
                                sample_id=payload["sample_id"],
                                run_id=payload["run_id"],
                                files=payload["files"],
                                local_paths=payload["local_paths"],
                                payload=payload,
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

        if inbound_to_validate_messages:
            with Session(engine) as session:
                for message in inbound_to_validate_messages:
                    payload = json.loads(message.body)
                    log.info(
                        f"Submitting to_validate message #{message.basic_deliver.delivery_tag} to snoop_db"
                    )
                    try:
                        session.add(
                            inbound_to_validate_table(
                                uuid=payload["uuid"],
                                timestamp=payload["match_timestamp"],
                                site_code=payload["site"],
                                project=payload["project"],
                                uploaders=payload["uploaders"],
                                artifact=payload["artifact"],
                                sample_id=payload["sample_id"],
                                run_id=payload["run_id"],
                                files=payload["files"],
                                local_paths=payload["local_paths"],
                                onyx_test_status_code=payload["onyx_test_status_code"],
                                onyx_test_errors=payload["onyx_test_errors"],
                                onyx_test_status=payload["onyx_test_status"],
                                payload=message.body,
                            )
                        )
                    except Exception as e:
                        log.error(
                            f"Unable to submit to_validate message #{message.basic_deliver.delivery_tag} to snoop_db session with error: {e}"
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
                                mid=payload["mid"],
                                timestamp=payload["match_timestamp"],
                                site_code=payload["site"],
                                project=payload["project"],
                                artifact=payload["artifact"],
                                sample_id=payload["sample_id"],
                                run_id=payload["run_id"],
                                files=payload["files"],
                                local_paths=payload["local_paths"],
                                onyx_status_code=payload["onyx_test_status_code"],
                                onyx_errors=payload["onyx_test_errors"],
                                onyx_status=payload["onyx_test_status"],
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
                                climb_id=payload["climb_id"],
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
