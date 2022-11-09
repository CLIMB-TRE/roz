from sqlmodel import Field, Session, SQLModel, create_engine, select
from snoop_db.models import (
    new_file_table,
    matched_triplet_table,
    validation_result_table,
    new_artifact_table,
)
import snoop_db.db

import roz.varys

import queue
import os
import json
import time


class snooper:
    def __init__(self, varys_config, profile, logfile, log_level):
        self.__varys_config = varys_config
        self.__profile = profile
        self.__logfile = logfile

        self.cfg = roz.varys.configurator(self.__profile, self.__varys_config)

        self.queue = queue.Queue()

        self.consumer = roz.varys.consumer(
            received_messages=self.queue,
            configuration=self.cfg,
            log_file=self.__logfile,
            log_level=log_level
        ).start()

    def snoop(self):
        messages = []
        if not self.queue.empty():
            while not self.queue.empty():
                try:
                    messages.append(self.queue.get(block=False))
                except:
                    break
        else:
            return False
        return messages


def main():

    roz_cfg_path = os.getenv("ROZ_PROFILE_CFG")
    snooper_log_path = os.getenv("SNOOPER_LOG_PATH")

    log = roz.varys.init_logger("snoop_db", snooper_log_path, os.getenv("ROZ_LOG_LEVEL"))

    matched_triplet_snooper = snooper(
        varys_config=roz_cfg_path,
        profile="matched_triplets_snoop",
        logfile=snooper_log_path,
        log_level=os.getenv("ROZ_LOG_LEVEL")
    )

    validation_result_snooper = snooper(
        varys_config=roz_cfg_path,
        profile="validated_triplets_snoop",
        logfile=snooper_log_path,
        log_level=os.getenv("ROZ_LOG_LEVEL")
    )

    new_artifact_snooper = snooper(
        varys_config=roz_cfg_path,
        profile="new_artifacts_snoop",
        logfile=snooper_log_path,
        log_level=os.getenv("ROZ_LOG_LEVEL")
    )

    engine = snoop_db.db.make_engine()

    while True:
        try:
            matched_triplet_messages = matched_triplet_snooper.snoop()
        except queue.Empty:
            matched_triplet_messages = False

        try:
            validation_result_messages = validation_result_snooper.snoop()
        except queue.Empty:
            validation_result_messages = False

        try:
            new_artifact_messages = new_artifact_snooper.snoop()
        except queue.Empty:
            new_artifact_messages = False

        if matched_triplet_messages:
            with Session(engine) as session:
                for message in matched_triplet_messages:
                    payload = json.loads(message.body)
                    log.info(f"Submitting matched_triplet message #{message.basic_deliver.delivery_tag} to snoop_db")
                    try:
                        session.add(
                            matched_triplet_table(
                                timestamp=payload["match_timestamp"],
                                site_code=payload["site"],
                                pathogen_code=payload["pathogen_code"],
                                artifact=payload["artifact"],
                                csv_url=payload["files"]["csv"]["path"],
                                csv_md5=payload["files"]["csv"]["md5"],
                                fasta_url=payload["files"]["fasta"]["path"],
                                fasta_md5=payload["files"]["fasta"]["md5"],
                                bam_url=payload["files"]["bam"]["path"],
                                bam_md5=payload["files"]["bam"]["md5"],
                                payload=message.body,
                            )
                        )
                    except Exception as e:
                        log.error(f"Unable to submit message #{message.basic_deliver.delivery_tag} to snoop_db session with error: {e}")
                try:
                    session.commit()
                except Exception as e:
                    log.error(f"Unable to commit session to snoop_db with error: {e}")

        if validation_result_messages:
            with Session(engine) as session:
                for message in validation_result_messages:
                    payload = json.loads(message.body)
                    log.info(f"Submitting validation_result message #{message.basic_deliver.delivery_tag} to snoop_db")
                    try:
                        session.add(
                            validation_result_table(
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
                        log.error(f"Unable to submit message #{message.basic_deliver.delivery_tag} to snoop_db session with error: {e}")
                try:
                    session.commit()
                except Exception as e:
                    log.error(f"Unable to commit session to snoop_db with error: {e}")

        if new_artifact_messages:
            with Session(engine) as session:
                for message in new_artifact_messages:
                    payload = json.loads(message.body)
                    log.info(f"Submitting new_artifact message #{message.basic_deliver.delivery_tag} to snoop_db")
                    try:
                        session.add(
                            new_artifact_table(
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
                        log.error(f"Unable to submit message #{message.basic_deliver.delivery_tag} to snoop_db session with error: {e}")
                try:
                    session.commit()
                except Exception as e:
                    log.error(f"Unable to commit session to snoop_db with error: {e}")



            time.sleep(5)


if __name__ == "__main__":
    main()
