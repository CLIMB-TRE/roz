from sqlmodel import Field, Session, SQLModel, create_engine, select
from snoop_db.models import (
    new_file_table,
    matched_triplet_table,
    validation_result_table,
    new_artifact_table,
)
from snoop_db.db import main

import varys
import queue
import os
import json
import time


class snooper:
    def __init__(self, varys_config, profile, model, logfile):
        self.__varys_config = varys_config
        self.__profile = profile
        self.__logfile = logfile

        self.cfg = varys.configurator(self.__profile, self.__varys_config)

        self.queue = queue.Queue()

        self.consumer = varys.consumer(
            received_messages=self.queue,
            configuration=self.cfg,
            log_file=self.__logfile,
        ).start()

    def __snoop(self):
        new_message = self.queue.get(block=False)
        yield new_message


def main():

    with open(os.getenv("ROZ_CONFIG_JSON"), "rt") as validation_cfg_fh:
        varys_config = json.load(validation_cfg_fh)

    matched_triplet_snooper = snooper(
        varys_config=varys_config,
        profile="matched_triplets",
        logfile=os.getenv("SNOOPER_LOG_PATH"),
    )

    validation_result_snooper = snooper(
        varys_config=varys_config,
        profile="validated_triplets",
        logfile=os.getenv("SNOOPER_LOG_PATH"),
    )

    new_artifact_snooper = snooper(
        varys_config=varys_config,
        profile="new_artifacts",
        logfile=os.getenv("SNOOPER_LOG_PATH"),
    )

    engine = db.main()

    with Session(engine) as session:
        while True:
            try:
                matched_triplet_message = matched_triplet_snooper.__snoop()
            except queue.Empty:
                matched_triplet_message = False

            try:
                validation_result_message = validation_result_snooper.__snoop()
            except queue.Empty:
                validation_result_message = False

            try:
                new_artifact_message = new_artifact_snooper.__snoop()
            except queue.Empty:
                new_artifact_message = False

            if matched_triplet_message:
                payload = json.loads(matched_triplet_message.body)
                session.add(
                    matched_triplet_table(
                        timestamp=payload["match_timestamp"],
                        site_code=payload["uploader"],
                        pathogen_code=payload["pathogen_code"],
                        artifact=payload["artifact"],
                        csv_url=payload["csv"]["path"],
                        csv_md5=payload["csv"]["md5"],
                        fasta_url=payload["fasta"]["path"],
                        fasta_md5=payload["fasta"]["hash"],
                        bam_url=payload["bam"]["path"],
                        bam_md5=payload["bam"]["hash"],
                        payload=matched_triplet_message.body,
                    )
                )

            if validation_result_message:
                payload = json.loads(validation_result_message.body)
                session.add(
                    validation_result_table(
                        timestamp=payload["match_timestamp"],
                        site_code=payload["uploader"],
                        pathogen_code=payload["pathogen_code"],
                        artifact=payload["artifact"],
                        triplet_result=payload["triplet_result"],
                        csv_result=payload["validation"]["csv"]["result"],
                        csv_messages=json.dumps(
                            payload["validation"]["csv"]["messages"]
                        ),
                        fasta_result=payload["validation"]["csv"]["result"],
                        fasta_messages=json.dumps(
                            payload["validation"]["csv"]["messages"]
                        ),
                        bam_result=payload["validation"]["csv"]["result"],
                        bam_messages=json.dumps(
                            payload["validation"]["csv"]["messages"]
                        ),
                        payload=matched_triplet_message.body,
                    )
                )

            if new_artifact_message:
                payload = json.loads(new_artifact_message.body)
                session.add(
                    new_artifact_table(
                        cid=payload["cid"],
                        timestamp=payload["match_timestamp"],
                        site_code=payload["uploader"],
                        pathogen_code=payload["pathogen_code"],
                        artifact=payload["artifact"],
                        csv_url=payload["csv"]["path"],
                        csv_md5=payload["csv"]["md5"],
                        fasta_url=payload["fasta"]["path"],
                        fasta_md5=payload["fasta"]["hash"],
                        bam_url=payload["bam"]["path"],
                        bam_md5=payload["bam"]["hash"],
                        payload=new_artifact_message.body,
                    )
                )

            session.commit()

            time.sleep(5)


if __name__ == "__main__":
    main()
