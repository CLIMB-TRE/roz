import os
import sys
from urllib.parse import urlparse
from pathlib import Path
import time
import json
from types import SimpleNamespace

from varys import mqtt_client
from varys import wrap_stderr

from validation import csv_validator, fasta_validator, bam_validator


# BIG TODO -> write worker funcs so that multiprocessing / job queueing can be utilised.


class triplet_parser:
    def __init__(self, messages=set):
        self.__triplets = {}

    def populate_triplets(self, messages):
        for message in messages:
            parsed_url = urlparse(message["url"])
            parsed_path = Path(parsed_url.path)
            if parsed_path.suffix not in (".fasta", ".bam", ".csv"):
                continue
            else:
                self.__triplets[parsed_path.stem][parsed_path.suffix] = message

    def store_triplets():
        # TODO: Add some sort of SQL? Db for all messages, decisions, etc -> NOT A BIG OL JSON FILE EW
        pass

    def return_matched(self):
        for k, v in self.__triplets.items():
            if len(v) == 3:
                yield v

    def download_matched(self):
        # TODO: Discuss with rads how this should be implemented
        # Also add local path to file triplet store
        pass


# Ensure that all required environmental variables are set
def get_env_variables():
    env_vars = {
        "user": "ROZ_MQTT_USER",
        "pass": "ROZ_MQTT_PASS",
        "mqtt_host": "MQTT_HOST",
        "mqtt_port": "MQTT_PORT",
        "temp_dir": "ROZ_TEMP_DIR",
        "idx_ref_dir": "ROZ_REF_ROOT",
        "compound_ref_path": "ROZ_CPD_REF_PATH",
        "json_config": "ROZ_CONFIG_JSON",
    }

    config = {k: os.getenv(v) for k, v in env_vars.items()}
    
    if any(True for v in config.values() if v == None):
        none_vals = ", ".join(str(env_vars[k]) for k, v in config.items() if v == None)
        print(
            f"The following required environmental variables must be set for ROZ to function: {none_vals}.",
            file=sys.stderr,
        )
        sys.exit(10)
        
    return SimpleNamespace(**config)


def run(args):
    env_vars = get_env_variables()

    try:
        validation_config = json.load(env_vars["json_config"][args.pathogen_code])
    except:
        wrap_stderr(
            "ROZ configuration JSON could not be parsed, ensure it is valid JSON and restart"
        )
        sys.exit(2)

    mqtt = mqtt_client(
        env_vars["mqtt_host"],
        env_vars["mqtt_port"],
        env_vars["user"],
        env_vars["pwd"],
        verbosity=2,
    )

    mqtt.subscribe(args.inbound_topic, persistent=True)

    triplets = triplet_parser()

    while True:
        inbound_messages = mqtt.consume_messages(args.inbound_topic)
        if inbound_messages:
            triplets.populate_triplets(inbound_messages)
            for triplet_name, file_triplet in triplets.return_matched().items():
                triplet_errors = {}
                csv_url_parsed = urlparse(file_triplet[".csv"]["url"])
                fasta_url_parsed = urlparse(file_triplet[".fasta"]["url"])

                with open(file_triplet[".csv"]["url"], "rt") as csv_fh:
                    csv = csv_validator(validation_config, csv_fh, csv_url_parsed.path)
                    csv_valid = csv.validate()
                    triplet_errors["csv"] = csv.errors

                with open(file_triplet[".fasta"]["url"]) as fasta_fh:
                    fasta = fasta_validator(
                        validation_config, fasta_fh, fasta_url_parsed.path
                    )
                    fasta_valid = fasta.validate()
                    triplet_errors["fasta"] = fasta.errors

                bam = bam_validator(
                    validation_config,
                    file_triplet[".bam"]["url"],
                    csv.csv_data["seq_platform"],
                )
                bam_valid = bam.validate()
                triplet_errors["bam"] = bam.errors

                # TODO: Add all this to db
                triplet_payload = json.dumps(
                    {
                        triplet_name: {
                            "urls": {
                                "csv": file_triplet[".csv"]["url"],
                                "fasta": file_triplet[".csv"]["url"],
                                "bam": file_triplet[".bam"]["url"],
                            },
                            "errors": triplet_errors,
                        }
                    }
                )

                if csv_valid and fasta_valid and bam_valid:
                    mqtt.publish(args.outbound_topic, triplet_payload)
                else:
                    mqtt.publish(args.rejected_topic, triplet_payload)

        else:
            time.sleep(60 * args.check_interval)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--pathogen_code", required=True)
    # TODO: Add default topic constructors
    parser.add_argument(
        "--inbound_topic",
        default=None,
        help="Topic to watch for inbound file messages. default: ROZ/{pathogen_code}/inbound/",
    )
    parser.add_argument(
        "--outbound_topic",
        default=None,
        help="Topic to publish validated triplet messages to. default: ROZ/{pathogen_code}/validated_triplets/",
    )
    parser.add_argument(
        "--rejected_topic",
        default=None,
        help="Topic to publish rejected triplet messages to. default: ROZ/{pathogen_code}/rejected_triplets/",
    )
    parser.add_argument(
        "--check_interval",
        default=1,
        help="How often to attempt to check for inbound files (minutes)",
    )
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
