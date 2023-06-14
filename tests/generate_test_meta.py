import sys
import csv
import secrets
import random


def main():
    with open(sys.argv[1], "w") as testdata:
        fieldnames = [
            "sample_id",
            "run_name",
            "biosample_source_id",
            "sample_site",
            "sample_type",
            "batch_id",
            "lab_sample_id",
            "study_id",
            "study_centre_id",
            "collection_date",
            "received_date",
            "adm1_country",
            "adm2_region",
        ]
        writer = csv.DictWriter(testdata, fieldnames=fieldnames, delimiter=",")
        writer.writeheader()
        for _ in range(int(sys.argv[2])):
            writer.writerow(
                {
                    "sample_id": f"sample-{secrets.token_hex(3)}",
                    "run_name": f"run-{random.randint(1, 5)}",
                    "biosample_source_id": f"source-{secrets.token_hex(5).upper()}",
                    "sample_site": random.choice(
                        [
                            "respiratory",
                            "faecal",
                            "tissue",
                            "stool",
                            "blood",
                            "plasma",
                            "urine",
                        ]
                    ),
                    "sample_type": random.choice(
                        [
                            "swab",
                            "bal",
                            "aspirate",
                            "biopsy",
                            "other",
                        ]
                    ),
                    "batch_id": f"batch-{secrets.token_hex(3)}",
                    "lab_sample_id": f"lab-{secrets.token_hex(3)}",
                    "study_id": f"study-{secrets.token_hex(3)}",
                    "study_centre_id": f"study_centre-{secrets.token_hex(3)}",
                    "collection_date": f"2023-{random.randint(1, 5)}",
                    "received_date": f"2023-{random.randint(1, 5)}",
                    "adm1_country": random.choice(["eng", "scot", "wales", "ni"]),
                    "adm2_region": random.choice(["ne", "se"]),
                }
            )


if __name__ == "__main__":
    main()
