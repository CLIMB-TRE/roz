import csv
import os
import sys
import urllib.request
import boto3
import datetime

rows_of_interest = [
    "sample_id",
    "run_id",
    "sample_source",
    "sample_type",
    "collection_date",
    "study_id",
    "study_centre_id",
    "public_database_name",
    "public_database_accession",
    "library_protocol",
    "sequencing_protocol",
    "sample_latitude",
    "sample_longitude",
]

s3_client = boto3.client(
    "s3",
    endpoint_url="https://s3.climb.ac.uk",
    aws_access_key_id=os.getenv("ROZ_AWS_ACCESS"),
    aws_secret_access_key=os.getenv("ROZ_AWS_SECRET"),
)

with open(sys.argv[1]) as manifest_fh:
    reader = csv.DictReader(manifest_fh, delimiter=",")

    for row in reader:
        fastq_1 = None
        fastq_2 = None

        out_cols = {x: row[x] for x in rows_of_interest}

        if row["collection_date"] in ["2010-01-01", "2013-01-01", "2014-01-01"]:
            out_cols["is_approximate_date"] = "Y"
        else:
            out_cols["is_approximate_date"] = "N"

        out_cols["is_public_dataset"] = "Y"

        out_cols["collection_date"] = datetime.datetime.strptime(
            row["collection_date"], "%Y-%m-%d"
        ).strftime("%Y-%m")

        out_cols["input_type"] = "sample"

        if out_cols["sample_source"] == "gut":
            out_cols["sample_source"] = "faecal"

        out_cols["sample_type"] = "other"

        if row["sequencing_protocol"] == "ILLUMINA":
            with open(f"mscape.{row['sample_id']}.{row['run_id']}.csv", "wt") as csv_fh:
                writer = csv.DictWriter(csv_fh, fieldnames=out_cols.keys())
                writer.writeheader()
                writer.writerow(out_cols)

            ftp_split = row["submitted_ftp"].split(";")

            if len(ftp_split) != 2:
                print(f"Skipping single file illumina record")
                continue

            # fastq_1 = ftp_split[0]
            # fastq_2 = ftp_split[1]

            # local_path_1, response_1 = urllib.request.urlretrieve(
            #     f"ftp://{fastq_1}",
            #     f"{os.getcwd()}/mscape.{row['sample_id']}.{row['run_id']}.1.fastq.gz",
            # )
            # local_path_2, response_2 = urllib.request.urlretrieve(
            #     f"ftp://{fastq_2}",
            #     f"{os.getcwd()}/mscape.{row['sample_id']}.{row['run_id']}.2.fastq.gz",
            # )

            # s3_client.upload_file(
            #     local_path_1,
            #     "mscape-public-illumina-prod",
            #     f"mscape.{row['sample_id']}.{row['run_id']}.1.fastq.gz",
            # )

            # s3_client.upload_file(
            #     local_path_2,
            #     "mscape-public-illumina-prod",
            #     f"mscape.{row['sample_id']}.{row['run_id']}.2.fastq.gz",
            # )

            # s3_client.upload_file(
            #     f"mscape.{row['sample_id']}.{row['run_id']}.illumina.csv",
            #     "mscape-public-illumina-prod",
            #     f"mscape.{row['sample_id']}.{row['run_id']}.illumina.csv",
            # )

            # os.remove(local_path_1)
            # os.remove(local_path_2)
            # os.remove(
            #     f"mscape.{row['sample_id']}.{row['run_id'']}.illumina.csv"
            # )

        elif row["sequencing_protocol"] == "OXFORD NANOPORE":
            with open(
                f"mscape.{row['sample_id']}.{row['run_id']}.ont.csv", "wt"
            ) as csv_fh:
                writer = csv.DictWriter(csv_fh, fieldnames=out_cols.keys())
                writer.writeheader()
                writer.writerow(out_cols)

            # local_path, response = urllib.request.urlretrieve(
            #     f"ftp://{row['submitted_ftp']}",
            #     f"{os.getcwd()}/mscape.{row['sample_id']}.{row['run_id']}.fastq.gz",
            # )

            # s3_client.upload_file(
            #     local_path_1,
            #     "mscape-public-ont-prod",
            #     f"mscape.{row['sample_id']}.{row['run_id']}.fastq.gz",
            # )

            # s3_client.upload_file(
            #     f"mscape.{row['sample_id']}.{row['run_id']}.ont.csv",
            #     "mscape-public-ont-prod",
            #     f"mscape.{row['sample_id']}.{row['run_id']}.ont.csv",
            # )

            # os.remove(local_path_1)
            # os.remove(local_path_2)
            # os.remove(
            #     f"mscape.{row['sample_id']}.{row['run_id'']}.illumina.csv"
            # )
