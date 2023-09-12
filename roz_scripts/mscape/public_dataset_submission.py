import csv
import os
import sys
import urllib.request
import boto3
import datetime

rows_of_interest = [
    "sample_id",
    "run_name",
    "sample_site",
    "sample_type",
    "collection_date",
    "study_id",
    "study_centre_id",
    "is_public",
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
            out_cols["approximate_date"] = "Y"
        else:
            out_cols["approximate_date"] = "N"

        out_cols["collection_date"] = datetime.datetime.strptime(
            row["collection_date"], "%Y-%m-%d"
        ).strftime("%Y-%m")

        if row["sequencing_protocol"] == "ILLUMINA":
            with open(
                f"mscapetest.{row['sample_id']}.{row['run_name']}.illumina.csv", "wt"
            ) as csv_fh:
                writer = csv.DictWriter(csv_fh, fieldnames=out_cols.keys())
                writer.writeheader()
                writer.writerow(out_cols)

            ftp_split = row["submitted_ftp"].split(";")

            fastq_1 = ftp_split[0]
            fastq_2 = ftp_split[1]

            local_path_1, response_1 = urllib.request.urlretrieve(f"ftp://{fastq_1}")
            local_path_2, response_2 = urllib.request.urlretrieve(f"ftp://{fastq_2}")

            if response_1.status_code == 200 and response_2.status_code == 200:
                s3_client.upload_file(
                    local_path_1,
                    "mscapetest-public-illumina-prod",
                    f"mscapetest.{row['sample_id']}.{row['run_name']}.1.fastq.gz",
                )

                s3_client.upload_file(
                    local_path_2,
                    "mscapetest-public-illumina-prod",
                    f"mscapetest.{row['sample_id']}.{row['run_name']}.2.fastq.gz",
                )

                s3_client.upload_file(
                    f"mscapetest.{row['sample_id']}.{row['run_name']}.illumina.csv",
                    "mscapetest-public-illumina-prod",
                    f"mscapetest.{row['sample_id']}.{row['run_name']}.illumina.csv",
                )

                # os.remove(local_path_1)
                # os.remove(local_path_2)
                # os.remove(
                #     f"mscapetest.{row['sample_id']}.{row['run_name']}.illumina.csv"
                # )
            else:
                print(
                    f"Failed to download 1 or 2 for artifact: {row['sample_id']}.{row['run_name']}"
                )
        elif row["sequencing_protocol"] == "OXFORD NANOPORE":
            with open(
                f"mscapetest.{row['sample_id']}.{row['run_name']}.ont.csv", "wt"
            ) as csv_fh:
                writer = csv.DictWriter(csv_fh, fieldnames=out_cols.keys())
                writer.writeheader()
                writer.writerow(out_cols)

            local_path, response = urllib.request.urlretrieve(
                f"ftp://{row['submitted_ftp']}"
            )

            if response.status_code == 200:
                s3_client.upload_file(
                    local_path_1,
                    "mscapetest-public-ont-prod",
                    f"mscapetest.{row['sample_id']}.{row['run_name']}.fastq.gz",
                )

                s3_client.upload_file(
                    f"mscapetest.{row['sample_id']}.{row['run_name']}.ont.csv",
                    "mscapetest-public-ont-prod",
                    f"mscapetest.{row['sample_id']}.{row['run_name']}.ont.csv",
                )

                # os.remove(local_path_1)
                # os.remove(local_path_2)
                # os.remove(
                #     f"mscapetest.{row['sample_id']}.{row['run_name']}.illumina.csv"
                # )
            else:
                print(
                    f"Failed to download fastq for artifact: {row['sample_id']}.{row['run_name']}"
                )
