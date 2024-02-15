import boto3
from botocore.config import Config
from botocore import UNSIGNED
import os
from ftplib import FTP
import urllib.request
from urllib.error import URLError
import datetime

base_db_path = "/shared/public/db/kraken2/"


def k2_db_generator():
    bucket = "genome-idx"

    s3 = boto3.client(
        "s3", config=Config(signature_version=UNSIGNED), region_name="eu-west-2"
    )

    response = s3.list_objects_v2(Bucket=bucket, Prefix="kraken/", Delimiter="/")

    keys = [
        x["Key"]
        for x in response["Contents"]
        if x["Key"].split("/")[-1].startswith("k2") and x["Key"].endswith(".tar.gz")
    ]

    for key in keys:
        if key in (
            "kraken/k2_nt_20230502_missing_bracken150.tar.gz",
            "kraken/k2_standard_eupath_20201202.tar.gz",
            "kraken/k2_eupathdb48_20230407.tar.gz",
            "kraken/k2_eupathdb48_20201113.tar.gz",
        ):
            continue

        if len(key.split("_")) == 3:
            k2, db, date = key.split("_")
        else:
            k2, db, size, date = key.split("_")

            size = size.lstrip("0")

            db = f"{db}_{size}"

        date = date.rstrip(".tar.gz")

        date = datetime.datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")

        yield db, date, key


def get_k2_db(db, date, key):
    bucket = "genome-idx"

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    s3.download_file(
        Bucket=bucket,
        Key=key,
        Filename=os.path.join(base_db_path, f"{db}_{date}.tar.gz"),
    )

    os.makedirs(os.path.join(base_db_path, "kraken2", db, date))

    os.system(
        f"tar -xvf {base_db_path}/{db}_{date}.tar.gz -C {os.path.join(base_db_path, 'kraken2', db, date)}"
    )

    os.system(f"rm {base_db_path}/{db}_{date}.tar.gz")


def ncbi_taxonomy_generator():
    ftp = FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "ftplib-example-1")

    dbs = ftp.nlst("pub/taxonomy/taxdump_archive")
    for url in dbs:
        if not url.endswith(".zip"):
            continue
        splits = url.split("/")
        if splits[-1].startswith("new_taxdump"):
            date = splits[-1].split("_")[2].rstrip(".zip")
            yield url, splits[-1], date


def get_ncbi_taxonomy(ftp_url, filename, date):
    # ftp = FTP("ftp.ncbi.nlm.nih.gov")
    # ftp.login("anonymous", "ftplib-example-1")

    # ftp.cwd("pub/taxonomy/taxdump_archive")

    urllib.request.urlretrieve(
        f"ftp://ftp.ncbi.nlm.nih.gov/{ftp_url}",
        f"{base_db_path}/{filename}",
    )

    os.system(f"unzip {base_db_path}/{filename} -d {base_db_path}/taxonomy/{date}")

    os.system(f"rm {base_db_path}/{filename}")


def get_ncbi_blast():
    ftp = FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "ftplib-example-1")

    ftp.cwd("blast/db")

    segments = ftp.nlst()

    for segment in segments:
        db = segment.split(".")[0]
        if not os.path.exists(os.path.join(base_db_path, "blast", db)):
            if dry_run:
                print(f"Would make dir: {os.path.join(base_db_path, 'blast', db)}")

            else:
                os.makedirs(os.path.join(base_db_path, "blast", db))

        if segment.endswith(".tar.gz") and db in ("nt", "nr"):
            if dry_run:
                print(
                    f"Would get: {segment} to path: {str(os.path.join(base_db_path, 'blast', db))}"
                )
                continue

            retry_count = 0
            while retry_count < 3:
                try:
                    urllib.request.urlretrieve(
                        f"ftp://ftp.ncbi.nlm.nih.gov/blast/db/{segment}",
                        f"{base_db_path}/{segment}",
                    )
                except:
                    retry_count += 1
                    continue

            if retry_count == 3:
                raise URLError("Failed to download file")

            os.system(f"tar -xvf {base_db_path}/{segment} -C {base_db_path}/blast/{db}")

            os.system(f"rm {base_db_path}/{segment}")

    ftp.quit()


def run():
    k2_to_get = {}

    for db, date, key in k2_db_generator():
        db_path = os.path.join(base_db_path, "kraken2", db)

        if not os.path.exists(db_path):
            if dry_run:
                print(f"Would make dir: {db_path}")
            else:
                os.makedirs(db_path)

        k2_to_get.setdefault(db, (date, key))

        if datetime.datetime.strptime(
            k2_to_get[db][0], "%Y-%m-%d"
        ) < datetime.datetime.strptime(date, "%Y-%m-%d"):
            k2_to_get[db] = (date, key)

    for db, (date, key) in k2_to_get.items():
        if not os.path.exists(os.path.join(base_db_path, "kraken2", db, date)):
            if dry_run:
                print(
                    f"Would get: {db} {date} {key} to path: {str(os.path.join(base_db_path, 'kraken2', db, date))}"
                )
                continue

            get_k2_db(db, date, key)
            os.symlink(
                date,
                os.path.join(base_db_path, "kraken2", db, "latest"),
            )

    taxonomy_to_get = False

    for ftp_url, filename, date in ncbi_taxonomy_generator():
        db_path = os.path.join(base_db_path, "taxonomy")

        if not os.path.exists(db_path):
            os.makedirs(db_path)

        if taxonomy_to_get:
            if datetime.datetime.strptime(
                taxonomy_to_get[2], "%Y-%m-%d"
            ) < datetime.datetime.strptime(date, "%Y-%m-%d"):
                taxonomy_to_get = (ftp_url, filename, date)
        else:
            taxonomy_to_get = (ftp_url, filename, date)

    if not os.path.exists(os.path.join(base_db_path, "taxonomy", taxonomy_to_get[2])):
        if dry_run:
            print(
                f"Would get: {taxonomy_to_get[0]} to path: {str(os.path.join(base_db_path, 'taxonomy', taxonomy_to_get[2]))}"
            )
        else:
            os.makedirs(os.path.join(base_db_path, "taxonomy", taxonomy_to_get[2]))

            get_ncbi_taxonomy(
                taxonomy_to_get[0], taxonomy_to_get[1], taxonomy_to_get[2]
            )

            os.symlink(
                taxonomy_to_get[2],
                os.path.join(base_db_path, "taxonomy", "latest"),
            )

    get_ncbi_blast()


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--base-db-path", required=True, help="Base path for dbs")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    global base_db_path
    global dry_run

    base_db_path = args.base_db_path
    dry_run = args.dry_run

    run()


if __name__ == "__main__":
    main()
