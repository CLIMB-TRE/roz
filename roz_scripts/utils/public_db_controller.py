import boto3
from botocore.config import Config
from botocore import UNSIGNED
import os
from ftplib import FTP
import urllib.request
from urllib.error import URLError
import datetime
import requests
import doi
import re

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
        elif len(key.split("_")) == 4:
            k2, db, size, date = key.split("_")

            size = size.lstrip("0")

            db = f"{db}_{size}"
        elif len(key.split("_")) == 5 and key.split("_")[4] == "GB":
            k2, db, size, gb, date = key.split("_")

            size = size.lstrip("0")

            db = f"{db}_{size}"
        else:
            print(f"Unknown key format: {key} -> skipping")

        date = date.rstrip(".tar.gz")

        try:
            date = datetime.datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            try:
                date = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
            except ValueError:
                print(f"Failed to parse date: {date} -> skipping")
                continue

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

    resp = ftp.nlst()

    segments = [x for x in resp if x.endswith(".tar.gz")]

    ftp.quit()

    to_dl = {}

    for segment in segments:
        db = segment.split(".")[0]
        if db not in ("nr", "nt", "nt_viruses"):
            continue

        if os.path.exists(os.path.join(base_db_path, "blast", db)):
            continue

        to_dl.setdefault(db, []).append(segment)

    for db, segments in to_dl.items():
        if not os.path.exists(os.path.join(base_db_path, "blast", db)):
            if dry_run:
                print(f"Would make dir: {os.path.join(base_db_path, 'blast', db)}")

            else:
                os.makedirs(os.path.join(base_db_path, "blast", db))

        for segment in segments:

            if segment.endswith(".tar.gz") and db in ("nr", "nt", "nt_viruses"):
                if dry_run:
                    print(
                        f"Would get: {segment} to path: {str(os.path.join(base_db_path, 'blast', db))}"
                    )
                    continue

                retry_count = 0
                while retry_count < 3:
                    if retry_count == 3:
                        raise URLError("Failed to download file")

                    try:
                        urllib.request.urlretrieve(
                            f"ftp://ftp.ncbi.nlm.nih.gov/blast/db/{segment}",
                            f"{base_db_path}/{segment}",
                        )
                        break
                    except Exception:
                        retry_count += 1
                        continue

                os.system(
                    f"tar -xvf {base_db_path}/{segment} -C {base_db_path}/blast/{db}"
                )

                os.system(f"rm {base_db_path}/{segment}")


def get_bakta_db():

    latest_url = doi.get_real_url_from_doi("10.5281/zenodo.4247252")

    resp = requests.get(latest_url)

    try:
        db_version_file = requests.get(f"{resp.url}/files/db-versions.json")

        db_versions = db_version_file.json()

    except Exception:
        print(f"Failed to get db-versions.json from {resp.url}")

    latest_db = max(
        datetime.datetime.strptime(x["date"], "%Y-%m-%d") for x in db_versions
    )

    for db in db_versions:
        if not datetime.datetime.strptime(db["date"], "%Y-%m-%d") == latest_db:
            continue

        if os.path.exists(os.path.join(base_db_path, "bakta", db["date"])):
            print(f"Already have latest bakta db: {db['date']}")
            break

        doi_url = f"{doi.get_real_url_from_doi(db['doi'])}"

        db_url = f"{requests.get(doi_url).url}/files/db.tar.xz"

        if not dry_run:
            urllib.request.urlretrieve(db_url, f"{base_db_path}/db.tar.xz")

            os.makedirs(os.path.join(base_db_path, "bakta", db["date"]))

            os.system(
                f"tar -xvf {base_db_path}/db.tar.xz -C {base_db_path}/bakta/{db['date']}"
            )

            os.system(f"rm {base_db_path}/db.tar.xz")

            try:
                os.remove(os.path.join(base_db_path, "bakta", "latest"))
            except OSError:
                pass

            os.symlink(
                db["date"],
                os.path.join(base_db_path, "bakta", "latest"),
            )

        else:
            print(
                f"Would get: {db_url} to path: {str(os.path.join(base_db_path, 'bakta', db['date']))}"
            )


def get_sylph_globdb():
    index = urllib.request.urlopen("https://fileshare.lisc.univie.ac.at/globdb/")
    resp = index.read()

    versions = re.findall(r"globdb_r...", resp.decode("utf-8"))

    versions = set(versions)

    latest_version = max(versions, key=lambda x: int(x.replace("globdb_r", "")))

    if os.path.exists(os.path.join(base_db_path, "sylph", "globdb", latest_version)):
        print(f"Found existing globdb version: {latest_version}")
        return
    else:
        print(f"New globdb version found: {latest_version}")

    if not dry_run:
        os.makedirs(os.path.join(base_db_path, "sylph", "globdb", latest_version))
    else:
        print(
            f"Would make dir: {os.path.join(base_db_path, 'sylph', 'globdb', latest_version)}"
        )

    # Download the latest globdb version
    globdb_url = f"https://fileshare.lisc.univie.ac.at/globdb/{latest_version}/taxonomic_profiling/"

    version_index = urllib.request.urlopen(globdb_url)
    resp = version_index.read()

    files = re.findall(r'href="%s_.+?"' % latest_version, resp.decode("utf-8"))

    files = set(files)

    for file in files:
        file = file.replace('href="', "").replace('"', "")
        if "sylph" not in file:
            continue

        file_url = f"https://fileshare.lisc.univie.ac.at/globdb/{latest_version}/taxonomic_profiling/{file}"

        if not dry_run:
            urllib.request.urlretrieve(
                file_url, f"{base_db_path}/sylph/globdb/{latest_version}/{file}"
            )
        else:
            print(
                f"Would get: {file_url} to path: {str(os.path.join(base_db_path, 'sylph', 'globdb', latest_version, file))}"
            )


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

            try:
                os.remove(os.path.join(base_db_path, "kraken2", db, "latest"))
            except OSError:
                pass

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
            try:
                os.remove(os.path.join(base_db_path, "taxonomy", "latest"))
            except OSError:
                pass

            os.symlink(
                taxonomy_to_get[2],
                os.path.join(base_db_path, "taxonomy", "latest"),
            )

    get_ncbi_blast()

    get_bakta_db()

    get_sylph_globdb()


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
