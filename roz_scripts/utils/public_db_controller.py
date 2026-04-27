import boto3
from botocore.config import Config
from botocore import UNSIGNED
import concurrent.futures
import glob
import hashlib
import json
import os
import time
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
        elif len(key.split("_")) == 5 and key.split("_")[3] == "GB":
            k2, db, size, gb, date = key.split("_")

            size = size.lstrip("0")

            db = f"{db}_{size}gb"
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


_BLAST_DB_METADATA = {
    "nr": "nr-prot-metadata.json",
    "nt": "nt-nucl-metadata.json",
    "core_nt": "core_nt-nucl-metadata.json",
    "nt_viruses": "nt_viruses-nucl-metadata.json",
}

_BLAST_FTP_BASE = "ftp://ftp.ncbi.nlm.nih.gov/blast/db"


def _fetch_blast_metadata(db):
    url = f"{_BLAST_FTP_BASE}/{_BLAST_DB_METADATA[db]}"
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


def _verify_md5(file_path, md5_path):
    expected = open(md5_path).read().split()[0].strip()
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest() == expected


def _segment_stem(segment):
    return segment[: segment.index(".tar.gz")]


def _segment_extracted(db_dir, segment):
    return bool(glob.glob(os.path.join(db_dir, f"{_segment_stem(segment)}.*")))


def _download_and_extract_segment(file_url, db_dir):
    segment = os.path.basename(file_url)
    dest = os.path.join(base_db_path, segment)
    md5_dest = dest + ".md5"

    if _segment_extracted(db_dir, segment):
        print(f"{segment}: already extracted, skipping")
        return

    # Existing download on disk — verify before re-downloading
    if os.path.exists(dest):
        if not os.path.exists(md5_dest):
            try:
                urllib.request.urlretrieve(file_url + ".md5", md5_dest)
            except Exception:
                pass
        if os.path.exists(md5_dest) and _verify_md5(dest, md5_dest):
            print(f"{segment}: resuming from valid existing download")
        else:
            print(f"{segment}: existing download is invalid, re-downloading")
            for path in (dest, md5_dest):
                if os.path.exists(path):
                    os.remove(path)

    if not os.path.exists(dest):
        downloaded = False
        for attempt in range(10):
            try:
                urllib.request.urlretrieve(file_url, dest)
                urllib.request.urlretrieve(file_url + ".md5", md5_dest)
                downloaded = True
                break
            except Exception as e:
                print(f"{segment}: download attempt {attempt + 1}/10 failed: {e}")
                for path in (dest, md5_dest):
                    if os.path.exists(path):
                        os.remove(path)
                time.sleep(min(2**attempt, 300))

        if not downloaded:
            raise URLError(f"Failed to download {segment} after 10 attempts")

        if not _verify_md5(dest, md5_dest):
            os.remove(dest)
            os.remove(md5_dest)
            raise ValueError(f"MD5 checksum mismatch for {segment} — file removed")
        os.remove(md5_dest)

    ret = os.system(f"tar -xf {dest} -C {db_dir}")
    if ret != 0:
        raise RuntimeError(f"tar extraction failed for {segment} (exit {ret})")

    os.remove(dest)


def get_ncbi_blast():
    for db in _BLAST_DB_METADATA:
        meta = _fetch_blast_metadata(db)
        date = datetime.datetime.fromisoformat(meta["last-updated"]).strftime(
            "%Y-%m-%d"
        )

        archive_dir = os.path.join(base_db_path, "blast", f"{db}_archive", date)
        symlink_path = os.path.join(base_db_path, "blast", db)

        if os.path.isdir(archive_dir):
            print(f"{db}: already have version {date}, skipping")
            continue

        if dry_run:
            print(f"Would make dir: {archive_dir}")
            for file_url in meta["files"]:
                print(f"Would get: {os.path.basename(file_url)} to path: {archive_dir}")
            continue

        os.makedirs(archive_dir, exist_ok=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    _download_and_extract_segment, file_url, archive_dir
                ): file_url
                for file_url in meta["files"]
            }
            for future in concurrent.futures.as_completed(futures):
                file_url = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Failed to process {os.path.basename(file_url)}: {e}")
                    raise

        try:
            os.remove(symlink_path)
        except OSError:
            pass

        os.symlink(os.path.join(f"{db}_archive", date), symlink_path)


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


def get_gtdb_db():
    index = urllib.request.urlopen("https://data.gtdb.aau.ecogenomic.org/releases/")

    resp = index.read()

    versions = re.findall(r"release\d{2,4}", resp.decode("utf-8"))
    versions = set(versions)

    version_numbers = [int(x.replace("release", "")) for x in versions]

    latest_version = max(versions, key=lambda x: int(x.replace("release", "")))
    latest_version_number = max(version_numbers)

    if os.path.exists(os.path.join(base_db_path, "gtdb", latest_version)):
        print(f"Found existing gtdb version: {latest_version}")
        return
    else:
        print(f"New gtdb version found: {latest_version}")

    if not dry_run:
        os.makedirs(os.path.join(base_db_path, "gtdb", latest_version))
    else:
        print(f"Would make dir: {os.path.join(base_db_path, 'gtdb', latest_version)}")

    # Download the latest gtdb version
    gtdb_url = f"https://data.gtdb.aau.ecogenomic.org/releases/{latest_version}/{latest_version_number}.0/auxillary_files/gtdbtk_package/full_package/gtdbtk_r{latest_version_number}_data.tar.gz"

    if not dry_run:
        urllib.request.urlretrieve(
            gtdb_url, f"{base_db_path}/gtdb/gtdbtk_r{latest_version_number}_data.tar.gz"
        )

        os.system(
            f"tar -xvf {base_db_path}/gtdb/gtdbtk_r{latest_version_number}_data.tar.gz -C {base_db_path}/gtdb/{latest_version}"
        )

        os.system(f"rm {base_db_path}/gtdb/gtdbtk_r{latest_version_number}_data.tar.gz")

        try:
            os.remove(os.path.join(base_db_path, "gtdb", "latest"))
        except OSError:
            pass

        os.symlink(
            latest_version,
            os.path.join(base_db_path, "gtdb", "latest"),
        )
    else:
        print(
            f"Would get: {gtdb_url} to path: {str(os.path.join(base_db_path, 'gtdb', latest_version))}"
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

    get_gtdb_db()


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
