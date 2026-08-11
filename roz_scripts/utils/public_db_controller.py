import boto3
from botocore.config import Config
from botocore import UNSIGNED
from contextlib import contextmanager
import concurrent.futures
import glob
import hashlib
import html
import json
import os
import subprocess
import time
from ftplib import FTP
import urllib.request
import urllib.parse
from urllib.error import URLError
import datetime
import requests
import doi
import re

base_db_path = "/shared/public/db/"
dry_run = False


def _run(cmd, description):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"{description} failed (exit {result.returncode}): {result.stderr}"
        )


@contextmanager
def _atomic_version_dir(path):
    """Create `path`, mark it incomplete, and only clear the marker on a clean exit.

    On failure the marker is left in place (and the partially-populated dir is
    kept, not deleted) so a later run can resume rather than being poisoned
    into treating an interrupted download/extraction as complete.
    """
    os.makedirs(path, exist_ok=True)
    marker = os.path.join(path, ".incomplete")
    open(marker, "w").close()
    yield path
    os.remove(marker)


def _version_complete(path):
    return os.path.isdir(path) and not os.path.exists(os.path.join(path, ".incomplete"))


def k2_db_generator():
    bucket = "genome-idx"

    s3 = boto3.client(
        "s3", config=Config(signature_version=UNSIGNED), region_name="eu-west-2"
    )

    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix="kraken/", Delimiter="/"):
        keys.extend(
            x["Key"]
            for x in page.get("Contents", [])
            if x["Key"].split("/")[-1].startswith("k2") and x["Key"].endswith(".tar.gz")
        )

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
            continue

        date = date.removesuffix(".tar.gz")

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

    tarball = os.path.join(base_db_path, f"{db}_{date}.tar.gz")
    s3.download_file(Bucket=bucket, Key=key, Filename=tarball)

    version_dir = os.path.join(base_db_path, "kraken2", db, date)

    try:
        with _atomic_version_dir(version_dir):
            _run(["tar", "-xf", tarball, "-C", version_dir], f"extracting {db} {date}")
    finally:
        if os.path.exists(tarball):
            os.remove(tarball)


def ncbi_taxonomy_generator():
    ftp = FTP("ftp.ncbi.nlm.nih.gov")
    ftp.login("anonymous", "ftplib-example-1")

    dbs = ftp.nlst("pub/taxonomy/taxdump_archive")
    for url in dbs:
        if not url.endswith(".zip"):
            continue
        splits = url.split("/")
        if splits[-1].startswith("new_taxdump"):
            date = splits[-1].split("_")[2].removesuffix(".zip")
            yield url, splits[-1], date


def get_ncbi_taxonomy(ftp_url, filename, date):
    archive = os.path.join(base_db_path, filename)

    urllib.request.urlretrieve(
        f"ftp://ftp.ncbi.nlm.nih.gov/{ftp_url}",
        archive,
    )

    version_dir = os.path.join(base_db_path, "taxonomy", date)

    try:
        with _atomic_version_dir(version_dir):
            _run(["unzip", "-o", archive, "-d", version_dir], f"extracting taxonomy {date}")
    finally:
        if os.path.exists(archive):
            os.remove(archive)


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
    with open(md5_path) as f:
        expected = f.read().split()[0].strip()
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
            os.remove(md5_dest)
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
                if attempt < 9:
                    time.sleep(min(2**attempt, 300))

        if not downloaded:
            raise URLError(f"Failed to download {segment} after 10 attempts")

        if not _verify_md5(dest, md5_dest):
            os.remove(dest)
            os.remove(md5_dest)
            raise ValueError(f"MD5 checksum mismatch for {segment} — file removed")
        os.remove(md5_dest)

    try:
        _run(["tar", "-xf", dest, "-C", db_dir], f"extracting {segment}")
    except Exception:
        for path in glob.glob(os.path.join(db_dir, f"{_segment_stem(segment)}.*")):
            os.remove(path)
        raise

    os.remove(dest)


def get_ncbi_blast():
    for db in _BLAST_DB_METADATA:
        meta = _fetch_blast_metadata(db)
        date = datetime.datetime.fromisoformat(meta["last-updated"]).strftime(
            "%Y-%m-%d"
        )

        archive_dir = os.path.join(base_db_path, "blast", f"{db}_archive", date)
        symlink_path = os.path.join(base_db_path, "blast", db)

        if _version_complete(archive_dir):
            print(f"{db}: already have version {date}, skipping")
            continue

        if dry_run:
            print(f"Would make dir: {archive_dir}")
            for file_url in meta["files"]:
                print(f"Would get: {os.path.basename(file_url)} to path: {archive_dir}")
            continue

        if os.path.isdir(archive_dir):
            print(f"{db}: previous run of {date} was incomplete, resuming")

        with _atomic_version_dir(archive_dir):
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
    except Exception as e:
        raise RuntimeError(f"Failed to get db-versions.json from {resp.url}: {e}")

    latest_db = max(
        datetime.datetime.strptime(x["date"], "%Y-%m-%d") for x in db_versions
    )

    for db in db_versions:
        if not datetime.datetime.strptime(db["date"], "%Y-%m-%d") == latest_db:
            continue

        version_dir = os.path.join(base_db_path, "bakta", db["date"])

        if _version_complete(version_dir):
            print(f"Already have latest bakta db: {db['date']}")
            break

        doi_url = f"{doi.get_real_url_from_doi(db['doi'])}"

        db_url = f"{requests.get(doi_url).url}/files/db.tar.xz"

        if dry_run:
            print(f"Would get: {db_url} to path: {version_dir}")
            continue

        archive = os.path.join(base_db_path, "db.tar.xz")
        urllib.request.urlretrieve(db_url, archive)

        try:
            with _atomic_version_dir(version_dir):
                _run(
                    ["tar", "-xf", archive, "-C", version_dir],
                    f"extracting bakta {db['date']}",
                )
        finally:
            if os.path.exists(archive):
                os.remove(archive)

        try:
            os.remove(os.path.join(base_db_path, "bakta", "latest"))
        except OSError:
            pass

        os.symlink(
            db["date"],
            os.path.join(base_db_path, "bakta", "latest"),
        )


def get_gtdb_db():
    index = urllib.request.urlopen("https://data.gtdb.aau.ecogenomic.org/releases/")

    resp = index.read()

    versions = re.findall(r"release\d{2,4}", resp.decode("utf-8"))
    versions = set(versions)

    latest_version_number = max(int(x.removeprefix("release")) for x in versions)
    latest_version = f"release{latest_version_number}"

    version_dir = os.path.join(base_db_path, "gtdb", latest_version)

    if _version_complete(version_dir):
        print(f"Found existing gtdb version: {latest_version}")
        return
    else:
        print(f"New gtdb version found: {latest_version}")

    gtdb_url = f"https://data.gtdb.aau.ecogenomic.org/releases/{latest_version}/{latest_version_number}.0/auxillary_files/gtdbtk_package/full_package/gtdbtk_r{latest_version_number}_data.tar.gz"

    if dry_run:
        print(f"Would get: {gtdb_url} to path: {version_dir}")
        return

    gtdb_dir = os.path.join(base_db_path, "gtdb")
    os.makedirs(gtdb_dir, exist_ok=True)

    archive = os.path.join(gtdb_dir, f"gtdbtk_r{latest_version_number}_data.tar.gz")
    urllib.request.urlretrieve(gtdb_url, archive)

    try:
        with _atomic_version_dir(version_dir):
            _run(["tar", "-xf", archive, "-C", version_dir], f"extracting gtdb {latest_version}")
    finally:
        if os.path.exists(archive):
            os.remove(archive)

    try:
        os.remove(os.path.join(base_db_path, "gtdb", "latest"))
    except OSError:
        pass

    os.symlink(
        latest_version,
        os.path.join(base_db_path, "gtdb", "latest"),
    )


# sylph-stuff hosts pre-built sylph databases directly; the docs page below is
# kept up to date with what's actually downloadable there (the raw Apache
# directory listing at faust.compbio.cs.cmu.edu is known to lag behind it).
SYLPH_DOCS_URL = "https://sylph-docs.github.io/pre%E2%80%90built-databases/"
# The %E2%80%90 above is a percent-encoded U+2010 Unicode hyphen — the real
# URL is "pre‐built-databases", not "pre-built-databases". An ASCII hyphen 404s.

_SYLPH_LINK_RE = re.compile(
    r'href="(https?://faust\.compbio\.cs\.cmu\.edu/sylph-stuff/[^"]+\.syldb)"', re.I
)
_SYLPH_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.I | re.S)
_SYLPH_CELL_RE = re.compile(r"<t([dh])([^>]*)>(.*?)</t\1>", re.I | re.S)
_SYLPH_TAG_RE = re.compile(r"<[^>]+>")
_SYLPH_FILE_C_RE = re.compile(r"[-_]c(\d+)[-_.]")
_SYLPH_GTDB_RE = re.compile(r"^gtdb-r(\d+)-c(\d+)-dbv(\d+)\.syldb$", re.I)

# Precedence order matters: a real content version (date/release number)
# ranks above a database-format revision (dbv) or tool version (v0.3), which
# reflect tooling, not data freshness.
_SYLPH_VERSION_PATTERNS = (
    ("date", re.compile(r"(\d{4})-(\d{2})-(\d{2})")),
    ("release", re.compile(r"[-_]r(\d+)[-_.]")),
    ("dbv", re.compile(r"dbv(\d+)")),
    ("ver", re.compile(r"[-_]v(\d+(?:\.\d+)*)[-_.]")),
)

_SYLPH_FAMILY_TOKEN_RE = re.compile(
    r"^(r\d+|v[\d_]+|dbv\d+|\d{4}|\d{4}_\d{2}_\d{2}|\d+|latest)$"
)


def _sylph_slug(text):
    text = _SYLPH_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = re.sub(r"\([^)]*\)", " ", text)
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _sylph_family(name_slug):
    tokens = [t for t in name_slug.split("_") if not _SYLPH_FAMILY_TOKEN_RE.match(t)]
    family = "_".join(tokens)
    return family or name_slug


def _sylph_version_key(filename):
    lowered = filename.lower()
    for kind, pattern in _SYLPH_VERSION_PATTERNS:
        m = pattern.search(lowered)
        if not m:
            continue
        if kind == "date":
            value = tuple(int(g) for g in m.groups())
        elif kind == "ver":
            value = tuple(int(p) for p in m.group(1).split("."))
        else:
            value = (int(m.group(1)),)
        return kind, value
    return None


def _parse_sylph_docs(page_html):
    rows = []
    current_type = None

    for row_match in _SYLPH_ROW_RE.finditer(page_html):
        cells = _SYLPH_CELL_RE.findall(row_match.group(1))
        if not cells:
            continue

        # The type column is always the first cell, but grouped rows leave it
        # blank rather than using an actual HTML rowspan — so only update
        # current_type when this row's cell has text, otherwise carry over.
        _, _, type_inner = cells[0]
        cells = cells[1:]
        type_text = html.unescape(_SYLPH_TAG_RE.sub(" ", type_inner)).strip()
        if type_text:
            # e.g. "Prokaryotic (GTDB)" -> "gtdb"; "Viral" -> "viral" — the
            # parenthetical, when present, is the meaningful discriminator.
            paren_match = re.search(r"\(([^)]+)\)", type_text)
            current_type = _sylph_slug(paren_match.group(1) if paren_match else type_text)

        if current_type is None:
            continue

        link_match = _SYLPH_LINK_RE.search(row_match.group(1))
        if not link_match:
            # No direct faust.compbio.cs.cmu.edu file link (e.g. GlobDB rows
            # point at globdb.org) — structurally excluded, no hardcoded list.
            continue

        url = link_match.group(1)
        filename = os.path.basename(urllib.parse.urlparse(url).path)

        c_match = _SYLPH_FILE_C_RE.search(filename)
        if not c_match:
            row_text = _SYLPH_TAG_RE.sub(" ", row_match.group(1))
            c_match = re.search(r"-c\s*(\d+)", row_text)
        if not c_match:
            print(f"sylph: no -c value found for {filename} -> skipping")
            continue
        c_value = c_match.group(1)

        name_text = None
        for _, _, cell_inner in cells:
            text = html.unescape(_SYLPH_TAG_RE.sub(" ", cell_inner)).strip()
            if not text:
                continue
            if re.fullmatch(r"-?\s*c\s*\d+", text, re.I):
                continue
            if text.lower().startswith("http"):
                continue
            if text.lower() in {"download", "link", "here", "url", "file"}:
                continue
            name_text = text
            break

        if name_text is None:
            name_text = filename.rsplit(".syldb", 1)[0]
            print(f"sylph: no name column found for {filename}, using filename")

        rows.append(
            {
                "type_slug": current_type,
                "name_slug": _sylph_slug(name_text),
                "c": c_value,
                "url": url,
                "filename": filename,
            }
        )

    seen = set()
    deduped = []
    for row in rows:
        key = (row["type_slug"], row["name_slug"], row["c"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return deduped


def _select_latest_sylph_rows(rows):
    gtdb_rows = [r for r in rows if r["type_slug"] == "gtdb"]
    other_rows = [r for r in rows if r["type_slug"] != "gtdb"]

    selected = []

    gtdb_releases = [
        int(m.group(1)) for r in gtdb_rows if (m := _SYLPH_GTDB_RE.match(r["filename"]))
    ]
    if gtdb_releases:
        latest_release = max(gtdb_releases)
        for r in gtdb_rows:
            m = _SYLPH_GTDB_RE.match(r["filename"])
            if m and int(m.group(1)) == latest_release:
                selected.append({**r, "is_latest": True})
            else:
                print(f"sylph: skipping non-latest GTDB file {r['filename']}")
    elif gtdb_rows:
        print("sylph: could not determine latest GTDB release -> skipping all GTDB rows")

    groups = {}
    for r in other_rows:
        family = _sylph_family(r["name_slug"])
        groups.setdefault((r["type_slug"], family, r["c"]), []).append(r)

    for (type_slug, family, c_value), members in groups.items():
        if len(members) == 1:
            selected.append({**members[0], "is_latest": True, "family": family})
            continue

        keyed = [(_sylph_version_key(m["filename"]), m) for m in members]
        kinds = {k[0] for k, _ in keyed if k is not None}

        if all(k is not None for k, _ in keyed) and len(kinds) == 1:
            ordered = sorted(keyed, key=lambda item: item[0][1], reverse=True)
            for _, m in ordered[1:]:
                print(
                    f"sylph: skipping non-latest {type_slug}/{family} c{c_value} ({m['filename']})"
                )
            selected.append({**ordered[0][1], "is_latest": True, "family": family})
        else:
            print(
                f"sylph: unrecognised/mixed versioning for {type_slug}/{family} c{c_value} -> keeping all"
            )
            for i, m in enumerate(members):
                selected.append({**m, "is_latest": i == 0, "family": family})

    return selected


def get_sylph_dbs():
    try:
        with urllib.request.urlopen(SYLPH_DOCS_URL) as resp:
            page_html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"sylph: failed to fetch {SYLPH_DOCS_URL}: {e}")
        return

    rows = _parse_sylph_docs(page_html)
    if not rows:
        print("sylph: no downloadable databases found on docs page -> skipping")
        return

    rows = _select_latest_sylph_rows(rows)

    latest_links = {}

    for row in rows:
        type_slug = row["type_slug"]
        name_slug = row["name_slug"]
        c_value = row["c"]

        target_dir = os.path.join(
            base_db_path, "sylph", type_slug, f"{name_slug}_c{c_value}"
        )
        target_file = os.path.join(target_dir, row["filename"])

        # GTDB is pre-narrowed to a single latest release before this loop
        # runs, so no latest/ symlink is needed there.
        if type_slug != "gtdb" and row.get("is_latest"):
            family = row["family"]
            link_dir = os.path.join(base_db_path, "sylph", type_slug, "latest")
            link_path = os.path.join(link_dir, f"{family}_c{c_value}")
            latest_links[link_path] = (
                link_dir,
                f"../{name_slug}_c{c_value}",
                target_dir,
            )

        if _version_complete(target_dir):
            print(f"sylph: already have {type_slug}/{name_slug}_c{c_value} -> skipping")
            continue

        if dry_run:
            print(f"Would make dir: {target_dir}")
            print(f"Would get: {row['url']} to path: {target_file}")
            continue

        try:
            with _atomic_version_dir(target_dir):
                part_file = target_file + ".part"
                urllib.request.urlretrieve(row["url"], part_file)
                os.replace(part_file, target_file)
        except Exception as e:
            print(f"sylph: failed to fetch {row['url']}: {e}")
            continue

    for link_path, (link_dir, relative_target, target_dir) in latest_links.items():
        if dry_run:
            print(f"Would make dir: {link_dir}")
            print(f"Would symlink: {link_path} -> {relative_target}")
            continue

        if not _version_complete(target_dir):
            print(
                f"sylph: latest target {target_dir} not present/complete -> not repointing {link_path}"
            )
            continue

        os.makedirs(link_dir, exist_ok=True)

        try:
            os.remove(link_path)
        except OSError:
            pass

        os.symlink(relative_target, link_path)


def run():
    k2_to_get = {}

    for db, date, key in k2_db_generator():
        db_path = os.path.join(base_db_path, "kraken2", db)

        if not os.path.exists(db_path):
            if dry_run:
                print(f"Would make dir: {db_path}")
            else:
                os.makedirs(db_path, exist_ok=True)

        k2_to_get.setdefault(db, (date, key))

        if datetime.datetime.strptime(
            k2_to_get[db][0], "%Y-%m-%d"
        ) < datetime.datetime.strptime(date, "%Y-%m-%d"):
            k2_to_get[db] = (date, key)

    for db, (date, key) in k2_to_get.items():
        version_dir = os.path.join(base_db_path, "kraken2", db, date)

        if not _version_complete(version_dir):
            if dry_run:
                print(f"Would get: {db} {date} {key} to path: {version_dir}")
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

    taxonomy_to_get = None

    for ftp_url, filename, date in ncbi_taxonomy_generator():
        if taxonomy_to_get is None:
            taxonomy_to_get = (ftp_url, filename, date)
        elif datetime.datetime.strptime(
            taxonomy_to_get[2], "%Y-%m-%d"
        ) < datetime.datetime.strptime(date, "%Y-%m-%d"):
            taxonomy_to_get = (ftp_url, filename, date)

    if taxonomy_to_get is None:
        print("No NCBI taxonomy archives found -> skipping")
    else:
        db_path = os.path.join(base_db_path, "taxonomy")

        if not os.path.exists(db_path):
            if dry_run:
                print(f"Would make dir: {db_path}")
            else:
                os.makedirs(db_path, exist_ok=True)

        version_dir = os.path.join(base_db_path, "taxonomy", taxonomy_to_get[2])

        if not _version_complete(version_dir):
            if dry_run:
                print(f"Would get: {taxonomy_to_get[0]} to path: {version_dir}")
            else:
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

    get_sylph_dbs()

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
