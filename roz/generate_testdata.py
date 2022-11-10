import argparse
import json
import os
import pathlib
import random
import string
import datetime
import shutil
from dateutil.relativedelta import relativedelta
from Bio import SeqIO
import csv

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True, type=pathlib.Path)
parser.add_argument("--fasta", required=True, type=pathlib.Path)
parser.add_argument("--bam", required=True)
parser.add_argument("--number", default=1, type=int)
parser.add_argument("--pathogen_code", default="mpx")
parser.add_argument("--outdir", default=os.getcwd())
args = parser.parse_args()


def range_to_len(len_range):
    limits = len_range.split("-")
    len = random.choice(range(int(limits[0]), int(limits[1])))
    return len


def text_gen(length):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


def month_gen():
    month = datetime.datetime.now() - relativedelta(months=random.randint(1, 12))
    return month.strftime("%Y-%m")


with open(args.config) as conf_fh:
    config = json.load(conf_fh)

template_version = config["version"]

for i in range(1, args.number + 1):

    out_data = {}

    # site = out_data["site"] = "BIRM"

    # samp_id = out_data["sample_id"] = text_gen(
    #     range_to_len(config["configs"][args.pathogen_code]["sample_id"])
    # )
    # run_name = out_data["run_name"] = text_gen(
    #     range_to_len(config["configs"][args.pathogen_code]["run_name"])
    # )

    n_opt_fields = random.randint(
        0, len(config["configs"][args.pathogen_code]["csv"]["optional_fields"])
    )
    optional_fields = random.sample(
        config["configs"][args.pathogen_code]["csv"]["optional_fields"], n_opt_fields
    )

    config["configs"][args.pathogen_code]["csv"]["required_fields"].extend(
        optional_fields
    )

    for field in config["configs"][args.pathogen_code]["csv"]["required_fields"]:
        if field == "csv_template_version":
            out_data[field] = template_version
            continue
        elif field == "pathogen_code":
            out_data[field] = args. pathogen_code
            continue

        ftype = config["configs"][args.pathogen_code]["csv"]["field_datatypes"][field]


        if ftype == "text":
            out_data[field] = text_gen(
                range_to_len(
                    config["configs"][args.pathogen_code]["csv"]["character_limits"][field]
                )
            )

        elif ftype == "choice":
            out_data[field] = random.choice(
                config["configs"][args.pathogen_code]["csv"]["field_choices"][field]
            )

        elif ftype == "month":
            out_data[field] = month_gen()

    out_path = f"{args.outdir}/{out_data['sample_id']}.{out_data['run_name']}"

    with open(args.fasta, "rt") as fasta_fh:
        fasta = SeqIO.read(fasta_fh, "fasta")
        fasta.id = f"{out_data['site']}.{out_data['sample_id']}.{out_data['run_name']}"
        SeqIO.write(fasta, out_path + ".fasta", "fasta")

    shutil.copy(args.bam, out_path + ".bam")

    with open(out_path + ".csv", "wt") as csv_fh:
        writer = csv.DictWriter(csv_fh, delimiter=",", fieldnames=out_data.keys())
        writer.writeheader()
        writer.writerow(out_data)