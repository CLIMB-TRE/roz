import argparse
import json
import os
import pathlib
import random
import string

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True, type=pathlib.Path)
parser.add_argument("--fasta", required=True)
parser.add_argument("--bam", required=True)
parser.add_argument("--number", default=1)
parser.add_argument("--pathogen_code", default="mpx")
parser.add_argument("--outdir", default=os.getcwd())
args = parser.add_argument()

def range_to_len(len_range):
    limits = len_range.split("-")
    len = random.choice(range(int(limits[0]), int(limits[1])))
    return len

def text_gen(length):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))

def month_gen():
    return datetime.

with open(args.config) as conf_fh:
    config = json.load(conf_fh)

site = "BIRM"

samp_id = text_gen(range_to_len(config["configs"][args.pathogen_code]["sample_id"]))
run_name = text_gen(range_to_len(config["configs"][args.pathogen_code]["run_name"]))

