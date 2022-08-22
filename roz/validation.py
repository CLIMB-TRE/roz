from csv import DictReader
import datetime
import string
import pysam
from Bio import SeqIO
import re
import os
from pathlib import Path

import mappy as mp

def expand_int_ranges(range_string):
    r = []
    for i in range_string.split(","):
        if "-" not in i:
            r.append(int(i))
        else:
            l, h = map(int, i.split("-"))
            r += range(l, h + 1)
    return r


def validate_dehumanised(config, env_vars, bam_path, minimap_preset):

    # Check if indexed compound ref to requested preset exists in $ROZ_REF_ROOT and create it if not
    if not os.path.exists(f"{env_vars.idx_ref_dir}/{minimap_preset}.mmi"):
        mp.Aligner(
            fn_idx_in=env_vars.compound_ref_path, fn_idx_out=f"{env_vars.idx_ref_dir}/{minimap_preset}.mmi"
        )

    # Dump BAM reads to fastq file for mapping
    fastq_name = bam_path.split("/")[-1].replace(".bam", ".fastq")
    os.system(f"samtools fastq {bam_path} > {env_vars.temp_dir}/{fastq_name}")

    # Align them to the compound ref
    aligner = mp.Aligner(fn_idx_in=f"{env_vars.idx_ref_dir}/{minimap_preset}.mmi")

    best_hits_dict = {"total": 0, config["compound_ref_pathogen_name"]: 0, "human": 0}

    # Iterate through the seqs in the dumped fastq file and re-align them to the compound reference,
    # storing a count of how many align to each contig best

    pathogen = config["compound_ref_pathogen_name"]

    for name, seq, qual in mp.fastx_read(f"{env_vars.temp_dir}/{fastq_name}"):
        seq_mapping_quals = {}
        best_hits_dict["total"] += 1
        for hit in aligner.map(seq):
            seq_mapping_quals[hit.ctg] = hit.mapq
        if seq_mapping_quals:
            top_hit = max(seq_mapping_quals, key=seq_mapping_quals.get)
            if top_hit == pathogen:
                best_hits_dict[pathogen] += 1
            else:
                best_hits_dict["human"] += 1
    
    human_proportion = best_hits_dict["human"] / best_hits_dict["total"]

    os.system(f"rm -f {env_vars.temp_dir}/{fastq_name}")
    if human_proportion > config["max_acceptible_human"]:
        return False, human_proportion
    else:
        return True, human_proportion

def validate_datetime(date_string):
    try:
        datetime.datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def expand_character_ranges(character_range_string):
    allowed_characters = []
    splits = character_range_string.split(",")
    for split in splits:
        if split == "alphanumeric":
            allowed_characters.extend(string.ascii_uppercase)
            allowed_characters.extend(range(0, 10))
        else:
            allowed_characters.extend(split)
    return [str(character) for character in allowed_characters]


class csv_validator:
    def __init__(self, config, csv, csv_path):
        self.config = config["csv"]
        self.csv = csv
        self.csv_path = Path(csv_path)
        self.errors = []

    def validate(self):
        reader = DictReader(self.csv)

        csv_list = [row for row in reader]

        # Ensure the CSV contains data
        if len(csv_list) == 0:
            self.errors.append(
                {
                    "type": "formatting",
                    "text": "The CSV does not appear to contain a data row, please ensure you include a header row as well as a data row",
                }
            )
            return False

        # Ensure the CSV doesn't contain more rows
        if len(csv_list) > 1:
            self.errors.append(
                {
                    "type": "formatting",
                    "text": "Number of lines in provided CSV is greater than 2, CSV validation halted at this point so this list of errors may be inexhaustive",
                }
            )
            return False
        else:
            csv_data = csv_list[0]
        self.csv_data = csv_data

        # Ensure no required fields are empty / they exist
        for field in self.config.get("required_fields"):
            if not csv_data.get(field):
                self.errors.append(
                    {
                        "type": "content",
                        "text": f"The required field: {field} is empty / does not exist",
                    }
                )
                return False

        # Check template version is current according to the config
        try:
            if (
                not int(csv_data["csv_template_version"])
                == self.config["current_version"]
            ):
                self.errors.append(
                    {
                        "type": "formatting",
                        "text": f"The CSV template version ({csv_data['csv_template_version']}) is not what is currently defined within the specification ({self.config['current_version']}), CSV validation halted at this point so this list of errors may be inexhaustive",
                    }
                )
                return False
        except KeyError:
            self.errors.append(
                {
                    "type": "formatting",
                    "text": f"The CSV does not contain the field 'csv_template_version' so not checking can be performed, CSV validation halted at this point so this list of errors may be inexhaustive",
                }
            )
            return False

        # Ensure that CSV contains enough headings
        if any(key == None for key in csv_data.keys()):
            self.errors.append(
                {
                    "type": "formatting",
                    "text": "The CSV does not have enough field headings for the data row, CSV validation halted at this point so this list of errors may be inexhaustive",
                }
            )
            return False

        # Ensure the CSV doesn't contain duplicate columns
        if len(set(reader.fieldnames)) < len(reader.fieldnames):
            self.errors.append(
                {
                    "type": "formatting",
                    "text": f"The CSV contains duplicate field headings, CSV validation halted at this point so this list of errors may be inexhaustive",
                }
            )
            return False

        # Check sender_sample_id only contains allowed characters
        if self.config.get("sample_name_allowed_characters"):
            allowed_characters = expand_character_ranges(
                self.config.get("sample_name_allowed_characters")
            )
            if any(
                character.upper() not in allowed_characters
                for character in csv_data["sender_sample_id"]
            ):
                self.errors.append(
                    {
                        "type": "content",
                        "text": "The sender_sample_id field contains at least one disallowed character",
                    }
                )

        # Check that the sample_id / runname in the filename are match the metadata
        filename_splits = self.csv_path.name.split(".")

        if filename_splits[0] != csv_data["sender_sample_id"]:
            self.errors.append(
                {
                    "type": "format",
                    "text": "The 'sender_sample_id' section of the filename disagrees with the CSV metadata",
                }
            )

        if filename_splits[1] != csv_data["run_name"]:
            self.errors.append(
                {
                    "type": "format",
                    "text": "The 'run_name' section of the filename disagrees with the CSV metadata",
                }
            )

        # Throw a warning if a field is included which is not an accepted field
        for csv_field in csv_data.keys():
            if csv_field not in self.config[
                "required_fields"
            ] and csv_field not in self.config.get("optional_fields"):
                self.errors.append(
                    {
                        "type": "warning",
                        "text": f"The unsupported field {csv_field} was included in the metadata CSV, since the field is unsupported it will not be ingested as metadata",
                    }
                )

        # Check datatypes and valid choices
        for field, type in self.config.get("field_datatypes").items():
            try:
                if type == "choice":
                    if csv_data[field] not in self.config["field_choices"][field]:
                        choices_string = ", ".join(
                            str(choice)
                            for choice in self.config["field_choices"][field]
                        )
                        self.errors.append(
                            {
                                "type": "content",
                                "text": f"The {field} field can only contain one of: {choices_string}",
                            }
                        )
                elif type == "date":
                    if not validate_datetime(csv_data[field]):
                        self.errors.append(
                            {
                                "type": "content",
                                "text": f"The {field} field must be in the format YYYY-MM-DD not {csv_data[field]}",
                            }
                        )

            except KeyError:
                if field not in self.config["required_fields"]:
                    continue

        # Ensure fields adhere to character limits
        if self.config.get("character_limits"):
            for field, range in self.config.get("character_limits").items():
                if csv_data.get(field):
                    if not len(csv_data[field]) in expand_int_ranges(range):
                        self.errors.append(
                            {
                                "type": "content",
                                "text": f"The field: {field} contains {len(csv_data[field])} characters which is outside of the range {range}",
                            }
                        )

        # Ensure sender_sample_id not in other fields
        if self.config.get("disallow_sample_id_elsewhere"):
            for field, contents in csv_data.items():
                sample_id = csv_data["sender_sample_id"]
                if field == "sender_sample_id":
                    continue
                else:
                    if sample_id in str(contents):
                        self.errors.append(
                            {
                                "type": "content",
                                "text": f"The field: {field} contains the sender_sample_id",
                            }
                        )

        if len(self.errors) == 0:
            validation_result = True
        elif set(error["type"] for error in self.errors) == {"warning"}:
            validation_result = True
        else:
            validation_result = False

        return validation_result


class fasta_validator:
    def __init__(self, config, fasta, fasta_path):
        self.config = config["fasta"]
        self.fasta = fasta
        self.errors = []
        self.fasta_path = Path(fasta_path)

    def validate(self):
        try:
            fasta = SeqIO.read(self.fasta, format="fasta")
            self.fasta_record = fasta
        except ValueError:
            self.errors.append(
                {
                    "type": "format",
                    "text": "The fasta does not appear to contain one sequence",
                }
            )
            return False

        if len(fasta.seq) == 0:
            self.errors.append(
                {"type": "content", "text": "The fasta does not contain a sequence",}
            )
            return False

        if self.config.get("iupac_only"):
            iupac_chars = set("ACGTRYKMSWBDHVN")

            if any(base.upper() not in iupac_chars for base in fasta.seq):
                self.errors.append(
                    {
                        "type": "content",
                        "text": "The fasta contains at least one non-IUPAC (ACGTRYKMSWBDHVN) character",
                    }
                )

        if len(fasta.seq) < int(self.config.get("min_length")):
            self.errors.append(
                {
                    "type": "content",
                    "text": f"The fasta sequence is below the minimum length of {self.config['min_length']}",
                }
            )

        if self.config.get("header_allowed_characters"):
            allowed_characters = expand_character_ranges(
                self.config["header_allowed_characters"] + ",>"
            )

            if any(True for character in str(fasta.id) if character.upper() not in allowed_characters):
                self.errors.append(
                    {
                        "type": "content",
                        "text": "The fasta header contains at least one disallowed character",
                    }
                )

        if self.config.get("header_format"):
            if not re.fullmatch(".{1,}\..{1,}\..{1,}", str(fasta.id)):
                self.errors.append(
                    {
                        "type": "content",
                        "text": "The fasta header does not appear to match the specified pattern",
                    }
                )
                return False

        filename_splits = self.fasta_path.name.split(".")
        header_splits = fasta.id.split(".")


        try:
            if filename_splits[0] != header_splits[1]:
                self.errors.append(
                    {
                        "type": "format",
                        "text": "The 'sender_sample_id' section of the filename disagrees with the Fasta header",
                    }
                )
            if filename_splits[1] != header_splits[2]:
                self.errors.append(
                    {
                        "type": "format",
                        "text": "The 'run_name' section of the filename disagrees with the Fasta header",
                    }
                )
        except IndexError:
            self.errors.append(
                    {
                        "type": "format",
                        "text": "The Fasta header appears to be malformed, ensure it is in the format: '>[uploader].[sender_sample_id].[run_name]'",
                    }
                )

        if len(self.errors) == 0:
            validation_result = True
        elif set(error["type"] for error in self.errors) == {"warning"}:
            validation_result = True
        else:
            validation_result = False

        return validation_result


class bam_validator:
    def __init__(self, config, env_vars, bam_path, platform):
        self.config = config["bam"]
        self.env_vars = env_vars
        self.bam_path = bam_path
        self.errors = []
        self.platform = platform

    def validate(self):
        if self.config.get("size_limit_gb"):
            bam_size = os.path.getsize(self.bam_path)

            if bam_size > self.config["size_limit_gb"] * (10 ** 9):
                self.errors.append(
                    {
                        "type": "content",
                        "text": f"The bam file is larger than the size limit of {self.config['size_limit_gb']}G, please subsample the BAM and resubmit",
                    }
                )
                return False

        if self.config.get("require_samtools_quickcheck"):
            retcode = os.system(f"samtools quickcheck {self.bam_path}")
            if retcode != 0:
                self.errors.append(
                    {
                        "type": "content",
                        "text": "The bam file failed samtools quickcheck, please ensure the submitted bam file is valid and resubmit",
                    }
                )
                return False

        with pysam.AlignmentFile(self.bam_path, "rb") as bam_fh:
            # TODO: check behaviour of "rb" for samfiles
            if any(True for ref in bam_fh.references if ref not in self.config.get("allowed_refs")):
                allowed_ref_string = ", ".join(
                    str(ref) for ref in self.config.get("allowed_refs")
                )
                self.errors.append(
                    {
                        "type": "content",
                        "text": f"The bam contains at least one disallowed reference header, headers included are: {str(bam_fh.references)}, allowed headers are: {allowed_ref_string}",
                    }
                )

            headers = bam_fh.header.to_dict()

            if self.config.get("require_sorted"):
                if headers["HD"]["SO"] != "coordinate":
                    self.errors.append(
                        {
                            "type": "content",
                            "text": f"The bam file is not coordinate sorted, please properly sort the bam file and resubmit",
                        }
                    )
            n_reads = 0
            for read in bam_fh:
                n_reads += 1
                break

            if n_reads == 0:
                self.errors.append(
                    {
                        "type": "content",
                        "text": "The bam file does not appear to contain any aligned reads",
                    }
                )

        if self.config.get("check_dehumanised"):
            result, human_proportion = validate_dehumanised(self.config, self.env_vars, self.bam_path, self.config.get("mapping_presets")[self.platform])

            if not result:
                self.errors.append(
                    {
                        "type": "content",
                        "text": f"The bam file contains an unacceptible amount ({round(human_proportion * 100, 2)}%) human reads, please dehumanise the bam file more stringently and re-submit",
                    }
                )

        if len(self.errors) == 0:
            validation_result = True
        elif set(error["type"] for error in self.errors) == {"warning"}:
            validation_result = True
        else:
            validation_result = False

        return validation_result


class triplet_validation:
    def __init__(self, config, csv_path, fasta_path, bam_path):
        self.config = config
        self.csv_path = Path(csv_path)
        self.fasta_path = Path(fasta_path)
        self.bam_path = Path(bam_path)
        self.errors = []

    def filename_agreements(self):
        csv_name = self.csv_path.stem
        fasta_name = self.fasta_path.stem
        bam_name = self.bam_path.stem

        if len({csv_name, fasta_name, bam_name}) > 1:
            self.errors.append(
                {
                    "type": "formatting",
                    "text": "The files included within the triplet do not appear to match",
                }
            )

