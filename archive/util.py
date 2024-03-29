import sys
import os
from types import SimpleNamespace
import copy
from collections import namedtuple

# from roz.validation import csv_validator, fasta_validator, bam_validator

validation_tuple = namedtuple(
    "validation_tuple", "artifact success payload attempts exception"
)


def get_env_variables():
    env_vars = {
        "temp_dir": "ROZ_TEMP_DIR",
        "idx_ref_dir": "ROZ_REF_ROOT",
        "compound_ref_path": "ROZ_CPD_REF_PATH",
        "json_config": "ROZ_CONFIG_JSON",
        "profile_config": "ROZ_PROFILE_CFG",
        "logfile": "ROZ_LOG_PATH",
        "log_level": "ROZ_LOG_LEVEL",
    }

    config = {k: os.getenv(v) for k, v in env_vars.items()}

    if any(True for v in config.values() if v == None):
        none_vals = ", ".join(str(env_vars[k]) for k, v in config.items() if v == None)
        print(
            f"The following required environmental variables must be set for ROZ to function: {none_vals}.",
            file=sys.stderr,
        )
        sys.exit(10)

    return SimpleNamespace(**config)


# def validate_triplet(config, env_vars, to_validate, log):
#     try:
#         out_payload = copy.copy(to_validate.payload)

#         out_payload["validation"] = {}

#         log.info(f"Attempting to validate CSV for artifact {to_validate.artifact}")
#         with open(to_validate.payload["files"]["csv"]["path"], "rt") as csv_fh:
#             csv_check = csv_validator(
#                 config, csv_fh, to_validate.payload["files"]["csv"]["path"]
#             )
#             csv_pass = csv_check.validate()
#             out_payload["validation"]["csv"] = {
#                 "result": csv_pass,
#                 "errors": csv_check.errors,
#             }
#             try:
#                 platform = csv_check.csv_data["seq_platform"]
#             except:
#                 platform = False

#         log.info(f"Attempting to validate Fasta for artifact {to_validate.artifact}")
#         with open(to_validate.payload["files"]["fasta"]["path"], "rt") as fasta_fh:
#             fasta_check = fasta_validator(
#                 config, fasta_fh, to_validate.payload["files"]["fasta"]["path"]
#             )
#             fasta_pass = fasta_check.validate()
#             out_payload["validation"]["fasta"] = {
#                 "result": fasta_pass,
#                 "errors": fasta_check.errors,
#             }

#         log.info(f"Attempting to validate BAM for artifact {to_validate.artifact}")
#         if platform:
#             bam_check = bam_validator(
#                 config, env_vars, to_validate.payload["files"]["bam"]["path"], platform
#             )
#             bam_pass = bam_check.validate()
#             out_payload["validation"]["bam"] = {
#                 "result": bam_pass,
#                 "errors": bam_check.errors,
#             }
#         else:
#             bam_pass = False
#             out_payload["validation"]["bam"] = {
#                 "result": bam_pass,
#                 "errors": [
#                     {
#                         "type": "formatting",
#                         "text": "The CSV does not appear to contain a data row, please ensure you include a header row as well as a data row",
#                     }
#                 ],
#             }

#         callback = validation_tuple(
#             to_validate.artifact,
#             True,
#             out_payload,
#             to_validate.attempts + 1,
#             "",
#         )
#     except Exception as e:
#         callback = validation_tuple(
#             to_validate.artifact,
#             False,
#             to_validate.payload,
#             to_validate.attempts + 1,
#             e,
#         )

#     return callback
