import pysam
import sys
import mappy as mp
import os


def validate_dehumanised(
    bam_path, minimap_preset, pathogen_name, human_name, max_allowable_human
):
    # Get env variables
    temp_dir = os.getenv("ROZ_TEMP_DIR")
    n_threads = os.getenv("ROZ_MM2_THREADS")
    ref_root = os.getenv("ROZ_REF_ROOT")
    compound_ref_path = os.getenv("ROZ_CPD_REF_PATH")

    # Exit if env variables unset
    if any(temp_dir, n_threads, ref_root, compound_ref_path) == None:
        print("At least one required environmental variable is unset", file=sys.stderr)
        sys.exit(11)

    # Check if indexed compound ref to requested preset exists in $ROZ_REF_ROOT and create it if not
    if not os.path.exists(f"{ref_root}/{minimap_preset}.mmi"):
        mp.Aligner(
            fn_idx_in=compound_ref_path, fn_idx_out=f"{ref_root}/{minimap_preset}.mmi"
        )

    # Dump BAM reads to fastq file for mapping
    fastq_name = bam_path.split("/")[-1].replace(".bam", ".fastq")
    os.system(f"samtools fastq {bam_path} > {temp_dir}/{fastq_name}")

    # Align them to the compound ref
    aligner = mp.Aligner(fn_idx_in=f"{ref_root}/{minimap_preset}.mmi")

    best_hits_dict = {"total": 0, pathogen_name: 0, "human": 0}

    # Iterate through the seqs in the dumped fastq file and re-align them to the compound reference,
    # storing a count of how many align to each contig best
    for name, seq, qual in mp.fastx_read(f"{temp_dir}/{fastq_name}"):
        seq_mapping_quals = {}
        best_hits_dict["total"] += 1
        for hit in aligner.map(seq):
            seq_mapping_quals[hit.ctg] = hit.mapq
        if seq_mapping_quals:
            top_hit = max(seq_mapping_quals, key=seq_mapping_quals.get)
            best_hits_dict[pathogen_name] += 1 if top_hit == pathogen_name else best_hits_dict["human"] += 1

    human_proportion = best_hits_dict[human_name] / best_hits_dict["total"]

    if human_proportion > max_allowable_human:
        return False, human_proportion
    else:
        return True, human_proportion