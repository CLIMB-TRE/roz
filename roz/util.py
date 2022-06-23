import pysam
import sys
import mappy as mp
import os


def validate_dehumanised(config, bam_path, minimap_preset, pathogen_name, max_allowable_human
):
    # Check if indexed compound ref to requested preset exists in $ROZ_REF_ROOT and create it if not
    if not os.path.exists(f"{config.idx_ref_dir}/{minimap_preset}.mmi"):
        mp.Aligner(
            fn_idx_in=config.compound_ref_path, fn_idx_out=f"{config.idx_ref_dir}/{minimap_preset}.mmi"
        )

    # Dump BAM reads to fastq file for mapping
    fastq_name = bam_path.split("/")[-1].replace(".bam", ".fastq")
    os.system(f"samtools fastq {bam_path} > {config.temp_dir}/{fastq_name}")

    # Align them to the compound ref
    aligner = mp.Aligner(fn_idx_in=f"{config.idx_ref_dir}/{minimap_preset}.mmi")

    best_hits_dict = {"total": 0, pathogen_name: 0, "human": 0}

    # Iterate through the seqs in the dumped fastq file and re-align them to the compound reference,
    # storing a count of how many align to each contig best
    for name, seq, qual in mp.fastx_read(f"{config.temp_dir}/{fastq_name}"):
        seq_mapping_quals = {}
        best_hits_dict["total"] += 1
        for hit in aligner.map(seq):
            seq_mapping_quals[hit.ctg] = hit.mapq
        if seq_mapping_quals:
            top_hit = max(seq_mapping_quals, key=seq_mapping_quals.get)
            if top_hit == pathogen_name:
                best_hits_dict[pathogen_name] += 1
            else:
                best_hits_dict["human"] += 1
    
    human_proportion = best_hits_dict["human"] / best_hits_dict["total"]

    os.system(f"rm -f {config.temp_dir}/{fastq_name}")
    if human_proportion > max_allowable_human:
        return False, human_proportion
    else:
        return True, human_proportion