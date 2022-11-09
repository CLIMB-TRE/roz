import pytest
import io

from roz.validation import fasta_validator


@pytest.fixture
def config():
    return {
        "fasta": {
            "header_format": ">{uploader}{delimiter}{sample_id}{delimiter}{run_name}",
            "delimiter": ".",
            "header_allowed_characters": "alphanumeric,_,-,.",
            "iupac_only": True,
            "min_length": 10,
        }
    }


def test_empty_fasta(config):
    test_fasta = io.StringIO("")
    validator = fasta_validator(
        config,
        test_fasta,
        "/fake/dir/BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0.fasta",
    )
    assert validator.validate() == False
    assert {
        "type": "format",
        "text": "The fasta does not appear to contain one sequence",
    } in validator.errors


def test_long_fasta(config):
    test_fasta = io.StringIO(
        ">BIRM.BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0\nGTCAGTCATGCATCAYACTA\n>BIRM.BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0\nGTCAGTCATGCATCAYACTA"
    )
    validator = fasta_validator(
        config,
        test_fasta,
        "/fake/dir/BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0.fasta",
    )
    assert validator.validate() == False
    assert {
        "type": "format",
        "text": "The fasta does not appear to contain one sequence",
    } in validator.errors


def test_header_mismatch(config):
    test_fasta = io.StringIO(
        ">BIRM.BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0\nGTCAGTCATGCATCAYACTA\n"
    )
    validator = fasta_validator(
        config, test_fasta, "/fake/dir/BIRM-123457.YYMMDD_AB000000_1234_ABCDEFGHI.fasta"
    )
    assert validator.validate() == False
    assert {
        "type": "format",
        "text": "The 'sample_id' section of the filename disagrees with the Fasta header",
    } in validator.errors
    assert {
        "type": "format",
        "text": "The 'run_name' section of the filename disagrees with the Fasta header",
    } in validator.errors


def test_header_only(config):
    test_fasta = io.StringIO(">BIRM.BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0\n")
    validator = fasta_validator(
        config,
        test_fasta,
        "/fake/dir/BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0.fasta",
    )
    assert validator.validate() == False
    assert {
        "type": "content",
        "text": "The fasta does not contain a sequence",
    } in validator.errors


def test_minimum_length(config):
    test_fasta = io.StringIO(
        ">BIRM.BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0\nGTCAGTCAT"
    )
    validator = fasta_validator(
        config,
        test_fasta,
        "/fake/dir/BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0.fasta",
    )
    assert validator.validate() == False
    assert {
        "type": "content",
        "text": "The fasta sequence is below the minimum length of 10",
    } in validator.errors


def test_non_iupac(config):
    test_fasta = io.StringIO(
        ">BIRM.BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0\nGTCAGTCATG_CATCAYACTA"
    )
    validator = fasta_validator(
        config,
        test_fasta,
        "/fake/dir/BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0.fasta",
    )
    assert validator.validate() == False
    assert {
        "type": "content",
        "text": "The fasta contains at least one non-IUPAC (ACGTRYKMSWBDHVN) character",
    } in validator.errors


def test_passing_fasta(config):
    test_fasta = io.StringIO(
        ">BIRM.BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0\nGTCAGTCATGCATCAYACTA"
    )
    validator = fasta_validator(
        config,
        test_fasta,
        "/fake/dir/BIRM-123456.YYMMDD_AB000000_1234_ABCDEFGHI0.fasta",
    )
    assert validator.validate() == True
    assert validator.errors == []

