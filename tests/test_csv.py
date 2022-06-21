import csv
import pytest
import io

from roz.validation import csv_validator


@pytest.fixture
def config():
    return {
        "csv": {
            "disallow_sample_id_elsewhere": True,
            "sample_name_allowed_characters": "alphanumeric,_,-",
            "current_version": 1,
            "required_fields": [
                "sender_sample_id",
                "run_name",
                "csv_template_version",
                "test_text",
                "test_integer",
                "test_choice",
                "test_date",
            ],
            "optional_fields": ["test_optional"],
            "field_datatypes": {
                "sender_sample_id": "text",
                "run_name": "text",
                "test_text": "text",
                "test_integer": "integer",
                "test_optional": "text",
                "test_choice": "choice",
                "test_date": "date",
            },
            "character_limits": {"test_text": "1-8"},
            "field_choices": {"test_choice": ["test1", "test2"]},
        }
    }


def test_short_csv(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional\n"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "formatting",
        "text": "The CSV does not appear to contain a data row, please ensure you include a header row as well as a data row",
    } in validator.errors


def test_long_csv(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional\ntestid_1,test_runname,1,text,2022-05-12,15,test1,s\ntestid_1,test_runname,1,text,2022-05-12,15,test1,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "formatting",
        "text": "Number of lines in provided CSV is greater than 2, CSV validation halted at this point so this list of errors may be inexhaustive",
    } in validator.errors


def test_duplicate_cols(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_text,test_date,test_integer,test_choice,test_optional\ntestid_1,test_runname,1,text,text1,2022-05-12,15,test1,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "formatting",
        "text": f"The CSV contains duplicate field headings, CSV validation halted at this point so this list of errors may be inexhaustive",
    } in validator.errors


def test_specification_version(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional\ntestid_1,test_runname,2,text,2022-05-12,15,test1,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "formatting",
        "text": "The CSV template version (2) is not what is currently defined within the specification (1), CSV validation halted at this point so this list of errors may be inexhaustive",
    } in validator.errors


def test_sufficient_headers(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice\ntestid_1,test_runname,1,text,2022-05-12,15,test1,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "formatting",
        "text": "The CSV does not have enough field headings for the data row, CSV validation halted at this point so this list of errors may be inexhaustive",
    } in validator.errors


def test_required_fields(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_optional\ntestid_1,test_runname,1,text,2022-05-12,15,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "content",
        "text": "The required field: test_choice is empty / does not exist",
    } in validator.errors


def test_sample_id_allowed_characers(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional,test_extra\ntestid_1$,test_runname,1,text,2022-05-12,15,test1,s,5"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "content",
        "text": "The sender_sample_id field contains at least one disallowed character",
    } in validator.errors


def test_check_dtypes(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional\ntestid_1,test_runname,1,text,01/12/22,15,test3,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "content",
        "text": "The test_date field must be in the format YYYY-MM-DD not 01/12/22",
    } in validator.errors
    assert {
        "type": "content",
        "text": "The test_choice field can only contain one of: test1, test2",
    } in validator.errors


def test_filename_validation(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional\ntestid_2,test_runnam,1,text,2022-05-12,15,test1,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "format",
        "text": "The 'sender_sample_id' section of the filename disagrees with the CSV metadata",
    } in validator.errors
    assert {
        "type": "format",
        "text": "The 'run_name' section of the filename disagrees with the CSV metadata",
    } in validator.errors


def test_check_character_ranges(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional\ntestid_1,test_runname,1,text12345678,2022-05-12,15,test1,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "content",
        "text": "The field: test_text contains 12 characters which is outside of the range 1-8",
    } in validator.errors


def test_sample_id_elsewhere(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,test_text,test_integer,test_choice,test_date,csv_template_version\ntest_id,test_runname,test_id1234567,field_2,test56,test_id-01/12/22,1"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == False
    assert {
        "type": "content",
        "text": f"The field: test_date contains the sender_sample_id",
    } in validator.errors


def test_extra_columns(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional,test_extra\ntestid_1,test_runname,1,text,2022-05-12,15,test1,s,5"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == True
    assert {
        "type": "warning",
        "text": f"The unsupported field test_extra was included in the metadata CSV, since the field is unsupported it will not be ingested as metadata",
    } in validator.errors


def test_pass_csv(config):
    test_csv = io.StringIO(
        "sender_sample_id,run_name,csv_template_version,test_text,test_date,test_integer,test_choice,test_optional\ntestid_1,test_runname,1,text,2022-05-12,15,test1,s"
    )
    validator = csv_validator(config, test_csv, "/fake/dir/testid_1.test_runname.csv")
    assert validator.validate() == True
    assert validator.errors == []
