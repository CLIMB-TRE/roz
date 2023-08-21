import logging
import pytest
import argparse
import json
import os

from unittest.mock import Mock, mock_open, patch, MagicMock, call
from unittest import TestCase

from botocore.exceptions import ClientError

from onyx import OnyxClient

import roz_scripts.mscape.mscape_ingest_validation

# from roz_scripts.mscape.mscape_ingest_validation import onyx_submission
# from roz_scripts.mscape.mscape_ingest_validation import onyx_update

# from roz.mscape.mscape_ingest_validation import execute_validation_pipeline
# from roz.mscape.mscape_ingest_validation import add_taxon_records
# from roz.mscape.mscape_ingest_validation import push_report_file
# from roz.mscape.mscape_ingest_validation import add_reads_record
# from roz.mscape.mscape_ingest_validation import ret_0_parser


class MockResponse:
    def __init__(self, status_code, json_data=None):
        self.status_code = status_code
        self.json_data = json_data

    def json(self):
        return self.json_data


class MockPipeline:
    def __init__(self):
        self.execute_called = False

    def execute(self, params):
        self.execute_called = True
        return 0, False, "stdout_output", "stderr_output"


class MockS3Client:
    def __init__(self):
        self.uploaded_files = []

    def upload_file(self, local_path, bucket, s3_key):
        self.uploaded_files.append((local_path, bucket, s3_key))


class onyx_update_tests(TestCase):
    def test_onyx_update_success(self):
        with patch("roz_scripts.mscape_ingest_validation.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value._update.return_value = (
                MockResponse(status_code=200, json_data={})
            )

            mock_logger = MagicMock()
            payload = {"cid": "test_cid", "onyx_errors": []}
            fields = {"field_name": "field_value"}

            update_fail, payload = roz_scripts.mscape_ingest_validation.onyx_update(
                payload=payload, fields=fields, log=mock_logger
            )

        assert update_fail is False
        assert payload == payload
        mock_logger.error.assert_not_called()
        mock_logger.info.assert_called_with(
            "Successfully updated Onyx record for CID: test_cid"
        )

    def test_onyx_update_failure(self):
        with patch("roz_scripts.mscape_ingest_validation.OnyxClient") as mock_client:
            payload = {"cid": "test_cid", "onyx_errors": {}}
            fields = {"field_name": "field_value"}

            mock_client.return_value.__enter__.return_value._update.return_value = (
                MockResponse(
                    status_code=400,
                    json_data={"messages": {"field_name": ["Error message"]}},
                )
            )

            mock_logger = MagicMock()

            update_fail, payload = roz_scripts.mscape_ingest_validation.onyx_update(
                payload, fields, mock_logger
            )

            assert update_fail is True
            assert "onyx_errors" in payload
            assert "field_name" in payload["onyx_errors"]
            assert payload["onyx_errors"]["field_name"] == ["Error message"]
            mock_logger.error.assert_called_with(
                "Failed to update Onyx record for CID: test_cid with status code: 400"
            )

    def test_onyx_update_client_error(self):
        with patch("roz_scripts.mscape_ingest_validation.OnyxClient") as mock_client:
            payload = {"cid": "test_cid", "onyx_errors": {}}
            fields = {"field_name": "field_value"}

            mock_logger = MagicMock()

            mock_client.return_value.__enter__.return_value._update.side_effect = (
                Exception("TEST EXCEPTION")
            )

            mock_logger = MagicMock()
            payload = {"cid": "test_cid", "onyx_errors": {}}
            fields = {"field_name": "field_value"}

            update_fail, payload = roz_scripts.mscape_ingest_validation.onyx_update(
                payload, fields, mock_logger
            )

            assert update_fail is True
            assert "onyx_client_errors" in payload["onyx_errors"]
            assert payload["onyx_errors"]["onyx_client_errors"] == [
                "Unhandled client error TEST EXCEPTION"
            ]
            mock_logger.error.assert_called_once_with(
                "Failed to update Onyx record for CID: test_cid with unhandled onyx client error: TEST EXCEPTION"
            )


class test_execute_validation_pipeline(TestCase):
    def test_execute_validation_pipeline_ont(self):
        mock_logger = MagicMock()
        mock_args = Mock(spec=argparse.Namespace)
        mock_args.result_dir = "/path/to/result"
        mock_ingest_pipe = MockPipeline()

        payload = {
            "uuid": "test_uuid",
            "platform": "ont",
            "files": {".fastq.gz": {"uri": "s3://bucket/fastq.gz"}},
        }

        result = roz_scripts.mscape_ingest_validation.execute_validation_pipeline(
            payload, mock_args, mock_logger, mock_ingest_pipe
        )

        assert mock_ingest_pipe.execute_called
        assert result == (0, False, "stdout_output", "stderr_output")
        mock_logger.info.assert_called_once_with(
            f"Submitted ingest pipeline for UUID: {payload['uuid']}"
        )

    def test_execute_validation_pipeline_illumina(self):
        mock_logger = MagicMock()
        mock_args = Mock(spec=argparse.Namespace)
        mock_args.result_dir = "/path/to/result"
        mock_ingest_pipe = MockPipeline()

        payload = {
            "uuid": "test_uuid",
            "platform": "illumina",
            "files": {
                ".1.fastq.gz": {"uri": "s3://bucket/1.fastq.gz"},
                ".2.fastq.gz": {"uri": "s3://bucket/2.fastq.gz"},
            },
        }

        result = roz_scripts.mscape_ingest_validation.execute_validation_pipeline(
            payload, mock_args, mock_logger, mock_ingest_pipe
        )

        assert mock_ingest_pipe.execute_called
        assert result == (0, False, "stdout_output", "stderr_output")
        mock_logger.info.assert_called_once_with(
            f"Submitted ingest pipeline for UUID: {payload['uuid']}"
        )


class test_onyx_submission(TestCase):
    def test_onyx_submission_success(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.return_value.__next__.return_value = MockResponse(
                status_code=201, json_data={"data": {"cid": "test_cid"}}
            )
            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )

            assert submission_fail is False
            assert payload["onyx_create_status"] is True
            assert payload["created"] is True
            assert payload["cid"] == "test_cid"

            mock_logger.error.assert_not_called()

            mock_logger.info.assert_has_calls(
                [
                    call(
                        "Received match for artifact: test_artifact, now attempting to create record in Onyx"
                    ),
                    call(
                        "Successful create for UUID: test_uuid which has been assigned CID: test_cid"
                    ),
                ]
            )

    def test_onyx_ret_200(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.return_value.__next__.return_value = MockResponse(
                status_code=200, json_data={}
            )

            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )

            assert submission_fail is True
            assert payload["onyx_create_status"] is False
            assert payload["created"] is False
            assert payload["cid"] == ""
            assert payload["onyx_errors"]["onyx_client_errors"] == [
                "200 response status on onyx create (should be 201)"
            ]

            mock_logger.error.assert_called_once_with(
                "Onyx responded with 200 on a create request for UUID: test_uuid (this should be 201)"
            )

    def test_onyx_submission_internal_server_error(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.return_value.__next__.return_value = MockResponse(
                status_code=500, json_data={}
            )

            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )

            assert submission_fail is True
            assert payload["onyx_create_status"] is False
            assert payload["created"] is False
            assert payload["cid"] == ""
            assert payload["onyx_errors"]["onyx_client_errors"] == [
                "Onyx internal server error"
            ]
            assert mock_logger.error.called_once_with(
                "Onyx create for UUID: test_uuid lead to onyx internal server error"
            )

    def test_onyx_submission_project_doesnt_exist(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.return_value.__next__.return_value = MockResponse(
                status_code=404, json_data={}
            )

            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )

            assert submission_fail is True
            assert payload["onyx_create_status"] is False
            assert payload["created"] is False
            assert payload["cid"] == ""
            assert payload["onyx_errors"]["onyx_client_errors"] == [
                "Project test_project does not exist"
            ]
            assert mock_logger.error.called_once_with(
                "Onyx create for UUID: test_uuid failed because project: test_project does not exist"
            )

    def test_onyx_submission_permission_error(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.return_value.__next__.return_value = MockResponse(
                status_code=403, json_data={}
            )

            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )
            assert submission_fail is True
            assert payload["onyx_create_status"] is False
            assert payload["created"] is False
            assert payload["cid"] == ""
            assert payload["onyx_errors"]["onyx_client_errors"] == [
                "Permission error on Onyx create"
            ]
            assert mock_logger.error.called_once_with(
                "Onyx create for UUID: test_uuid failed due to a permission error"
            )

    def test_onyx_submission_incorrect_credentials(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.return_value.__next__.return_value = MockResponse(
                status_code=401, json_data={}
            )

            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )
            assert submission_fail is True
            assert payload["onyx_create_status"] is False
            assert payload["created"] is False
            assert payload["cid"] == ""
            assert payload["onyx_errors"]["onyx_client_errors"] == [
                "Incorrect Onyx credentials"
            ]
            mock_logger.error.called_once_with(
                "Onyx create for UUID: test_uuid failed due to incorrect credentials"
            )

    def test_onyx_submission_bad_request(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.return_value.__next__.return_value = MockResponse(
                status_code=400,
                json_data={"messages": {"field_name": ["Error message"]}},
            )

            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )
            assert submission_fail is True
            assert payload["onyx_create_status"] is False
            assert payload["created"] is False
            assert payload["cid"] == ""
            assert payload["onyx_errors"]["field_name"] == ["Error message"]
            assert mock_logger.error.called_once_with(
                "Onyx create for UUID: test_uuid failed due to a bad request"
            )

    def test_onyx_submission_unknown_status_code(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.return_value.__next__.return_value = MockResponse(
                status_code=69,
                json_data={"messages": {"field_name": ["Error message"]}},
            )

            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )
            assert submission_fail is True
            assert payload["onyx_create_status"] is False
            assert payload["created"] is False
            assert payload["cid"] == ""
            assert payload["onyx_errors"]["onyx_client_errors"] == [
                f"Unhandled response status code 69 from Onyx create"
            ]
            assert mock_logger.error.called_once_with(
                "Unhandled Onyx response status code 69 from Onyx create for UUID: test_uuid"
            )

    def test_onxy_submission_client_exception(self):
        with patch(
            "roz_scripts.mscape_ingest_validation.OnyxClient"
        ) as mock_client, patch(
            "roz_scripts.mscape_ingest_validation.s3_to_fh"
        ) as mock_s3_to_fh:
            mock_client.return_value.__enter__.return_value.csv_create.side_effect = (
                Exception("TEST EXCEPTION")
            )

            mock_s3_to_fh.return_value = MagicMock()

            mock_logger = MagicMock()

            payload = {
                "artifact": "test_artifact",
                "project": "test_project",
                "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
                "uuid": "test_uuid",
                "site_code": "test_site",
            }

            (
                submission_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.onyx_submission(
                mock_logger, payload
            )
            assert submission_fail is True
            assert payload["onyx_create_status"] is False
            assert payload["created"] is False
            assert payload["cid"] == ""
            assert payload["onyx_errors"]["onyx_client_errors"] == [
                f"Unhandled client error TEST EXCEPTION"
            ]
            assert mock_logger.error.called_once_with(
                "Onyx CSV create failed for UUID: {payload['uuid']} due to client error: TEST EXCEPTION"
            )


class test_add_records(TestCase):
    def test_add_taxon_records_illumina(self):
        mock_logger = MagicMock()

        mock_s3_client = MockS3Client()

        payload = {"platform": "illumina", "cid": "test_cid", "uuid": "test_uuid"}
        result_path = "/path/to/result"
        read_summary = {
            "taxon": "1",
            "human_readable": "Saiyan",
            "qc_metrics": {
                "num_reads": "10",
                "avg_qual": "9001",
                "mean_len": "150",
            },
            "filenames": ["1_1.fastq", "1_2.fastq"],
            "tax_level": "S",
        }

        read_summary_serialised = json.dumps([read_summary])

        nested_record = {
            "taxon_id": "1",
            "human_readable": "Saiyan",
            "n_reads": "10",
            "avg_quality": "9001",
            "mean_len": "150",
            "tax_level": "S",
            "reads_1": "s3://mscapetest-published-binned-reads/test_cid/1_1.fastq.gz",
            "reads_2": "s3://mscapetest-published-binned-reads/test_cid/1_2.fastq.gz",
        }

        with patch(
            "builtins.open", new_callable=mock_open, read_data=read_summary_serialised
        ), patch(
            "roz_scripts.mscape_ingest_validation.onyx_update"
        ) as mock_onyx_update:
            mock_onyx_update.return_value = (False, payload)

            (
                binned_read_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.add_taxon_records(
                payload, result_path, mock_logger, mock_s3_client
            )

        assert binned_read_fail is False
        assert mock_logger.error.not_called()

        assert mock_onyx_update.call_args[1]["payload"] == payload
        assert mock_onyx_update.call_args[1]["fields"] == {"taxa": [nested_record]}

        assert (
            "/path/to/result/reads_by_taxa/1_1.fastq.gz",
            "mscapetest-published-binned-reads",
            "test_cid/1_1.fastq.gz",
        ) in mock_s3_client.uploaded_files
        assert (
            "/path/to/result/reads_by_taxa/1_2.fastq.gz",
            "mscapetest-published-binned-reads",
            "test_cid/1_2.fastq.gz",
        ) in mock_s3_client.uploaded_files

    def test_add_taxon_records_ont(self):
        mock_logger = MagicMock()

        mock_s3_client = MockS3Client()

        payload = {"platform": "ont", "cid": "test_cid", "uuid": "test_uuid"}
        result_path = "/path/to/result"
        read_summary = {
            "taxon": "1",
            "human_readable": "Saiyan",
            "qc_metrics": {
                "num_reads": "10",
                "avg_qual": "9001",
                "mean_len": "150",
            },
            "filenames": ["1.fastq"],
            "tax_level": "S",
        }

        read_summary_serialised = json.dumps([read_summary])

        nested_record = {
            "taxon_id": "1",
            "human_readable": "Saiyan",
            "n_reads": "10",
            "avg_quality": "9001",
            "mean_len": "150",
            "tax_level": "S",
            "reads_1": "s3://mscapetest-published-binned-reads/test_cid/1.fastq.gz",
        }

        with patch(
            "builtins.open", new_callable=mock_open, read_data=read_summary_serialised
        ), patch(
            "roz_scripts.mscape_ingest_validation.onyx_update"
        ) as mock_onyx_update:
            mock_onyx_update.return_value = (False, payload)

            (
                binned_read_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.add_taxon_records(
                payload, result_path, mock_logger, mock_s3_client
            )

        assert binned_read_fail is False
        assert mock_logger.error.not_called()

        assert mock_onyx_update.call_args[1]["payload"] == payload
        assert mock_onyx_update.call_args[1]["fields"] == {"taxa": [nested_record]}

        assert (
            "/path/to/result/reads_by_taxa/1.fastq.gz",
            "mscapetest-published-binned-reads",
            "test_cid/1.fastq.gz",
        ) in mock_s3_client.uploaded_files

    def test_add_taxon_records_unrecognised_platform(self):
        mock_logger = MagicMock()

        mock_s3_client = MockS3Client()

        payload = {"platform": "test", "cid": "test_cid", "uuid": "test_uuid"}
        result_path = "/path/to/result"
        read_summary = {
            "taxon": "1",
            "human_readable": "Saiyan",
            "qc_metrics": {
                "num_reads": "10",
                "avg_qual": "9001",
                "mean_len": "150",
            },
            "filenames": ["1.fastq"],
            "tax_level": "S",
        }

        read_summary_serialised = json.dumps([read_summary])

        nested_record = {
            "taxon_id": "1",
            "human_readable": "Saiyan",
            "n_reads": "10",
            "avg_quality": "9001",
            "mean_len": "150",
            "tax_level": "S",
            "reads_1": "s3://mscapetest-published-binned-reads/test_cid/1.fastq.gz",
        }

        with patch(
            "builtins.open", new_callable=mock_open, read_data=read_summary_serialised
        ), patch(
            "roz_scripts.mscape_ingest_validation.onyx_update"
        ) as mock_onyx_update:
            (
                binned_read_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.add_taxon_records(
                payload, result_path, mock_logger, mock_s3_client
            )

        assert binned_read_fail is True
        assert mock_onyx_update.not_called()
        assert mock_logger.error.called_with("Unknown platform: test")

        assert "Unknown platform: test" in payload["ingest_errors"]

        assert mock_s3_client.uploaded_files == []


class test_report_file(TestCase):
    def test_push_report_file_success(self):
        mock_logger = Mock(spec=logging.Logger)
        mock_s3_client = MockS3Client()

        payload = {"uuid": "test_uuid", "cid": "test_cid", "ingest_errors": []}

        s3_bucket_name = "mscapetest-published-reports"
        mock_s3_client.upload_file = Mock()

        with patch("os.path.exists", return_value=True), patch(
            "roz_scripts.mscape_ingest_validation.onyx_update"
        ) as mock_onyx_update:
            (
                report_fail,
                payload,
            ) = roz_scripts.mscape_ingest_validation.push_report_file(
                payload, "/path/to/result", mock_logger, mock_s3_client
            )

        assert report_fail is False
        assert payload["ingest_errors"] == []
        assert len(mock_s3_client.uploaded_files) == 1
        assert mock_s3_client.uploaded_files[0] == (
            "/path/to/result/test_uuid_report.html",
            s3_bucket_name,
            "test_cid_report.html",
        )
        mock_s3_client.upload_file.assert_called_once()
        assert mock_logger.error.not_called

    def test_push_report_file_upload_failure(self):
        mock_logger = Mock(spec=logging.Logger)
        mock_s3_client = MockS3Client()

        payload = {"uuid": "test_uuid", "cid": "test_cid", "ingest_errors": []}

        mock_s3_client.upload_file = Mock(
            side_effect=ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "operation_name"
            )
        )

        with patch("os.path.exists", return_value=True):
            with patch("boto3.client", return_value=mock_s3_client):
                result = push_report_file(
                    payload, "/path/to/result", mock_logger, mock_s3_client
                )

        assert result[0] is True
        assert len(result[1]["ingest_errors"]) == 1
        assert (
            "Failed to upload scylla report to storage bucket"
            in result[1]["ingest_errors"]
        )
        mock_s3_client.upload_file.assert_called_once()

    def test_push_report_file_update_failure(self):
        mock_logger = Mock(spec=logging.Logger)
        mock_s3_client = MockS3Client()

        payload = {"uuid": "test_uuid", "cid": "test_cid", "ingest_errors": []}

        mock_s3_client.upload_file = Mock()

        with patch("os.path.exists", return_value=True):
            with patch(
                "roz_scripts.mscape.mscape_ingest_validation.onyx_update",
                side_effect=lambda *args, **kwargs: (True, payload),
            ):
                result = push_report_file(
                    payload, "/path/to/result", mock_logger, mock_s3_client
                )

        assert result[0] is True
        assert len(result[1]["ingest_errors"]) == 1
        assert (
            "Failed to upload scylla report to storage bucket"
            in result[1]["ingest_errors"]
        )
        mock_s3_client.upload_file.assert_called_once()


def test_add_reads_record_illumina():
    mock_logger = Mock(spec=logging.Logger)
    mock_s3_client = MockS3Client()

    payload = {
        "platform": "illumina",
        "uuid": "test_uuid",
        "cid": "test_cid",
        "ingest_errors": [],
    }

    s3_bucket_name = "mscapetest-published-reads"
    mock_s3_client.upload_file = Mock()

    with patch("os.path.exists", return_value=True):
        result = add_reads_record(
            payload, mock_s3_client, "/path/to/result", mock_logger
        )

    assert result[0] is False
    assert result[1] == payload
    assert len(mock_s3_client.uploaded_files) == 2
    assert (
        mock_s3_client.uploaded_files[0][0]
        == "/path/to/result/preprocess/test_uuid_1.fastp.fastq.gz"
    )
    assert mock_s3_client.uploaded_files[0][1] == s3_bucket_name
    assert mock_s3_client.uploaded_files[0][2] == "test_cid_1.fastq.gz"
    assert (
        mock_s3_client.uploaded_files[1][0]
        == "/path/to/result/preprocess/test_uuid_2.fastp.fastq.gz"
    )
    assert mock_s3_client.uploaded_files[1][1] == s3_bucket_name
    assert mock_s3_client.uploaded_files[1][2] == "test_cid_2.fastq.gz"


def test_add_reads_record_illumina_upload_failure():
    mock_logger = Mock(spec=logging.Logger)
    mock_s3_client = MockS3Client()

    payload = {
        "platform": "illumina",
        "uuid": "test_uuid",
        "cid": "test_cid",
        "ingest_errors": [],
    }

    mock_s3_client.upload_file = Mock(
        side_effect=ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "operation_name"
        )
    )

    with patch("os.path.exists", return_value=True):
        with patch("boto3.client", return_value=mock_s3_client):
            result = add_reads_record(
                payload, mock_s3_client, "/path/to/result", mock_logger
            )

    assert result[0] is True
    assert len(result[1]["ingest_errors"]) == 1
    assert "Failed to upload reads to storage bucket" in result[1]["ingest_errors"]
    mock_s3_client.upload_file.assert_called_once()


def test_add_reads_record_non_illumina():
    mock_logger = Mock(spec=logging.Logger)
    mock_s3_client = MockS3Client()

    payload = {
        "platform": "other_platform",
        "uuid": "test_uuid",
        "cid": "test_cid",
        "ingest_errors": [],
    }

    s3_bucket_name = "mscapetest-published-reads"
    mock_s3_client.upload_file = Mock()

    with patch("os.path.exists", return_value=True):
        result = add_reads_record(
            payload, mock_s3_client, "/path/to/result", mock_logger
        )

    assert result[0] is False
    assert result[1] == payload
    assert len(mock_s3_client.uploaded_files) == 1
    assert (
        mock_s3_client.uploaded_files[0][0]
        == "/path/to/result/preprocess/test_uuid.fastp.fastq.gz"
    )
    assert mock_s3_client.uploaded_files[0][1] == s3_bucket_name
    assert mock_s3_client.uploaded_files[0][2] == "test_cid.fastq.gz"


def test_add_reads_record_non_illumina_upload_failure():
    mock_logger = Mock(spec=logging.Logger)
    mock_s3_client = MockS3Client()

    payload = {
        "platform": "other_platform",
        "uuid": "test_uuid",
        "cid": "test_cid",
        "ingest_errors": [],
    }

    mock_s3_client.upload_file = Mock(
        side_effect=ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "operation_name"
        )
    )

    with patch("os.path.exists", return_value=True):
        with patch("boto3.client", return_value=mock_s3_client):
            result = add_reads_record(
                payload, mock_s3_client, "/path/to/result", mock_logger
            )

    assert result[0] is True
    assert len(result[1]["ingest_errors"]) == 1
    assert "Failed to upload reads to storage bucket" in result[1]["ingest_errors"]
    mock_s3_client.upload_file.assert_called_once()


def test_ret_0_parser_success():
    mock_logger = Mock(spec=logging.Logger)

    payload = {"uuid": "test_uuid", "ingest_errors": []}

    result_path = "/path/to/result"
    mock_open_func = mock_open(
        read_data="name:process1\texit:0\tstatus:COMPLETED\nname:process2\texit:0\tstatus:COMPLETED\n"
    )
    with patch("os.path.exists", return_value=True), patch(
        "builtins.open", mock_open_func
    ):
        result = ret_0_parser(mock_logger, payload, result_path)

    assert result[0] is False
    assert result[1] == payload
    mock_logger.error.assert_not_called()
    assert "ingest_errors" not in payload


def test_ret_0_parser_failure():
    mock_logger = Mock(spec=logging.Logger)

    payload = {"uuid": "test_uuid", "ingest_errors": []}

    result_path = "/path/to/result"
    mock_open_func = mock_open(
        read_data="name:process1\texit:0\tstatus:COMPLETED\nname:process2\texit:1\tstatus:FAILED\n"
    )
    with patch("os.path.exists", return_value=True), patch(
        "builtins.open", mock_open_func
    ):
        result = ret_0_parser(mock_logger, payload, result_path)

    assert result[0] is True
    assert result[1] == payload
    assert len(payload["ingest_errors"]) == 1
    assert "MScape validation pipeline (Scylla) failed" in payload["ingest_errors"]
    mock_logger.error.assert_called_once()


def test_ret_0_parser_trace_file_error():
    mock_logger = Mock(spec=logging.Logger)

    payload = {"uuid": "test_uuid", "ingest_errors": []}

    result_path = "/path/to/result"
    mock_open_func = mock_open()
    with patch("os.path.exists", return_value=True), patch(
        "builtins.open", mock_open_func, side_effect=Exception("Error opening file")
    ):
        result = ret_0_parser(mock_logger, payload, result_path)

    assert result[0] is True
    assert result[1] == payload
    assert len(payload["ingest_errors"]) == 1
    assert "couldn't open nxf ingest pipeline trace" in payload["ingest_errors"]
    mock_logger.error.assert_called_once()


if __name__ == "__main__":
    pytest.main()
