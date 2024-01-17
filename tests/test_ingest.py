from onyx.exceptions import (
    OnyxClientError,
    OnyxRequestError,
    OnyxServerError,
    OnyxConnectionError,
    OnyxConfigError,
)

from roz_scripts.utils.utils import init_logger, csv_create, csv_field_checks

import moto
import boto3
import unittest
from unittest.mock import patch, Mock
import os

example_match = {
    "uuid": "42c3796d-d767-4293-97a8-c4906bb5cca8",
    "payload_version": 1,
    "site": "birm",
    "uploaders": ["testuser"],
    "match_timestamp": 1697036668222422871,
    "artifact": "mscape.sample-test.run-test",
    "sample_id": "sample-test",
    "run_id": "run-test",
    "project": "mscape",
    "platform": "ont",
    "files": {
        ".fastq.gz": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "mscape.sample-test.run-test.fastq.gz",
        },
        ".csv": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.csv",
            "etag": "7022ea6a3adb39323b5039c1d6587d08",
            "key": "mscape.sample-test.run-test.csv",
        },
    },
    "test_flag": False,
}


class MockResponse:
    def __init__(self, status_code, json_data=None, ok=True):
        self.status_code = status_code
        self.json_data = json_data
        self.ok = ok

    def json(self):
        return self.json_data


class test_ingest(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://s3.climb.ac.uk"
        os.environ["ONYX_DOMAIN"] = "testing"
        os.environ["ONYX_TOKEN"] = "testing"

        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()

        self.s3_client = boto3.client("s3", endpoint_url="https://s3.climb.ac.uk")

        self.log = init_logger("test", "test.log", "DEBUG")

        self.s3_client.create_bucket(Bucket="mscape-birm-ont-prod")

    def tearDown(self):
        self.mock_s3.stop()
        self.s3_client.close()

    def test_csv_create(self):
        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"test",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create.return_value = {}

            success, alert, payload = csv_create(
                payload=example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertTrue(success)
            self.assertFalse(alert)
            self.assertNotIn("climb_id", payload.keys())

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create.return_value = {
                "climb_id": "test_climb_id"
            }

            success, alert, payload = csv_create(
                payload=example_match,
                log=self.log,
                test_submission=False,
            )

            self.assertTrue(success)
            self.assertFalse(alert)
            self.assertEqual("test_climb_id", payload["climb_id"])

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxClientError(
                    "File contains multiple records but this is not allowed. To upload multiple records, set 'multiline' = True."
                )
            )

            success, alert, payload = csv_create(
                payload=example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertIn(
                "File contains multiple records but this is not allowed. To upload multiple records, set 'multiline' = True.",
                payload["onyx_test_create_errors"]["onyx_errors"],
            )

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxRequestError(
                    message={
                        "data": [],
                        "messages": {"sample_id": "Test sample_id error handling"},
                    },
                    response=MockResponse(
                        status_code=400,
                        json_data={
                            "data": [],
                            "messages": {
                                "sample_id": ["Test sample_id error handling"]
                            },
                        },
                    ),
                )
            )

            success, alert, payload = csv_create(
                payload=example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertIn(
                "Test sample_id error handling",
                payload["onyx_test_create_errors"]["sample_id"],
            )

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxConnectionError()
            )

            success, alert, payload = csv_create(
                payload=example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertFalse(success)
            self.assertTrue(alert)
            csv_create_calls = (
                mock_client.return_value.__enter__.return_value.csv_create.mock_calls
            )

            self.assertEqual(len(csv_create_calls), 4)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxServerError(
                    message="Test server error handling",
                    response=MockResponse(status_code=500),
                )
            )

            success, alert, payload = csv_create(
                payload=example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertFalse(success)
            self.assertTrue(alert)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxConfigError()
            )

            success, alert, payload = csv_create(
                payload=example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertFalse(success)
            self.assertTrue(alert)

    def test_csv_field_checks(self):
        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"sample_id,run_id\nsample-test,run-test",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

        success, alert, payload = csv_field_checks(payload=example_match)

        self.assertTrue(success)
        self.assertFalse(alert)

        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"sample_id,run_id\nsample-yeet,run-yeet",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

        success, alert, payload = csv_field_checks(payload=example_match)

        self.assertFalse(success)
        self.assertFalse(alert)
        self.assertIn(
            "Field does not match filename.",
            payload["onyx_test_create_errors"]["sample_id"],
        )
        self.assertIn(
            "Field does not match filename.",
            payload["onyx_test_create_errors"]["run_id"],
        )
