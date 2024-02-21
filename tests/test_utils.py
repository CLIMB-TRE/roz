from onyx.exceptions import (
    OnyxClientError,
    OnyxRequestError,
    OnyxServerError,
    OnyxConnectionError,
    OnyxConfigError,
)

from roz_scripts.utils.utils import (
    init_logger,
    csv_create,
    csv_field_checks,
    check_artifact_published,
    onyx_identify,
    onyx_reconcile,
    get_s3_credentials,
    valid_character_checks,
)

import moto
import boto3
import unittest
from unittest.mock import patch, Mock
import os
import copy


DIR = os.path.dirname(__file__)

TEST_UTILS_LOG_FILENAME = os.path.join(DIR, "test_utils.log")


class MockResponse:
    def __init__(self, status_code, json_data=None, ok=True):
        self.status_code = status_code
        self.json_data = json_data
        self.ok = ok

    def json(self):
        return self.json_data


class test_utils(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://s3.climb.ac.uk"
        os.environ["ONYX_DOMAIN"] = "testing"
        os.environ["ONYX_TOKEN"] = "testing"
        # del os.environ["UNIT_TESTING"]

        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()

        self.s3_client = boto3.client("s3", endpoint_url="https://s3.climb.ac.uk")

        self.log = init_logger("test", TEST_UTILS_LOG_FILENAME, "DEBUG")

        self.s3_client.create_bucket(Bucket="mscape-birm-ont-prod")

        self.example_match = {
            "uuid": "42c3796d-d767-4293-97a8-c4906bb5cca8",
            "payload_version": 1,
            "site": "birm",
            "uploaders": ["testuser"],
            "match_timestamp": 1697036668222422871,
            "artifact": "mscape|sample-test|run-test",
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

        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"sample_id,run_id\nsample-test,run-test",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        self.example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

    def tearDown(self):
        self.mock_s3.stop()
        self.s3_client.close()

    def test_csv_create(self):

        self.example_match["sample_id"] = "test:sample-test-2"
        self.example_match["run_id"] = "test:run-test-2"

        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"test",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        print(get_s3_credentials())

        self.example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create.return_value = {}

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)
            self.assertNotIn("climb_id", payload.keys())

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create.return_value = {
                "climb_id": "test_climb_id",
                "sample_id": "test_sample_id",
                "run_id": "test_run_id",
            }

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=False,
            )
            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)
            self.assertEqual("test_climb_id", payload["climb_id"])

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client, patch(
            "roz_scripts.utils.utils.check_artifact_published"
        ) as mock_published_check:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create error handling",
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

            mock_published_check.return_value = (True, False, payload)

            success, alert, payload = csv_create(
                payload=self.example_match, log=self.log, test_submission=False
            )
            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client, patch(
            "roz_scripts.utils.utils.check_artifact_published"
        ) as mock_published_check:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create error handling",
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

            mock_published_check.return_value = (False, False, payload)

            success, alert, payload = csv_create(
                payload=self.example_match, log=self.log, test_submission=False
            )

            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxClientError(
                    "File contains multiple records but this is not allowed. To upload multiple records, set 'multiline' = True."
                )
            )

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertIn(
                "File contains multiple records but this is not allowed. To upload multiple records, set 'multiline' = True.",
                payload["onyx_test_create_errors"]["onyx_errors"],
            )

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create error handling",
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
                payload=self.example_match,
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
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

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
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

            self.assertFalse(success)
            self.assertTrue(alert)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxConfigError()
            )

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertFalse(success)
            self.assertTrue(alert)

    def test_csv_field_check_success(self):
        success, alert, payload = csv_field_checks(payload=self.example_match)

        print(payload)

        self.assertTrue(success)
        self.assertFalse(alert)

    def test_csv_field_check_failure(self):

        self.example_match["sample_id"] = "sample-test-2"
        self.example_match["run_id"] = "run-test-2"

        success, alert, payload = csv_field_checks(payload=self.example_match)

        print(payload)

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

    def test_published_check_true(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ({"yeet": "yeet", "climb_id": "test_id", "is_published": True},)
            )
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "sample_id",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }
            published, alert, payload = check_artifact_published(
                payload=self.example_match, log=self.log
            )
            print(payload)

            self.assertTrue(published)
            self.assertFalse(alert)
            self.assertFalse("climb_id" in payload)

    def test_published_check_false(self):
        # Test artifact is not published
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "sample_id",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }
            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ({"yeet": "yeet", "climb_id": "test_id", "is_published": False},)
            )

            published, alert, payload = check_artifact_published(
                payload=self.example_match, log=self.log
            )

            print(payload)

            self.assertFalse(published)
            self.assertFalse(alert)
            self.assertEqual(payload["climb_id"], "test_id")

    def test_published_check_error(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "sample_id",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }
            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ()
            )

            published, alert, payload = check_artifact_published(
                payload=self.example_match, log=self.log
            )
            print(payload)

            self.assertTrue(published)
            self.assertTrue(alert)

    def test_onyx_identify_true(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "sample_id",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            success, alert, payload = onyx_identify(
                payload=self.example_match, log=self.log, identity_field="sample_id"
            )
            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)
            self.assertEqual("S-1234567890", payload["climb_sample_id"])

    def test_onyx_identify_failure(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify = Mock(
                side_effect=OnyxRequestError(
                    message="test error handling",
                    response=MockResponse(
                        status_code=404,
                        json_data={
                            "data": [],
                            "messages": {"sample_id": "Test sample_id error handling"},
                        },
                    ),
                )
            )

            success, alert, payload = onyx_identify(
                payload=self.example_match, log=self.log, identity_field="sample_id"
            )

            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertFalse("climb_sample_id" in payload)

    def test_onyx_reconcile(self):
        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"sample_id,run_id,adm1_country,adm2_region,study_centre_id\nsample-test,run-test,GB,GB-ENG,1234567890",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        self.example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")
        # Test
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "sample_id",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                (
                    {
                        "sample_id": "S-1234567890",
                        "run_id": "R-12354453",
                        "adm1_country": "GB",
                        "adm2_region": "GB-ENG",
                        "study_centre_id": "1234567890",
                    },
                    {
                        "sample_id": "S-1234567890",
                        "run_id": "R-12354412312353",
                        "adm1_country": "GB",
                        "adm2_region": "GB-ENG",
                        "study_centre_id": "1234567890",
                    },
                )
            )

            success, alert, payload = onyx_reconcile(
                payload=self.example_match,
                log=self.log,
                identifier="sample_id",
                fields_to_reconcile=["adm1_country", "adm2_region", "study_centre_id"],
            )

            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)

    def test_onyx_reconcile_failure(self):
        # Test failure
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "sample_id",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                (
                    {
                        "sample_id": "S-1234567890",
                        "run_id": "R-12354453",
                        "adm1_country": "ES",
                        "adm2_region": "GB-ENG",
                        "study_centre_id": "1234567890",
                    },
                    {
                        "sample_id": "S-1234567890",
                        "run_id": "R-12354412312353",
                        "adm1_country": "GB",
                        "adm2_region": "GB-ENG",
                        "study_centre_id": "1234567890",
                    },
                )
            )

            success, alert, payload = onyx_reconcile(
                payload=self.example_match,
                log=self.log,
                identifier="sample_id",
                fields_to_reconcile=["adm1_country", "adm2_region", "study_centre_id"],
            )

            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertIn(
                "Onyx records for sample_id: S-1234567890 disagree for the following fields: adm1_country",
                payload["onyx_errors"]["reconcile_errors"],
            )

    def test_onyx_reconcile_no_filter_return(self):
        # Test no filter return
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "sample_id",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ()
            )

            success, alert, payload = onyx_reconcile(
                payload=self.example_match,
                log=self.log,
                identifier="sample_id",
                fields_to_reconcile=["adm1_country", "adm2_region", "study_centre_id"],
            )

            print(payload)

            self.assertFalse(success)
            self.assertTrue(alert)

            self.assertIn(
                "Failed to find records with Onyx sample_id for: S-1234567890 despite successful identification by Onyx",
                payload["onyx_errors"]["onyx_errors"],
            )

    def test_valid_character_check_success(self):
        success, alert, payload = valid_character_checks(payload=self.example_match)

        print(payload)

        self.assertTrue(success)
        self.assertFalse(alert)

    def test_valid_character_check_failure(self):
        self.example_match["sample_id"] = "test:sample-test-2"
        self.example_match["run_id"] = "test:run-test-2"

        success, alert, payload = valid_character_checks(payload=self.example_match)

        print(payload)

        self.assertFalse(success)
        self.assertFalse(alert)
        self.assertIn(
            "sample_id contains invalid characters, must be alphanumeric and contain only hyphens and underscores",
            payload["onyx_test_create_errors"]["sample_id"],
        )
        self.assertIn(
            "run_id contains invalid characters, must be alphanumeric and contain only hyphens and underscores",
            payload["onyx_test_create_errors"]["run_id"],
        )
