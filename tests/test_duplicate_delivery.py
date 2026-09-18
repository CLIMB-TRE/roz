"""Duplicate-delivery tests: the evidence base for accepting at-least-once delivery.

Acking only after a confirmed publish means a failure between the two replays the
message, so the same artifact can arrive twice. These tests exercise the real
`csv_create` -> `check_artifact_published` path (only OnyxClient is mocked) to show
that a redelivery cannot produce a second published artifact, and that the result
JSON is overwritten rather than duplicated.
"""

import json
import os
import unittest
from unittest.mock import patch

import boto3
import moto
from onyx.exceptions import OnyxRequestError

from roz_scripts.utils.config import load_config, site_bucket
from roz_scripts.utils.utils import csv_create, init_logger, put_result_json

DIR = os.path.dirname(__file__)
TEST_CONFIG_PATH = os.path.join(DIR, "fixtures", "test_config.json")
LOG_FILENAME = os.path.join(DIR, "test_duplicate_delivery.log")

ALREADY_EXISTS = "This combination of run_index, run_id already exists."


class MockResponse:
    def __init__(self, status_code, json_data=None, ok=True):
        self.status_code = status_code
        self.json_data = json_data
        self.ok = ok

    def json(self):
        return self.json_data


def already_exists_error():
    """The error Onyx returns when this run_index/run_id pair was already created."""
    return OnyxRequestError(
        message=ALREADY_EXISTS,
        response=MockResponse(
            status_code=400,
            json_data={"data": [], "messages": {"non_field_errors": [ALREADY_EXISTS]}},
        ),
    )


class TestDuplicateDelivery(unittest.TestCase):

    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://s3.climb.ac.uk"
        os.environ["ONYX_DOMAIN"] = "testing"
        os.environ["ONYX_TOKEN"] = "testing"

        self.config = load_config(TEST_CONFIG_PATH)
        self.log = init_logger("test_duplicate_delivery", LOG_FILENAME, "DEBUG")

        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()
        self.s3_client = boto3.client("s3", endpoint_url="https://s3.climb.ac.uk")

        self.ingest_bucket = "mscape-birm-ont-prod"
        self.results_bucket = site_bucket(self.config, "mscape", "birm", "results")

        self.s3_client.create_bucket(Bucket=self.ingest_bucket)
        self.s3_client.create_bucket(Bucket=self.results_bucket)

        self.s3_client.put_object(
            Bucket=self.ingest_bucket,
            Key="mscape.sample-test.run-test.csv",
            Body=b"run_index,run_id\nsample-test,run-test",
        )
        etag = self.s3_client.head_object(
            Bucket=self.ingest_bucket,
            Key="mscape.sample-test.run-test.csv",
        )["ETag"].replace('"', "")

        self.payload = {
            "uuid": "42c3796d-d767-4293-97a8-c4906bb5cca8",
            "payload_version": 1,
            "site": "birm",
            "raw_site": "birm",
            "uploaders": ["testuser"],
            "match_timestamp": 1697036668222422871,
            "artifact": "mscape|sample-test|run-test",
            "run_index": "sample-test",
            "run_id": "run-test",
            "project": "mscape",
            "platform": "ont",
            "files": {
                ".csv": {
                    "uri": f"s3://{self.ingest_bucket}/mscape.sample-test.run-test.csv",
                    "etag": etag,
                    "key": "mscape.sample-test.run-test.csv",
                },
            },
            "test_flag": False,
        }

    def tearDown(self):
        self.mock_s3.stop()
        self.s3_client.close()

    def _patched_onyx(self, mock_client, is_published):
        """Onyx as it behaves on a redelivery: the create is refused as a duplicate,
        and the existing record is found with the given publication state."""
        inner = mock_client.return_value.__enter__.return_value
        inner.csv_create.side_effect = already_exists_error()
        inner.identify.return_value = {
            "field": "run_index",
            "value": "hidden-value",
            "identifier": "S-1234567890",
        }
        inner.filter.return_value = iter(
            ({"climb_id": "test_climb_id", "is_published": is_published},)
        )

    def test_redelivery_before_publication_proceeds(self):
        """A redelivery of a created-but-unpublished artifact is a harmless retry.

        The create is refused, the record is found unpublished, and the artifact
        continues down the pipeline carrying the climb_id it was already given.
        """
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            self._patched_onyx(mock_client, is_published=False)

            success, alert, payload = csv_create(
                payload=self.payload, log=self.log, test_submission=True
            )

        self.assertTrue(success)
        self.assertFalse(alert)
        self.assertEqual(payload["climb_id"], "test_climb_id")

    def test_redelivery_after_publication_is_rejected(self):
        """The case that makes at-least-once safe: no second published artifact.

        Once the artifact is published, a redelivery is refused outright rather
        than creating a duplicate record.
        """
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            self._patched_onyx(mock_client, is_published=True)

            success, alert, payload = csv_create(
                payload=self.payload, log=self.log, test_submission=True
            )

        self.assertFalse(success)
        self.assertFalse(alert)
        self.assertIn(
            ALREADY_EXISTS,
            payload["onyx_test_create_errors"]["non_field_errors"],
        )

    def test_redelivery_after_publication_is_rejected_on_real_create(self):
        """Same again on the non-test create, which is what actually writes to Onyx."""
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            self._patched_onyx(mock_client, is_published=True)

            success, alert, payload = csv_create(
                payload=self.payload, log=self.log, test_submission=False
            )

        self.assertFalse(success)
        self.assertFalse(alert)
        self.assertIn(
            ALREADY_EXISTS,
            payload["onyx_create_errors"]["non_field_errors"],
        )

    def test_result_json_is_overwritten_not_duplicated(self):
        """The result JSON key is deterministic, so a redelivery overwrites it."""
        put_result_json(payload=self.payload, log=self.log, config=self.config)
        put_result_json(payload=self.payload, log=self.log, config=self.config)

        listing = self.s3_client.list_objects_v2(Bucket=self.results_bucket)

        self.assertEqual(listing["KeyCount"], 1)
        self.assertEqual(
            listing["Contents"][0]["Key"],
            "mscape.sample-test.run-test.result.json",
        )

    def test_result_json_reflects_the_latest_delivery(self):
        """Overwriting must not leave the first delivery's payload behind."""
        put_result_json(payload=self.payload, log=self.log, config=self.config)

        second = dict(self.payload, climb_id="test_climb_id")
        put_result_json(payload=second, log=self.log, config=self.config)

        body = self.s3_client.get_object(
            Bucket=self.results_bucket,
            Key="mscape.sample-test.run-test.result.json",
        )["Body"].read()

        self.assertEqual(json.loads(body)["climb_id"], "test_climb_id")


if __name__ == "__main__":
    unittest.main()
