import unittest
from unittest.mock import Mock, mock_open, patch, MagicMock, call

from roz_scripts import s3_matcher

from types import SimpleNamespace
import multiprocessing as mp
import time
from threading import Thread
import tempfile
import os
import json
from varys import varys
from moto import mock_s3
import boto3
import uuid

DIR = os.path.dirname(__file__)
S3_MATCHER_LOG_FILENAME = os.path.join(DIR, "s3_matcher.log")
ROZ_INGEST_LOG_FILENAME = os.path.join(DIR, "ingest.log")

VARYS_CFG_PATH = os.path.join(DIR, "varys_cfg.json")
TEXT = "Hello, world!"

example_csv_msg = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "ceph:s3",
            "awsRegion": "",
            "eventTime": "2023-10-10T06:39:35.470367Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "testuser"},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {
                "x-amz-request-id": "testdata",
                "x-amz-id-2": "testdata",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "inbound.s3",
                "bucket": {
                    "name": "mscapetest-birm-ont-prod",
                    "ownerIdentity": {"principalId": "testuser"},
                    "arn": "arn:aws:s3:::mscapetest-birm-ont-prod",
                    "id": "testdata",
                },
                "object": {
                    "key": "mscapetest.sample-test.run-test.ont.csv",
                    "size": 275,
                    "eTag": "c48a8da4d9cc82cb0d8fc8fb794d676c",
                    "versionId": "",
                    "sequencer": "testdata",
                    "metadata": [
                        {"key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD"},
                        {"key": "x-amz-date", "val": "testdata"},
                    ],
                    "tags": [],
                },
            },
            "eventId": "testdata",
            "opaqueData": "",
        }
    ]
}

example_csv_msg_2 = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "ceph:s3",
            "awsRegion": "",
            "eventTime": "2023-10-10T06:39:35.470367Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "testuser"},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {
                "x-amz-request-id": "testdata",
                "x-amz-id-2": "testdata",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "inbound.s3",
                "bucket": {
                    "name": "mscapetest-birm-ont-prod",
                    "ownerIdentity": {"principalId": "testuser"},
                    "arn": "arn:aws:s3:::mscapetest-birm-ont-prod",
                    "id": "testdata",
                },
                "object": {
                    "key": "mscapetest.sample-test.run-test.ont.csv",
                    "size": 275,
                    "eTag": "29d33a6a67446891caf00d228b954ba7",
                    "versionId": "",
                    "sequencer": "testdata",
                    "metadata": [
                        {"key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD"},
                        {"key": "x-amz-date", "val": "testdata"},
                    ],
                    "tags": [],
                },
            },
            "eventId": "testdata",
            "opaqueData": "",
        }
    ]
}

example_fastq_msg = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "ceph:s3",
            "awsRegion": "",
            "eventTime": "2023-10-10T06:39:35.470367Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "testuser"},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {
                "x-amz-request-id": "testdata",
                "x-amz-id-2": "testdata",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "inbound.s3",
                "bucket": {
                    "name": "mscapetest-birm-ont-prod",
                    "ownerIdentity": {"principalId": "testuser"},
                    "arn": "arn:aws:s3:::mscapetest-birm-ont-prod",
                    "id": "testdata",
                },
                "object": {
                    "key": "mscapetest.sample-test.run-test.fastq.gz",
                    "size": 123123123,
                    "eTag": "179d94f8cd22896c2a80a9a7c98463d2-21",
                    "versionId": "",
                    "sequencer": "testdata",
                    "metadata": [
                        {"key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD"},
                        {"key": "x-amz-date", "val": "testdata"},
                    ],
                    "tags": [],
                },
            },
            "eventId": "testdata",
            "opaqueData": "",
        }
    ]
}


class MockResponse:
    def __init__(self, status_code, json_data=None):
        self.status_code = status_code
        self.json_data = json_data

    def json(self):
        return self.json_data


class TestRoz(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        config = {
            "version": "0.1",
            "profiles": {
                "roz": {
                    "username": "guest",
                    "password": "guest",
                    "amqp_url": "127.0.0.1",
                    "port": 5672,
                }
            },
        }

        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket="mscapetest-birm-ont-prod")
        s3_client.create_bucket(Bucket="pathsafetest-birm-ont-prod")

        with open(VARYS_CFG_PATH, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        os.environ["VARYS_CFG"] = VARYS_CFG_PATH
        os.environ["S3_MATCHER_LOG"] = S3_MATCHER_LOG_FILENAME
        os.environ["INGEST_LOG_LEVEL"] = "DEBUG"
        os.environ["ROZ_CONFIG_JSON"] = "config/config.json"
        os.environ["ONYX_ROZ_PASSWORD"] = "password"
        os.environ["ROZ_INGEST_LOG"] = ROZ_INGEST_LOG_FILENAME

    def tearDown(self):
        self.mock_s3.stop()

    def test_s3_successful_match(self):
        varys_client = varys("roz", S3_MATCHER_LOG_FILENAME)

        args = SimpleNamespace(sleep_time=5)

        s3_matcher_process = mp.Process(target=s3_matcher.run, args=(args,))
        s3_matcher_process.start()

        varys_client.send(
            example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )
        varys_client.send(
            example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )

        time.sleep(1)

        message = varys_client.receive(
            exchange="inbound.matched",
            queue_suffix="s3_matcher",
            timeout=20,
        )

        self.assertIsNotNone(message)
        message_dict = json.loads(message.body)

        self.assertEqual(message_dict["sample_id"], "sample-test")
        self.assertEqual(message_dict["artifact"], "mscapetest.sample-test.run-test")
        self.assertEqual(message_dict["run_name"], "run-test")
        self.assertEqual(message_dict["project"], "mscapetest")
        self.assertEqual(message_dict["platform"], "ont")
        self.assertEqual(message_dict["site"], "birm")
        self.assertEqual(message_dict["uploaders"], ["testuser"])
        self.assertEqual(
            message_dict["files"][".csv"]["key"],
            "mscapetest.sample-test.run-test.ont.csv",
        )
        self.assertEqual(
            message_dict["files"][".fastq.gz"]["key"],
            "mscapetest.sample-test.run-test.fastq.gz",
        )
        self.assertTrue(uuid.UUID(message_dict["uuid"], version=4))

        s3_matcher_process.kill()

    def test_s3_incorrect_match(self):
        varys_client = varys("roz", S3_MATCHER_LOG_FILENAME)

        args = SimpleNamespace(sleep_time=5)

        s3_matcher_process = mp.Process(target=s3_matcher.run, args=(args,))
        s3_matcher_process.start()

        varys_client.send(
            example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )
        varys_client.send(
            example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )

        time.sleep(1)

        message = varys_client.receive(
            exchange="inbound.matched",
            queue_suffix="s3_matcher",
            timeout=10,
        )
        self.assertIsNone(message)

        s3_matcher_process.kill()

    def test_s3_updated_csv(self):
        varys_client = varys("roz", S3_MATCHER_LOG_FILENAME)

        with patch("roz_scripts.mscape_ingest_validation.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value._filter.return_value = (
                MockResponse(status_code=200, json_data=[])
            )

            args = SimpleNamespace(sleep_time=5)

            s3_matcher_process = mp.Process(target=s3_matcher.run, args=(args,))
            s3_matcher_process.start()

            varys_client.send(
                example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )
            varys_client.send(
                example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )

            message = varys_client.receive(
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
                timeout=20,
            )

            self.assertIsNotNone(message)

            varys_client.send(
                example_csv_msg_2, exchange="inbound.s3", queue_suffix="s3_matcher"
            )

            message_2 = varys_client.receive(
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
                timeout=20,
            )

            message_dict = json.loads(message_2.body)

            self.assertEqual(message_dict["sample_id"], "sample-test")
            self.assertEqual(
                message_dict["artifact"], "mscapetest.sample-test.run-test"
            )
            self.assertEqual(message_dict["run_name"], "run-test")
            self.assertEqual(message_dict["project"], "mscapetest")
            self.assertEqual(message_dict["platform"], "ont")
            self.assertEqual(message_dict["site"], "birm")
            self.assertEqual(message_dict["uploaders"], ["testuser"])
            self.assertEqual(
                message_dict["files"][".csv"]["key"],
                "mscapetest.sample-test.run-test.ont.csv",
            )
            self.assertEqual(
                message_dict["files"][".fastq.gz"]["key"],
                "mscapetest.sample-test.run-test.fastq.gz",
            )
            self.assertTrue(uuid.UUID(message_dict["uuid"], version=4))

    def test_s3_identical_csv(self):
        varys_client = varys("roz", S3_MATCHER_LOG_FILENAME)

        with patch("roz_scripts.mscape_ingest_validation.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value._filter.return_value = (
                MockResponse(status_code=200, json_data=[])
            )

            args = SimpleNamespace(sleep_time=5)

            s3_matcher_process = mp.Process(target=s3_matcher.run, args=(args,))
            s3_matcher_process.start()

            varys_client.send(
                example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )
            varys_client.send(
                example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )

            message = varys_client.receive(
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
                timeout=20,
            )

            self.assertIsNotNone(message)

            varys_client.send(
                example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )

            message_2 = varys_client.receive(
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
                timeout=20,
            )

            self.assertIsNone(message_2)


# example_out = {
#     "uuid": "8ce60e10-f67e-48ad-b490-d352ba40e39d",
#     "payload_version": 1,
#     "s ite": "birm",
#     "uploaders": ["bryn-pathsafe"],
#     "match_timestamp": 1695189612872083699,
#     "artifact": "pathsafetest.sample-12.run-1",
#     "sample_id": "sa mple-12",
#     "run_name": "run-1",
#     "project": "pathsafetest",
#     "platform": "illumina",
#     "files": {
#         ".1.fastq.gz": {
#             "uri": "s3://pathsafetest-birm-illumin a-prod/pathsafetest.sample-12.run-1.1.fastq.gz",
#             "etag": "8ad9f33f120c73ab81f45848231bfba1-21",
#             "key": "pathsafetest.sample-12.run-1.1.fastq.gz",
#         },
#         ".2.fastq.gz": {
#             "uri": "s3://pathsafetest-birm-illumina-prod/pathsafetest.sample-12.run-1.2.fastq.gz",
#             "etag": "179d94f8cd22896c2a80a9a7c98463d2- 21",
#             "key": "pathsafetest.sample-12.run-1.2.fastq.gz",
#         },
#         ".csv": {
#             "uri": "s3://pathsafetest-birm-illumina-prod/pathsafetest.sample-12.run-1.illumin a.csv",
#             "etag": "c48a8da4d9cc82cb0d8fc8fb794d676c",
#             "key": "pathsafetest.sample-12.run-1.illumina.csv",
#         },
#     },
#     "test_flag": false,
# }
