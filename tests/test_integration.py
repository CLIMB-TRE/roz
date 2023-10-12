import unittest
from unittest.mock import Mock, mock_open, patch, MagicMock, call

from roz_scripts import s3_matcher, ingest

from types import SimpleNamespace
import multiprocessing as mp
import time
import os
import json
from varys import varys
from moto import mock_s3
import boto3
import uuid
import pika

DIR = os.path.dirname(__file__)
S3_MATCHER_LOG_FILENAME = os.path.join(DIR, "s3_matcher.log")
ROZ_INGEST_LOG_FILENAME = os.path.join(DIR, "ingest.log")
TEST_MESSAGE_LOG_FILENAME = os.path.join(DIR, "test_messages.log")

TEST_CSV_FILENAME = os.path.join(DIR, "test.csv")

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
                    "eTag": "7022ea6a3adb39323b5039c1d6587d08",
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

incorrect_fastq_msg = {
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
                    "key": "mscapetest.sample-test-2.run-test.fastq.gz",
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

example_match_message = {
    "uuid": "42c3796d-d767-4293-97a8-c4906bb5cca8",
    "payload_version": 1,
    "site": "birm",
    "uploaders": ["testuser"],
    "match_timestamp": 1697036668222422871,
    "artifact": "mscapetest.sample-test.run-test",
    "sample_id": "sample-test",
    "run_name": "run-test",
    "project": "mscapetest",
    "platform": "ont",
    "files": {
        ".fastq.gz": {
            "uri": "s3://mscapetest-birm-ont-prod/mscapetest.sample-test.run-test.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "mscapetest.sample-test.run-test.fastq.gz",
        },
        ".csv": {
            "uri": "s3://mscapetest-birm-ont-prod/mscapetest.sample-test.run-test.ont.csv",
            "etag": "7022ea6a3adb39323b5039c1d6587d08",
            "key": "mscapetest.sample-test.run-test.ont.csv",
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


class Test_S3_matcher(unittest.TestCase):
    def setUp(self):
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

        with open(VARYS_CFG_PATH, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        os.environ["VARYS_CFG"] = VARYS_CFG_PATH
        os.environ["S3_MATCHER_LOG"] = S3_MATCHER_LOG_FILENAME
        os.environ["INGEST_LOG_LEVEL"] = "DEBUG"
        os.environ["ROZ_CONFIG_JSON"] = "config/config.json"
        os.environ["ONYX_ROZ_PASSWORD"] = "password"
        os.environ["ROZ_INGEST_LOG"] = ROZ_INGEST_LOG_FILENAME

        self.varys_client = varys("roz", TEST_MESSAGE_LOG_FILENAME)

    def tearDown(self):
        self.varys_client.close()

        credentials = pika.PlainCredentials("guest", "guest")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", credentials=credentials)
        )
        channel = connection.channel()

        channel.queue_delete(queue="inbound.s3")
        channel.queue_delete(queue="inbound.matched")

        connection.close()
        time.sleep(1)

    def test_s3_successful_match(self):
        args = SimpleNamespace(sleep_time=5)

        s3_matcher_process = mp.Process(target=s3_matcher.run, args=(args,))
        s3_matcher_process.start()

        self.varys_client.send(
            example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )
        self.varys_client.send(
            example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )

        message = self.varys_client.receive(
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
        args = SimpleNamespace(sleep_time=5)

        s3_matcher_process = mp.Process(target=s3_matcher.run, args=(args,))
        s3_matcher_process.start()

        self.varys_client.send(
            example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )
        self.varys_client.send(
            incorrect_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )

        message = self.varys_client.receive(
            exchange="inbound.matched",
            queue_suffix="s3_matcher",
            timeout=10,
        )
        self.assertIsNone(message)

        s3_matcher_process.kill()

    def test_s3_updated_csv(self):
        with patch("roz_scripts.s3_matcher.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value._filter.return_value.__next__.return_value = MockResponse(
                status_code=200, json_data={"data": []}
            )

            args = SimpleNamespace(sleep_time=5)

            s3_matcher_process = mp.Process(target=s3_matcher.run, args=(args,))
            s3_matcher_process.start()

            self.varys_client.send(
                example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )
            self.varys_client.send(
                example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )

            message = self.varys_client.receive(
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
                timeout=30,
            )

            self.assertIsNotNone(message)

            self.varys_client.send(
                example_csv_msg_2, exchange="inbound.s3", queue_suffix="s3_matcher"
            )

            message_2 = self.varys_client.receive(
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
                timeout=30,
            )

            self.assertIsNotNone(message_2)

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

            s3_matcher_process.kill()

    def test_s3_identical_csv(self):
        with patch("roz_scripts.s3_matcher.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value._filter.return_value.__next__.return_value = MockResponse(
                status_code=200, json_data={"data": []}
            )

            args = SimpleNamespace(sleep_time=5)

            s3_matcher_process = mp.Process(target=s3_matcher.run, args=(args,))
            s3_matcher_process.start()

            self.varys_client.send(
                example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )
            self.varys_client.send(
                example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )

            message = self.varys_client.receive(
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
                timeout=30,
            )

            self.assertIsNotNone(message)

            self.varys_client.send(
                example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
            )

            message_2 = self.varys_client.receive(
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
                timeout=30,
            )

            self.assertIsNone(message_2)

            s3_matcher_process.kill()


class Test_ingest(unittest.TestCase):
    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        self.s3_client = boto3.client("s3")
        self.s3_client.create_bucket(Bucket="mscapetest-birm-ont-prod")

        with open(TEST_CSV_FILENAME, "w") as f:
            f.write("sample_id,run_name,project,platform,site\n")
            f.write("sample-test,run-test,mscapetest,ont,birm")

        self.s3_client.upload_file(
            TEST_CSV_FILENAME,
            "mscapetest-birm-ont-prod",
            "mscapetest.sample-test.run-test.ont.csv",
        )

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

        with open(VARYS_CFG_PATH, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        os.environ["VARYS_CFG"] = VARYS_CFG_PATH
        os.environ["S3_MATCHER_LOG"] = S3_MATCHER_LOG_FILENAME
        os.environ["INGEST_LOG_LEVEL"] = "DEBUG"
        os.environ["ROZ_CONFIG_JSON"] = "config/config.json"
        os.environ["ONYX_ROZ_PASSWORD"] = "password"
        os.environ["ROZ_INGEST_LOG"] = ROZ_INGEST_LOG_FILENAME

        self.varys_client = varys("roz", TEST_MESSAGE_LOG_FILENAME)

    def tearDown(self):
        self.varys_client.close()
        self.mock_s3.stop()

        credentials = pika.PlainCredentials("guest", "guest")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", credentials=credentials)
        )
        channel = connection.channel()

        channel.queue_delete(queue="inbound.matched")
        channel.queue_delete(queue="inbound.to_validate.mscapetest")

        connection.close()
        time.sleep(1)

    def test_ingest_successful(self):
        with patch("roz_scripts.ingest.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value._csv_create.return_value.__next__.return_value = MockResponse(
                status_code=201, json_data={"data": []}, ok=True
            )

            ingest_process = mp.Process(target=ingest.main)
            ingest_process.start()

            self.varys_client.send(
                example_match_message,
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
            )

            message = self.varys_client.receive(
                exchange="inbound.to_validate.mscapetest",
                queue_suffix="ingest",
                timeout=30,
            )

            self.assertIsNotNone(message)

            message_dict = json.loads(message.body)

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
            self.assertEqual(message_dict["onyx_test_create_errors"], {})
            self.assertEqual(message_dict["onyx_test_status_code"], 201)
            self.assertTrue(message_dict["onyx_test_create_status"])
            self.assertFalse(message_dict["cid"])
            self.assertFalse(message_dict["test_flag"])
            self.assertTrue(uuid.UUID(message_dict["uuid"], version=4))
