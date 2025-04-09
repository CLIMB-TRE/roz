import unittest.mock
import moto
import boto3
import unittest
import json
import os

from roz_scripts.general.s3_onyx_updates import csv_update
from roz_scripts.utils.utils import init_logger

from onyx.exceptions import OnyxRequestError

DIR = os.path.dirname(__file__)

FAKE_VARYS_CFG_PATH = os.path.join(DIR, "fake_varys_cfg.json")
FAKE_ROZ_CFG_PATH = os.path.join(DIR, "fake_roz_cfg.json")
FAKE_AWS_CREDS = os.path.join(DIR, "fake_aws_creds.json")
S3_ONYX_UPDATE_LOG_FILENAME = os.path.join(DIR, "s3_onyx_updates.log")

fake_roz_cfg_dict = {
    "version": "1",
    "pathogen_configs": ["project1", "project2"],
    "configs": {
        "project1": {
            "artifact_layout": "project|run_index|run_id",
            "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
            "sites": ["subsite1.site1.project1", "site2.project1"],
            "bucket_policies": {
                "site_ingest": ["get", "put", "list", "delete"],
                "site_read": ["get", "list"],
                "project_read": ["get", "list"],
                "project_private": [],
            },
            "csv_updates": False,
            "site_buckets": {
                "ingest": {
                    "name_layout": "{project}-{site}-{platform}-{test_flag}",
                    "policy": "site_ingest",
                }
            },
            "project_buckets": {
                "fake_files": {
                    "name_layout": "{project}-fake-files",
                    "policy": "project_private",
                },
                "fake_files_2": {
                    "name_layout": "{project}-fake-files-2",
                    "policy": "project_read",
                },
            },
            "file_specs": {
                "illumina": {
                    ".1.fastq.gz": {
                        "layout": "project.run_index.run_id.direction.ftype.gzip",
                    },
                    ".2.fastq.gz": {
                        "layout": "project.run_index.run_id.direction.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.run_index.run_id.ftype",
                    },
                },
                "illumina.se": {
                    ".fastq.gz": {
                        "layout": "project.run_index.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.run_index.run_id.ftype",
                    },
                },
                "ont": {
                    ".fastq.gz": {
                        "layout": "project.run_index.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.run_index.run_id.ftype",
                    },
                },
            },
        },
        "project2": {
            "artifact_layout": "project|run_index|run_id",
            "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
            "sites": ["subsite1.site1.project2", "site2.project2"],
            "bucket_policies": {
                "site_ingest": ["get", "put", "list", "delete"],
                "site_read": ["get", "list"],
                "project_read": ["get", "list"],
                "project_private": [],
            },
            "site_buckets": {
                "ingest": {
                    "name_layout": "{project}-{site}-{platform}-{test_flag}",
                    "policy": "site_ingest",
                }
            },
            "csv_updates": True,
            "project_buckets": {
                "fake_files": {
                    "name_layout": "{project}-fake-files",
                    "policy": "project_private",
                },
                "fake_files_2": {
                    "name_layout": "{project}-fake-files-2",
                    "policy": "project_read",
                },
            },
            "file_specs": {
                "illumina": {
                    ".1.fastq.gz": {
                        "layout": "project.run_index.run_id.direction.ftype.gzip",
                    },
                    ".2.fastq.gz": {
                        "layout": "project.run_index.run_id.direction.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.run_index.run_id.ftype",
                    },
                },
                "ont": {
                    ".fastq.gz": {
                        "layout": "project.run_index.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.run_index.run_id.ftype",
                    },
                },
                "pacbio": {
                    ".fastq.gz": {
                        "layout": "project.run_index.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.run_index.run_id.ftype",
                    },
                },
            },
        },
    },
}

fake_aws_cred_dict = {
    "project1": {
        "site1": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site1",
        },
        "site2": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site2",
        },
    },
    "project2": {
        "site1": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site1",
        },
        "site2": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site2",
        },
    },
    "admin": {
        "aws_access_key_id": "",
        "aws_secret_access_key": "",
        "username": "admin",
    },
}


class MockResponse:
    def __init__(self, status_code, json_data=None, ok=True):
        self.status_code = status_code
        self.json_data = json_data
        self.ok = ok

    def json(self):
        return self.json_data


class test_s3_onyx_updates(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://s3.climb.ac.uk"
        os.environ["ONYX_DOMAIN"] = "testing"
        os.environ["ONYX_TOKEN"] = "testing"
        os.environ["FAKE_VARYS_CFG_PATH"] = FAKE_VARYS_CFG_PATH
        os.environ["FAKE_ROZ_CFG_PATH"] = FAKE_ROZ_CFG_PATH

        with open(FAKE_VARYS_CFG_PATH, "w") as f:
            json.dump(fake_aws_cred_dict, f)

        with open(FAKE_ROZ_CFG_PATH, "w") as f:
            json.dump(fake_roz_cfg_dict, f)

        with open(FAKE_AWS_CREDS, "w") as f:
            json.dump(fake_aws_cred_dict, f)

        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()

        self.s3_client = boto3.client("s3", endpoint_url="https://s3.climb.ac.uk")

        self.s3_client.create_bucket(
            Bucket="project1-site1-illumina-prod",
        )
        self.s3_client.create_bucket(
            Bucket="project2-site1-illumina-prod",
        )
        self.s3_client.create_bucket(
            Bucket="project2-site1-results",
        )

        self.logger = init_logger(
            "test_s3_onyx_updates", S3_ONYX_UPDATE_LOG_FILENAME, "DEBUG"
        )

    def tearDown(self):
        self.mock_s3.stop()
        self.s3_client.close()

    def test_successful_csv_update(self):
        csv_record = {
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
                            "name": "project2-site1-illumina-prod",
                            "ownerIdentity": {"principalId": "bryn-site1"},
                            "arn": "arn:aws:s3:::project2-site1-illumina-prod",
                            "id": "testdata",
                        },
                        "object": {
                            "key": "project2.sample1.run1.csv",
                            "size": 123123123,
                            "eTag": "",
                            "versionId": "",
                            "sequencer": "",
                            "metadata": [
                                {
                                    "key": "",
                                    "val": "",
                                },
                                {"key": "", "val": ""},
                            ],
                            "tags": [],
                        },
                    },
                    "eventId": "",
                    "opaqueData": "",
                }
            ]
        }

        self.s3_client.put_object(
            Bucket="project2-site1-illumina-prod",
            Key="project2.sample1.run1.csv",
            Body="run_index,run_id,some_field\nsample1,run1,some_entry\n",
        )

        resp = self.s3_client.head_object(
            Bucket="project2-site1-illumina-prod",
            Key="project2.sample1.run1.csv",
        )

        csv_record["Records"][0]["s3"]["object"]["eTag"] = resp["ETag"].replace('"', "")

        with unittest.mock.patch(
            "roz_scripts.utils.utils.OnyxClient"
        ) as mock_client, unittest.mock.patch(
            "roz_scripts.general.s3_onyx_updates.OnyxClient"
        ) as mock_client_2:
            mock_client_2.return_value.__enter__.return_value.identify.return_value = {
                "identifier": "fake_identifier"
            }
            mock_client_2.return_value.__enter__.return_value.filter.return_value = (
                iter(([{"climb_id": "fake_climb_id", "is_published": True}]))
            )
            mock_client.return_value.__enter__.return_value.update.return_value = {
                "status": "success"
            }

            update_success, payload = csv_update(
                parsed_message=csv_record,
                config_dict=fake_roz_cfg_dict,
                log=self.logger,
            )

            print(payload)

            self.assertTrue(update_success)
            self.assertTrue(payload)
            self.assertEqual(payload["run_index"], "sample1")
            self.assertEqual(payload["run_id"], "run1")
            self.assertEqual(payload["project"], "project2")
            self.assertNotIn("files", payload.keys())
            self.assertEqual(payload["update_status"], "success")

    def test_onyx_update_failure(self):
        csv_record = {
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
                            "name": "project2-site1-illumina-prod",
                            "ownerIdentity": {"principalId": "bryn-site1"},
                            "arn": "arn:aws:s3:::project2-site1-illumina-prod",
                            "id": "testdata",
                        },
                        "object": {
                            "key": "project2.sample1.run1.csv",
                            "size": 123123123,
                            "eTag": "",
                            "versionId": "",
                            "sequencer": "",
                            "metadata": [
                                {
                                    "key": "",
                                    "val": "",
                                },
                                {"key": "", "val": ""},
                            ],
                            "tags": [],
                        },
                    },
                    "eventId": "",
                    "opaqueData": "",
                }
            ]
        }

        self.s3_client.put_object(
            Bucket="project2-site1-illumina-prod",
            Key="project2.sample1.run1.csv",
            Body="run_index,run_id,some_field\nsample1,run1,some_entry\n",
        )

        resp = self.s3_client.head_object(
            Bucket="project2-site1-illumina-prod",
            Key="project2.sample1.run1.csv",
        )

        csv_record["Records"][0]["s3"]["object"]["eTag"] = resp["ETag"].replace('"', "")

        with unittest.mock.patch(
            "roz_scripts.utils.utils.OnyxClient"
        ) as mock_client, unittest.mock.patch(
            "roz_scripts.general.s3_onyx_updates.OnyxClient"
        ) as mock_client_2:
            mock_client_2.return_value.__enter__.return_value.identify.return_value = {
                "identifier": "fake_identifier"
            }
            mock_client_2.return_value.__enter__.return_value.filter.return_value = (
                iter(([{"climb_id": "fake_climb_id", "is_published": True}]))
            )
            mock_client.return_value.__enter__.return_value.update = unittest.mock.Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create exception",
                    response=MockResponse(
                        status_code=400,
                        json_data={
                            "data": [],
                            "messages": {
                                "run_index": ["Test run_index error handling"]
                            },
                        },
                    ),
                )
            )

            update_success, payload = csv_update(
                parsed_message=csv_record,
                config_dict=fake_roz_cfg_dict,
                log=self.logger,
            )

            print(payload)

            self.assertTrue(update_success)
            self.assertTrue(payload)
            self.assertEqual(payload["run_index"], "sample1")
            self.assertEqual(payload["run_id"], "run1")
            self.assertEqual(payload["project"], "project2")
            self.assertNotIn("files", payload.keys())
            self.assertNotIn("uuid", payload.keys())
            self.assertNotIn("artifact", payload.keys())
            self.assertEqual(payload["update_status"], "failed")

    def test_artifact_unpublished(self):
        csv_record = {
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
                            "name": "project2-site1-illumina-prod",
                            "ownerIdentity": {"principalId": "bryn-site1"},
                            "arn": "arn:aws:s3:::project2-site1-illumina-prod",
                            "id": "testdata",
                        },
                        "object": {
                            "key": "project2.sample1.run1.csv",
                            "size": 123123123,
                            "eTag": "",
                            "versionId": "",
                            "sequencer": "",
                            "metadata": [
                                {
                                    "key": "",
                                    "val": "",
                                },
                                {"key": "", "val": ""},
                            ],
                            "tags": [],
                        },
                    },
                    "eventId": "",
                    "opaqueData": "",
                }
            ]
        }

        self.s3_client.put_object(
            Bucket="project2-site1-illumina-prod",
            Key="project2.sample1.run1.csv",
            Body="run_index,run_id,some_field\nsample1,run1,some_entry\n",
        )

        resp = self.s3_client.head_object(
            Bucket="project2-site1-illumina-prod",
            Key="project2.sample1.run1.csv",
        )

        csv_record["Records"][0]["s3"]["object"]["eTag"] = resp["ETag"].replace('"', "")

        with unittest.mock.patch(
            "roz_scripts.utils.utils.OnyxClient"
        ) as mock_client, unittest.mock.patch(
            "roz_scripts.general.s3_onyx_updates.OnyxClient"
        ) as mock_client_2:
            mock_client_2.return_value.__enter__.return_value.identify.return_value = {
                "identifier": "fake_identifier"
            }
            mock_client_2.return_value.__enter__.return_value.filter.return_value = (
                iter(([{"climb_id": "fake_climb_id", "is_published": False}]))
            )
            mock_client.return_value.__enter__.return_value.update = unittest.mock.Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create exception",
                    response=MockResponse(
                        status_code=400,
                        json_data={
                            "data": [],
                            "messages": {
                                "run_index": ["Test run_index error handling"]
                            },
                        },
                    ),
                )
            )

            update_success, payload = csv_update(
                parsed_message=csv_record,
                config_dict=fake_roz_cfg_dict,
                log=self.logger,
            )

            print(payload)

            self.assertTrue(update_success)
            self.assertFalse(payload)

    def test_csv_mismatch(self):
        csv_record = {
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
                            "name": "project2-site1-illumina-prod",
                            "ownerIdentity": {"principalId": "bryn-site1"},
                            "arn": "arn:aws:s3:::project2-site1-illumina-prod",
                            "id": "testdata",
                        },
                        "object": {
                            "key": "project2.sample1.run1.csv",
                            "size": 123123123,
                            "eTag": "",
                            "versionId": "",
                            "sequencer": "",
                            "metadata": [
                                {
                                    "key": "",
                                    "val": "",
                                },
                                {"key": "", "val": ""},
                            ],
                            "tags": [],
                        },
                    },
                    "eventId": "",
                    "opaqueData": "",
                }
            ]
        }

        self.s3_client.put_object(
            Bucket="project2-site1-illumina-prod",
            Key="project2.sample1.run1.csv",
            Body="run_index,run_id,some_field\nsample2,run21,some_entry\n",
        )

        resp = self.s3_client.head_object(
            Bucket="project2-site1-illumina-prod",
            Key="project2.sample1.run1.csv",
        )

        csv_record["Records"][0]["s3"]["object"]["eTag"] = resp["ETag"].replace('"', "")

        with unittest.mock.patch(
            "roz_scripts.utils.utils.OnyxClient"
        ) as mock_client, unittest.mock.patch(
            "roz_scripts.general.s3_onyx_updates.OnyxClient"
        ) as mock_client_2:
            mock_client_2.return_value.__enter__.return_value.identify.return_value = {
                "identifier": "fake_identifier"
            }
            mock_client_2.return_value.__enter__.return_value.filter.return_value = (
                iter(([{"climb_id": "fake_climb_id", "is_published": True}]))
            )
            mock_client.return_value.__enter__.return_value.update.return_value = {
                "status": "success"
            }

            update_success, payload = csv_update(
                parsed_message=csv_record,
                config_dict=fake_roz_cfg_dict,
                log=self.logger,
            )

            print(payload)

            self.assertTrue(update_success)
            self.assertTrue(payload)

    def test_non_csv(self):
        non_csv_record = {
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
                            "name": "project2-site1-illumina-prod",
                            "ownerIdentity": {"principalId": "bryn-site1"},
                            "arn": "arn:aws:s3:::project2-site1-illumina-prod",
                            "id": "testdata",
                        },
                        "object": {
                            "key": "project2.sample1.run1.2.fastq.gz",
                            "size": 123123123,
                            "eTag": "179d94f8cd22896c2a80a9a7c98463d2-21",
                            "versionId": "",
                            "sequencer": "testdata",
                            "metadata": [
                                {
                                    "key": "x-amz-content-sha256",
                                    "val": "UNSIGNED-PAYLOAD",
                                },
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

        update_success, payload = csv_update(
            parsed_message=non_csv_record,
            config_dict=fake_roz_cfg_dict,
            log=self.logger,
        )

        print(payload)

        self.assertTrue(update_success)
        self.assertFalse(payload)

    def test_no_update_project(self):
        non_csv_record = {
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
                            "name": "project1-site1-illumina-prod",
                            "ownerIdentity": {"principalId": "bryn-site1"},
                            "arn": "arn:aws:s3:::project1-site1-illumina-prod",
                            "id": "testdata",
                        },
                        "object": {
                            "key": "project1.sample1.run1.csv",
                            "size": 123123123,
                            "eTag": "179d94f8cd22896c2a80a9a7c98463d2-21",
                            "versionId": "",
                            "sequencer": "testdata",
                            "metadata": [
                                {
                                    "key": "x-amz-content-sha256",
                                    "val": "UNSIGNED-PAYLOAD",
                                },
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

        update_success, payload = csv_update(
            parsed_message=non_csv_record,
            config_dict=fake_roz_cfg_dict,
            log=self.logger,
        )

        print(payload)

        self.assertTrue(update_success)
        self.assertFalse(payload)
