import moto
import boto3
import unittest
import json
import os

from roz_scripts import s3_matcher

DIR = os.path.dirname(__file__)

FAKE_VARYS_CFG_PATH = os.path.join(DIR, "fake_varys_cfg.json")
FAKE_ROZ_CFG_PATH = os.path.join(DIR, "fake_roz_cfg.json")
FAKE_AWS_CREDS = os.path.join(DIR, "fake_aws_creds.json")

fake_roz_cfg_dict = {
    "version": "1",
    "pathogen_configs": ["project1", "project2"],
    "configs": {
        "project1": {
            "artifact_layout": "project|sample_id|run_id",
            "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
            "sites": ["site1", "site2"],
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
                        "layout": "project.sample_id.run_id.direction.ftype.gzip",
                    },
                    ".2.fastq.gz": {
                        "layout": "project.sample_id.run_id.direction.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.sample_id.run_id.ftype",
                    },
                },
                "ont": {
                    ".fastq.gz": {
                        "layout": "project.sample_id.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.sample_id.run_id.ftype",
                    },
                },
            },
        },
        "project2": {
            "artifact_layout": "project|sample_id|run_id",
            "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
            "sites": ["site1", "site2"],
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
                        "layout": "project.sample_id.run_id.direction.ftype.gzip",
                    },
                    ".2.fastq.gz": {
                        "layout": "project.sample_id.run_id.direction.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.sample_id.run_id.ftype",
                    },
                },
                "ont": {
                    ".fastq.gz": {
                        "layout": "project.sample_id.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.sample_id.run_id.ftype",
                    },
                },
                "pacbio": {
                    ".fastq.gz": {
                        "layout": "project.sample_id.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "layout": "project.sample_id.run_id.ftype",
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


class test_s3_matcher(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://s3.climb.ac.uk"
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

    def tearDown(self):
        self.mock_s3.stop()
        self.s3_client.close()

    def test_get_existing_objects(self):
        self.s3_client.create_bucket(Bucket="project1-site1-illumina-prod")
        self.s3_client.create_bucket(Bucket="project1-site1-ont-prod")
        self.s3_client.create_bucket(Bucket="project1-site1-illumina-test")
        self.s3_client.create_bucket(Bucket="project2-site1-illumina-prod")

        self.s3_client.put_object(
            Bucket="project1-site1-illumina-prod",
            Key="project1.sample1.run1.1.fastq.gz",
            Body="",
        )
        self.s3_client.put_object(
            Bucket="project1-site1-illumina-prod",
            Key="project1.sample1.run1.2.fastq.gz",
            Body="",
        )
        self.s3_client.put_object(
            Bucket="project1-site1-illumina-prod",
            Key="project1.sample1.run1.csv",
            Body="",
        )
        self.s3_client.put_object(
            Bucket="project1-site1-ont-prod",
            Key="project1.sample2.run1.fastq.gz",
            Body="",
        )
        self.s3_client.put_object(
            Bucket="project1-site1-ont-prod", Key="project1.sample3.run1.csv", Body=""
        )
        self.s3_client.put_object(
            Bucket="project1-site1-illumina-test",
            Key="project1.sample1.run1.1.fastq.gz",
            Body="",
        )

        existing_objects = s3_matcher.get_existing_objects(
            s3_client=self.s3_client,
            to_check=[
                "project1-site1-illumina-prod",
                "project1-site1-ont-prod",
                "project1-site1-illumina-test",
            ],
        )

        self.assertTrue(len(existing_objects) == 3)
        self.assertTrue(len(existing_objects["project1-site1-illumina-prod"]) == 3)
        self.assertTrue(len(existing_objects["project1-site1-ont-prod"]) == 2)
        self.assertTrue(len(existing_objects["project1-site1-illumina-test"]) == 1)
        self.assertNotIn("project2-site1-illumina-prod", existing_objects)

    def test_get_existing_objects_no_bucket(self):
        self.s3_client.create_bucket(Bucket="project1-site1-illumina-prod")

        self.s3_client.put_object(
            Bucket="project1-site1-illumina-prod",
            Key="project1.sample1.run1.1.fastq.gz",
            Body="",
        )

        buckets = ["project1-site1-illumina-prod", "project1-site1-illumina-test"]

        existing_objects = s3_matcher.get_existing_objects(
            s3_client=self.s3_client, to_check=buckets
        )

        print(existing_objects)
        self.assertTrue(len(existing_objects) == 1)

    def test_parse_object_key(self):
        key = "project1.sample1.run1.1.fastq.gz"

        extension, parsed_key = s3_matcher.parse_object_key(
            key, fake_roz_cfg_dict, "project1", "illumina"
        )
        self.assertEqual(extension, ".1.fastq.gz")

        self.assertEqual(parsed_key["project"], "project1")
        self.assertEqual(parsed_key["sample_id"], "sample1")
        self.assertEqual(parsed_key["run_id"], "run1")
        self.assertEqual(parsed_key["direction"], "1")
        self.assertEqual(parsed_key["ftype"], "fastq")
        self.assertEqual(parsed_key["gzip"], "gz")

        bad_extension_key = "project1.sample1.run1._2.fastq"

        extension, parsed_key = s3_matcher.parse_object_key(
            bad_extension_key, fake_roz_cfg_dict, "project1", "illumina"
        )
        self.assertFalse(extension)
        self.assertFalse(parsed_key)

        nonsense_key = "foobar"

        extension, parsed_key = s3_matcher.parse_object_key(
            nonsense_key, fake_roz_cfg_dict, "project1", "illumina"
        )
        self.assertFalse(extension)
        self.assertFalse(parsed_key)

    def test_generate_artifact(self):
        artifact_layout = "project|sample_id|run_id"
        parsed_key = {
            "project": "project1",
            "sample_id": "sample1",
            "run_id": "run1",
            "direction": "1",
            "ftype": "fastq",
            "gzip": "gz",
        }

        artifact = s3_matcher.generate_artifact(parsed_key, artifact_layout)

        self.assertEqual(artifact, "project1|sample1|run1")

        parsed_key = {
            "project": "project1",
            "sample_id": "sample1",
            "run_id": "run1",
            "ftype": "csv",
        }

        artifact = s3_matcher.generate_artifact(parsed_key, artifact_layout)

        self.assertEqual(artifact, "project1.sample1.run1")

        parsed_key = {"sample_id": "sample1", "run_id": "run1", "ftype": "csv"}

        artifact = s3_matcher.generate_artifact(parsed_key, artifact_layout)

        self.assertFalse(artifact)

    def test_generate_s3_uri(self):
        bucket = "project1-site1-illumina-prod"
        key = "project1.sample1.run1.csv"

        s3_uri = s3_matcher.gen_s3_uri(bucket, key)
        self.assertEqual(
            s3_uri, "s3://project1-site1-illumina-prod/project1.sample1.run1.csv"
        )

    def test_is_artifact_dict_complete(self):
        index_tuple_1 = (
            "project1.sample1.run1",
            "project1",
            "site1",
            "illumina",
            "prod",
        )
        index_tuple_2 = ("project1.sample1.run1", "project1", "site1", "ont", "prod")

        existing_object_dict = {
            index_tuple_1: {
                "files": {
                    ".1.fastq.gz": {
                        "uri": "s3://project1-site1-illumina-prod/project1.sample1.run1.1.fastq.gz",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.1.fastq.gz",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "direction": "1",
                            "ftype": "fastq",
                            "gzip": "gz",
                        },
                    },
                    ".csv": {
                        "uri": "s3://project1-site1-illumina-prod/project1.sample1.run1.csv",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.csv",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "ftype": "csv",
                        },
                    },
                },
                "objects": {},
            },
            index_tuple_2: {
                "files": {
                    ".fastq.gz": {
                        "uri": "s3://project1-site1-ont-prod/project1.sample1.run1.fastq.gz",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.fastq.gz",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "ftype": "fastq",
                            "gzip": "gz",
                        },
                    },
                    ".csv": {
                        "uri": "s3://project1-site1-ont-prod/project1.sample1.run1.csv",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.csv",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "ftype": "csv",
                        },
                    },
                },
                "objects": {},
            },
        }

        self.assertFalse(
            s3_matcher.is_artifact_dict_complete(
                index_tuple=index_tuple_1,
                existing_object_dict=existing_object_dict,
                config_dict=fake_roz_cfg_dict,
            )
        )
        self.assertTrue(
            s3_matcher.is_artifact_dict_complete(
                index_tuple=index_tuple_2,
                existing_object_dict=existing_object_dict,
                config_dict=fake_roz_cfg_dict,
            )
        )

        existing_object_dict[index_tuple_1]["files"][".2.fastq.gz"] = {
            "uri": "s3://project1-site1-illumina-prod/project1.sample1.run1.2.fastq.gz",
            "etag": " cfade0850023c5552624423beec6c20f-9422",
            "key": "project1.sample1.run1.2.fastq.gz",
            "submitter": "bryn-site1",
            "parsed_fname": {
                "project": "project1",
                "sample_id": "sample1",
                "run_id": "run1",
                "direction": "2",
                "ftype": "fastq",
                "gzip": "gz",
            },
        }

        self.assertTrue(
            s3_matcher.is_artifact_dict_complete(
                index_tuple=index_tuple_1,
                existing_object_dict=existing_object_dict,
                config_dict=fake_roz_cfg_dict,
            )
        )

    def test_parse_existing_objects(self):
        self.s3_client.create_bucket(Bucket="project1-site1-illumina-prod")
        self.s3_client.create_bucket(Bucket="project1-site1-ont-prod")
        self.s3_client.create_bucket(Bucket="project1-site1-illumina-test")
        self.s3_client.create_bucket(Bucket="project2-site1-illumina-prod")

        self.s3_client.put_object(
            Bucket="project1-site1-illumina-prod",
            Key="project1.sample1.run1.1.fastq.gz",
            Body="",
        )
        self.s3_client.put_object(
            Bucket="project1-site1-illumina-prod",
            Key="project1.sample1.run1.2.fastq.gz",
            Body="",
        )
        self.s3_client.put_object(
            Bucket="project1-site1-illumina-prod",
            Key="project1.sample1.run1.csv",
            Body="",
        )
        self.s3_client.put_object(
            Bucket="project1-site1-ont-prod",
            Key="project1.sample2.run1.fastq.gz",
            Body="",
        )
        self.s3_client.put_object(
            Bucket="project1-site1-ont-prod", Key="project1.sample3.run1.csv", Body=""
        )
        self.s3_client.put_object(
            Bucket="project1-site1-illumina-test",
            Key="project1.sample1.run1.1.fastq.gz",
            Body="",
        )

        existing_objects = s3_matcher.get_existing_objects(
            s3_client=self.s3_client,
            to_check=[
                "project1-site1-illumina-prod",
                "project1-site1-ont-prod",
                "project1-site1-illumina-test",
            ],
        )

        parsed_objects = s3_matcher.parse_existing_objects(
            existing_objects, fake_roz_cfg_dict
        )
        self.assertTrue(len(parsed_objects) == 4)

        self.assertTrue(
            s3_matcher.is_artifact_dict_complete(
                index_tuple=(
                    "project1.sample1.run1",
                    "project1",
                    "site1",
                    "illumina",
                    "prod",
                ),
                existing_object_dict=parsed_objects,
                config_dict=fake_roz_cfg_dict,
            )
        )

        self.assertFalse(
            s3_matcher.is_artifact_dict_complete(
                index_tuple=(
                    "project1.sample2.run1",
                    "project1",
                    "site1",
                    "ont",
                    "prod",
                ),
                existing_object_dict=parsed_objects,
                config_dict=fake_roz_cfg_dict,
            )
        )

        self.assertFalse(
            s3_matcher.is_artifact_dict_complete(
                index_tuple=(
                    "project1.sample3.run1",
                    "project1",
                    "site1",
                    "ont",
                    "prod",
                ),
                existing_object_dict=parsed_objects,
                config_dict=fake_roz_cfg_dict,
            )
        )

        self.assertFalse(
            s3_matcher.is_artifact_dict_complete(
                index_tuple=(
                    "project1.sample1.run1",
                    "project1",
                    "site1",
                    "illumina",
                    "test",
                ),
                existing_object_dict=parsed_objects,
                config_dict=fake_roz_cfg_dict,
            )
        )

    def test_generate_payload(self):
        index_tuple = (
            "project1.sample1.run1",
            "project1",
            "site1",
            "illumina",
            "prod",
        )

        existing_object_dict = {
            index_tuple: {
                "files": {
                    ".1.fastq.gz": {
                        "uri": "s3://project1-site1-illumina-prod/project1.sample1.run1.1.fastq.gz",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.1.fastq.gz",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "direction": "1",
                            "ftype": "fastq",
                            "gzip": "gz",
                        },
                    },
                    ".2.fastq.gz": {
                        "uri": "s3://project1-site1-illumina-prod/project1.sample1.run1.2.fastq.gz",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.2.fastq.gz",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "direction": "2",
                            "ftype": "fastq",
                            "gzip": "gz",
                        },
                    },
                    ".csv": {
                        "uri": "s3://project1-site1-illumina-prod/project1.sample1.run1.csv",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.csv",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "ftype": "csv",
                        },
                    },
                },
                "objects": {},
            }
        }

        payload = s3_matcher.generate_payload(
            index_tuple=index_tuple,
            existing_object_dict=existing_object_dict,
        )

        self.assertFalse(payload["test_flag"])
        self.assertEqual(payload["project"], "project1")
        self.assertEqual(payload["site"], "site1")
        self.assertEqual(payload["platform"], "illumina")
        self.assertEqual(payload["run_id"], "run1")
        self.assertEqual(payload["sample_id"], "sample1")
        self.assertEqual(payload["files"], existing_object_dict[index_tuple]["files"])
        self.assertEqual(payload["uploaders"], ["bryn-site1"])

    def test_parse_new_object_message(self):
        index_tuple = (
            "project1|sample1|run1",
            "project1",
            "site1",
            "illumina",
            "prod",
        )

        existing_object_dict = {
            index_tuple: {
                "files": {
                    ".1.fastq.gz": {
                        "uri": "s3://project1-site1-illumina-prod/project1.sample1.run1.1.fastq.gz",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.1.fastq.gz",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "direction": "1",
                            "ftype": "fastq",
                            "gzip": "gz",
                        },
                    },
                    ".csv": {
                        "uri": "s3://project1-site1-illumina-prod/project1.sample1.run1.csv",
                        "etag": " cfade0850023c5552624423beec6c20f-9422",
                        "key": "project1.sample1.run1.csv",
                        "submitter": "bryn-site1",
                        "parsed_fname": {
                            "project": "project1",
                            "sample_id": "sample1",
                            "run_id": "run1",
                            "ftype": "csv",
                        },
                    },
                },
                "objects": {},
            }
        }

        message_1 = {
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
                            "key": "project1.sample1.run1.2.fastq.gz",
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

        message_2 = {
            "Records": [
                {
                    "eventVersion": "2.2",
                    "eventSource": "ceph:s3",
                    "awsRegion": "",
                    "eventTime": "2023-10-10T06:39:35.470367Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "bryn-site1"},
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
                            "ownerIdentity": {"principalId": "admin"},
                            "arn": "arn:aws:s3:::project1-site1-illumina-prod",
                            "id": "testdata",
                        },
                        "object": {
                            "key": "project1.sample1.run2.csv",
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

        message_3 = {
            "Records": [
                {
                    "eventVersion": "2.2",
                    "eventSource": "ceph:s3",
                    "awsRegion": "",
                    "eventTime": "2023-10-10T06:39:35.470367Z",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "bryn-site1"},
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
                            "ownerIdentity": {"principalId": "admin"},
                            "arn": "arn:aws:s3:::project1-site1-illumina-prod",
                            "id": "testdata",
                        },
                        "object": {
                            "key": "project1.sample1.run2.nonsense",
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

        (
            artifact_complete,
            existing_object_dict,
            index_tuple_ret,
        ) = s3_matcher.parse_new_object_message(
            existing_object_dict=existing_object_dict,
            new_object_message=message_1,
            config_dict=fake_roz_cfg_dict,
        )

        self.assertTrue(artifact_complete)
        self.assertEqual(index_tuple, index_tuple_ret)

        index_tuple_2 = (
            "project1|sample1|run2",
            "project1",
            "site1",
            "illumina",
            "prod",
        )

        (
            artifact_complete,
            existing_object_dict,
            index_tuple_ret_2,
        ) = s3_matcher.parse_new_object_message(
            existing_object_dict=existing_object_dict,
            new_object_message=message_2,
            config_dict=fake_roz_cfg_dict,
        )

        self.assertFalse(artifact_complete)
        self.assertEqual(index_tuple_2, index_tuple_ret_2)

        expected_existing_obj_entry = {
            ".csv": {
                "uri": "s3://project1-site1-illumina-prod/project1.sample1.run2.csv",
                "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
                "key": "project1.sample1.run2.csv",
                "submitter": "bryn-site1",
                "parsed_fname": {
                    "project": "project1",
                    "sample_id": "sample1",
                    "run_id": "run2",
                    "ftype": "csv",
                },
            },
        }

        self.assertEqual(
            existing_object_dict[index_tuple_2]["files"], expected_existing_obj_entry
        )

        (
            artifact_complete,
            existing_object_dict,
            index_tuple,
        ) = s3_matcher.parse_new_object_message(
            existing_object_dict=existing_object_dict,
            new_object_message=message_3,
            config_dict=fake_roz_cfg_dict,
        )

        self.assertFalse(artifact_complete)
        self.assertTrue(index_tuple)

        artifact, project, site, platform, test_flag = index_tuple

        self.assertFalse(artifact)
        self.assertTrue(all((project, site, platform, test_flag)))
