import moto
from moto.core import set_initial_no_auth_action_count
from roz_scripts import s3_controller
import os
import boto3
from botocore.exceptions import ClientError
import json
from unittest.mock import patch, Mock
from types import SimpleNamespace

import unittest

DIR = os.path.dirname(__file__)

FAKE_VARYS_CFG_PATH = os.path.join(DIR, "fake_varys_cfg.json")
FAKE_ROZ_CFG_PATH = os.path.join(DIR, "fake_roz_cfg.json")
FAKE_AWS_CREDS = os.path.join(DIR, "fake_aws_creds.json")

fake_roz_cfg_dict = {
    "version": "1",
    "pathogen_configs": ["project1", "project2"],
    "configs": {
        "project1": {
            "artifact_layout": "project.run_index.run_id",
            "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
            "sites": {
                "site1.project1": "analysis",
                "subsite1.site2.project1": "uploader",
                "subsite2.site2.project1": "uploader",
            },
            "bucket_policies": {
                "site_ingest": ["get", "put", "list", "delete"],
                "site_read": ["get", "list"],
                "project_read": ["get", "list"],
                "project_private": [],
            },
            "site_buckets": {
                "ingest": {
                    "name_layout": "{project}-{site}-{platform}-{test_flag}",
                    "policy": {
                        "analysis": "site_ingest",
                        "uploader": "site_ingest",
                    },
                }
            },
            "notification_bucket_configs": {
                "ingest": {
                    "rmq_exchange": "inbound-s3",
                    "rmq_queue_env": "s3_matcher",
                    "amqps": False,
                }
            },
            "project_buckets": {
                "fake_files": {
                    "name_layout": "{project}-fake-files",
                    "policy": {"analysis": "project_read", "uploader": "project_read"},
                },
                "fake_files_2": {
                    "name_layout": "{project}-fake-files-2",
                    "policy": {},
                },
            },
            "file_specs": {
                "illumina": {
                    ".1.fastq.gz": {
                        "sections": 6,
                        "layout": "project.run_index.run_id.direction.ftype.gzip",
                    },
                    ".2.fastq.gz": {
                        "sections": 6,
                        "layout": "project.run_index.run_id.direction.ftype.gzip",
                    },
                    ".csv": {
                        "sections": 4,
                        "layout": "project.run_index.run_id.ftype",
                    },
                    "match_size": 3,
                    "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
                },
                "ont": {
                    ".fastq.gz": {
                        "sections": 5,
                        "layout": "project.run_index.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "sections": 4,
                        "layout": "project.run_index.run_id.ftype",
                    },
                    "match_size": 2,
                    "files": [".ont.fastq.gz", ".csv"],
                },
            },
        },
        "project2": {
            "artifact_layout": "project.run_index.run_id",
            "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
            "sites": {
                "subsite1.site1.project2": "analysis",
                "subsite2.site1.project2": "analysis",
                "site2.project2": "analysis",
            },
            "bucket_policies": {
                "site_ingest": ["get", "put", "list", "delete"],
                "site_read": ["get", "list"],
                "project_read": ["get", "list"],
                "project_private": [],
            },
            "site_buckets": {
                "ingest": {
                    "name_layout": "{project}-{site}-{platform}-{test_flag}",
                    "policy": {
                        "analysis": "site_ingest",
                        "uploader": "site_ingest",
                    },
                }
            },
            "notification_bucket_configs": {
                "ingest": {
                    "rmq_exchange": "inbound-s3",
                    "rmq_queue_env": "s3_matcher",
                    "amqps": False,
                }
            },
            "project_buckets": {
                "fake_files": {
                    "name_layout": "{project}-fake-files",
                    "policy": {
                        "analysis": "project_read",
                        "uploader": "project_read",
                    },
                },
                "fake_files_2": {
                    "name_layout": "{project}-fake-files-2",
                    "policy": {},
                },
            },
            "file_specs": {
                "illumina": {
                    ".1.fastq.gz": {
                        "sections": 6,
                        "layout": "project.run_index.run_id.direction.ftype.gzip",
                    },
                    ".2.fastq.gz": {
                        "sections": 6,
                        "layout": "project.run_index.run_id.direction.ftype.gzip",
                    },
                    ".csv": {
                        "sections": 4,
                        "layout": "project.run_index.run_id.ftype",
                    },
                    "match_size": 3,
                    "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
                },
                "ont": {
                    ".fastq.gz": {
                        "sections": 5,
                        "layout": "project.run_index.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "sections": 4,
                        "layout": "project.run_index.run_id.ftype",
                    },
                    "match_size": 2,
                    "files": [".fastq.gz", ".csv"],
                },
                "pacbio": {
                    ".fastq.gz": {
                        "sections": 5,
                        "layout": "project.run_index.run_id.ftype.gzip",
                    },
                    ".csv": {
                        "sections": 4,
                        "layout": "project.run_index.run_id.ftype",
                    },
                    "match_size": 2,
                    "files": [".fastq.gz", ".csv"],
                },
            },
        },
    },
}

fake_aws_cred_dict = {
    "project1": {
        "site1.project1": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site1.project1",
        },
        "subsite1.site2.project1": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site2.subsite1.project1",
        },
        "subsite2.site2.project1": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site2.subsite2.project1",
        },
    },
    "project2": {
        "subsite1.site1.project2": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site1.subsite1.project2",
        },
        "subsite2.site1.project2": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site2.subsite2.project2",
        },
        "site2.project2": {
            "aws_access_key_id": "",
            "aws_secret_access_key": "",
            "username": "bryn-site2.project2",
        },
    },
    "project3": {
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


class mock_response:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self.data = data
        self.text = json.dumps(data)
        self.url = "nonsense"
        self.request = SimpleNamespace(body="nonsense")

    def json(self):
        return self.data


class TestS3Controller(unittest.TestCase):
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

        self.mock_iam = moto.mock_iam()
        self.mock_iam.start()

        self.mock_sns = moto.mock_sns()
        self.mock_sns.start()

        self.s3_client = boto3.client("s3", endpoint_url="https://s3.climb.ac.uk")
        self.iam_client = boto3.client("iam")

        self.iam_client.create_user(UserName="bryn-site1.project1")

        resp = self.iam_client.create_access_key(UserName="bryn-site1.project1")

        fake_aws_cred_dict["project1"]["site1.project1"]["aws_access_key_id"] = resp[
            "AccessKey"
        ]["AccessKeyId"]

        fake_aws_cred_dict["project1"]["site1.project1"]["aws_secret_access_key"] = (
            resp["AccessKey"]["SecretAccessKey"]
        )

    def TearDown(self):
        self.mock_s3.stop()
        self.mock_iam.stop()

        self.s3_client.close()
        self.iam_client.close()
        self.mock_sns.stop()

    def test_project_bucket_exists(self):
        self.s3_client.create_bucket(Bucket="fake_bucket")

        bucket_exists = s3_controller.check_project_bucket_exists(
            "fake_bucket", fake_aws_cred_dict, "project1", "site1.project1"
        )

        self.assertTrue(bucket_exists)

        bucket_does_not_exist = s3_controller.check_project_bucket_exists(
            "other_fake_bucket",
            fake_aws_cred_dict,
            "project1",
            "site1.project1",
        )

        self.assertFalse(bucket_does_not_exist)

    def test_site_bucket_exists(self):
        with patch("roz_scripts.general.s3_controller.requests") as mock_requests:
            mock_requests.get = Mock(
                side_effect=[
                    mock_response(
                        200,
                        {
                            "Buckets": [
                                {"Name": "fake_bucket"},
                            ]
                        },
                    ),
                    mock_response(404, {}),
                ]
            )

            bucket_exists = s3_controller.check_site_bucket_exists(
                "fake_bucket", "site1.project1"
            )

            self.assertTrue(bucket_exists)

            bucket_does_not_exist = s3_controller.check_site_bucket_exists(
                "other_fake_bucket", "site1.project1"
            )

            self.assertFalse(bucket_does_not_exist)

    def test_create_site_bucket(self):
        with patch("roz_scripts.general.s3_controller.requests") as mock_requests:
            mock_requests.post = Mock(
                side_effect=[
                    mock_response(201, {}),
                    mock_response(404, {}),
                ]
            )

            create_success = s3_controller.create_site_bucket(
                "fake_bucket", "site1.project1", {}
            )

            self.assertTrue(create_success)

            with self.assertRaises(SystemExit) as caught_exception:
                s3_controller.create_site_bucket("fake_bucket", "site1.project1", {})
                self.assertEqual(caught_exception.exception.code, 404)

    def test_create_project_bucket(self):
        s3_controller.create_project_bucket(
            "fake_bucket", "project1", "subsite1.site2.project1", fake_aws_cred_dict
        )

        bucket_exists = s3_controller.check_project_bucket_exists(
            "fake_bucket", fake_aws_cred_dict, "project1", "subsite1.site2.project1"
        )

        self.assertTrue(bucket_exists)

    @set_initial_no_auth_action_count(3)
    def test_can_site_list_objects(self):
        self.s3_client.create_bucket(
            Bucket="fake_bucket",
        )

        self.s3_client.put_object(Bucket="fake_bucket", Key="fake_key")

        self.assertTrue(
            s3_controller.can_site_list_objects(
                "fake_bucket",
                fake_aws_cred_dict,
                "project1",
                "subsite1.site2.project1",
            )
        )

        self.assertFalse(
            s3_controller.can_site_list_objects(
                "fake_bucket", fake_aws_cred_dict, "project1", "subsite1.site2.project1"
            )
        )

    @set_initial_no_auth_action_count(2)
    def test_can_site_put_objects(self):
        self.s3_client.create_bucket(
            Bucket="fake_bucket",
        )

        self.assertTrue(
            s3_controller.can_site_put_object(
                "fake_bucket",
                fake_aws_cred_dict,
                "project1",
                "subsite1.site2.project1",
            )
        )

        self.assertFalse(
            s3_controller.can_site_put_object(
                "fake_bucket", fake_aws_cred_dict, "project1", "subsite1.site2.project1"
            )
        )

    def test_create_config_map(self):
        config_map = s3_controller.create_config_map(fake_roz_cfg_dict)
        print(config_map)

        for project, project_config in config_map.items():
            self.assertTrue(
                set(project_config["sites"].keys())
                == set(fake_roz_cfg_dict["configs"][project]["sites"])
            )
            self.assertTrue(
                len(project_config["project_buckets"])
                == len(fake_roz_cfg_dict["configs"][project]["project_buckets"])
            )

            for bucket, bucket_arn in project_config["project_buckets"]:
                splits = bucket_arn.split("-", 1)
                self.assertTrue(splits[0] == project)

            for site, site_config in project_config["sites"].items():
                # Probably aught to be more general
                self.assertTrue(
                    len(site_config["site_buckets"])
                    == (len(fake_roz_cfg_dict["configs"][project]["file_specs"]) * 2)
                )
                for bucket, bucket_arn in site_config["site_buckets"]:
                    splits = bucket_arn.split("-")
                    self.assertTrue(splits[0] == project)
                    self.assertTrue(splits[1] == site)
                    self.assertTrue(
                        splits[2] in fake_roz_cfg_dict["configs"][project]["file_specs"]
                    )
                    self.assertIn(
                        splits[3],
                        ("prod", "test"),
                    )

    def test_check_bucket_exists_and_create(self):
        config_map = s3_controller.create_config_map(fake_roz_cfg_dict)

        with patch("roz_scripts.general.s3_controller.requests") as mock_requests:
            mock_requests.post.return_value = mock_response(201, {})
            mock_requests.get.return_value = mock_response(404, {})

            s3_controller.check_bucket_exist_and_create(
                fake_aws_cred_dict, config_map, fake_roz_cfg_dict
            )

            mock_requests.get.return_value = mock_response(200, {})

            # Create the buckets that would be created by bryn
            for project, project_config in config_map.items():
                for site, site_config in project_config["sites"].items():
                    for bucket, bucket_arn in site_config["site_buckets"]:
                        self.s3_client.create_bucket(Bucket=bucket_arn)

            for project, project_config in config_map.items():
                for bucket, bucket_arn in project_config["project_buckets"]:
                    self.assertTrue(
                        s3_controller.check_project_bucket_exists(
                            bucket_arn, fake_aws_cred_dict, project, "admin"
                        )
                    )

                for site, site_config in project_config["sites"].items():
                    for bucket, bucket_arn in site_config["site_buckets"]:
                        self.assertTrue(
                            s3_controller.check_site_bucket_exists(bucket_arn, site)
                        )

    def test_bucket_audit(self):
        config_map = s3_controller.create_config_map(fake_roz_cfg_dict)

        with patch("roz_scripts.general.s3_controller.requests") as mock_requests:
            mock_requests.post.return_value = mock_response(201, {})
            mock_requests.get.return_value = mock_response(404, {})

            s3_controller.check_bucket_exist_and_create(
                fake_aws_cred_dict, config_map, fake_roz_cfg_dict
            )

            # Create the buckets that would be created by bryn
            for project, project_config in config_map.items():
                for site, site_config in project_config["sites"].items():
                    for bucket, bucket_arn in site_config["site_buckets"]:
                        self.s3_client.create_bucket(Bucket=bucket_arn)

            audit = s3_controller.audit_all_buckets(fake_aws_cred_dict, config_map)

            for project, project_config in config_map.items():
                for bucket, bucket_arn in project_config["project_buckets"]:
                    self.assertTrue(
                        audit[project]["project_buckets"][(bucket, bucket_arn)]
                    )

                for site, site_config in project_config["sites"].items():
                    for bucket, bucket_arn in site_config["site_buckets"]:
                        self.assertTrue(
                            audit[project]["site_buckets"][site][(bucket, bucket_arn)]
                        )

    def test_test_policies(self):
        config_map = s3_controller.create_config_map(fake_roz_cfg_dict)

        with patch("roz_scripts.general.s3_controller.requests") as mock_requests:
            mock_requests.post.return_value = mock_response(201, {})
            mock_requests.get.return_value = mock_response(404, {})

            s3_controller.check_bucket_exist_and_create(
                fake_aws_cred_dict, config_map, fake_roz_cfg_dict
            )

            # Create the buckets that would be created by bryn
            for project, project_config in config_map.items():
                for site, site_config in project_config["sites"].items():
                    for bucket, bucket_arn in site_config["site_buckets"]:
                        self.s3_client.create_bucket(Bucket=bucket_arn)

            mock_requests.get.return_value = mock_response(200, {})

            audit = s3_controller.audit_all_buckets(fake_aws_cred_dict, config_map)

            policy_results = s3_controller.test_policies(audit, fake_roz_cfg_dict)

            for project, project_config in config_map.items():
                for bucket, bucket_arn in project_config["project_buckets"]:
                    self.assertIn(
                        (bucket, bucket_arn, project),
                        policy_results["project_buckets"],
                    )

                for site, site_config in project_config["sites"].items():
                    for bucket, bucket_arn in site_config["site_buckets"]:
                        self.assertIn(
                            (bucket, bucket_arn, project, site),
                            policy_results["site_buckets"],
                        )

    def test_resolve_credentials(self):
        self.assertEqual(
            s3_controller.resolve_credentials(fake_aws_cred_dict, "project1", "admin"),
            fake_aws_cred_dict["admin"],
        )
        self.assertEqual(
            s3_controller.resolve_credentials(fake_aws_cred_dict, "admin", "site1"),
            fake_aws_cred_dict["admin"],
        )
        self.assertEqual(
            s3_controller.resolve_credentials(
                fake_aws_cred_dict, "project1", "site1.project1"
            ),
            fake_aws_cred_dict["project1"]["site1.project1"],
        )

    def test_setup_sns_topic_amqp_protocol(self):
        # setup_sns_topic must honour the `amqps` flag when building the AMQP(S)
        # push-endpoint URL, rather than assuming one protocol.
        with patch("roz_scripts.general.s3_controller.boto3.client") as mock_boto_client:
            mock_sns = Mock()
            mock_sns.create_topic.return_value = {"TopicArn": "arn:aws:sns:s3::plain"}
            mock_boto_client.return_value = mock_sns

            topic_arn = s3_controller.setup_sns_topic(
                aws_credentials_dict=fake_aws_cred_dict,
                topic_name="plain",
                amqp_host="rabbitmq.internal",
                amqp_user="guest",
                amqp_pass="guest",
                amqp_exchange="test-exchange",
                amqps=False,
            )

            self.assertEqual(topic_arn, "arn:aws:sns:s3::plain")
            _, kwargs = mock_sns.create_topic.call_args
            self.assertEqual(
                kwargs["Attributes"]["push-endpoint"],
                "amqp://guest:guest@rabbitmq.internal:5672",
            )

        with patch("roz_scripts.general.s3_controller.boto3.client") as mock_boto_client:
            mock_sns = Mock()
            mock_sns.create_topic.return_value = {"TopicArn": "arn:aws:sns:s3::tls"}
            mock_boto_client.return_value = mock_sns

            s3_controller.setup_sns_topic(
                aws_credentials_dict=fake_aws_cred_dict,
                topic_name="tls",
                amqp_host="rabbitmq.internal",
                amqp_user="guest",
                amqp_pass="guest",
                amqp_exchange="test-exchange",
                amqps=True,
            )

            _, kwargs = mock_sns.create_topic.call_args
            self.assertEqual(
                kwargs["Attributes"]["push-endpoint"],
                "amqps://guest:guest@rabbitmq.internal:5671",
            )

    def test_setup_sns_topic_secure_and_durable_defaults(self):
        # persistent=true decouples the S3 op from AMQP delivery/ack (only the
        # write to RGW's local persistent queue is synchronous). verify-ssl
        # can only safely be "true" once a CA location is configured for RGW
        # to validate the rmq server's certificate against (the rmq server
        # uses a self-signed cert, which RGW has no other way to trust) -
        # without one, "true" would just break delivery outright.
        with patch("roz_scripts.general.s3_controller.boto3.client") as mock_boto_client:
            mock_sns = Mock()
            mock_sns.create_topic.return_value = {"TopicArn": "arn:aws:sns:s3::topic"}
            mock_boto_client.return_value = mock_sns

            s3_controller.setup_sns_topic(
                aws_credentials_dict=fake_aws_cred_dict,
                topic_name="topic",
                amqp_host="rabbitmq.internal",
                amqp_user="guest",
                amqp_pass="guest",
                amqp_exchange="test-exchange",
                amqps=True,
            )

            _, kwargs = mock_sns.create_topic.call_args
            self.assertEqual(kwargs["Attributes"]["persistent"], "true")
            self.assertEqual(kwargs["Attributes"]["verify-ssl"], "false")
            self.assertNotIn("ca-location", kwargs["Attributes"])

        with patch("roz_scripts.general.s3_controller.boto3.client") as mock_boto_client:
            mock_sns = Mock()
            mock_sns.create_topic.return_value = {"TopicArn": "arn:aws:sns:s3::topic-ca"}
            mock_boto_client.return_value = mock_sns

            s3_controller.setup_sns_topic(
                aws_credentials_dict=fake_aws_cred_dict,
                topic_name="topic-ca",
                amqp_host="rabbitmq.internal",
                amqp_user="guest",
                amqp_pass="guest",
                amqp_exchange="test-exchange",
                amqps=True,
                amqp_ca_location="/etc/ceph/rmq-ca.pem",
            )

            _, kwargs = mock_sns.create_topic.call_args
            self.assertEqual(kwargs["Attributes"]["verify-ssl"], "true")
            self.assertEqual(
                kwargs["Attributes"]["ca-location"], "/etc/ceph/rmq-ca.pem"
            )

    def test_setup_messaging_resolves_admin_credentials(self):
        # Regression test: setup_messaging used to look up
        # aws_credentials_dict[project][site] unconditionally, which raised a
        # KeyError for site="admin" since admin credentials are a top-level key,
        # not nested under each project. This is why messaging setup for
        # project/admin-owned buckets always crashed.
        with patch("roz_scripts.general.s3_controller.boto3.client") as mock_boto_client:
            mock_s3 = Mock()
            mock_s3.put_bucket_notification_configuration.return_value = {
                "ResponseMetadata": {"HTTPStatusCode": 200}
            }
            mock_boto_client.return_value = mock_s3

            success = s3_controller.setup_messaging(
                aws_credentials_dict=fake_aws_cred_dict,
                bucket_name="fake-bucket",
                site="admin",
                project="project1",
                topic_arn="arn:aws:sns:s3::test-topic",
                amqp_topic="test-topic",
            )

            self.assertTrue(success)

            _, kwargs = mock_boto_client.call_args
            self.assertEqual(
                kwargs["aws_access_key_id"],
                fake_aws_cred_dict["admin"]["aws_access_key_id"],
            )

    def test_test_bucket_messaging(self):
        with patch("roz_scripts.general.s3_controller.boto3.client") as mock_boto_client:
            mock_s3 = Mock()
            mock_s3.get_bucket_notification_configuration.return_value = {
                "ResponseMetadata": {"HTTPStatusCode": 200},
                "TopicConfigurations": [{"Id": "test-topic"}],
            }
            mock_boto_client.return_value = mock_s3

            self.assertTrue(
                s3_controller.test_bucket_messaging(
                    aws_credentials_dict=fake_aws_cred_dict,
                    bucket_name="fake-bucket",
                    correct_topic="test-topic",
                    project="project1",
                    site="admin",
                )
            )

            self.assertFalse(
                s3_controller.test_bucket_messaging(
                    aws_credentials_dict=fake_aws_cred_dict,
                    bucket_name="fake-bucket",
                    correct_topic="wrong-topic",
                    project="project1",
                    site="admin",
                )
            )

    def test_audit_bucket_messaging_only_flags_notification_buckets(self):
        config_map = s3_controller.create_config_map(fake_roz_cfg_dict)

        with patch("roz_scripts.general.s3_controller.boto3.client") as mock_boto_client:
            mock_s3 = Mock()
            # No bucket has any notification configuration set up yet.
            mock_s3.get_bucket_notification_configuration.return_value = {
                "ResponseMetadata": {"HTTPStatusCode": 200}
            }
            mock_boto_client.return_value = mock_s3

            to_fix = s3_controller.audit_bucket_messaging(
                aws_credentials_dict=fake_aws_cred_dict,
                config_map=config_map,
                config_dict=fake_roz_cfg_dict,
            )

        flagged_buckets = {bucket for bucket, _, _, _ in to_fix}

        # Only buckets with a "notification_bucket_configs" entry ("ingest")
        # should ever be flagged - project buckets like "fake_files" have no
        # notification config and must be left alone.
        self.assertEqual(flagged_buckets, {"ingest"})
        self.assertTrue(all(project in ("project1", "project2") for _, _, project, _ in to_fix))
