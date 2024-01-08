import unittest
from roz_scripts import s3_notifications

from varys import varys
import boto3
from moto.server import ThreadedMotoServer
import os
import multiprocessing as mp
import json
import pika

DIR = os.path.dirname(__file__)

VARYS_CFG_PATH = os.path.join(DIR, "varys_cfg.json")
TEST_MESSAGE_LOG_FILENAME = os.path.join(DIR, "test_messages.log")
ROZ_CONFIG_PATH = os.path.join(DIR, "roz_config.json")


class test_s3_notifications_emulation(unittest.TestCase):
    def setUp(self) -> None:
        self.server = ThreadedMotoServer()
        self.server.start()

        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        os.environ["UNIT_TESTING"] = "True"

        os.environ["VARYS_CFG"] = VARYS_CFG_PATH

        config = {
            "version": "0.1",
            "profiles": {
                "roz": {
                    "username": "guest",
                    "password": "guest",
                    "amqp_url": "127.0.0.1",
                    "port": 5672,
                    "use_tls": False,
                }
            },
        }

        roz_config = {
            "version": "1",
            "pathogen_configs": ["project1", "project2"],
            "configs": {
                "project1": {
                    "artifact_layout": "project.sample_name.run_name",
                    "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
                    "sites": ["site1", "site2"],
                    "bucket_policies": {
                        "site_ingest": ["get", "put", "list", "delete"],
                        "site_read": ["get", "list"],
                        "project_read": ["get", "list"],
                        "project_private": [],
                    },
                    "notification_bucket_configs": {
                        "ingest": {
                            "rmq_exchange": "inbound.test",
                            "rmq_queue_env": "s3_matcher",
                            "amqps": False,
                        }
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
                                "sections": 6,
                                "layout": "project.sample_id.run_name.direction.ftype.gzip",
                            },
                            ".2.fastq.gz": {
                                "sections": 6,
                                "layout": "project.sample_id.run_name.direction.ftype.gzip",
                            },
                            ".csv": {
                                "sections": 4,
                                "layout": "project.sample_id.run_name.ftype",
                            },
                            "match_size": 3,
                            "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
                        },
                        "ont": {
                            ".fastq.gz": {
                                "sections": 5,
                                "layout": "project.sample_id.run_name.ftype.gzip",
                            },
                            ".csv": {
                                "sections": 4,
                                "layout": "project.sample_id.run_name.ftype",
                            },
                            "match_size": 2,
                            "files": [".fastq.gz", ".csv"],
                        },
                    },
                },
                "project2": {
                    "artifact_layout": "project.sample_id.run_name",
                    "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
                    "sites": ["site1", "site2"],
                    "bucket_policies": {
                        "site_ingest": ["get", "put", "list", "delete"],
                        "site_read": ["get", "list"],
                        "project_read": ["get", "list"],
                        "project_private": [],
                    },
                    "notification_bucket_configs": {
                        "ingest": {
                            "rmq_exchange": "inbound.test",
                            "rmq_queue_env": "s3_matcher",
                            "amqps": True,
                        }
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
                                "sections": 6,
                                "layout": "project.sample_id.run_name.direction.ftype.gzip",
                            },
                            ".2.fastq.gz": {
                                "sections": 6,
                                "layout": "project.sample_id.run_name.direction.ftype.gzip",
                            },
                            ".csv": {
                                "sections": 4,
                                "layout": "project.sample_id.run_name.ftype",
                            },
                            "match_size": 3,
                            "files": [".1.fastq.gz", ".2.fastq.gz", ".csv"],
                        },
                        "ont": {
                            ".fastq.gz": {
                                "sections": 5,
                                "layout": "project.sample_id.run_name.ftype.gzip",
                            },
                            ".csv": {
                                "sections": 4,
                                "layout": "project.sample_id.run_name.ftype",
                            },
                            "match_size": 2,
                            "files": [".fastq.gz", ".csv"],
                        },
                        "pacbio": {
                            ".fastq.gz": {
                                "sections": 5,
                                "layout": "project.sample_id.run_name.ftype.gzip",
                            },
                            ".csv": {
                                "sections": 4,
                                "layout": "project.sample_id.run_name.ftype",
                            },
                            "match_size": 2,
                            "files": [".fastq.gz", ".csv"],
                        },
                    },
                },
            },
        }

        with open(VARYS_CFG_PATH, "w") as f:
            json.dump(config, f)

        with open(os.path.join(DIR, ROZ_CONFIG_PATH), "w") as f:
            json.dump(roz_config, f)

        self.s3_client = boto3.client("s3", endpoint_url="http://localhost:5000")
        self.varys_client = varys("roz", logfile=TEST_MESSAGE_LOG_FILENAME)

        self.s3_client.create_bucket(Bucket="project1-site1-illumina-prod")
        self.s3_client.create_bucket(Bucket="project1-site2-illumina-prod")

        self.s3_notifications = mp.Process(
            target=s3_notifications.run, args={"sleep_interval": 1}
        )

    def tearDown(self) -> None:
        self.server.stop()

        self.varys_client.close()

        self.s3_notifications.terminate()

        credentials = pika.PlainCredentials("guest", "guest")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", credentials=credentials)
        )
        channel = connection.channel()

        channel.queue_delete(queue="inbound.test")

    def test_s3_notifications(self):
        for i in range(1, 2000):
            self.s3_client.put_object(
                Bucket="project1-site1-illumina-prod",
                Key=f"project1.sample_{i}.run_name.fastq.gz",
                Body=f"{i}",
            )

            self.s3_client.put_object(
                Bucket="project1-site2-illumina-prod",
                Key=f"project1.sample_{i}.run_name.fastq.gz",
                Body=f"{i}",
            )

        messages = []

        timeout = False

        while not timeout:
            message = self.varys_client.receive(
                exchange="inbound.test", queue_suffix="s3_matcher", timeout=1
            )
            if not message:
                timeout = True
                break

            messages.append(
                (
                    message["Records"][0]["s3"]["bucket"]["name"],
                    message["Records"][0]["s3"]["object"]["key"],
                )
            )

        self.assertEqual(len(messages), 4000)
