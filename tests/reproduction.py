from moto import mock_s3
import boto3
import json
import os
from moto.core import set_initial_no_auth_action_count
import unittest


class repro_testcase(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        self.s3_client = boto3.client("s3")

    def TearDown(self):
        self.mock_s3.stop()

        self.s3_client.close()

    @set_initial_no_auth_action_count(3)
    def test_start(self):
        self.s3_client.create_bucket(Bucket="mybucket", ACL="private")
        self.s3_client.list_buckets()
        self.s3_client.list_buckets()

    def test_reproduce(self):
        credentials_dict = {
            "user_1": {
                "aws_access_key_id": "testing",
                "aws_secret_access_key": "testing",
            },
            "user_2": {
                "aws_access_key_id": "testing2",
                "aws_secret_access_key": "testing2",
            },
        }
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "user1"},  # admin username
                    "Action": [
                        "s3:AbortMultipartUpload",
                        "s3:CreateBucket",
                        "s3:DeleteBucketPolicy",
                        "s3:DeleteBucketWebsite",
                        "s3:DeleteBucket",
                        "s3:DeleteObject",
                        "s3:DeleteObjectVersion",
                        "s3:GetBucketAcl",
                        "s3:GetBucketLogging",
                        "s3:GetBucketNotification",
                        "s3:GetBucketPolicy",
                        "s3:GetBucketTagging",
                        "s3:GetBucketVersioning",
                        "s3:GetBucketWebsite",
                        "s3:GetLifecycleConfiguration",
                        "s3:GetObjectAcl",
                        "s3:GetObject",
                        "s3:GetObjectVersionAcl",
                        "s3:GetObjectVersion",
                        "s3:GetObjectVersionTorrent",
                        "s3:ListAllMyBuckets",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListBucket",
                        "s3:ListBucketVersions",
                        "s3:ListMultipartUploadParts",
                        "s3:PutBucketAcl",
                        "s3:PutBucketLogging",
                        "s3:PutBucketNotification",
                        "s3:PutBucketPolicy",
                        "s3:PutBucketRequestPayment",
                        "s3:PutBucketTagging",
                        "s3:PutBucketVersioning",
                        "s3:PutBucketWebsite",
                        "s3:PutLifecycleConfiguration",
                        "s3:PutObjectAcl",
                        "s3:PutObject",
                        "s3:PutObjectVersionAcl",
                        "s3:RestoreObject",
                    ],
                    "Resource": ["arn:aws:s3:::mybucket/*"],
                }
            ],
        }

        self.s3_client.create_bucket(Bucket="mybucket", ACL="private")

        self.s3_client.put_object(Bucket="mybucket", Key="test", Body=b"test")

        self.s3_client.put_bucket_policy(Bucket="mybucket", Policy=json.dumps(policy))

        self.s3_client.delete_object(Bucket="mybucket", Key="test")

        policy = self.s3_client.get_bucket_policy(Bucket="mybucket")
        print(policy)

        self.s3_client.delete_bucket_policy(Bucket="mybucket")

        self.s3_client.put_bucket_policy(Bucket="mybucket", Policy=json.dumps(policy))

        self.s3_client.delete_object(Bucket="mybucket", Key="test")
