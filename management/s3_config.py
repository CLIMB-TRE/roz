import boto3
from botocore.client import Config
import sys
from collections import namedtuple
import configparser
import os
import pathlib


def get_s3_credentials(args):
    __s3_creds = namedtuple(
        "s3_credentials",
        ["access_key", "secret_key", "endpoint", "region", "profile_name"],
    )

    credential_file = configparser.ConfigParser()

    credential_file.read_file(open(os.path.expanduser("~/.aws/credentials"), "rt"))

    profile = "default" if not args.profile else args.profile

    endpoint = "https://s3.climb.ac.uk"

    region = "s3"

    if credential_file:
        access_key = credential_file[profile]["aws_access_key_id"]
        secret_key = credential_file[profile]["aws_secret_access_key"]

    if os.getenv("AWS_ACCESS_KEY_ID"):
        access_key = os.getenv("AWS_ACCESS_KEY_ID")

    if os.getenv("AWS_SECRET_ACCESS_KEY"):
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if args.access_key:
        access_key = args.access_key

    if args.secret_key:
        secret_key = args.secret_key

    if not access_key or not secret_key:
        error = """CLIMB S3 credentials could not be found, please provide valid credentials in one of the following ways:
            - In a correctly formatted config file (~/.aws/credentials)
            - As environmental variables 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY'
            - As a command line argument, see --help for more details
        """
        print(error, file=sys.stderr)
        sys.exit(1)

    s3_credentials = __s3_creds(
        access_key=access_key,
        secret_key=secret_key,
        endpoint=endpoint,
        region=region,
        profile_name=profile,
    )

    return s3_credentials


def setup_messaging(
    s3_credentials,
    bucket_name,
    topic_name,
    amqp_host,
    amqp_user,
    amqp_pass,
    amqp_exchange,
    amqp_topic,
    amqps,
):
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        region_name=s3_credentials.region,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    sns_client = boto3.client(
        "sns",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
        region_name=s3_credentials.region,
        use_ssl=True,
        config=Config(signature_version="s3"),
    )

    amqp_port = 5671 if amqps else 5672

    protocol = "amqps" if amqps else "amqp"

    # to see the list of available "regions" use:
    # radosgw-admin realm zonegroup list

    # this is standard AWS services call, using custom attributes to add AMQP endpoint information to the topic

    if amqp_user and amqp_pass:
        push_endpoint = f"{protocol}://{amqp_user}:{amqp_pass}@{amqp_host}:{amqp_port}"
    else:
        push_endpoint = f"{protocol}://{amqp_host}:{amqp_port}"

    attributes = {
        "push-endpoint": push_endpoint,
        "amqp-exchange": amqp_exchange,
        "amqp-ack-level": "broker",
        "verify-ssl": "false",
    }

    resp = sns_client.create_topic(Name=topic_name, Attributes=attributes)

    topic_arn = resp["TopicArn"]

    topic_conf_list = [
        {
            "TopicArn": topic_arn,
            "Events": [
                "s3:ObjectCreated:*",
            ],
            "Id": amqp_topic,  # Id is mandatory!
        },
    ]

    resp = s3_client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={"TopicConfigurations": topic_conf_list},
    )

    print(resp, file=sys.stdout)


def create_bucket(s3_credentials, bucket_name, bucket_acl):
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    resp = s3_client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)

    print(resp, file=sys.stdout)


def configure_access():
    pass


def upload_files(s3_credentials, bucket_name, files, file_keys):
    if file_keys:
        if len(files) != len(file_keys):
            print("Number of file-keys args must equal the number of files")
            sys.exit(2)

    else:
        file_keys = [str(x.name) for x in files]

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    for f_path, f_key in zip(files, file_keys):
        print(
            s3_client.upload_file(
                f_path,
                bucket_name,
                f_key,
            ),
            file=sys.stdout,
        )


def main():
    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    parser.add_argument("--access-key", help="CLIMB S3 access key to use")
    parser.add_argument("--secret-key", help="CLIMB S3 secret key to use")
    parser.add_argument("--profile", help="CLIMB S3 credential profile")
    parser.add_argument("--bucket-name", required=True)

    create_bucket_parser = subparsers.add_parser("create_bucket")
    create_bucket_parser.add_argument("--bucket-acl", default="private")

    setup_messaging_parser = subparsers.add_parser("setup_messaging")
    setup_messaging_parser.add_argument("--rmq-host", required=True)
    setup_messaging_parser.add_argument("--rmq-topic", required=True)
    setup_messaging_parser.add_argument("--rmq-exchange", required=True)
    setup_messaging_parser.add_argument("--bucket-topic", required=True)
    setup_messaging_parser.add_argument("--amqps", action="store_true", default=True)
    setup_messaging_parser.add_argument("--rmq-user")
    setup_messaging_parser.add_argument("--rmq-pass")

    configure_access_parser = subparsers.add_parser("configure_access")

    upload_files_parser = subparsers.add_parser("upload_files")
    upload_files_parser.add_argument("files", nargs="+", type=pathlib.Path)
    upload_files_parser.add_argument("--file-keys", nargs="+", type=str)

    args = parser.parse_args()

    s3_credentials = get_s3_credentials(args)

    if args.command == "create_bucket":
        create_bucket(s3_credentials, args.bucket_name, args.bucket_acl)

    elif args.command == "setup_messaging":
        setup_messaging(
            s3_credentials=s3_credentials,
            bucket_name=args.bucket_name,
            topic_name=args.bucket_topic,
            amqp_host=args.rmq_host,
            amqp_user=args.rmq_user,
            amqp_pass=args.rmq_pass,
            amqp_exchange=args.rmq_exchange,
            amqp_topic=args.rmq_topic,
            amqps=args.amqps,
        )

    elif args.command == "upload_files":
        upload_files(
            s3_credentials=s3_credentials,
            bucket_name=args.bucket_name,
            files=args.files,
            file_keys=args.file_keys,
        )


if __name__ == "__main__":
    main()
