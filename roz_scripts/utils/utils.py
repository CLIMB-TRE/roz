import boto3
from collections import namedtuple
import configparser
import os
import sys
from io import StringIO

__s3_creds = namedtuple(
    "s3_credentials",
    ["access_key", "secret_key", "endpoint", "region", "profile_name"],
)


def get_credentials(
    args=None,
) -> __s3_creds:
    """
    Get credentials for S3 from a config file, environment variables or command line arguments.

    Args:
        args (argparse.Namespace): Command line arguments

    Returns:
        namedtuple: Named tuple containing the access key, secret key, endpoint, region and profile name
    """

    credential_file = configparser.ConfigParser()

    try:
        credential_file.read_file(open(os.path.expanduser("~/.aws/credentials"), "rt"))
        access_key = credential_file[profile]["aws_access_key_id"]
        secret_key = credential_file[profile]["aws_secret_access_key"]
    except FileNotFoundError:
        pass

    if args:
        profile = "default" if not args.profile else args.profile
    else:
        profile = "default"

    endpoint = "https://s3.climb.ac.uk"

    region = "s3"

    if os.getenv("AWS_ACCESS_KEY_ID"):
        access_key = os.getenv("AWS_ACCESS_KEY_ID")

    if os.getenv("AWS_SECRET_ACCESS_KEY"):
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if args:
        if args.access_key:
            access_key = args.access_key

        if args.secret_key:
            secret_key = args.secret_key

    # Make this actually work
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


def s3_to_fh(s3_uri: str, eTag: str) -> StringIO:
    """
    Take file from S3 URI and return a file handle-like object using StringIO
    Requires an S3 URI and an ETag to confirm the file has not been modified since upload.

    Args:
        s3_uri (str): S3 URI of the file to be downloaded
        eTag (str): ETag of the file to be downloaded

    Returns:
        StringIO: File handle-like object of the downloaded file
    """

    s3_credentials = get_credentials()

    bucket = s3_uri.replace("s3://", "").split("/")[0]

    key = s3_uri.replace("s3://", "").split("/", 1)[1]

    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_credentials.endpoint,
        aws_access_key_id=s3_credentials.access_key,
        region_name=s3_credentials.region,
        aws_secret_access_key=s3_credentials.secret_key,
    )

    file_obj = s3_client.get_object(Bucket=bucket, Key=key)

    if file_obj["ETag"].replace('"', "") != eTag:
        raise Exception("ETag mismatch, CSV has been modified since upload")

    return StringIO(file_obj["Body"].read().decode("utf-8-sig"))
