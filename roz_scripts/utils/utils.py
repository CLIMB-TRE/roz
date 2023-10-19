import boto3
from collections import namedtuple
import configparser
import os
import sys
from io import StringIO
import logging
import subprocess
from pathlib import Path

from onyx import OnyxClient

__s3_creds = namedtuple(
    "s3_credentials",
    ["access_key", "secret_key", "endpoint", "region", "profile_name"],
)


class pipeline:
    def __init__(
        self,
        pipe: str,
        config: Path,
        nxf_executable: Path,
        profile=None,
        timeout_seconds=7200,
    ):
        """
        Run a nxf pipeline as a subprocess, this is only advisable for use with cloud executors, specifically k8s.
        If local execution is needed then you should use something else.

        Args:
            pipe (str): The pipeline to run as a github repo in the format 'user/repo'
            config (str): Path to a nextflow config file
            nxf_executable (str): Path to the nextflow executable
            profile (str): The nextflow profile to use
            timeout_seconds (int): The number of seconds to wait before timing out the pipeline

        """

        self.pipe = pipe
        self.config = Path(config) if config else None
        self.nxf_executable = nxf_executable
        self.profile = profile
        self.timeout_seconds = timeout_seconds
        self.cmd = None

    def execute(self, params: dict) -> tuple[int, bool, str, str]:
        """
        Execute the pipeline with the given parameters

        Args:
            params (dict): A dictionary of parameters to pass to the pipeline in the format {'param_name': 'param_value'} (no --)

        Returns:
            tuple[int, bool, str, str]: A tuple containing the return code, a bool indicating whether the pipeline timed out, stdout and stderr
        """

        timeout = False

        cmd = [self.nxf_executable, "run", "-r", "main", "-latest", self.pipe]

        if self.config:
            cmd.extend(["-c", self.config.resolve()])

        if self.profile:
            cmd.extend(["-profile", self.profile])

        if params:
            for k, v in params.items():
                cmd.extend([f"--{k}", v])

        try:
            self.cmd = cmd
            proc = subprocess.run(
                args=cmd,
                capture_output=True,
                universal_newlines=True,
                text=True,
                timeout=self.timeout_seconds,
            )

        except subprocess.TimeoutExpired:
            timeout = True

        return (proc.returncode, timeout, proc.stdout, proc.stderr)

    def cleanup(self, stdout: str) -> tuple[int, bool, str, str]:
        """Cleanup the pipeline intermediate files

        Args:
            stdout (str): The stdout from the pipeline execution

        Returns:
            tuple[int, bool, str, str]: A tuple containing the return code, a bool indicating whether the pipeline timed out, stdout and stderr
        """

        timeout = False

        pipeline_id = (
            stdout.split("\n")[3].split(" ")[2].replace("[", "").replace("]", "")
        )

        cmd = [self.nxf_executable, "clean", "-f", pipeline_id]

        try:
            proc = subprocess.run(
                args=cmd,
                capture_output=True,
                universal_newlines=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            timeout = True

        return (proc.returncode, timeout, proc.stdout, proc.stderr)


def init_logger(name, log_path, log_level):
    log = logging.getLogger(name)
    log.propagate = False
    log.setLevel(log_level)
    if not (log.hasHandlers()):
        logging_fh = logging.FileHandler(log_path)
        logging_fh.setFormatter(
            logging.Formatter("%(name)s\t::%(levelname)s::%(asctime)s::\t%(message)s")
        )
        log.addHandler(logging_fh)
    return log


def onyx_submission(
    log: logging.getLogger,
    payload: dict,
) -> tuple[bool, dict]:
    """This function is responsible for submitting a record to Onyx, it is called from the main ingest function
    when a match is received for a given artifact.

    Args:
        log (logging.getLogger): The logger object
        payload (dict): The payload dict of the currently ingesting artifact
        varys_client (varys.varys): A varys client object

    Returns:
        tuple[bool, dict]: A tuple containing a bool indicating the status of the Onyx create request and the payload dict modified to include information about the Onyx create request
    """

    submission_fail = False

    if not payload.get("onyx_errors"):
        payload["onyx_errors"] = {}

    if not payload.get("onyx_create_status"):
        payload["onyx_create_status"] = False

    if not payload.get("created"):
        payload["created"] = False

    if not payload.get("cid"):
        payload["cid"] = ""

    with OnyxClient(env_password=True) as client:
        log.info(
            f"Received match for artifact: {payload['artifact']}, now attempting to create record in Onyx"
        )

        try:
            response_generator = client._csv_create(
                payload["project"],
                csv_file=s3_to_fh(
                    payload["files"][".csv"]["uri"],
                    payload["files"][".csv"]["etag"],
                ),
                fields={"suppressed": True, "site": payload["site"]},
            )

            response = next(response_generator)

            payload["onyx_status_code"] = response.status_code

            if response.status_code == 500:
                log.error(
                    f"Onyx create for UUID: {payload['uuid']} lead to onyx internal server error"
                )
                submission_fail = True
                payload["onyx_errors"]["onyx_client_errors"] = [
                    "Onyx internal server error"
                ]

            elif response.status_code == 404:
                log.error(
                    f"Onyx create for UUID: {payload['uuid']} failed because project: {payload['project']} does not exist"
                )
                submission_fail = True
                payload["onyx_errors"]["onyx_client_errors"] = [
                    f"Project {payload['project']} does not exist"
                ]

            elif response.status_code == 403:
                log.error(
                    f"Onyx create for UUID: {payload['uuid']} failed due to a permission error"
                )
                submission_fail = True
                payload["onyx_errors"]["onyx_client_errors"] = [
                    "Permission error on Onyx create"
                ]

            elif response.status_code == 401:
                log.error(
                    f"Onyx create for UUID: {payload['uuid']} failed due to incorrect credentials"
                )
                submission_fail = True
                payload["onyx_errors"]["onyx_client_errors"] = [
                    "Incorrect Onyx credentials"
                ]

            elif response.status_code == 400:
                log.error(
                    f"Onyx create for UUID: {payload['uuid']} failed due to a bad request"
                )
                submission_fail = True
                if response.json().get("messages"):
                    for field, messages in response.json()["messages"].items():
                        if payload["onyx_errors"].get(field):
                            payload["onyx_errors"][field].extend(messages)
                        else:
                            payload["onyx_errors"][field] = messages

            elif response.status_code == 200:
                log.error(
                    f"Onyx responded with 200 on a create request for UUID: {payload['uuid']} (this should be 201)"
                )
                submission_fail = True
                payload["onyx_errors"]["onyx_client_errors"] = [
                    "200 response status on onyx create (should be 201)"
                ]

            elif response.status_code == 201:
                log.info(
                    f"Successful create for UUID: {payload['uuid']} which has been assigned CID: {response.json()['data']['cid']}"
                )
                payload["onyx_create_status"] = True
                payload["created"] = True
                payload["cid"] = response.json()["data"]["cid"]

            else:
                log.error(
                    f"Unhandled Onyx response status code {response.status_code} from Onyx create for UUID: {payload['uuid']}"
                )
                submission_fail = True
                payload["onyx_errors"]["onyx_client_errors"] = [
                    f"Unhandled response status code {response.status_code} from Onyx create"
                ]

        except Exception as e:
            log.error(
                f"Onyx CSV create failed for UUID: {payload['uuid']} due to client error: {e}"
            )
            payload["onyx_errors"]["onyx_client_errors"] = [
                f"Unhandled client error {e}"
            ]
            submission_fail = True

    return (submission_fail, payload)


def onyx_update(
    payload: dict, fields: dict, log: logging.getLogger
) -> tuple[bool, dict]:
    """
    Update an existing Onyx record with the given fields

    Args:
        payload (dict): Payload dict for the current artifact
        fields (dict): Fields to update in the format {'field_name': 'field_value'}
        log (logging.getLogger): Logger object

    Returns:
        tuple[bool, dict]: Tuple containing a bool indicating whether the update failed and the updated payload dict
    """
    update_fail = False

    with OnyxClient(env_password=True) as client:
        try:
            response = client._update(
                project=payload["project"],
                cid=payload["cid"],
                fields=fields,
            )

            if response.status_code == 200:
                log.info(f"Successfully updated Onyx record for CID: {payload['cid']}")

            else:
                update_fail = True
                log.error(
                    f"Failed to update Onyx record for CID: {payload['cid']} with status code: {response.status_code}"
                )
                if response.json().get("messages"):
                    if not payload.get("onyx_errors"):
                        payload["onyx_errors"] = {}
                    for field, messages in response.json()["messages"].items():
                        if payload["onyx_errors"].get(field):
                            payload["onyx_errors"][field].extend(messages)
                        else:
                            payload["onyx_errors"][field] = messages

        except Exception as e:
            log.error(
                f"Failed to update Onyx record for CID: {payload['cid']} with unhandled onyx client error: {e}"
            )
            if payload.get("onyx_client_errors"):
                payload["onyx_errors"]["onyx_client_errors"].extend(
                    f"Unhandled client error {e}"
                )
            else:
                payload["onyx_errors"]["onyx_client_errors"] = [
                    f"Unhandled client error {e}"
                ]
            update_fail = True

    return (update_fail, payload)


def onyx_unsuppress(payload: dict, log: logging.getLogger) -> tuple[bool, dict]:
    """Function to unsuppress an existing Onyx record

    Args:
        payload (dict): Payload dict for the current artifact
        log (logging.getLogger): Logger object

    Returns:
        tuple[bool, dict]: Tuple containing a bool indicating whether the unsuppress failed and the updated payload dict
    """
    unsuppress_fail = False

    try:
        with OnyxClient(env_password=True) as client:
            response = client._update(
                project=payload["project"],
                cid=payload["cid"],
                fields={"suppressed": False},
            )

        if response.status_code == 200:
            log.info(f"Successfully unsupressed Onyx record for CID: {payload['cid']}")

        else:
            unsuppress_fail = True
            log.error(
                f"Failed to unsupress Onyx record for CID: {payload['cid']} with status code: {response.status_code}"
            )
            if response.json().get("messages"):
                if not payload.get("onyx_errors"):
                    payload["onyx_errors"] = {}
                for field, messages in response.json()["messages"].items():
                    if payload["onyx_errors"].get(field):
                        payload["onyx_errors"][field].extend(messages)
                    else:
                        payload["onyx_errors"][field] = messages

    except Exception as e:
        log.error(
            f"Failed to unsupress Onyx record for CID: {payload['cid']} with unhandled onyx client error: {e}"
        )
        if payload.get("onyx_client_errors"):
            payload["onyx_errors"]["onyx_client_errors"].extend(
                f"Unhandled client error {e}"
            )
        else:
            payload["onyx_errors"]["onyx_client_errors"] = [
                f"Unhandled client error {e}"
            ]
        unsuppress_fail = True

    return (unsuppress_fail, payload)


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

    credentials = {}

    try:
        credential_file.read_file(open(os.path.expanduser("~/.aws/credentials"), "rt"))
        credentials["access_key"] = credential_file[profile]["aws_access_key_id"]
        credentials["secret_key"] = credential_file[profile]["aws_secret_access_key"]
    except FileNotFoundError:
        pass

    if args:
        profile = "default" if not args.profile else args.profile
    else:
        profile = "default"

    if not os.getenv("UNIT_TESTING"):
        endpoint = "https://s3.climb.ac.uk"
    else:
        endpoint = "http://localhost:5000"

    region = "s3"

    if os.getenv("AWS_ACCESS_KEY_ID"):
        credentials["access_key"] = os.getenv("AWS_ACCESS_KEY_ID")

    if os.getenv("AWS_SECRET_ACCESS_KEY"):
        credentials["secret_key"] = os.getenv("AWS_SECRET_ACCESS_KEY")

    if args:
        if args.access_key:
            credentials["access_key"] = args.access_key

        if args.secret_key:
            credentials["secret_key"] = args.secret_key

    # Make this actually work
    if not credentials.get("access_key") or not credentials.get("secret_key"):
        error = """CLIMB S3 credentials could not be found, please provide valid credentials in one of the following ways:
            - In a correctly formatted config file (~/.aws/credentials)
            - As environmental variables 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY'
            - As a command line argument, see --help for more details
        """
        print(error, file=sys.stderr)
        sys.exit(1)

    s3_credentials = __s3_creds(
        access_key=credentials["access_key"],
        secret_key=credentials["secret_key"],
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
