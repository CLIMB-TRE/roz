import boto3
from collections import namedtuple
import configparser
import os
import sys
from io import StringIO
import logging
import subprocess
from pathlib import Path
from types import SimpleNamespace
import time
import csv

from onyx import (
    OnyxClient,
    OnyxConfig,
)

from onyx.exceptions import (
    OnyxRequestError,
    OnyxConnectionError,
    OnyxServerError,
    OnyxConfigError,
    OnyxClientError,
)

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
        timeout=3600,
    ):
        """
        Run a nxf pipeline as a subprocess, this is only advisable for use with cloud executors, specifically k8s.
        If local execution is needed then you should use something else.

        Args:
            pipe (str): The pipeline to run as a github repo in the format 'user/repo'
            config (str): Path to a nextflow config file
            nxf_executable (str): Path to the nextflow executable
            profile (str): The nextflow profile to use

        """

        self.pipe = pipe
        self.config = Path(config) if config else None
        self.nxf_executable = nxf_executable
        self.timeout = timeout
        self.profile = profile
        self.cmd = None

    def execute(self, params: dict, logdir: Path) -> tuple[int, str, str]:
        """
        Execute the pipeline with the given parameters

        Args:
            params (dict): A dictionary of parameters to pass to the pipeline in the format {'param_name': 'param_value'} (no --)

        Returns:
            tuple[int, str, str]: A tuple containing the return code, stdout and stderr
        """

        cmd = [self.nxf_executable]

        if logdir:
            logfile_path = os.path.join(logdir.resolve(), "nextflow.log")
            cmd.extend(
                [
                    "-log",
                    logfile_path,
                ]
            )

        cmd.extend(["run", "-r", "main", "-latest", self.pipe])

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
                timeout=self.timeout,
            )

        except BaseException as subprocess_exception:
            proc = SimpleNamespace(
                returncode=1, stdout=str(subprocess_exception), stderr=""
            )

        return (proc.returncode, proc.stdout, proc.stderr)

    def cleanup(self, stdout: str) -> tuple[int, str, str]:
        """Cleanup the pipeline intermediate files

        Args:
            stdout (str): The stdout from the pipeline execution

        Returns:
            tuple[int, str, str]: A tuple containing the return code, stdout and stderr
        """

        try:
            pipeline_id = (
                stdout.split("\n")[3].split(" ")[2].replace("[", "").replace("]", "")
            )

            cmd = [self.nxf_executable, "clean", "-f", pipeline_id]

            proc = subprocess.run(
                args=cmd,
                capture_output=True,
                universal_newlines=True,
                text=True,
                timeout=60,
            )

        except BaseException as cleanup_exception:
            proc = SimpleNamespace(
                returncode=1, stdout=str(cleanup_exception), stderr=""
            )

        return (proc.returncode, proc.stdout, proc.stderr)


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


def csv_create(
    payload: dict,
    log: logging.getLogger,
    test_submission: bool = False,
) -> tuple[bool, bool, dict]:
    """Function to create a new record in onyx from a metadata CSV file, can be used for testing or for real submissions

    Args:
        payload (dict): Payload dict for the current artifact
        log (logging.getLogger): Logger object
        test_submission (bool, optional): Bool to indicate if submission is a test or not. Defaults to False.

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the create was successful, a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """
    # Not sure how to fully generalise this, the idea is to have a csv as the only file that will always exist, so I guess this is okay?
    # CSV file must always be called '.csv' though

    onyx_config = get_onyx_credentials()

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                # Test create from the metadata CSV
                response = client.csv_create(
                    payload["project"],
                    csv_file=s3_to_fh(
                        payload["files"][".csv"]["uri"],
                        payload["files"][".csv"]["etag"],
                    ),  # I don't like having a hardcoded metadata file name like this but hypothetically we should always have a metadata CSV
                    test=test_submission,
                    fields={"site": payload["site"]},
                    multiline=False,
                )

                if not test_submission:
                    payload["cid"] = response["cid"]

                return (True, False, payload)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 3 seconds"
                    )
                    time.sleep(3)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )
                    if test_submission:
                        payload.setdefault("onyx_test_create_errors", {})
                        payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                        payload["onyx_test_create_errors"]["onyx_errors"].append(str(e))
                    else:
                        payload.setdefault("onyx_create_errors", {})
                        payload["onyx_create_errors"].setdefault("onyx_errors", [])
                        payload["onyx_create_errors"]["onyx_errors"].append(str(e))

                    return (False, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                if test_submission:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(str(e))
                else:
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(str(e))

                return (False, True, payload)

            except OnyxClientError as e:
                log.info(
                    f"Onyx csv create failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}"
                )

                if test_submission:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(str(e))
                else:
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(str(e))

                return (False, False, payload)

            except OnyxRequestError as e:
                log.info(
                    f"Onyx test csv create failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}"
                )

                if test_submission:
                    # Handle the case where the record already exists but isn't published when field is added to onyx
                    payload.setdefault("onyx_test_create_errors", {})
                    for field, messages in e.response.json()["messages"].items():
                        payload["onyx_test_create_errors"].setdefault(field, [])
                        payload["onyx_test_create_errors"][field].extend(messages)
                else:
                    payload.setdefault("onyx_create_errors", {})
                    for field, messages in e.response.json()["messages"].items():
                        payload["onyx_create_errors"].setdefault(field, [])
                        payload["onyx_create_errors"][field].extend(messages)

                return (False, False, payload)

            except Exception as e:
                if test_submission:
                    log.error(f"Unhandled error: {e}")
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_test_create_errors"]["onyx_errors"].append(str(e))
                else:
                    log.error(f"Unhandled error: {e}")
                    payload.setdefault("onyx_create_errors", {})
                    payload["onyx_create_errors"].setdefault("onyx_errors", [])
                    payload["onyx_create_errors"]["onyx_errors"].append(str(e))

                return (False, True, payload)

        # This should never be reached
        if test_submission:
            payload.setdefault("onyx_test_create_errors", {})
            payload["onyx_test_create_errors"].setdefault("onyx_errors", [])
            payload["onyx_test_create_errors"]["onyx_errors"].append(
                "End of csv_create func reached, this should never happen!"
            )
        else:
            payload.setdefault("onyx_create_errors", {})
            payload["onyx_create_errors"].setdefault("onyx_errors", [])
            payload["onyx_create_errors"]["onyx_errors"].append(
                "End of csv_create func reached, this should never happen!"
            )

        return (False, True, payload)


def csv_field_checks(payload: dict) -> tuple[bool, bool, dict]:
    """Function to check that the required fields are present in the metadata CSV and that they match the filename

    Args:
        payload (dict): Payload dict for the current artifact

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the field checks failed, a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """

    try:
        with s3_to_fh(
            payload["files"][".csv"]["uri"],
            payload["files"][".csv"]["etag"],
        ) as csv_fh:
            reader = csv.DictReader(csv_fh, delimiter=",")

            metadata = next(reader)

            name_matches = {
                x: metadata[x] == payload[x] for x in ("sample_id", "run_name")
            }

            for k, v in name_matches.items():
                if not v:
                    payload.setdefault("onyx_test_create_errors", {})
                    payload["onyx_test_create_errors"].setdefault(k, [])
                    payload["onyx_test_create_errors"][k].append(
                        "Field does not match filename."
                    )

            if not all(name_matches.values()):
                return (False, False, payload)
            else:
                return (True, False, payload)

    except Exception as e:
        payload.setdefault("onyx_test_create_errors", {})
        payload["onyx_test_create_errors"].setdefault("roz_errors", [])
        payload["onyx_test_create_errors"]["roz_errors"].append(e)
        return (False, True, payload)


def onyx_update(
    payload: dict, fields: dict, log: logging.getLogger
) -> tuple[bool, bool, dict]:
    """
    Update an existing Onyx record with the given fields

    Args:
        payload (dict): Payload dict for the current artifact
        fields (dict): Fields to update in the format {'field_name': 'field_value'}
        log (logging.getLogger): Logger object

    Returns:
        tuple[bool, bool, dict]: Tuple containing a bool indicating whether the update failed, a bool indicating whether to squawk in the alerts channel, and the updated payload dict
    """

    onyx_config = get_onyx_credentials()

    with OnyxClient(config=onyx_config) as client:
        reconnect_count = 0
        while reconnect_count <= 3:
            try:
                client.update(
                    project=payload["project"],
                    cid=payload["cid"],
                    fields=fields,
                )

                return (False, True, payload)

            except OnyxConnectionError as e:
                if reconnect_count < 3:
                    reconnect_count += 1
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}. Retrying in 5 seconds"
                    )
                    time.sleep(5)
                    continue

                else:
                    log.error(
                        f"Failed to connect to Onyx {reconnect_count} times with error: {e}"
                    )

                    payload.setdefault("onyx_errors", {})
                    payload["onyx_errors"].setdefault("onyx_errors", [])
                    payload["onyx_errors"]["onyx_errors"].append(e)

                    return (True, True, payload)

            except (OnyxServerError, OnyxConfigError) as e:
                log.error(f"Unhandled Onyx error: {e}")
                payload.setdefault("onyx_update_errors", {})
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(e)

                return (True, True, payload)

            except OnyxClientError as e:
                log.error(
                    f"Onyx update failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}"
                )
                payload.setdefault("onyx_update_errors", {})
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(e)

                return (True, False, payload)

            except OnyxRequestError as e:
                log.error(
                    f"Onyx update failed for artifact: {payload['artifact']}, UUID: {payload['uuid']}"
                )

                payload.setdefault("onyx_update_errors", {})
                for field, messages in e.response.json()["messages"].items():
                    payload["onyx_update_errors"].setdefault(field, [])
                    payload["onyx_update_errors"][field].extend(messages)

                return (True, False, payload)

            except Exception as e:
                log.error(f"Unhandled error: {e}")
                payload["onyx_update_errors"].setdefault("onyx_errors", [])
                payload["onyx_update_errors"]["onyx_errors"].append(e)

                return (True, True, payload)

    # This should never be reached
    payload.setdefault("onyx_update_errors", {})
    payload["onyx_update_errors"].setdefault("onyx_errors", [])
    payload["onyx_update_errors"]["onyx_errors"].append(
        "End of onyx_update func reached, this should never happen!"
    )
    return (True, True, payload)


def get_onyx_credentials():
    config = OnyxConfig(
        domain=os.environ["ONYX_DOMAIN"],
        username=os.environ["ONYX_USERNAME"],
        password=os.environ["ONYX_PASSWORD"],
    )
    return config


def get_s3_credentials(
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

    if args:
        profile = "default" if not args.profile else args.profile
    else:
        profile = "default"

    try:
        credential_file.read_file(open(os.path.expanduser("~/.aws/credentials"), "rt"))
        credentials["access_key"] = credential_file[profile]["aws_access_key_id"]
        credentials["secret_key"] = credential_file[profile]["aws_secret_access_key"]
    except FileNotFoundError:
        pass

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

    s3_credentials = get_s3_credentials()

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
