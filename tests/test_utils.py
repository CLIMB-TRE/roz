from onyx.exceptions import (
    OnyxClientError,
    OnyxRequestError,
    OnyxServerError,
    OnyxConnectionError,
    OnyxConfigError,
)

from roz_scripts.utils.utils import (
    init_logger,
    csv_create,
    csv_field_checks,
    check_artifact_published,
    onyx_identify,
    onyx_reconcile,
    onyx_update,
    get_s3_credentials,
    valid_character_checks,
    pipeline,
    send_admin_alert,
    PodResources,
    PodResourceError,
    parse_cpu_quantity,
    parse_memory_quantity,
)

from kubernetes.client.exceptions import ApiException

import moto
import boto3
import unittest
from unittest.mock import patch, Mock
import os
import copy
import tempfile
from pathlib import Path


DIR = os.path.dirname(__file__)

TEST_UTILS_LOG_FILENAME = os.path.join(DIR, "test_utils.log")


class MockResponse:
    def __init__(self, status_code, json_data=None, ok=True):
        self.status_code = status_code
        self.json_data = json_data
        self.ok = ok

    def json(self):
        return self.json_data


class test_utils(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["MOTO_S3_CUSTOM_ENDPOINTS"] = "https://s3.climb.ac.uk"
        os.environ["ONYX_DOMAIN"] = "testing"
        os.environ["ONYX_TOKEN"] = "testing"
        # del os.environ["UNIT_TESTING"]

        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()

        self.s3_client = boto3.client("s3", endpoint_url="https://s3.climb.ac.uk")

        self.log = init_logger("test", TEST_UTILS_LOG_FILENAME, "DEBUG")

        self.s3_client.create_bucket(Bucket="mscape-birm-ont-prod")

        self.example_match = {
            "uuid": "42c3796d-d767-4293-97a8-c4906bb5cca8",
            "payload_version": 1,
            "site": "birm",
            "uploaders": ["testuser"],
            "match_timestamp": 1697036668222422871,
            "artifact": "mscape|sample-test|run-test",
            "run_index": "sample-test",
            "run_id": "run-test",
            "project": "mscape",
            "platform": "ont",
            "files": {
                ".fastq.gz": {
                    "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.fastq.gz",
                    "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
                    "key": "mscape.sample-test.run-test.fastq.gz",
                },
                ".csv": {
                    "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.csv",
                    "etag": "7022ea6a3adb39323b5039c1d6587d08",
                    "key": "mscape.sample-test.run-test.csv",
                },
            },
            "test_flag": False,
        }

        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"run_index,run_id\nsample-test,run-test",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        self.example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

    def tearDown(self):
        self.mock_s3.stop()
        self.s3_client.close()

    def test_csv_create(self):

        self.example_match["run_index"] = "sample-test-2"
        self.example_match["run_id"] = "run-test-2"

        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"test",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        print(get_s3_credentials())

        self.example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create.return_value = {
                "climb_id": "test_climb_id",
                "run_index": "test_sample_id",
                "run_id": "test_run_id",
                "biosample_id": "test_biosample_id",
                "biosample_source_id": "",
            }

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)
            self.assertNotIn("climb_id", payload.keys())

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create.return_value = {
                "climb_id": "test_climb_id",
                "run_index": "test_sample_id",
                "run_id": "test_run_id",
                "biosample_id": "test_biosample_id",
                "biosample_source_id": "test_biosample_source_id",
            }

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=False,
            )
            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)
            self.assertEqual("test_climb_id", payload["climb_id"])

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client, patch(
            "roz_scripts.utils.utils.check_artifact_published"
        ) as mock_published_check:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create error handling",
                    response=MockResponse(
                        status_code=400,
                        json_data={
                            "data": [],
                            "messages": {
                                "run_index": ["Test sample_id error handling"]
                            },
                        },
                    ),
                )
            )

            mock_published_check.return_value = (True, False, payload)

            success, alert, payload = csv_create(
                payload=self.example_match, log=self.log, test_submission=False
            )
            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client, patch(
            "roz_scripts.utils.utils.check_artifact_published"
        ) as mock_published_check:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create exception",
                    response=MockResponse(
                        status_code=400,
                        json_data={
                            "data": [],
                            "messages": {
                                "non_field_errors": [
                                    "This combination of run_index, run_id already exists."
                                ]
                            },
                        },
                    ),
                )
            )

            mock_published_check.return_value = (False, False, payload)

            success, alert, payload = csv_create(
                payload=self.example_match, log=self.log, test_submission=False
            )

            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxClientError(
                    "File contains multiple records but this is not allowed. To upload multiple records, set 'multiline' = True."
                )
            )

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertIn(
                "File contains multiple records but this is not allowed. To upload multiple records, set 'multiline' = True.",
                payload["onyx_test_create_errors"]["onyx_errors"],
            )

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client, patch(
            "roz_scripts.utils.utils.check_artifact_published"
        ) as mock_published_check:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create error handling",
                    response=MockResponse(
                        status_code=400,
                        json_data={
                            "data": [],
                            "messages": {
                                "run_index": ["Test sample_id error handling"]
                            },
                        },
                    ),
                )
            )

            mock_published_check.return_value = (False, False, payload)

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertIn(
                "Test sample_id error handling",
                payload["onyx_test_create_errors"]["run_index"],
            )

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxConnectionError()
            )

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

            self.assertFalse(success)
            self.assertTrue(alert)
            csv_create_calls = (
                mock_client.return_value.__enter__.return_value.csv_create.mock_calls
            )

            self.assertEqual(len(csv_create_calls), 4)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxServerError(
                    message="Test server error handling",
                    response=MockResponse(status_code=500),
                )
            )

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)

        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxConfigError()
            )

            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )

            self.assertFalse(success)
            self.assertTrue(alert)

    def test_csv_create_non_plaintext(self):
        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"\xff\xfe\x00\x01binarydata",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        self.example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

        with patch("roz_scripts.utils.utils.OnyxClient"):
            success, alert, payload = csv_create(
                payload=self.example_match,
                log=self.log,
                test_submission=True,
            )
            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertTrue(
                any(
                    "not valid UTF-8 plaintext" in msg
                    for msg in payload["onyx_test_create_errors"]["onyx_errors"]
                )
            )

    def test_csv_field_check_success(self):
        success, alert, payload = csv_field_checks(payload=self.example_match)

        print(payload)

        self.assertTrue(success)
        self.assertFalse(alert)

    def test_csv_field_check_failure(self):

        self.example_match["run_index"] = "sample-test-2"
        self.example_match["run_id"] = "run-test-2"

        success, alert, payload = csv_field_checks(payload=self.example_match)

        print(payload)

        self.assertFalse(success)
        self.assertFalse(alert)
        self.assertIn(
            "Field does not match filename.",
            payload["onyx_test_create_errors"]["run_index"],
        )
        self.assertIn(
            "Field does not match filename.",
            payload["onyx_test_create_errors"]["run_id"],
        )

    def test_csv_field_check_non_plaintext(self):
        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"\xff\xfe\x00\x01binarydata",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        self.example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")

        success, alert, payload = csv_field_checks(payload=self.example_match)

        print(payload)

        self.assertFalse(success)
        self.assertFalse(alert)
        self.assertTrue(
            any(
                "not valid UTF-8 plaintext" in msg
                for msg in payload["onyx_test_create_errors"]["roz_errors"]
            )
        )

    def test_published_check_true(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ({"yeet": "yeet", "climb_id": "test_id", "is_published": True},)
            )
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "run_index",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }
            published, alert, payload = check_artifact_published(
                payload=self.example_match, log=self.log
            )
            print(payload)

            self.assertTrue(published)
            self.assertFalse(alert)
            self.assertFalse("climb_id" in payload)

    def test_published_check_false(self):
        # Test artifact is not published
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "run_index",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }
            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ({"yeet": "yeet", "climb_id": "test_id", "is_published": False},)
            )

            published, alert, payload = check_artifact_published(
                payload=self.example_match, log=self.log
            )

            print(payload)

            self.assertFalse(published)
            self.assertFalse(alert)
            self.assertEqual(payload["climb_id"], "test_id")

    def test_published_check_error(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "run_index",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }
            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ()
            )

            published, alert, payload = check_artifact_published(
                payload=self.example_match, log=self.log
            )
            print(payload)

            self.assertTrue(published)
            self.assertTrue(alert)

    def test_onyx_identify_true(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "run_index",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            success, alert, payload = onyx_identify(
                payload=self.example_match, log=self.log, identity_field="run_index"
            )
            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)
            self.assertEqual("S-1234567890", payload["anonymised_run_index"])

    def test_onyx_identify_failure(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify = Mock(
                side_effect=OnyxRequestError(
                    message="test error handling",
                    response=MockResponse(
                        status_code=404,
                        json_data={
                            "data": [],
                            "messages": {"run_index": "Test sample_id error handling"},
                        },
                    ),
                )
            )

            success, alert, payload = onyx_identify(
                payload=self.example_match, log=self.log, identity_field="run_index"
            )

            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertFalse("anonymised_run_index" in payload)

    def test_onyx_reconcile(self):
        self.s3_client.put_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
            Body=b"run_index,run_id,adm1_country,adm2_region,study_centre_id\nsample-test,run-test,GB,GB-ENG,1234567890",
        )
        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        self.example_match["files"][".csv"]["etag"] = resp["ETag"].replace('"', "")
        # Test
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "run_index",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                (
                    {
                        "run_index": "S-1234567890",
                        "run_id": "R-12354453",
                        "adm1_country": "GB",
                        "adm2_region": "GB-ENG",
                        "study_centre_id": "1234567890",
                    },
                    {
                        "run_index": "S-1234567890",
                        "run_id": "R-12354412312353",
                        "adm1_country": "GB",
                        "adm2_region": "GB-ENG",
                        "study_centre_id": "1234567890",
                    },
                )
            )

            success, alert, payload = onyx_reconcile(
                payload=self.example_match,
                log=self.log,
                identifier="run_index",
                fields_to_reconcile=["adm1_country", "adm2_region", "study_centre_id"],
            )

            print(payload)

            self.assertTrue(success)
            self.assertFalse(alert)

    def test_onyx_reconcile_failure(self):
        # Test failure
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "run_index",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                (
                    {
                        "run_index": "S-1234567890",
                        "run_id": "R-12354453",
                        "adm1_country": "ES",
                        "adm2_region": "GB-ENG",
                        "study_centre_id": "1234567890",
                    },
                    {
                        "run_index": "S-1234567890",
                        "run_id": "R-12354412312353",
                        "adm1_country": "GB",
                        "adm2_region": "GB-ENG",
                        "study_centre_id": "1234567890",
                    },
                )
            )

            success, alert, payload = onyx_reconcile(
                payload=self.example_match,
                log=self.log,
                identifier="run_index",
                fields_to_reconcile=["adm1_country", "adm2_region", "study_centre_id"],
            )

            print(payload)

            self.assertFalse(success)
            self.assertFalse(alert)
            self.assertIn(
                "Onyx records for run_index: S-1234567890 disagree for the following fields: adm1_country",
                payload["onyx_warnings"]["reconcile_errors"],
            )

    def test_onyx_reconcile_no_filter_return(self):
        # Test no filter return
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "run_index",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ()
            )

            success, alert, payload = onyx_reconcile(
                payload=self.example_match,
                log=self.log,
                identifier="run_index",
                fields_to_reconcile=["adm1_country", "adm2_region", "study_centre_id"],
            )

            print(payload)

            self.assertFalse(success)
            self.assertTrue(alert)

    def test_valid_character_check_success(self):
        success, alert, payload = valid_character_checks(payload=self.example_match)

        print(payload)

        self.assertTrue(success)
        self.assertFalse(alert)

    def test_valid_character_check_failure(self):
        self.example_match["run_index"] = "test:sample-test-2"
        self.example_match["run_id"] = "test:run-test-2"

        success, alert, payload = valid_character_checks(payload=self.example_match)

        print(payload)

        self.assertFalse(success)
        self.assertFalse(alert)
        self.assertIn(
            "run_index contains invalid characters, must be alphanumeric and contain only hyphens and underscores",
            payload["onyx_test_create_errors"]["run_index"],
        )
        self.assertIn(
            "run_id contains invalid characters, must be alphanumeric and contain only hyphens and underscores",
            payload["onyx_test_create_errors"]["run_id"],
        )


class test_send_admin_alert(unittest.TestCase):
    def test_sends_source_and_description(self):
        mock_varys = Mock()
        send_admin_alert(mock_varys, source="s3_matcher", description="boom")

        mock_varys.send.assert_called_once_with(
            message={"source": "s3_matcher", "description": "boom"},
            exchange="remote-announce",
            queue_suffix="alert",
        )

    def test_includes_uuid_when_given(self):
        mock_varys = Mock()
        send_admin_alert(
            mock_varys, source="mscape", description="boom", uuid="some-uuid"
        )

        mock_varys.send.assert_called_once_with(
            message={
                "source": "mscape",
                "description": "boom",
                "uuid": "some-uuid",
            },
            exchange="remote-announce",
            queue_suffix="alert",
        )

    def test_omits_uuid_when_none(self):
        mock_varys = Mock()
        send_admin_alert(mock_varys, source="mscape", description="boom", uuid=None)

        sent_message = mock_varys.send.call_args.kwargs["message"]
        self.assertNotIn("uuid", sent_message)


class test_pipeline_execute(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.logdir = Path(self.tempdir.name, "logs")
        self.logdir.mkdir(parents=True, exist_ok=True)

        self.pipe = pipeline(
            pipe="some/pipeline",
            branch="main",
            config=None,
            nxf_image="some_image",
            job_prefix="test",
        )

        self.execute_kwargs = dict(
            params={},
            logdir=self.logdir,
            timeout=3600,
            env_vars={},
            namespace="test-namespace",
            job_id="test-job-id",
            stdout_path=os.path.join(self.logdir, "stdout"),
            stderr_path=os.path.join(self.logdir, "stderr"),
            workingdir=self.logdir,
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def make_status(self, succeeded=None, failed=None, start_time=None):
        status = Mock()
        status.succeeded = succeeded
        status.failed = failed
        status.start_time = start_time
        resp = Mock()
        resp.status = status
        return resp

    @patch("roz_scripts.utils.utils.time.sleep")
    @patch("roz_scripts.utils.utils.BatchV1Api")
    @patch("roz_scripts.utils.utils.k8s_config")
    def test_job_does_not_exist_is_created(
        self, mock_k8s_config, mock_batch_cls, mock_sleep
    ):
        api_instance = mock_batch_cls.return_value
        api_instance.read_namespaced_job_status.side_effect = [
            ApiException(status=404),
            self.make_status(succeeded=1),
        ]

        returncode = self.pipe.execute(**self.execute_kwargs)

        self.assertEqual(returncode, 0)
        api_instance.create_namespaced_job.assert_called_once()
        api_instance.delete_namespaced_job.assert_not_called()

    @patch("roz_scripts.utils.utils.time.sleep")
    @patch("roz_scripts.utils.utils.BatchV1Api")
    @patch("roz_scripts.utils.utils.k8s_config")
    def test_pre_existing_failed_job_is_deleted_and_recreated(
        self, mock_k8s_config, mock_batch_cls, mock_sleep
    ):
        api_instance = mock_batch_cls.return_value
        api_instance.read_namespaced_job_status.side_effect = [
            self.make_status(failed=5),
            ApiException(status=404),
            self.make_status(succeeded=1),
        ]

        returncode = self.pipe.execute(**self.execute_kwargs)

        self.assertEqual(returncode, 0)
        api_instance.delete_namespaced_job.assert_called_once()
        api_instance.create_namespaced_job.assert_called_once()

    @patch("roz_scripts.utils.utils.time.sleep")
    @patch("roz_scripts.utils.utils.BatchV1Api")
    @patch("roz_scripts.utils.utils.k8s_config")
    def test_pre_existing_active_job_is_left_alone(
        self, mock_k8s_config, mock_batch_cls, mock_sleep
    ):
        api_instance = mock_batch_cls.return_value
        api_instance.read_namespaced_job_status.side_effect = [
            self.make_status(succeeded=None, failed=None, start_time=None),
            self.make_status(succeeded=1),
        ]

        returncode = self.pipe.execute(**self.execute_kwargs)

        self.assertEqual(returncode, 0)
        api_instance.create_namespaced_job.assert_not_called()
        api_instance.delete_namespaced_job.assert_not_called()

    @patch("roz_scripts.utils.utils.time.sleep")
    @patch("roz_scripts.utils.utils.BatchV1Api")
    @patch("roz_scripts.utils.utils.k8s_config")
    def test_default_pod_resources_in_manifest(
        self, mock_k8s_config, mock_batch_cls, mock_sleep
    ):
        api_instance = mock_batch_cls.return_value
        api_instance.read_namespaced_job_status.side_effect = [
            ApiException(status=404),
            self.make_status(succeeded=1),
        ]

        self.pipe.execute(**self.execute_kwargs)

        body = api_instance.create_namespaced_job.call_args.kwargs["body"]
        container = body["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(
            container["resources"],
            {
                "requests": {"cpu": "1", "memory": "8G"},
                "limits": {"cpu": "1", "memory": "8G"},
            },
        )

    @patch("roz_scripts.utils.utils.time.sleep")
    @patch("roz_scripts.utils.utils.BatchV1Api")
    @patch("roz_scripts.utils.utils.k8s_config")
    def test_custom_pod_resources_in_manifest(
        self, mock_k8s_config, mock_batch_cls, mock_sleep
    ):
        api_instance = mock_batch_cls.return_value
        api_instance.read_namespaced_job_status.side_effect = [
            ApiException(status=404),
            self.make_status(succeeded=1),
        ]

        custom_pipe = pipeline(
            pipe="some/pipeline",
            branch="main",
            config=None,
            nxf_image="some_image",
            job_prefix="test",
            pod_resources=PodResources(
                cpu_request="4", memory_request="32G", no_limits=True
            ),
        )

        custom_pipe.execute(**self.execute_kwargs)

        body = api_instance.create_namespaced_job.call_args.kwargs["body"]
        container = body["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(
            container["resources"],
            {"requests": {"cpu": "4", "memory": "32G"}},
        )


class test_pod_resources(unittest.TestCase):
    def test_parse_cpu_quantity(self):
        self.assertEqual(parse_cpu_quantity("1"), 1)
        self.assertEqual(parse_cpu_quantity("0.5"), 0.5)
        self.assertEqual(parse_cpu_quantity("500m"), 0.5)

        with self.assertRaises(PodResourceError):
            parse_cpu_quantity("bogus")

        with self.assertRaises(PodResourceError):
            parse_cpu_quantity("-1")

    def test_parse_memory_quantity(self):
        self.assertEqual(parse_memory_quantity("8G"), 8_000_000_000)
        self.assertEqual(parse_memory_quantity("8Gi"), 8 * 1024**3)
        self.assertEqual(parse_memory_quantity("512Mi"), 512 * 1024**2)

        with self.assertRaises(PodResourceError):
            parse_memory_quantity("bogus")

        with self.assertRaises(PodResourceError):
            parse_memory_quantity("0")

    def test_default_manifest_matches_historical_hardcoded_values(self):
        self.assertEqual(
            PodResources().to_manifest(),
            {
                "requests": {"cpu": "1", "memory": "8G"},
                "limits": {"cpu": "1", "memory": "8G"},
            },
        )

    def test_no_limits_omits_limits_key_entirely(self):
        manifest = PodResources(no_limits=True).to_manifest()
        self.assertNotIn("limits", manifest)
        self.assertEqual(manifest["requests"], {"cpu": "1", "memory": "8G"})

    def test_per_dimension_none_limit(self):
        manifest = PodResources(cpu_limit="none", memory_limit="16G").to_manifest()
        self.assertNotIn("cpu", manifest["limits"])
        self.assertEqual(manifest["limits"]["memory"], "16G")

    def test_ephemeral_storage_included_when_set(self):
        manifest = PodResources(
            ephemeral_storage_request="2Gi", ephemeral_storage_limit="4Gi"
        ).to_manifest()
        self.assertEqual(manifest["requests"]["ephemeral-storage"], "2Gi")
        self.assertEqual(manifest["limits"]["ephemeral-storage"], "4Gi")

    def test_ephemeral_storage_omitted_when_unset(self):
        manifest = PodResources().to_manifest()
        self.assertNotIn("ephemeral-storage", manifest["requests"])
        self.assertNotIn("ephemeral-storage", manifest["limits"])

    def test_validate_rejects_no_limits_with_explicit_limit(self):
        with self.assertRaises(PodResourceError):
            PodResources(no_limits=True, cpu_limit="2").validate()

    def test_validate_rejects_limit_below_request(self):
        with self.assertRaises(PodResourceError):
            PodResources(cpu_request="2", cpu_limit="1").validate()

        with self.assertRaises(PodResourceError):
            PodResources(memory_request="16G", memory_limit="8G").validate()

    def test_validate_accepts_sensible_defaults(self):
        PodResources().validate()
        PodResources(no_limits=True).validate()
        PodResources(cpu_limit="none", memory_limit="16G").validate()


class test_onyx_update_payload_key_errors(unittest.TestCase):
    """Regression tests: onyx_update must not raise KeyError from inside its
    own exception handlers when the payload doesn't carry 'artifact'/'uuid'
    keys (e.g. chimera payloads, which use 'match_uuid' and no 'artifact')."""

    def setUp(self):
        os.environ["ONYX_DOMAIN"] = "testing"
        os.environ["ONYX_TOKEN"] = "testing"
        self.log = Mock()
        self.chimera_payload = {"project": "mscape", "climb_id": "CLIMB001", "match_uuid": "match-1234"}

    @patch("roz_scripts.utils.utils.OnyxClient")
    def test_client_error_does_not_raise_keyerror_on_chimera_payload(self, mock_client_cls):
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.update.side_effect = OnyxClientError("bad request")

        fail, alert, payload = onyx_update(
            payload=self.chimera_payload, fields={"foo": "bar"}, log=self.log
        )

        self.assertTrue(fail)
        self.assertFalse(alert)
        self.assertIn("onyx_errors", payload["onyx_update_errors"])

    @patch("roz_scripts.utils.utils.OnyxClient")
    def test_request_error_does_not_raise_keyerror_on_chimera_payload(self, mock_client_cls):
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_response = MockResponse(400, json_data={"messages": {"foo": ["bad value"]}})
        mock_client.update.side_effect = OnyxRequestError("bad request", mock_response)

        fail, alert, payload = onyx_update(
            payload=self.chimera_payload, fields={"foo": "bar"}, log=self.log
        )

        self.assertTrue(fail)
        self.assertFalse(alert)
        self.assertEqual(payload["onyx_update_errors"]["foo"], ["bad value"])

    @patch("roz_scripts.utils.utils.OnyxClient")
    def test_unhandled_exception_does_not_raise_keyerror(self, mock_client_cls):
        mock_client = mock_client_cls.return_value.__enter__.return_value
        mock_client.update.side_effect = ValueError("something unexpected")

        fail, alert, payload = onyx_update(
            payload=self.chimera_payload, fields={"foo": "bar"}, log=self.log
        )

        self.assertTrue(fail)
        self.assertTrue(alert)
        self.assertIn("onyx_errors", payload["onyx_update_errors"])
        self.assertIn(
            "Unhandled onyx_update error", payload["onyx_update_errors"]["onyx_errors"][0]
        )
