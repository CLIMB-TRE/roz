import argparse
import json
import os
import sys
import unittest
from collections import namedtuple
from unittest.mock import MagicMock, call, patch

import pytest

from roz_scripts.mscape.mscape_ingest_validation import (
    validate,
    worker_pool_handler,
    run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_message(uuid="test-uuid-1234"):
    msg = MagicMock()
    msg.body = json.dumps({"uuid": uuid})
    return msg


def base_payload(
    uuid="test-uuid-1234",
    project="mscape",
    site="birm",
    test_flag=False,
    low_priority=False,
    rerun=False,
):
    return {
        "uuid": uuid,
        "project": project,
        "site": site,
        "test_flag": test_flag,
        "low_priority": low_priority,
        "rerun": rerun,
        "climb_id": "CLIMB001",
        "anonymised_run_id": "anon-run-001",
        "anonymised_run_index": "anon-idx-001",
        "anonymised_biosample_id": "anon-sample-001",
        "platform": "ont",
    }


def make_args(**kwargs):
    defaults = dict(
        logfile=None,
        log_level="DEBUG",
        ingest_pipeline="artic-network/scylla",
        pipeline_branch="main",
        project="mscape",
        nxf_config=None,
        nxf_image="quay.io/climb-tre/nextflow:25.04.8",
        k2_host="localhost",
        result_dir="/tmp/results",
        n_workers=5,
        retry_delay=0,
        max_human_reads=10000,
        publish_delay_log=None,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# worker_pool_handler tests
# ---------------------------------------------------------------------------

class TestWorkerPoolHandlerInit(unittest.TestCase):
    @patch("multiprocessing.Pool")
    def test_init_creates_pool(self, mock_pool_cls):
        log = MagicMock()
        varys = MagicMock()

        handler = worker_pool_handler(workers=4, logger=log, varys_client=varys, project="mscape")

        mock_pool_cls.assert_called_once_with(processes=4)
        self.assertEqual(handler._project, "mscape")


class TestWorkerPoolHandlerSubmitJob(unittest.TestCase):
    def setUp(self):
        with patch("multiprocessing.Pool"):
            self.handler = worker_pool_handler(
                workers=2, logger=MagicMock(), varys_client=MagicMock(), project="mscape"
            )
        self.message = make_message()
        self.args = make_args()
        self.ingest_pipe = MagicMock()

    def test_submit_job_calls_apply_async_with_correct_kwargs(self):
        self.handler.submit_job(self.message, self.args, self.ingest_pipe, low_priority=True)

        self.handler.worker_pool.apply_async.assert_called_once_with(
            func=validate,
            kwds={
                "message": self.message,
                "args": self.args,
                "ingest_pipe": self.ingest_pipe,
                "low_priority": True,
            },
            callback=self.handler.callback,
            error_callback=self.handler.error_callback,
        )

    def test_submit_job_low_priority_defaults_to_false(self):
        self.handler.submit_job(self.message, self.args, self.ingest_pipe)

        _, kwargs = self.handler.worker_pool.apply_async.call_args
        self.assertFalse(kwargs["kwds"]["low_priority"])


class TestWorkerPoolHandlerCallback(unittest.TestCase):
    def setUp(self):
        with patch("multiprocessing.Pool"):
            self.handler = worker_pool_handler(
                workers=2, logger=MagicMock(), varys_client=MagicMock(), project="mscape"
            )
        self.message = make_message()

    # --- Success path: normal (not test, not low priority) ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_sends_result_and_new_artifact(self, mock_put_result, mock_put_linkage):
        payload = base_payload()
        self.handler.callback((True, False, [], payload, self.message))

        self.handler._varys_client.send.assert_any_call(
            message=payload,
            exchange="inbound-results-mscape-birm",
            queue_suffix="validator",
        )
        mock_put_result.assert_called_once_with(payload, self.handler._log)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_normal_sends_new_artifact_to_standard_exchange(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload()
        self.handler.callback((True, False, [], payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        new_artifact_exchanges = [
            c.kwargs["exchange"]
            for c in send_calls
            if "new_artifact" in c.kwargs.get("exchange", "")
        ]
        self.assertIn("inbound-new_artifact-mscape", new_artifact_exchanges)
        self.assertNotIn("inbound-new_artifact_rerun-mscape", new_artifact_exchanges)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_normal_calls_put_linkage_json(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload()
        self.handler.callback((True, False, [], payload, self.message))

        mock_put_linkage.assert_called_once_with(payload=payload, log=self.handler._log)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_acknowledges_message(self, mock_put_result, mock_put_linkage):
        payload = base_payload()
        self.handler.callback((True, False, [], payload, self.message))

        self.handler._varys_client.acknowledge_message.assert_called_once_with(self.message)

    # --- Success path: low_priority ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_low_priority_sends_to_rerun_exchange(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload(low_priority=True)
        self.handler.callback((True, False, [], payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        new_artifact_exchanges = [
            c.kwargs["exchange"]
            for c in send_calls
            if "new_artifact" in c.kwargs.get("exchange", "")
        ]
        self.assertIn("inbound-new_artifact_rerun-mscape", new_artifact_exchanges)
        self.assertNotIn("inbound-new_artifact-mscape", new_artifact_exchanges)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_low_priority_skips_put_linkage_json(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload(low_priority=True)
        self.handler.callback((True, False, [], payload, self.message))

        mock_put_linkage.assert_not_called()

    # --- Success path: test_flag ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_test_flag_skips_new_artifact(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload(test_flag=True)
        # hcid_alerts is False when test_flag path is hit in validate
        self.handler.callback((True, False, False, payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        new_artifact_exchanges = [
            c.kwargs["exchange"]
            for c in send_calls
            if "new_artifact" in c.kwargs.get("exchange", "")
        ]
        self.assertEqual(new_artifact_exchanges, [])
        mock_put_linkage.assert_not_called()

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_test_flag_still_sends_result_and_acknowledges(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload(test_flag=True)
        self.handler.callback((True, False, False, payload, self.message))

        self.handler._varys_client.send.assert_any_call(
            message=payload,
            exchange="inbound-results-mscape-birm",
            queue_suffix="validator",
        )
        mock_put_result.assert_called_once_with(payload, self.handler._log)
        self.handler._varys_client.acknowledge_message.assert_called_once_with(self.message)

    # --- Alert flag ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_alert_sends_to_alert_exchange(self, mock_put_result, mock_put_linkage):
        payload = base_payload()
        self.handler.callback((True, True, [], payload, self.message))

        self.handler._varys_client.send.assert_any_call(
            message=payload,
            exchange="mscape-restricted-announce",
            queue_suffix="alert",
        )
        self.handler._varys_client.send.assert_any_call(
            message={"uuid": payload["uuid"], "description": "Ingest alert: manual intervention required"},
            exchange="mscape-remote-announce",
            queue_suffix="alert",
        )

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_no_alert_does_not_send_to_alert_exchange(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload()
        self.handler.callback((True, False, [], payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        alert_exchanges = [
            c.kwargs["exchange"]
            for c in send_calls
            if c.kwargs.get("queue_suffix") == "alert"
        ]
        self.assertEqual(alert_exchanges, [])

    # --- HCID alerts ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_hcid_alerts_sent_to_hcid_exchange(self, mock_put_result, mock_put_linkage):
        payload = base_payload()
        hcid_alert = {"taxon_id": "1234", "human_readable": "Scary pathogen"}
        self.handler.callback((True, False, [hcid_alert], payload, self.message))

        self.handler._varys_client.send.assert_any_call(
            message={"taxon_id": "1234", "human_readable": "Scary pathogen", "climb_id": "CLIMB001"},
            exchange="mscape-restricted-hcid",
            queue_suffix="alert",
        )

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_hcid_alerts_adds_climb_id_to_each_alert(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload()
        hcid_alerts = [
            {"taxon_id": "1234", "human_readable": "Pathogen A"},
            {"taxon_id": "5678", "human_readable": "Pathogen B"},
        ]
        self.handler.callback((True, False, hcid_alerts, payload, self.message))

        for alert in hcid_alerts:
            self.assertEqual(alert["climb_id"], "CLIMB001")

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_empty_hcid_alerts_sends_no_hcid_messages(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload()
        self.handler.callback((True, False, [], payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        hcid_exchanges = [
            c.kwargs["exchange"]
            for c in send_calls
            if "hcid" in c.kwargs.get("exchange", "")
        ]
        self.assertEqual(hcid_exchanges, [])

    # --- biosample_source_id ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_includes_biosample_source_id_when_present(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload()
        payload["anonymised_biosample_source_id"] = "anon-source-001"
        self.handler.callback((True, False, [], payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        new_artifact_calls = [
            c for c in send_calls if "new_artifact" in c.kwargs.get("exchange", "")
        ]
        self.assertEqual(len(new_artifact_calls), 1)
        sent_payload = new_artifact_calls[0].kwargs["message"]
        self.assertEqual(sent_payload["biosample_source_id"], "anon-source-001")

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_omits_biosample_source_id_when_absent(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload()
        self.handler.callback((True, False, [], payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        new_artifact_calls = [
            c for c in send_calls if "new_artifact" in c.kwargs.get("exchange", "")
        ]
        self.assertEqual(len(new_artifact_calls), 1)
        sent_payload = new_artifact_calls[0].kwargs["message"]
        self.assertNotIn("biosample_source_id", sent_payload)

    # --- Failure path: no rerun ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_no_rerun_acknowledges_message(self, mock_put_result):
        payload = base_payload(rerun=False)
        self.handler.callback((False, False, False, payload, self.message))

        self.handler._varys_client.acknowledge_message.assert_called_once_with(self.message)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_no_rerun_sends_result(self, mock_put_result):
        payload = base_payload(rerun=False)
        self.handler.callback((False, False, False, payload, self.message))

        self.handler._varys_client.send.assert_called_with(
            message=payload,
            exchange="inbound-results-mscape-birm",
            queue_suffix="validator",
        )
        mock_put_result.assert_called_once_with(payload, self.handler._log)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_no_rerun_does_not_nack(self, mock_put_result):
        payload = base_payload(rerun=False)
        self.handler.callback((False, False, False, payload, self.message))

        self.handler._varys_client.nack_message.assert_not_called()

    # --- Failure path: rerun ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_rerun_nacks_without_acking(self, mock_put_result):
        payload = base_payload(rerun=True)
        self.handler.callback((False, False, False, payload, self.message))

        self.handler._varys_client.acknowledge_message.assert_not_called()
        self.handler._varys_client.nack_message.assert_called_once_with(self.message)
        mock_put_result.assert_not_called()

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_rerun_no_alert_under_threshold(self, mock_put_result):
        payload = base_payload(rerun=True)
        for _ in range(4):
            self.handler.callback((False, False, False, payload, self.message))

        alert_calls = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
        ]
        self.assertEqual(alert_calls, [])

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_rerun_sends_alert_at_fifth_failure(self, mock_put_result):
        payload = base_payload(rerun=True)
        for _ in range(5):
            self.handler.callback((False, False, False, payload, self.message))

        alert_calls = self.handler._varys_client.send.call_args_list

        restricted_alerts = [
            c for c in alert_calls
            if c.kwargs.get("queue_suffix") == "alert"
            and c.kwargs.get("exchange") == "mscape-restricted-announce"
        ]
        remote_alerts = [
            c for c in alert_calls
            if c.kwargs.get("queue_suffix") == "alert"
            and c.kwargs.get("exchange") == "mscape-remote-announce"
        ]
        self.assertEqual(len(restricted_alerts), 1)
        self.assertEqual(len(remote_alerts), 1)
        self.assertEqual(remote_alerts[0].kwargs["message"]["uuid"], payload["uuid"])

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_rerun_sends_alert_on_every_subsequent_failure(self, mock_put_result):
        payload = base_payload(rerun=True)
        for _ in range(7):
            self.handler.callback((False, False, False, payload, self.message))

        all_alert_calls = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
        ]
        restricted_alerts = [c for c in all_alert_calls if c.kwargs.get("exchange") == "mscape-restricted-announce"]
        remote_alerts = [c for c in all_alert_calls if c.kwargs.get("exchange") == "mscape-remote-announce"]
        self.assertEqual(len(restricted_alerts), 3)
        self.assertEqual(len(remote_alerts), 3)


class TestWorkerPoolHandlerErrorCallback(unittest.TestCase):
    def setUp(self):
        with patch("multiprocessing.Pool"):
            self.handler = worker_pool_handler(
                workers=2, logger=MagicMock(), varys_client=MagicMock(), project="mscape"
            )

    @patch("os.remove")
    def test_error_callback_sends_dead_worker_message(self, mock_os_remove):
        exc = Exception("Worker exploded")
        self.handler.error_callback(exc)

        self.handler._varys_client.send.assert_called_once_with(
            message=f"mscape ingest worker failed with unhandled exception: {exc}",
            exchange="mscape-restricted-announce",
            queue_suffix="dead_worker",
        )

    @patch("roz_scripts.mscape.mscape_ingest_validation.Path")
    def test_error_callback_removes_healthy_file(self, mock_path):
        self.handler.error_callback(Exception("boom"))
        mock_path.assert_called_with("/tmp/healthy")
        mock_path.return_value.unlink.assert_called_once_with(missing_ok=True)


# ---------------------------------------------------------------------------
# run() — rerun low priority / message prioritisation tests
# ---------------------------------------------------------------------------

class TestRunMessagePrioritisation(unittest.TestCase):
    """Test the message prioritisation logic inside run():
    - A priority message preempts any pending rerun message (which is nacked).
    - When only a rerun message is available it is submitted normally.
    - When only a priority message is available it is submitted normally.
    - When no messages are available the loop sleeps and continues.
    """

    def _make_args(self, **kwargs):
        return make_args(**kwargs)

    def _run_one_iteration(
        self,
        priority_msg,
        rerun_msg,
        mock_varys_cls,
        mock_pipeline_cls,
        mock_pool_cls,
        mock_logger_cls,
    ):
        """Configure mocks so run() processes one iteration then terminates."""
        mock_varys = mock_varys_cls.return_value
        mock_pool = mock_pool_cls.return_value

        # First iteration returns the desired messages; second iteration raises
        # KeyboardInterrupt which is caught by `except BaseException:` in run().
        call_count = [0]

        def receive_side_effect(**kwargs):
            call_count[0] += 1
            exchange = kwargs.get("exchange", "")
            if call_count[0] == 1:
                return priority_msg
            elif call_count[0] == 2:
                return rerun_msg
            else:
                raise KeyboardInterrupt("stop loop")

        mock_varys.receive.side_effect = receive_side_effect

        return mock_varys, mock_pool

    @patch("sys.exit")
    @patch("os.remove")
    @patch("os.path.exists", return_value=False)
    @patch("time.sleep")
    @patch("roz_scripts.mscape.mscape_ingest_validation.worker_pool_handler")
    @patch("roz_scripts.mscape.mscape_ingest_validation.pipeline")
    @patch("roz_scripts.mscape.mscape_ingest_validation.Varys")
    @patch("roz_scripts.mscape.mscape_ingest_validation.init_logger")
    def test_priority_message_is_submitted_when_both_present(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
        mock_sleep, mock_exists, mock_remove, mock_exit
    ):
        priority_msg = make_message("priority-uuid")
        rerun_msg = make_message("rerun-uuid")

        mock_varys, mock_pool = self._run_one_iteration(
            priority_msg, rerun_msg,
            mock_varys_cls, mock_pipeline_cls, mock_pool_cls, mock_logger
        )

        run(self._make_args())

        mock_pool.submit_job.assert_called_once_with(
            message=priority_msg,
            args=unittest.mock.ANY,
            ingest_pipe=mock_pipeline_cls.return_value,
        )

    @patch("sys.exit")
    @patch("os.remove")
    @patch("os.path.exists", return_value=False)
    @patch("time.sleep")
    @patch("roz_scripts.mscape.mscape_ingest_validation.worker_pool_handler")
    @patch("roz_scripts.mscape.mscape_ingest_validation.pipeline")
    @patch("roz_scripts.mscape.mscape_ingest_validation.Varys")
    @patch("roz_scripts.mscape.mscape_ingest_validation.init_logger")
    def test_rerun_message_is_nacked_when_priority_present(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
        mock_sleep, mock_exists, mock_remove, mock_exit
    ):
        priority_msg = make_message("priority-uuid")
        rerun_msg = make_message("rerun-uuid")

        mock_varys, mock_pool = self._run_one_iteration(
            priority_msg, rerun_msg,
            mock_varys_cls, mock_pipeline_cls, mock_pool_cls, mock_logger
        )

        run(self._make_args())

        mock_varys.nack_message.assert_called_once_with(rerun_msg)

    @patch("sys.exit")
    @patch("os.remove")
    @patch("os.path.exists", return_value=False)
    @patch("time.sleep")
    @patch("roz_scripts.mscape.mscape_ingest_validation.worker_pool_handler")
    @patch("roz_scripts.mscape.mscape_ingest_validation.pipeline")
    @patch("roz_scripts.mscape.mscape_ingest_validation.Varys")
    @patch("roz_scripts.mscape.mscape_ingest_validation.init_logger")
    def test_only_priority_message_submitted_with_no_nack(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
        mock_sleep, mock_exists, mock_remove, mock_exit
    ):
        priority_msg = make_message("priority-uuid")

        mock_varys, mock_pool = self._run_one_iteration(
            priority_msg, None,  # no rerun message
            mock_varys_cls, mock_pipeline_cls, mock_pool_cls, mock_logger
        )

        run(self._make_args())

        mock_pool.submit_job.assert_called_once_with(
            message=priority_msg,
            args=unittest.mock.ANY,
            ingest_pipe=mock_pipeline_cls.return_value,
        )
        mock_varys.nack_message.assert_not_called()

    @patch("sys.exit")
    @patch("os.remove")
    @patch("os.path.exists", return_value=False)
    @patch("time.sleep")
    @patch("roz_scripts.mscape.mscape_ingest_validation.worker_pool_handler")
    @patch("roz_scripts.mscape.mscape_ingest_validation.pipeline")
    @patch("roz_scripts.mscape.mscape_ingest_validation.Varys")
    @patch("roz_scripts.mscape.mscape_ingest_validation.init_logger")
    def test_rerun_message_submitted_when_no_priority(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
        mock_sleep, mock_exists, mock_remove, mock_exit
    ):
        rerun_msg = make_message("rerun-uuid")

        mock_varys, mock_pool = self._run_one_iteration(
            None, rerun_msg,  # no priority message
            mock_varys_cls, mock_pipeline_cls, mock_pool_cls, mock_logger
        )

        run(self._make_args())

        mock_pool.submit_job.assert_called_once_with(
            message=rerun_msg,
            args=unittest.mock.ANY,
            ingest_pipe=mock_pipeline_cls.return_value,
        )
        mock_varys.nack_message.assert_not_called()

    @patch("sys.exit")
    @patch("os.remove")
    @patch("os.path.exists", return_value=False)
    @patch("time.sleep")
    @patch("roz_scripts.mscape.mscape_ingest_validation.worker_pool_handler")
    @patch("roz_scripts.mscape.mscape_ingest_validation.pipeline")
    @patch("roz_scripts.mscape.mscape_ingest_validation.Varys")
    @patch("roz_scripts.mscape.mscape_ingest_validation.init_logger")
    def test_no_messages_sleeps_and_does_not_submit(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
        mock_sleep, mock_exists, mock_remove, mock_exit
    ):
        """When both queues are empty the loop should sleep without submitting a job."""
        mock_varys = mock_varys_cls.return_value
        mock_pool = mock_pool_cls.return_value

        call_count = [0]

        def receive_side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                return None  # both queues empty on first iteration
            raise KeyboardInterrupt("stop loop")

        mock_varys.receive.side_effect = receive_side_effect

        run(self._make_args())

        mock_pool.submit_job.assert_not_called()
        mock_sleep.assert_any_call(60)
