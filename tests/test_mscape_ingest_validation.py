import argparse
import json
import os
import shutil
import sys
import tempfile
import unittest
from collections import namedtuple
from unittest.mock import MagicMock, call, patch

import pytest

from roz_scripts.mscape.mscape_ingest_validation import (
    add_classifier_calls,
    handle_spike_ins,
    log_publish_delay,
    validate,
    worker_pool_handler,
    prepare_published_rerun,
    ret_0_parser,
    run,
)
from roz_scripts.utils.health import HealthState
from roz_scripts.utils.utils import PodResources


def setUpModule():
    os.environ.update({
        "ONYX_DOMAIN": "https://test.onyx",
        "ONYX_TOKEN": "testtoken",
        "VARYS_CFG": "/tmp/varys.cfg",
        "AWS_ACCESS_KEY_ID": "test-key-id",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "NXF_WORK": "/tmp/nxf_work",
        "NXF_HOME": "/tmp/nxf_home",
        "SCYLLA_K2_DB_PATH": "/tmp/k2",
        "SCYLLA_K2_DB_DATE": "2024-01-01",
        "SCYLLA_TAXONOMY_PATH": "/tmp/taxonomy",
        "SCYLLA_TAXONOMY_DATE": "2024-01-01",
    })


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
        nxf_pod_resources=PodResources(),
        config={},
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

        handler = worker_pool_handler(
            workers=4,
            logger=log,
            varys_client=varys,
            project="mscape",
            health=MagicMock(),
            config=MagicMock(),
        )

        mock_pool_cls.assert_called_once_with(processes=4)
        self.assertEqual(handler._project, "mscape")


class TestWorkerPoolHandlerSubmitJob(unittest.TestCase):
    def setUp(self):
        with patch("multiprocessing.Pool"):
            self.handler = worker_pool_handler(
                workers=2,
                logger=MagicMock(),
                varys_client=MagicMock(),
                project="mscape",
                health=MagicMock(),
                config=MagicMock(),
            )
        self.message = make_message()
        self.args = make_args()
        self.ingest_pipe = MagicMock()

    def test_submit_job_calls_apply_async_with_correct_kwargs(self):
        self.handler.submit_job(self.message, self.args, self.ingest_pipe, low_priority=True)

        _, kwargs = self.handler.worker_pool.apply_async.call_args
        self.assertEqual(kwargs["func"], validate)
        self.assertEqual(
            kwargs["kwds"],
            {
                "message": self.message,
                "args": self.args,
                "ingest_pipe": self.ingest_pipe,
                "low_priority": True,
            },
        )
        self.assertEqual(kwargs["callback"], self.handler.callback)
        self.assertEqual(kwargs["error_callback"].func, self.handler.error_callback)
        self.assertEqual(kwargs["error_callback"].args, (self.message,))

    def test_submit_job_low_priority_defaults_to_false(self):
        self.handler.submit_job(self.message, self.args, self.ingest_pipe)

        _, kwargs = self.handler.worker_pool.apply_async.call_args
        self.assertFalse(kwargs["kwds"]["low_priority"])


class TestWorkerPoolHandlerCallback(unittest.TestCase):
    def setUp(self):
        with patch("multiprocessing.Pool"):
            self.handler = worker_pool_handler(
                workers=2,
                logger=MagicMock(),
                varys_client=MagicMock(),
                project="mscape",
                health=MagicMock(),
                config=MagicMock(),
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
        mock_put_result.assert_called_once_with(payload, self.handler._log, self.handler._config)

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

        mock_put_linkage.assert_called_once_with(payload=payload, log=self.handler._log, config=self.handler._config)

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

    # --- Success path: rerun_of_published ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_rerun_of_published_skips_result_and_json(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload(low_priority=True)
        payload["rerun_of_published"] = True
        self.handler.callback((True, False, [], payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        result_exchanges = [
            c.kwargs["exchange"]
            for c in send_calls
            if c.kwargs.get("exchange", "").startswith("inbound-results-")
        ]
        self.assertEqual(result_exchanges, [])
        mock_put_result.assert_not_called()

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_rerun_of_published_still_sends_rerun_new_artifact(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload(low_priority=True)
        payload["rerun_of_published"] = True
        self.handler.callback((True, False, [], payload, self.message))

        send_calls = self.handler._varys_client.send.call_args_list
        new_artifact_exchanges = [
            c.kwargs["exchange"]
            for c in send_calls
            if "new_artifact" in c.kwargs.get("exchange", "")
        ]
        self.assertIn("inbound-new_artifact_rerun-mscape", new_artifact_exchanges)
        self.handler._varys_client.acknowledge_message.assert_called_once_with(self.message)

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
        mock_put_result.assert_called_once_with(payload, self.handler._log, self.handler._config)
        self.handler._varys_client.acknowledge_message.assert_called_once_with(self.message)

    # --- Alert flag ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_alert_surfaces_recorded_reasons(
        self, mock_put_result, mock_put_linkage
    ):
        """Regression test: an alert must say which subsystem caused it,
        both in the local log and in the remote alert description. An
        unattributed alert leaves the operator guessing across nine
        subsystems."""
        payload = base_payload()
        payload["alert_reasons"] = ["taxon_reports", "spike_in", "taxon_reports"]

        self.handler.callback((True, True, [], payload, self.message))

        self.handler._varys_client.send.assert_any_call(
            message={
                "source": "mscape",
                "description": (
                    "Ingest alert: manual intervention required; "
                    "cause(s): taxon_reports, spike_in"
                ),
                "uuid": payload["uuid"],
                "priority": "critical",
            },
            exchange="remote-announce",
            queue_suffix="alert",
        )

        logged = " ".join(
            str(c.args[0]) for c in self.handler._log.error.call_args_list
        )
        self.assertIn("taxon_reports", logged)
        self.assertIn("spike_in", logged)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_alert_on_requeued_record_is_not_critical(
        self, mock_put_result, mock_put_linkage
    ):
        """Regression test: a record that alerts AND sets rerun comes back
        through _handle_result on every redelivery. Critical bypasses the
        consumer's rate limit without updating its cooldown, so these must
        stay routine or a requeue loop emits an unthrottled critical per
        attempt."""
        payload = base_payload(rerun=True)
        payload["alert_reasons"] = ["taxon_reports"]

        self.handler.callback((False, True, [], payload, self.message))

        remote = [
            c
            for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("exchange") == "remote-announce"
        ]
        self.assertTrue(remote, "no remote alert was sent")
        # send_admin_alert omits the key entirely for routine - only a
        # critical alert carries a priority field.
        self.assertNotIn("priority", remote[0].kwargs["message"])
        self.assertIn(
            "will be retried", remote[0].kwargs["message"]["description"]
        )

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_alert_on_terminal_record_stays_critical(
        self, mock_put_result, mock_put_linkage
    ):
        """Negative control: a terminal record produces exactly one alert
        ever, so it keeps its critical priority."""
        payload = base_payload(rerun=False)
        payload["alert_reasons"] = ["taxon_reports"]

        self.handler.callback((True, True, [], payload, self.message))

        remote = [
            c
            for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("exchange") == "remote-announce"
        ]
        self.assertTrue(remote, "no remote alert was sent")
        self.assertEqual(remote[0].kwargs["message"]["priority"], "critical")
        self.assertNotIn(
            "will be retried", remote[0].kwargs["message"]["description"]
        )

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_alert_without_reasons_says_so(
        self, mock_put_result, mock_put_linkage
    ):
        """An alert raised without recording a reason is a bug at the call
        site - the output must say so rather than silently omitting it."""
        payload = base_payload()
        self.handler.callback((True, True, [], payload, self.message))

        logged = " ".join(
            str(c.args[0]) for c in self.handler._log.error.call_args_list
        )
        self.assertIn("reason not recorded", logged)


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
            message={
                "source": "mscape",
                "description": (
                    "Ingest alert: manual intervention required; "
                    "cause(s): reason not recorded"
                ),
                "uuid": payload["uuid"],
                "priority": "critical",
            },
            exchange="remote-announce",
            queue_suffix="alert",
        )

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_manual_intervention_alert_is_always_critical(
        self, mock_put_result, mock_put_linkage
    ):
        """Every path that sets the `alert` flag also leaves `rerun` unset,
        so the message is acked rather than requeued - this is the only
        alert this record will ever produce, and must not be silently
        dropped by the rate limiter."""
        payload = base_payload()
        self.handler.callback((True, True, [], payload, self.message))

        remote_alerts = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
            and c.kwargs.get("exchange") == "remote-announce"
        ]
        self.assertEqual(len(remote_alerts), 1)
        self.assertEqual(remote_alerts[0].kwargs["message"]["priority"], "critical")

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
        mock_put_result.assert_called_once_with(payload, self.handler._log, self.handler._config)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_no_rerun_does_not_nack(self, mock_put_result):
        payload = base_payload(rerun=False)
        self.handler.callback((False, False, False, payload, self.message))

        self.handler._varys_client.nack_message.assert_not_called()

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_rerun_of_published_no_retry_skips_result_and_acknowledges(
        self, mock_put_result
    ):
        payload = base_payload(rerun=False, low_priority=True)
        payload["rerun_of_published"] = True
        self.handler.callback((False, False, False, payload, self.message))

        self.handler._varys_client.acknowledge_message.assert_called_once_with(self.message)
        send_calls = self.handler._varys_client.send.call_args_list
        result_exchanges = [
            c.kwargs["exchange"]
            for c in send_calls
            if c.kwargs.get("exchange", "").startswith("inbound-results-")
        ]
        self.assertEqual(result_exchanges, [])
        mock_put_result.assert_not_called()

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
            and c.kwargs.get("exchange") == "remote-announce"
        ]
        self.assertEqual(len(restricted_alerts), 1)
        self.assertEqual(len(remote_alerts), 1)
        self.assertEqual(remote_alerts[0].kwargs["message"]["uuid"], payload["uuid"])
        self.assertEqual(remote_alerts[0].kwargs["message"]["source"], "mscape")
        # First time this record has ever crossed the threshold - critical.
        self.assertEqual(remote_alerts[0].kwargs["message"]["priority"], "critical")

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_rerun_critical_only_on_first_crossing(self, mock_put_result):
        payload = base_payload(rerun=True)
        for _ in range(7):
            self.handler.callback((False, False, False, payload, self.message))

        remote_alerts = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
            and c.kwargs.get("exchange") == "remote-announce"
        ]
        priorities = [c.kwargs["message"].get("priority") for c in remote_alerts]
        self.assertEqual(priorities, ["critical", None, None])

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_failure_rerun_each_uuid_gets_its_own_critical_first_alert(
        self, mock_put_result
    ):
        payload_a = base_payload(uuid="uuid-a", rerun=True)
        payload_b = base_payload(uuid="uuid-b", rerun=True)
        for _ in range(5):
            self.handler.callback((False, False, False, payload_a, self.message))
        for _ in range(5):
            self.handler.callback((False, False, False, payload_b, self.message))

        remote_alerts = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
            and c.kwargs.get("exchange") == "remote-announce"
        ]
        self.assertEqual(len(remote_alerts), 2)
        for call in remote_alerts:
            self.assertEqual(call.kwargs["message"]["priority"], "critical")

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
        remote_alerts = [c for c in all_alert_calls if c.kwargs.get("exchange") == "remote-announce"]
        self.assertEqual(len(restricted_alerts), 3)
        self.assertEqual(len(remote_alerts), 3)

    # --- Failure path: rerun-of-published scylla deadletter ---
    # See .claude/plans/rerun-scylla-deadlettering.md

    def _dead_letter_sends(self):
        return [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "dead_letter"
        ]

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_scylla_failure_no_deadletter_at_twenty(self, mock_put_result):
        payload = base_payload(rerun=True, low_priority=True)
        payload["rerun_of_published"] = True
        payload["scylla_failure"] = "job_failed"
        for _ in range(20):
            self.handler.callback((False, False, False, payload, self.message))

        self.assertEqual(self._dead_letter_sends(), [])
        self.assertEqual(self.handler._varys_client.nack_message.call_count, 20)
        self.handler._varys_client.acknowledge_message.assert_not_called()

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_scylla_failure_deadletters_at_twenty_first(self, mock_put_result):
        payload = base_payload(rerun=True, low_priority=True)
        payload["rerun_of_published"] = True
        payload["scylla_failure"] = "job_failed"
        for _ in range(21):
            self.handler.callback((False, False, False, payload, self.message))

        dead_letter_calls = self._dead_letter_sends()
        self.assertEqual(len(dead_letter_calls), 1)
        self.assertEqual(dead_letter_calls[0].kwargs["exchange"], "mscape-restricted-announce")
        self.assertIs(dead_letter_calls[0].kwargs["message"], payload)
        # Only the 21st call deadletters - the first 20 nack as usual.
        self.assertEqual(self.handler._varys_client.nack_message.call_count, 20)
        self.handler._varys_client.acknowledge_message.assert_called_once_with(self.message)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_deadletter_alert_is_critical(self, mock_put_result):
        payload = base_payload(rerun=True, low_priority=True)
        payload["rerun_of_published"] = True
        payload["scylla_failure"] = "job_failed"
        for _ in range(21):
            self.handler.callback((False, False, False, payload, self.message))

        remote_alerts = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
            and c.kwargs.get("exchange") == "remote-announce"
            and "dead-lettered" in c.kwargs["message"]["description"]
        ]
        self.assertEqual(len(remote_alerts), 1)
        self.assertEqual(remote_alerts[0].kwargs["message"]["priority"], "critical")

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_scylla_failure_mixed_reasons_deadletter_breakdown(self, mock_put_result):
        payload = base_payload(rerun=True, low_priority=True)
        payload["rerun_of_published"] = True
        payload["climb_id"] = "CLIMB001"

        reasons = (["timeout"] * 14) + (["job_failed"] * 7)
        for reason in reasons:
            payload["scylla_failure"] = reason
            self.handler.callback((False, False, False, payload, self.message))

        self.assertEqual(len(self._dead_letter_sends()), 1)

        # The pre-existing alert-at-5-and-every-subsequent-failure mechanism
        # (keyed on `_failure_log`, unrelated to the scylla deadletter
        # counter) also fires along the way - filter down to the deadletter's
        # own alert specifically.
        remote_alerts = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
            and c.kwargs.get("exchange") == "remote-announce"
            and "dead-lettered" in c.kwargs["message"]["description"]
        ]
        self.assertEqual(len(remote_alerts), 1)
        description = remote_alerts[0].kwargs["message"]["description"]
        self.assertIn("14 timeout", description)
        self.assertIn("7 job_failed", description)
        # climb_id must never appear in the off-prem remote-announce alert -
        # only the opaque uuid is carried for cross-referencing. It's still
        # available to admins via the restricted dead_letter payload.
        self.assertNotIn("CLIMB001", description)
        self.assertEqual(
            self._dead_letter_sends()[0].kwargs["message"]["climb_id"], "CLIMB001"
        )

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_rerun_of_published_without_scylla_failure_tag_never_deadletters(
        self, mock_put_result
    ):
        """No `scylla_failure` tag - e.g. an infrastructure failure
        (RC_INFRASTRUCTURE) or a non-scylla failure such as the post-pipeline
        S3 upload path - must not count towards the threshold."""
        payload = base_payload(rerun=True, low_priority=True)
        payload["rerun_of_published"] = True
        for _ in range(25):
            self.handler.callback((False, False, False, payload, self.message))

        self.assertEqual(self._dead_letter_sends(), [])
        self.assertEqual(self.handler._varys_client.nack_message.call_count, 25)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_scylla_failure_without_rerun_of_published_never_deadletters(
        self, mock_put_result
    ):
        """A full re-validation rerun (no `rerun_of_published`) is out of
        scope - it keeps retrying forever, however many scylla failures."""
        payload = base_payload(rerun=True, low_priority=True)
        payload["scylla_failure"] = "job_failed"
        for _ in range(25):
            self.handler.callback((False, False, False, payload, self.message))

        self.assertEqual(self._dead_letter_sends(), [])

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_scylla_failure_first_run_never_deadletters(self, mock_put_result):
        payload = base_payload(rerun=True, low_priority=False)
        payload["scylla_failure"] = "job_failed"
        for _ in range(25):
            self.handler.callback((False, False, False, payload, self.message))

        self.assertEqual(self._dead_letter_sends(), [])

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_linkage_json")
    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_success_pops_scylla_failure_counter(
        self, mock_put_result, mock_put_linkage
    ):
        payload = base_payload(rerun=True, low_priority=True)
        payload["rerun_of_published"] = True
        payload["scylla_failure"] = "job_failed"
        for _ in range(5):
            self.handler.callback((False, False, False, payload, self.message))

        self.assertIn(payload["uuid"], self.handler._scylla_unknown_log)

        success_payload = base_payload(rerun=False, low_priority=True, test_flag=True)
        success_payload["rerun_of_published"] = True
        self.handler.callback((True, False, [], success_payload, self.message))

        self.assertNotIn(success_payload["uuid"], self.handler._scylla_unknown_log)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_deadletter_pops_scylla_failure_counter(self, mock_put_result):
        payload = base_payload(rerun=True, low_priority=True)
        payload["rerun_of_published"] = True
        payload["scylla_failure"] = "job_failed"
        for _ in range(21):
            self.handler.callback((False, False, False, payload, self.message))

        self.assertNotIn(payload["uuid"], self.handler._scylla_unknown_log)

    @patch("roz_scripts.mscape.mscape_ingest_validation.put_result_json")
    def test_callback_deadletter_does_not_requeue_or_put_result_json(
        self, mock_put_result
    ):
        payload = base_payload(rerun=True, low_priority=True)
        payload["rerun_of_published"] = True
        payload["scylla_failure"] = "job_failed"
        for _ in range(21):
            self.handler.callback((False, False, False, payload, self.message))

        mock_put_result.assert_not_called()
        self.handler._health.mark_fatal.assert_not_called()

        result_sends = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("exchange", "").startswith("inbound-results-")
        ]
        self.assertEqual(result_sends, [])

        # publish-before-ack: the dead_letter publish must be confirmed
        # before the message is acknowledged, or a failed publish would lose
        # the payload permanently (the bug class fixed by persist_publish_ack).
        call_names = [c[0] for c in self.handler._varys_client.mock_calls]
        dead_letter_idx = next(
            i
            for i, c in enumerate(self.handler._varys_client.mock_calls)
            if c[0] == "send" and c.kwargs.get("queue_suffix") == "dead_letter"
        )
        ack_idx = call_names.index("acknowledge_message")
        self.assertLess(dead_letter_idx, ack_idx)


class TestRet0ParserScyllaFailureTagging(unittest.TestCase):
    """See .claude/plans/rerun-scylla-deadlettering.md §4.1."""

    def _make_result_dir(self, uuid, rows):
        tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmpdir, ignore_errors=True)
        pipeline_info = os.path.join(tmpdir, "pipeline_info")
        os.makedirs(pipeline_info)
        with open(
            os.path.join(pipeline_info, f"execution_trace_{uuid}.txt"), "w"
        ) as fh:
            fh.write("name\texit\tstatus\n")
            for name, exit_code, status in rows:
                fh.write(f"{name}\t{exit_code}\t{status}\n")
        with open(
            os.path.join(pipeline_info, f"workflow_version_{uuid}.txt"), "w"
        ) as fh:
            fh.write("1.0.0\n")
        return tmpdir

    def test_unrecognised_exit_code_tagged_process_failure(self):
        uuid = "trace-uuid-1"
        result_path = self._make_result_dir(uuid, [("mystery_process", "42", "FAILED")])
        payload = base_payload(uuid=uuid)

        ingest_fail, payload = ret_0_parser(MagicMock(), payload, result_path)

        self.assertTrue(ingest_fail)
        self.assertTrue(payload["rerun"])
        self.assertEqual(payload["scylla_failure"], "process_failure")

    def test_unparsable_trace_tagged_trace_unparsable(self):
        uuid = "trace-uuid-2"
        result_path = self._make_result_dir(uuid, [])
        os.remove(
            os.path.join(result_path, "pipeline_info", f"execution_trace_{uuid}.txt")
        )
        payload = base_payload(uuid=uuid)

        ingest_fail, payload = ret_0_parser(MagicMock(), payload, result_path)

        self.assertTrue(ingest_fail)
        self.assertTrue(payload["rerun"])
        self.assertEqual(payload["scylla_failure"], "trace_unparsable")

    def test_known_exit_pair_not_tagged_scylla_failure(self):
        """Known (process, exit) pairs are already fully classified and
        terminal on the first attempt (§3.2) - they must not be tagged, or
        they would start counting towards the deadletter threshold."""
        uuid = "trace-uuid-3"
        result_path = self._make_result_dir(uuid, [("fastp", "255", "FAILED")])
        payload = base_payload(uuid=uuid)

        ingest_fail, payload = ret_0_parser(MagicMock(), payload, result_path)

        self.assertTrue(ingest_fail)
        self.assertFalse(payload["rerun"])
        self.assertNotIn("scylla_failure", payload)


class TestWorkerPoolHandlerErrorCallback(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp_dir.cleanup)
        self.health = HealthState(self._tmp_dir.name)

        with patch("multiprocessing.Pool"):
            self.handler = worker_pool_handler(
                workers=2,
                logger=MagicMock(),
                varys_client=MagicMock(),
                project="mscape",
                health=self.health,
                config=MagicMock(),
            )

    def test_error_callback_sends_dead_worker_message(self):
        exc = Exception("Worker exploded")
        self.handler.error_callback(make_message(), exc)

        self.handler._varys_client.send.assert_any_call(
            message=f"mscape ingest worker failed with unhandled exception: {exc}",
            exchange="mscape-restricted-announce",
            queue_suffix="dead_worker",
        )

    def test_error_callback_nacks_message(self):
        exc = Exception("Worker exploded")
        message = make_message()
        self.handler.error_callback(message, exc)

        self.handler._varys_client.nack_message.assert_called_once_with(message)

    def test_error_callback_sends_admin_alert(self):
        exc = Exception("Worker exploded")
        self.handler.error_callback(make_message(), exc)

        self.handler._varys_client.send.assert_any_call(
            message={
                "source": "mscape",
                "description": f"ingest worker failed with unhandled exception: {exc}",
            },
            exchange="remote-announce",
            queue_suffix="alert",
        )

    def test_error_callback_marks_health_fatal(self):
        self.handler.error_callback(make_message(), Exception("boom"))

        from pathlib import Path

        fatal_path = Path(self._tmp_dir.name) / "fatal"
        self.assertTrue(fatal_path.exists())
        self.assertIn("boom", fatal_path.read_text())


# ---------------------------------------------------------------------------
# prepare_published_rerun()
# ---------------------------------------------------------------------------

def published_record(**overrides):
    record = {
        "climb_id": "CLIMB001",
        "is_published": True,
        "platform": "ont",
        "site": "birm",
        "run_index": "anon-idx-001",
        "run_id": "anon-run-001",
        "biosample_id": "anon-sample-001",
        "biosample_source_id": None,
        "fastq_1": "s3://mscape-published-reads/CLIMB001.fastq.gz",
    }
    record.update(overrides)
    return record


def rerun_payload(**overrides):
    payload = {
        "uuid": "rerun-uuid-1234",
        "project": "mscape",
        "climb_id": "CLIMB001",
        "low_priority": True,
        "rerun": False,
    }
    payload.update(overrides)
    return payload


class TestPreparePublishedRerun(unittest.TestCase):
    def setUp(self):
        self.log = MagicMock()

    @patch("roz_scripts.mscape.mscape_ingest_validation.are_files_empty", return_value=False)
    @patch("roz_scripts.mscape.mscape_ingest_validation.do_uris_exist", return_value=True)
    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_ont_happy_path(self, mock_get_record, mock_exist, mock_empty):
        mock_get_record.return_value = (False, published_record())

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertTrue(ok)
        self.assertFalse(retryable)
        self.assertFalse(alert)
        self.assertEqual(read_uris, ("s3://mscape-published-reads/CLIMB001.fastq.gz",))
        self.assertEqual(payload["platform"], "ont")
        self.assertEqual(payload["site"], "birm")
        self.assertEqual(payload["anonymised_run_index"], "anon-idx-001")
        self.assertEqual(payload["anonymised_run_id"], "anon-run-001")
        self.assertEqual(payload["anonymised_biosample_id"], "anon-sample-001")
        self.assertNotIn("anonymised_biosample_source_id", payload)
        self.assertTrue(payload["onyx_create_status"])
        self.assertTrue(payload["created"])
        self.assertTrue(payload["rerun_of_published"])
        self.assertFalse(payload["test_flag"])

    @patch("roz_scripts.mscape.mscape_ingest_validation.are_files_empty", return_value=False)
    @patch("roz_scripts.mscape.mscape_ingest_validation.do_uris_exist", return_value=True)
    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_illumina_happy_path_read_uri_order(self, mock_get_record, mock_exist, mock_empty):
        mock_get_record.return_value = (
            False,
            published_record(
                platform="illumina",
                fastq_1="s3://mscape-published-reads/CLIMB001_1.fastq.gz",
                fastq_2="s3://mscape-published-reads/CLIMB001_2.fastq.gz",
            ),
        )

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertTrue(ok)
        self.assertEqual(
            read_uris,
            (
                "s3://mscape-published-reads/CLIMB001_1.fastq.gz",
                "s3://mscape-published-reads/CLIMB001_2.fastq.gz",
            ),
        )

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_record_alert_is_retryable(self, mock_get_record):
        mock_get_record.return_value = (True, None)

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertFalse(ok)
        self.assertTrue(retryable)
        self.assertTrue(alert)

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_no_record_found_is_not_retryable(self, mock_get_record):
        mock_get_record.return_value = (False, None)

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertFalse(ok)
        self.assertFalse(retryable)
        self.assertTrue(alert)

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_unpublished_record_is_not_retryable(self, mock_get_record):
        mock_get_record.return_value = (False, published_record(is_published=False))

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertFalse(ok)
        self.assertFalse(retryable)
        self.assertTrue(alert)

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_unrecognised_platform_is_not_retryable(self, mock_get_record):
        mock_get_record.return_value = (False, published_record(platform="pacbio"))

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertFalse(ok)
        self.assertFalse(retryable)

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_illumina_missing_fastq_2_is_not_retryable(self, mock_get_record):
        mock_get_record.return_value = (
            False,
            published_record(
                platform="illumina",
                fastq_1="s3://mscape-published-reads/CLIMB001_1.fastq.gz",
                fastq_2=None,
            ),
        )

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertFalse(ok)
        self.assertFalse(retryable)

    @patch("roz_scripts.mscape.mscape_ingest_validation.do_uris_exist", return_value=False)
    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_missing_published_reads_is_not_retryable(self, mock_get_record, mock_exist):
        mock_get_record.return_value = (False, published_record())

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertFalse(ok)
        self.assertFalse(retryable)

    @patch("roz_scripts.mscape.mscape_ingest_validation.are_files_empty", return_value=True)
    @patch("roz_scripts.mscape.mscape_ingest_validation.do_uris_exist", return_value=True)
    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_empty_published_reads_is_not_retryable(self, mock_get_record, mock_exist, mock_empty):
        mock_get_record.return_value = (False, published_record())

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertFalse(ok)
        self.assertFalse(retryable)

    @patch("roz_scripts.mscape.mscape_ingest_validation.are_files_empty", return_value=False)
    @patch("roz_scripts.mscape.mscape_ingest_validation.do_uris_exist", return_value=True)
    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_get_record")
    def test_biosample_source_id_included_when_present(self, mock_get_record, mock_exist, mock_empty):
        mock_get_record.return_value = (
            False, published_record(biosample_source_id="anon-source-001")
        )

        ok, retryable, alert, record, read_uris, payload = prepare_published_rerun(
            payload=rerun_payload(), log=self.log
        )

        self.assertTrue(ok)
        self.assertEqual(payload["anonymised_biosample_source_id"], "anon-source-001")


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

    def setUp(self):
        patcher = patch(
            "roz_scripts.mscape.mscape_ingest_validation.get_pod_namespace",
            return_value="climb-gre-test",
        )
        self.mock_get_pod_namespace = patcher.start()
        self.addCleanup(patcher.stop)

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
    @patch("roz_scripts.mscape.mscape_ingest_validation.HealthState")
    def test_priority_message_is_submitted_when_both_present(
        self, mock_health_cls, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
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
            low_priority=False,
        )

    @patch("sys.exit")
    @patch("os.remove")
    @patch("os.path.exists", return_value=False)
    @patch("time.sleep")
    @patch("roz_scripts.mscape.mscape_ingest_validation.worker_pool_handler")
    @patch("roz_scripts.mscape.mscape_ingest_validation.pipeline")
    @patch("roz_scripts.mscape.mscape_ingest_validation.Varys")
    @patch("roz_scripts.mscape.mscape_ingest_validation.init_logger")
    @patch("roz_scripts.mscape.mscape_ingest_validation.HealthState")
    def test_rerun_message_is_nacked_when_priority_present(
        self, mock_health_cls, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
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
    @patch("roz_scripts.mscape.mscape_ingest_validation.HealthState")
    def test_only_priority_message_submitted_with_no_nack(
        self, mock_health_cls, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
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
            low_priority=False,
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
    @patch("roz_scripts.mscape.mscape_ingest_validation.HealthState")
    def test_rerun_message_submitted_when_no_priority(
        self, mock_health_cls, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
        mock_sleep, mock_exists, mock_remove, mock_exit
    ):
        rerun_msg = make_message("rerun-uuid")

        mock_varys, mock_pool = self._run_one_iteration(
            None, rerun_msg,  # no priority message
            mock_varys_cls, mock_pipeline_cls, mock_pool_cls, mock_logger
        )

        run(self._make_args())

        # Regression test for a routing bug: submit_job() must be told this
        # came from the rerun exchange, otherwise validate() defaults
        # low_priority to False and a successful rerun gets routed to the
        # normal new_artifact exchange (and gets a spurious put_linkage_json
        # call) instead of the rerun one.
        mock_pool.submit_job.assert_called_once_with(
            message=rerun_msg,
            args=unittest.mock.ANY,
            ingest_pipe=mock_pipeline_cls.return_value,
            low_priority=True,
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
    @patch("roz_scripts.mscape.mscape_ingest_validation.HealthState")
    def test_no_messages_sleeps_and_does_not_submit(
        self, mock_health_cls, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_pool_cls,
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


class TestNestedFieldAlertSeverity(unittest.TestCase):
    """The infra-vs-data split that onyx_update makes must survive the
    callers: an OnyxRequestError (submitter data problem, returned as
    (True, False)) must not be escalated into an admin alert, while a
    genuine infrastructure failure ((True, True)) must still alert.

    Without the negative controls these tests would pass equally well
    against code that had simply deleted the alerts.
    """

    def setUp(self):
        self.log = MagicMock()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.payload = {
            "uuid": "test-uuid-1234",
            "climb_id": "C-1234567890",
            "project": "mscape",
            "platform": "ont",
        }

    def _write_classifier_inputs(self, n_rows: int):
        result_path = self.tmpdir.name

        os.makedirs(os.path.join(result_path, "pipeline_info"), exist_ok=True)
        os.makedirs(os.path.join(result_path, "classifications"), exist_ok=True)

        with open(
            os.path.join(
                result_path, "pipeline_info", f"params_{self.payload['uuid']}.log"
            ),
            "wt",
        ) as fh:
            json.dump({"kraken_database": {"default": {"name": "PlusPF"}}}, fh)

        with open(
            os.path.join(result_path, "classifications", "PlusPF.kraken_report.json"),
            "wt",
        ) as fh:
            json.dump(
                {
                    f"n{i}": {"taxid": i, "name": f"sp{i}", "count": i}
                    for i in range(n_rows)
                },
                fh,
            )

        return result_path

    def _write_spike_in_inputs(self):
        result_path = self.tmpdir.name
        os.makedirs(os.path.join(result_path, "qc"), exist_ok=True)

        with open(
            os.path.join(result_path, "qc", "spike_count_summary.json"), "wt"
        ) as fh:
            json.dump(
                {"SPIKE1": {"ref": {"taxid": 1, "human_readable": "x", "mapped_count": 5}}},
                fh,
            )

        with open(os.path.join(result_path, "qc", "spike_summary.json"), "wt") as fh:
            json.dump({"SPIKE1": "pass"}, fh)

        return result_path

    # --- classifier_calls batch writes ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_update")
    def test_classifier_calls_batch_request_error_does_not_alert(self, mock_update):
        result_path = self._write_classifier_inputs(5)
        mock_update.side_effect = [
            (False, False, self.payload),  # clear
            (True, False, self.payload),   # batch write, 4xx -> data problem
        ]

        fail, alert, payload = add_classifier_calls(
            payload=self.payload, result_path=result_path, log=self.log
        )

        self.assertTrue(fail)
        self.assertFalse(alert)

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_update")
    def test_classifier_calls_batch_infra_error_still_alerts(self, mock_update):
        """Negative control for the test above."""
        result_path = self._write_classifier_inputs(5)
        mock_update.side_effect = [
            (False, False, self.payload),  # clear
            (True, True, self.payload),    # batch write, infra failure
        ]

        fail, alert, payload = add_classifier_calls(
            payload=self.payload, result_path=result_path, log=self.log
        )

        self.assertTrue(fail)
        self.assertTrue(alert)

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_update")
    def test_classifier_calls_batch_failure_logs_batch_number(self, mock_update):
        result_path = self._write_classifier_inputs(150)
        mock_update.side_effect = [
            (False, False, self.payload),  # clear
            (False, False, self.payload),  # batch 1 of 2 succeeds
            (True, False, self.payload),   # batch 2 of 2 fails
        ]

        add_classifier_calls(
            payload=self.payload, result_path=result_path, log=self.log
        )

        logged = " ".join(str(c.args[0]) for c in self.log.error.call_args_list)
        self.assertIn("batch 2", logged)
        self.assertIn("classifier_calls", logged)
        self.assertIn(self.payload["uuid"], logged)

    # --- spike-in clear (consistency with its two siblings) ---

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_update")
    def test_spike_in_clear_request_error_does_not_alert(self, mock_update):
        result_path = self._write_spike_in_inputs()
        mock_update.return_value = (True, False, self.payload)

        fail, alert, payload = handle_spike_ins(
            payload=self.payload,
            result_path=result_path,
            log=self.log,
            spike_in="SPIKE1",
        )

        self.assertTrue(fail)
        self.assertFalse(alert)

    @patch("roz_scripts.mscape.mscape_ingest_validation.onyx_update")
    def test_spike_in_clear_infra_error_still_alerts(self, mock_update):
        """Negative control for the test above."""
        result_path = self._write_spike_in_inputs()
        mock_update.return_value = (True, True, self.payload)

        fail, alert, payload = handle_spike_ins(
            payload=self.payload,
            result_path=result_path,
            log=self.log,
            spike_in="SPIKE1",
        )

        self.assertTrue(fail)
        self.assertTrue(alert)


class TestLogPublishDelay(unittest.TestCase):
    def setUp(self):
        self.log = MagicMock()
        self.tmpdir = tempfile.mkdtemp()
        self.delay_log = os.path.join(self.tmpdir, "publish_delay.log")
        self.addCleanup(shutil.rmtree, self.tmpdir)

    def _payload(self, **overrides):
        payload = {
            "uuid": "test-uuid",
            "climb_id": "CLIMB001",
            "match_timestamp": 1_000_000_000,
        }
        payload.update(overrides)
        return payload

    def _args(self, publish_delay_log):
        return argparse.Namespace(publish_delay_log=publish_delay_log)

    def _entries(self):
        with open(self.delay_log) as fh:
            return [json.loads(line) for line in fh if line.strip()]

    def test_first_time_publish_is_logged(self):
        log_publish_delay(
            payload=self._payload(),
            args=self._args(self.delay_log),
            log=self.log,
        )

        entries = self._entries()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["climb_id"], "CLIMB001")
        self.assertGreater(entries[0]["publish_delay"], 0)

    def test_rerun_of_published_is_not_logged(self):
        """A rerun republishes an artifact that was already published on its
        first ingest, so its `delay` is really just rerun processing time and
        would skew the metric."""
        log_publish_delay(
            payload=self._payload(rerun_of_published=True),
            args=self._args(self.delay_log),
            log=self.log,
        )

        self.assertFalse(os.path.exists(self.delay_log))

    def test_no_delay_log_configured_is_a_noop(self):
        log_publish_delay(
            payload=self._payload(),
            args=self._args(None),
            log=self.log,
        )

        self.log.error.assert_not_called()

    def test_write_failure_is_logged_and_swallowed(self):
        log_publish_delay(
            payload=self._payload(),
            args=self._args(os.path.join(self.tmpdir, "nonexistent", "delay.log")),
            log=self.log,
        )

        self.log.error.assert_called_once()
