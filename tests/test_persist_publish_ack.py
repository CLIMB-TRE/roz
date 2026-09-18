"""Tests for the persist/publish/ack ordering wrapper.

These cover the failure modes behind the artifact loss that motivated it: a
publish that fails after the message was already acknowledged, and a broker
outage that breaks the alert and the nack as well as the publish.
"""

import unittest
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError
from varys import PublishFailedError, PublishTimeoutError

from roz_scripts.utils.utils import persist_publish_ack


def client_error():
    return ClientError({"Error": {"Code": "500", "Message": "nope"}}, "PutObject")


class TestPersistPublishAck(unittest.TestCase):

    def setUp(self):
        self.varys_client = MagicMock()
        self.message = MagicMock()
        self.log = MagicMock()

        sleep_patcher = patch("roz_scripts.utils.utils.time.sleep")
        self.addCleanup(sleep_patcher.stop)
        self.sleep = sleep_patcher.start()

        alert_patcher = patch("roz_scripts.utils.utils.send_admin_alert")
        self.addCleanup(alert_patcher.stop)
        self.alert = alert_patcher.start()

    def test_persists_then_publishes_then_acks(self):
        order = []
        persist = MagicMock(side_effect=lambda: order.append("persist"))
        self.varys_client.send.side_effect = lambda **kw: order.append(kw["exchange"])
        self.varys_client.acknowledge_message.side_effect = lambda m: order.append("ack")

        result = persist_publish_ack(
            self.varys_client,
            self.message,
            self.log,
            sends=[{"message": {}, "exchange": "cheap"}, {"message": {}, "exchange": "expensive"}],
            persists=[persist],
        )

        self.assertTrue(result)
        self.assertEqual(order, ["persist", "cheap", "expensive", "ack"])
        self.varys_client.nack_message.assert_not_called()

    def test_send_order_is_not_reordered(self):
        """Callers order sends cheapest-first, so the wrapper must preserve it."""
        sends = [{"message": {}, "exchange": f"e{i}"} for i in range(5)]

        persist_publish_ack(
            self.varys_client, self.message, self.log, sends=sends
        )

        self.assertEqual(
            self.varys_client.send.call_args_list, [call(**s) for s in sends]
        )

    def test_publish_failure_requeues_and_never_acks(self):
        self.varys_client.send.side_effect = PublishFailedError("broker said no")

        result = persist_publish_ack(
            self.varys_client,
            self.message,
            self.log,
            sends=[{"message": {}, "exchange": "x"}],
            source="ingest",
        )

        self.assertFalse(result)
        self.varys_client.acknowledge_message.assert_not_called()
        self.varys_client.nack_message.assert_called_once_with(self.message, requeue=True)
        self.alert.assert_called_once()

    def test_publish_timeout_also_requeues(self):
        """An indeterminate publish must requeue, not ack - duplicates are absorbed."""
        self.varys_client.send.side_effect = PublishTimeoutError("no confirm")

        result = persist_publish_ack(
            self.varys_client, self.message, self.log, sends=[{"message": {}, "exchange": "x"}]
        )

        self.assertFalse(result)
        self.varys_client.acknowledge_message.assert_not_called()
        self.varys_client.nack_message.assert_called_once_with(self.message, requeue=True)

    def test_persist_failure_publishes_nothing(self):
        persist = MagicMock(side_effect=client_error())

        result = persist_publish_ack(
            self.varys_client,
            self.message,
            self.log,
            sends=[{"message": {}, "exchange": "x"}],
            persists=[persist],
        )

        self.assertFalse(result)
        self.varys_client.send.assert_not_called()
        self.varys_client.acknowledge_message.assert_not_called()
        self.varys_client.nack_message.assert_called_once_with(self.message, requeue=True)

    def test_failed_alert_is_logged_only(self):
        """A downed broker breaks the alert too; that must not stop the requeue."""
        self.varys_client.send.side_effect = PublishFailedError("down")
        self.alert.side_effect = PublishFailedError("down")

        result = persist_publish_ack(
            self.varys_client, self.message, self.log, sends=[{"message": {}, "exchange": "x"}]
        )

        self.assertFalse(result)
        self.varys_client.nack_message.assert_called_once_with(self.message, requeue=True)
        self.assertTrue(self.log.exception.called)

    def test_failed_nack_is_tolerated(self):
        """Connection loss requeues unacked deliveries anyway, so a failed nack is fine."""
        self.varys_client.send.side_effect = PublishFailedError("down")
        self.varys_client.nack_message.side_effect = Exception("connection closed")

        result = persist_publish_ack(
            self.varys_client, self.message, self.log, sends=[{"message": {}, "exchange": "x"}]
        )

        self.assertFalse(result)
        self.varys_client.acknowledge_message.assert_not_called()

    def test_failed_ack_still_counts_as_done(self):
        """The work is committed by then; the broker will just redeliver."""
        self.varys_client.acknowledge_message.side_effect = Exception("connection closed")

        result = persist_publish_ack(
            self.varys_client, self.message, self.log, sends=[{"message": {}, "exchange": "x"}]
        )

        self.assertTrue(result)

    def test_heartbeat_is_beaten_around_each_step(self):
        """Publishing can block ~110s per send, which must not read as no progress."""
        heartbeat = MagicMock()

        persist_publish_ack(
            self.varys_client,
            self.message,
            self.log,
            sends=[{"message": {}, "exchange": "a"}, {"message": {}, "exchange": "b"}],
            persists=[MagicMock()],
            heartbeat=heartbeat,
        )

        self.assertEqual(heartbeat.call_count, 3)

    def test_heartbeat_is_beaten_during_backoff(self):
        self.varys_client.send.side_effect = PublishFailedError("down")
        heartbeat = MagicMock()

        persist_publish_ack(
            self.varys_client,
            self.message,
            self.log,
            sends=[{"message": {}, "exchange": "x"}],
            heartbeat=heartbeat,
        )

        # once before the send, twice around the backoff sleep
        self.assertEqual(heartbeat.call_count, 3)
        self.sleep.assert_called_once()

    def test_failing_heartbeat_does_not_lose_the_message(self):
        heartbeat = MagicMock(side_effect=Exception("shared storage gone"))

        result = persist_publish_ack(
            self.varys_client,
            self.message,
            self.log,
            sends=[{"message": {}, "exchange": "x"}],
            heartbeat=heartbeat,
        )

        self.assertTrue(result)
        self.varys_client.acknowledge_message.assert_called_once_with(self.message)


if __name__ == "__main__":
    unittest.main()
