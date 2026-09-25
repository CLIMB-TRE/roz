import os
import sys
import unittest


DIR = os.path.dirname(__file__)
SLACK_INTEGRATIONS_DIR = os.path.join(DIR, "..", "slack_integrations")

if SLACK_INTEGRATIONS_DIR not in sys.path:
    sys.path.insert(0, SLACK_INTEGRATIONS_DIR)

from remote_alerts import (
    format_alert,
    format_malformed_body_alert,
    should_post,
    PRIORITY_CRITICAL,
    MIN_SECONDS_BETWEEN_ALERTS,
    MALFORMED_BODY_SOURCE,
)


class TestShouldPost(unittest.TestCase):
    def test_routine_alert_posts_when_source_never_seen(self):
        state = {}
        self.assertTrue(should_post({"source": "mscape"}, state, now=1000.0))

    def test_routine_alert_records_timestamp_on_post(self):
        state = {}
        should_post({"source": "mscape"}, state, now=1000.0)
        self.assertEqual(state["mscape"], 1000.0)

    def test_routine_alert_blocked_inside_window(self):
        state = {"mscape": 1000.0}
        posted = should_post(
            {"source": "mscape"}, state, now=1000.0 + MIN_SECONDS_BETWEEN_ALERTS - 1
        )
        self.assertFalse(posted)

    def test_routine_alert_allowed_outside_window(self):
        state = {"mscape": 1000.0}
        now = 1000.0 + MIN_SECONDS_BETWEEN_ALERTS + 1
        self.assertTrue(should_post({"source": "mscape"}, state, now=now))

    def test_routine_alert_blocked_does_not_update_timestamp(self):
        state = {"mscape": 1000.0}
        now = 1000.0 + MIN_SECONDS_BETWEEN_ALERTS - 1
        should_post({"source": "mscape"}, state, now=now)
        self.assertEqual(state["mscape"], 1000.0)

    def test_different_sources_do_not_share_a_window(self):
        state = {"mscape": 1000.0}
        self.assertTrue(should_post({"source": "pathsafe"}, state, now=1000.1))

    def test_critical_alert_always_posts_inside_window(self):
        state = {"mscape": 1000.0}
        now = 1000.0 + 1
        posted = should_post(
            {"source": "mscape", "priority": PRIORITY_CRITICAL}, state, now=now
        )
        self.assertTrue(posted)

    def test_critical_alert_does_not_write_the_timestamp(self):
        """The easiest thing to regress: a critical alert must not start a
        fresh cooldown window over unrelated routine alerts sharing its
        source."""
        state = {"mscape": 1000.0}
        should_post({"source": "mscape", "priority": PRIORITY_CRITICAL}, state, now=1500.0)
        self.assertEqual(state["mscape"], 1000.0)

    def test_routine_alert_after_critical_still_obeys_original_window(self):
        state = {}
        should_post({"source": "mscape"}, state, now=1000.0)
        should_post(
            {"source": "mscape", "priority": PRIORITY_CRITICAL}, state, now=1500.0
        )
        posted = should_post(
            {"source": "mscape"}, state, now=1000.0 + MIN_SECONDS_BETWEEN_ALERTS - 1
        )
        self.assertFalse(posted)

    def test_two_consecutive_critical_alerts_both_post(self):
        state = {"mscape": 1000.0}
        self.assertTrue(
            should_post({"source": "mscape", "priority": PRIORITY_CRITICAL}, state, now=1000.0)
        )
        self.assertTrue(
            should_post({"source": "mscape", "priority": PRIORITY_CRITICAL}, state, now=1000.1)
        )

    def test_non_critical_priority_values_are_treated_as_routine(self):
        for priority in ("routine", "CRITICAL", True, {"x": 1}, None):
            with self.subTest(priority=priority):
                state = {"mscape": 1000.0}
                posted = should_post(
                    {"source": "mscape", "priority": priority},
                    state,
                    now=1000.0 + 1,
                )
                self.assertFalse(posted)

    def test_malformed_body_reports_share_a_synthetic_source_window(self):
        """Two malformed bodies in quick succession post once, under the
        synthetic source used for `main()`'s malformed-body reports - and a
        genuine alert in the same window, on a real source, is unaffected."""
        state = {}
        malformed = {"source": MALFORMED_BODY_SOURCE}

        self.assertTrue(should_post(malformed, state, now=1000.0))
        self.assertFalse(should_post(malformed, state, now=1000.1))
        self.assertTrue(should_post({"source": "mscape"}, state, now=1000.2))


class test_format_alert(unittest.TestCase):
    def test_formats_source_description_and_uuid(self):
        text = format_alert(
            {"source": "mscape", "description": "something broke", "uuid": "abc-123"}
        )

        self.assertIn("mscape", text)
        self.assertIn("something broke", text)
        self.assertIn("abc-123", text)

    def test_omits_uuid_when_absent(self):
        text = format_alert({"source": "s3_matcher", "description": "crashed"})

        self.assertIn("s3_matcher", text)
        self.assertIn("crashed", text)
        self.assertNotIn("uuid:", text)

    def test_ignores_extra_unexpected_fields(self):
        text = format_alert(
            {
                "source": "mscape",
                "description": "something broke",
                "biosample_id": "SHOULD-NOT-APPEAR",
                "run_id": "SHOULD-NOT-APPEAR-EITHER",
            }
        )

        self.assertNotIn("SHOULD-NOT-APPEAR", text)
        self.assertNotIn("SHOULD-NOT-APPEAR-EITHER", text)

    def test_defaults_for_missing_fields(self):
        text = format_alert({})

        self.assertIn("unknown", text)
        self.assertIn("(no description)", text)

    def test_priority_field_is_never_rendered(self):
        """`priority` is a control signal for `should_post` only - it must
        never appear in the Slack text itself."""
        with_priority = format_alert(
            {"source": "mscape", "description": "something broke", "priority": PRIORITY_CRITICAL}
        )
        without_priority = format_alert(
            {"source": "mscape", "description": "something broke"}
        )

        self.assertEqual(with_priority, without_priority)
        self.assertNotIn("critical", with_priority.lower())
        self.assertNotIn("priority", with_priority.lower())


class TestFormatMalformedBodyAlert(unittest.TestCase):
    def test_omits_channel_mention(self):
        text = format_malformed_body_alert("not json", reason="x", body_type="str")

        self.assertNotIn("<!channel>", text)

    def test_reports_reason_type_and_length(self):
        text = format_malformed_body_alert(
            "abc", reason="could not parse as JSON: JSONDecodeError", body_type="str"
        )

        self.assertIn("could not parse as JSON: JSONDecodeError", text)
        self.assertIn("type: str", text)
        self.assertIn("3 bytes", text)

    def test_truncates_long_bodies(self):
        marker = "SHOULD-NOT-APPEAR-PAST-CUTOFF"
        raw_body = ("a" * 200) + marker

        text = format_malformed_body_alert(raw_body, reason="x", body_type="str")

        self.assertNotIn(marker, text)

    def test_short_bodies_are_not_marked_as_truncated(self):
        text = format_malformed_body_alert("short body", reason="x", body_type="str")

        self.assertNotIn("...", text)

    def test_backticks_in_body_cannot_break_the_code_fence(self):
        text = format_malformed_body_alert(
            "```\nmalicious", reason="x", body_type="str"
        )

        # Exactly two code-fence delimiters: the ones this function itself
        # wraps the excerpt in - none contributed by the body.
        self.assertEqual(text.count("```"), 2)

    def test_reports_type_of_non_dict_parsed_json(self):
        text = format_malformed_body_alert(
            "[1, 2, 3]", reason="parsed JSON is not an object", body_type="list"
        )

        self.assertIn("type: list", text)

    def test_reports_exception_class_name_only(self):
        # The reason string is built by the caller (main()), but this
        # asserts the function renders whatever it's given verbatim, without
        # appending anything further (e.g. a full exception message).
        text = format_malformed_body_alert(
            "{bad json", reason="could not parse as JSON: JSONDecodeError", body_type="str"
        )

        self.assertIn("JSONDecodeError", text)


if __name__ == "__main__":
    unittest.main()
