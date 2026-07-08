import os
import sys
import unittest


DIR = os.path.dirname(__file__)
SLACK_INTEGRATIONS_DIR = os.path.join(DIR, "..", "slack_integrations")

if SLACK_INTEGRATIONS_DIR not in sys.path:
    sys.path.insert(0, SLACK_INTEGRATIONS_DIR)

from remote_alerts import format_alert


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


if __name__ == "__main__":
    unittest.main()
