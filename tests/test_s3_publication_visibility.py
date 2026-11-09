import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from roz_scripts.general import s3_publication_visibility as viz

DIR = os.path.dirname(__file__)


class TestClimbIdFromKey(unittest.TestCase):
    """Key -> climb_id, across every layout current code actually writes.

    The dangerous failure is not a key that fails to parse (that object is left
    alone), it is a key that parses to the WRONG climb_id and inherits another
    record's publication state - a suppressed record's file left visible.
    """

    def test_real_key_formats(self):
        cases = {
            # published_binned_reads
            "C-1234567890/C-1234567890_9606_1.fastq.gz": "C-1234567890",
            "C-1234567890/C-1234567890_9606.fastq.gz": "C-1234567890",
            # published_taxon_reports
            "C-1234567890/C-1234567890_report.html": "C-1234567890",
            "C-1234567890/C-1234567890_kraken.20240101.tsv": "C-1234567890",
            # published_scylla_reports (flat)
            "C-1234567890_scylla_report.html": "C-1234567890",
            # published_reads (flat)
            "C-1234567890_1.fastq.gz": "C-1234567890",
            "C-1234567890.fastq.gz": "C-1234567890",
            # published-read-fractions
            "C-1234567890/C-1234567890.viral_1.fastq.gz": "C-1234567890",
            # published_hcid_reports
            "C-1234567890/C-1234567890.hcid.json": "C-1234567890",
            # chimera
            "C-1234567890.chimera.bam": "C-1234567890",
            "C-1234567890.20240101.report.html": "C-1234567890",
            # pathsafe
            "C-1234567890.assembly.fasta": "C-1234567890",
        }

        for key, expected in cases.items():
            with self.subTest(key=key):
                self.assertEqual(viz.climb_id_from_key(key), expected)

    def test_internal_hyphen_is_not_a_delimiter(self):
        """climb_ids are 'C-' + alphanumerics; the hyphen is inside the id."""

        self.assertEqual(viz.climb_id_from_key("C-ABC123.bam"), "C-ABC123")

    def test_canary_and_unparseable_keys_yield_none(self):
        for key in ("test", "manifest.json", "README", "notaclimbid_x.txt", ""):
            with self.subTest(key=key):
                self.assertIsNone(viz.climb_id_from_key(key))

    def test_overlong_candidate_rejected(self):
        self.assertIsNone(viz.climb_id_from_key("C-12345678901234.bam"))


class TestDesiredVisibility(unittest.TestCase):
    def test_truth_table(self):
        pub_map = {"C-PUBLISHED": True, "C-HIDDEN": False}

        self.assertEqual(
            viz.desired_visibility("C-PUBLISHED.bam", pub_map), (True, "published")
        )
        self.assertEqual(
            viz.desired_visibility("C-HIDDEN.bam", pub_map), (False, "unpublished")
        )

    def test_no_record_and_unparseable_are_no_opinion(self):
        """Both must return None, never False.

        'No record' is also what a partial Onyx view looks like, and an
        unparseable key is a sign something else is wrong. Acting on either turns
        a mapping bug into an outage.
        """

        pub_map = {"C-PUBLISHED": True}

        self.assertEqual(
            viz.desired_visibility("C-UNKNOWN.bam", pub_map), (None, "no_record")
        )
        self.assertEqual(
            viz.desired_visibility("manifest.json", pub_map), (None, "unparseable")
        )


class TestPlanTagChanges(unittest.TestCase):
    def setUp(self):
        self.pub_map = {"C-PUB": True, "C-SUP": False}

    def test_published_untagged_is_shown(self):
        to_show, to_hide, _ = viz.plan_tag_changes([("C-PUB.bam", False)], self.pub_map)
        self.assertEqual(to_show, ["C-PUB.bam"])
        self.assertEqual(to_hide, [])

    def test_suppressed_tagged_is_hidden(self):
        """This is the 'untag' the request asks for."""

        to_show, to_hide, _ = viz.plan_tag_changes([("C-SUP.bam", True)], self.pub_map)
        self.assertEqual(to_hide, ["C-SUP.bam"])
        self.assertEqual(to_show, [])

    def test_already_correct_objects_produce_no_calls(self):
        to_show, to_hide, _ = viz.plan_tag_changes(
            [("C-PUB.bam", True), ("C-SUP.bam", False)], self.pub_map
        )
        self.assertEqual(to_show, [])
        self.assertEqual(to_hide, [])

    def test_no_opinion_objects_are_left_alone_by_default(self):
        objects = [("C-UNKNOWN.bam", True), ("manifest.json", True)]

        to_show, to_hide, counts = viz.plan_tag_changes(objects, self.pub_map)

        self.assertEqual(to_show, [])
        self.assertEqual(to_hide, [])
        self.assertEqual(counts["no_record"], 1)
        self.assertEqual(counts["unparseable"], 1)

    def test_opt_in_flags_hide_no_opinion_objects(self):
        objects = [("C-UNKNOWN.bam", True), ("manifest.json", True)]

        _, to_hide, _ = viz.plan_tag_changes(
            objects, self.pub_map, hide_unmappable=True, hide_orphans=True
        )

        self.assertEqual(sorted(to_hide), ["C-UNKNOWN.bam", "manifest.json"])


class TestCrossCheck(unittest.TestCase):
    """The only check that catches a silently site-scoped Onyx token.

    A partially scoped token returns a plausible, non-empty map in which another
    site's records are simply absent - and absent reads as 'no record'. An
    empty-result guard cannot catch that because the result is not empty.
    """

    def setUp(self):
        self.log = Mock()

    def test_empty_map_fails(self):
        self.assertFalse(
            viz.cross_check_publication_map("mscape", {}, "C-CANARY", self.log)
        )

    def test_canary_present_passes(self):
        self.assertTrue(
            viz.cross_check_publication_map(
                "mscape", {"C-CANARY": True}, "C-CANARY", self.log
            )
        )

    def test_canary_absent_but_resolvable_individually_fails(self):
        """The map is incomplete - the exact scenario this exists to catch."""

        with patch.object(
            viz, "onyx_get_record", return_value=(False, {"climb_id": "C-CANARY"})
        ):
            self.assertFalse(
                viz.cross_check_publication_map(
                    "mscape", {"C-OTHER": True}, "C-CANARY", self.log
                )
            )

    def test_canary_genuinely_gone_fails_loudly(self):
        with patch.object(viz, "onyx_get_record", return_value=(False, None)):
            self.assertFalse(
                viz.cross_check_publication_map(
                    "mscape", {"C-OTHER": True}, "C-CANARY", self.log
                )
            )

    def test_onyx_unreachable_during_crosscheck_fails(self):
        with patch.object(viz, "onyx_get_record", return_value=(True, None)):
            self.assertFalse(
                viz.cross_check_publication_map(
                    "mscape", {"C-OTHER": True}, "C-CANARY", self.log
                )
            )


class TestApplyTagChanges(unittest.TestCase):
    def setUp(self):
        self.log = Mock()

    def test_nothing_is_written_without_commit(self):
        client = Mock()

        applied = viz.apply_tag_changes(
            client, "bucket", ["a"], ["b"], self.log, commit=False
        )

        client.put_object_tagging.assert_not_called()
        client.delete_object_tagging.assert_not_called()
        self.assertEqual(applied, {"shown": 0, "hidden": 0, "failed": 0})

    def test_hide_uses_delete_object_tagging_not_delete_object(self):
        """Hiding removes the tag. It must never remove the object."""

        client = Mock()

        viz.apply_tag_changes(client, "bucket", [], ["b"], self.log, commit=True)

        client.delete_object_tagging.assert_called_once()
        client.delete_object.assert_not_called()

    def test_failures_are_counted_not_raised(self):
        client = Mock()
        client.put_object_tagging.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "no"}}, "PutObjectTagging"
        )

        applied = viz.apply_tag_changes(
            client, "bucket", ["a"], [], self.log, commit=True
        )

        self.assertEqual(applied["failed"], 1)


class TestOnyxFailureAbortsPass(unittest.TestCase):
    """The single most important test in this file.

    A failed or partial Onyx view makes every record look unpublished. If the
    pass continued it would hide every published object in every gated bucket.
    """

    def setUp(self):
        self.log = Mock()
        self.args = SimpleNamespace(
            config=os.path.join(DIR, "fixtures", "test_config.json"),
            commit=True,
            hide_unmappable=False,
            hide_orphans=False,
            report=None,
        )

    def _run(self):
        return viz.run_pass(self.args, self.log)

    def test_check_failure_writes_nothing_and_exits_nonzero(self):
        client = Mock()

        with patch.object(viz, "build_publication_map", return_value=(True, {})), \
             patch.object(viz, "get_s3_client", return_value=client), \
             patch.object(viz, "get_s3_credentials", return_value=Mock()), \
             patch.object(viz, "send_admin_alert"):
            rc = self._run()

        self.assertNotEqual(rc, 0)
        client.put_object_tagging.assert_not_called()
        client.delete_object_tagging.assert_not_called()
        client.delete_object.assert_not_called()

    def test_failed_cross_check_writes_nothing_and_exits_nonzero(self):
        client = Mock()

        with patch.object(
            viz, "build_publication_map", return_value=(False, {"C-X": True})
        ), patch.object(viz, "cross_check_publication_map", return_value=False), \
             patch.object(viz, "get_s3_client", return_value=client), \
             patch.object(viz, "get_s3_credentials", return_value=Mock()), \
             patch.object(viz, "send_admin_alert"):
            rc = self._run()

        self.assertNotEqual(rc, 0)
        client.put_object_tagging.assert_not_called()
        client.delete_object_tagging.assert_not_called()


class TestModuleCannotDelete(unittest.TestCase):
    def test_source_contains_no_delete_object_call(self):
        """Structural guarantee, not a convention.

        The user's requirement was explicitly that nothing is deleted. This fails
        the build if anyone adds a delete back.
        """

        with open(viz.__file__) as f:
            source = f.read()

        self.assertNotIn("delete_object(", source)
        self.assertNotIn("delete_objects(", source)


class TestListSkipsCanaryKey(unittest.TestCase):
    def test_test_key_is_never_listed(self):
        """'test' is the permission canary, and the gate probe depends on it
        staying untagged."""

        client = Mock()
        client.get_paginator.return_value.paginate.return_value = [
            {"Contents": [{"Key": "test"}, {"Key": "C-PUB.bam"}]}
        ]
        client.get_object_tagging.return_value = {"TagSet": []}

        objects = viz.list_bucket_objects(client, "bucket", Mock())

        self.assertEqual([k for k, _ in objects], ["C-PUB.bam"])
