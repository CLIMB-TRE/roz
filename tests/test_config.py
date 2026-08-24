import itertools
import os
import unittest

from roz_scripts.utils.config import (
    ConfigError,
    TEST_FLAGS,
    load_config,
    project_bucket,
    site_bucket,
    parse_bucket_name,
    try_parse_bucket_name,
    parse_ingest_bucket_name,
    short_site,
)

DIR = os.path.dirname(__file__)
TEST_CONFIG_PATH = os.path.join(DIR, "fixtures", "test_config.json")
ADVERSARIAL_CONFIG_PATH = os.path.join(DIR, "fixtures", "test_config_adversarial.json")


class test_short_site(unittest.TestCase):
    def test_dotted_site_returns_second_to_last_component(self):
        self.assertEqual(short_site("clinic1.trust1.mscape"), "trust1")

    def test_bare_site_returns_itself(self):
        self.assertEqual(short_site("mscape"), "mscape")

    def test_two_component_site(self):
        self.assertEqual(short_site("birm.mscape"), "birm")


class test_parse_bucket_name_round_trip(unittest.TestCase):
    """The single highest-value test here: for every project/scope/bucket/
    site/platform/test_flag combination the real fixture config can produce,
    construct the bucket name and assert parsing it recovers the same
    coordinates. If this ever fails, the constructor and parser have
    diverged - which is exactly the class of bug this whole change exists
    to make structurally impossible.
    """

    def setUp(self):
        self.config = load_config(TEST_CONFIG_PATH)

    def test_all_project_buckets_round_trip(self):
        checked = 0
        for project, project_config in self.config["configs"].items():
            for bucket, bucket_config in project_config["project_buckets"].items():
                labels = set(_placeholders(bucket_config["name_layout"]))
                for platform, test_flag in _platform_test_flag_combos(
                    project_config, labels
                ):
                    kwargs = {}
                    if "platform" in labels:
                        kwargs["platform"] = platform
                    if "test_flag" in labels:
                        kwargs["test_flag"] = test_flag

                    bucket_name = project_bucket(self.config, project, bucket, **kwargs)
                    match = parse_bucket_name(self.config, bucket_name)

                    self.assertEqual(match.project, project)
                    self.assertEqual(match.bucket, bucket)
                    self.assertEqual(match.scope, "project")
                    self.assertIsNone(match.site)
                    checked += 1

        self.assertGreater(checked, 0)

    def test_all_site_buckets_round_trip(self):
        checked = 0
        for project, project_config in self.config["configs"].items():
            for site in project_config["sites"]:
                for bucket, bucket_config in project_config["site_buckets"].items():
                    labels = set(_placeholders(bucket_config["name_layout"]))
                    for platform, test_flag in _platform_test_flag_combos(
                        project_config, labels
                    ):
                        kwargs = {}
                        if "platform" in labels:
                            kwargs["platform"] = platform
                        if "test_flag" in labels:
                            kwargs["test_flag"] = test_flag

                        bucket_name = site_bucket(
                            self.config, project, site, bucket, **kwargs
                        )
                        match = parse_bucket_name(self.config, bucket_name)

                        self.assertEqual(match.project, project)
                        self.assertEqual(match.bucket, bucket)
                        self.assertEqual(match.scope, "site")
                        self.assertEqual(match.site, site)
                        checked += 1

        self.assertGreater(checked, 0)


def _placeholders(name_layout):
    import regex as re

    return re.findall(r"{(\w*)}", name_layout)


def _platform_test_flag_combos(project_config, labels):
    platforms = project_config["file_specs"].keys() if "platform" in labels else [None]
    test_flags = TEST_FLAGS if "test_flag" in labels else [None]
    return itertools.product(platforms, test_flags)


class test_parse_bucket_name_adversarial(unittest.TestCase):
    """Cases the tame fixture can't exercise: hyphenated sites, multi-level
    dotted sites, short-site collisions, and the site/project ingest layout
    collision-by-design (site named "public" would collide with the
    project-level pseudo-site, which is exactly why construct-and-index
    - not a free-form regex - is the right approach).
    """

    def setUp(self):
        self.config = load_config(ADVERSARIAL_CONFIG_PATH)

    def test_hyphenated_site_parses_correctly(self):
        # This is the exact case the old `bucket_name.split("-")` positional
        # parse cannot handle - a site name that itself contains a hyphen
        # produces more than 4 fields.
        bucket_name = "adversarialscape-hyphen-site.adversarialscape-illumina-prod"
        match = parse_bucket_name(self.config, bucket_name)

        self.assertEqual(match.project, "adversarialscape")
        self.assertEqual(match.bucket, "ingest")
        self.assertEqual(match.scope, "site")
        self.assertEqual(match.site, "hyphen-site.adversarialscape")
        self.assertEqual(match.platform, "illumina")
        self.assertEqual(match.test_flag, "prod")

    def test_multi_level_dotted_site(self):
        bucket_name = "adversarialscape-clinic1.trust1.adversarialscape-illumina-prod"
        match = parse_bucket_name(self.config, bucket_name)
        self.assertEqual(match.site, "clinic1.trust1.adversarialscape")

    def test_illumina_se_platform_not_confused_with_illumina(self):
        bucket_name = "adversarialscape-adversarialscape-illumina.se-prod"
        match = parse_bucket_name(self.config, bucket_name)
        self.assertEqual(match.platform, "illumina.se")

    def test_bare_project_site(self):
        bucket_name = "adversarialscape-adversarialscape-illumina-prod"
        match = parse_bucket_name(self.config, bucket_name)
        self.assertEqual(match.site, "adversarialscape")

    def test_project_scope_ingest_bucket_uses_public_pseudo_site(self):
        bucket_name = "adversarialscape-public-illumina-prod"
        match = parse_bucket_name(self.config, bucket_name)

        self.assertEqual(match.scope, "project")
        self.assertIsNone(match.site)

        parsed = parse_ingest_bucket_name(self.config, bucket_name)
        self.assertEqual(parsed["raw_site"], "public")
        self.assertEqual(parsed["site"], "public")

    def test_short_site_collision_does_not_cause_bucket_name_ambiguity(self):
        # "collide" and "collide.adversarialscape" share a short site name,
        # but remain distinct full site names and therefore distinct bucket
        # names - no ConfigError building the index over this fixture.
        m1 = parse_bucket_name(
            self.config, "adversarialscape-collide-illumina-prod"
        )
        m2 = parse_bucket_name(
            self.config, "adversarialscape-collide.adversarialscape-illumina-prod"
        )
        self.assertEqual(m1.site, "collide")
        self.assertEqual(m2.site, "collide.adversarialscape")
        assert m1.site is not None and m2.site is not None
        self.assertEqual(short_site(m1.site), short_site(m2.site))


class test_parse_bucket_name_malformed(unittest.TestCase):
    def setUp(self):
        self.config = load_config(TEST_CONFIG_PATH)

    def _assert_rejected(self, bucket_name):
        with self.assertRaises(ConfigError):
            parse_bucket_name(self.config, bucket_name)
        self.assertIsNone(try_parse_bucket_name(self.config, bucket_name))

    def test_empty_string(self):
        self._assert_rejected("")

    def test_too_few_fields(self):
        self._assert_rejected("mscape")
        self._assert_rejected("mscape-birm.mscape")
        self._assert_rejected("mscape-birm.mscape-illumina")

    def test_too_many_fields(self):
        self._assert_rejected("mscape-birm.mscape-illumina-prod-extra")

    def test_invalid_test_flag(self):
        self._assert_rejected("mscape-birm.mscape-illumina-staging")

    def test_unknown_platform(self):
        self._assert_rejected("mscape-birm.mscape-nanopore-prod")

    def test_unknown_site(self):
        self._assert_rejected("mscape-nonexistent.mscape-illumina-prod")

    def test_unknown_project(self):
        self._assert_rejected("notaproject-birm.mscape-illumina-prod")

    def test_case_sensitivity(self):
        self._assert_rejected("MSCAPE-BIRM.MSCAPE-ILLUMINA-PROD")

    def test_empty_field(self):
        self._assert_rejected("mscape--illumina-prod")

    def test_unescaped_dot_regression(self):
        # A hand-rolled regex inverse (`{site}` as `.+` etc) could easily let
        # a "." in a literal position match any character. Construct-and-
        # index has no such risk, but pin it as a regression guard anyway.
        self._assert_rejected("mscapeXbirmXmscape-illumina-prod")


class test_parse_bucket_name_filters(unittest.TestCase):
    def setUp(self):
        self.config = load_config(TEST_CONFIG_PATH)

    def test_bucket_filter_accepts_matching(self):
        bucket_name = "mscape-birm.mscape-ont-prod"
        match = parse_bucket_name(self.config, bucket_name, bucket="ingest")
        self.assertEqual(match.bucket, "ingest")

    def test_bucket_filter_rejects_non_matching(self):
        bucket_name = project_bucket(self.config, "mscape", "published_reads")
        with self.assertRaises(ConfigError):
            parse_bucket_name(self.config, bucket_name, bucket="ingest")

    def test_scope_filter_rejects_non_matching(self):
        bucket_name = site_bucket(self.config, "mscape", "birm.mscape", "results")
        with self.assertRaises(ConfigError):
            parse_bucket_name(self.config, bucket_name, scope="project")


class test_build_bucket_index_collision_detection(unittest.TestCase):
    def test_colliding_layouts_raise_configerror_at_build_time(self):
        from roz_scripts.utils.config import _build_bucket_index

        colliding_config = {
            "configs": {
                "proj": {
                    "sites": {"siteA": "analysis"},
                    "file_specs": {"ont": {}},
                    "site_buckets": {
                        "ingest": {
                            "name_layout": "{project}-{site}",
                            "policy": {},
                            "owner": "{site}",
                        }
                    },
                    "project_buckets": {
                        "collider": {
                            "name_layout": "{project}-siteA",
                            "policy": {},
                            "owner": "admin",
                        }
                    },
                }
            }
        }

        with self.assertRaises(ConfigError):
            _build_bucket_index(colliding_config)


class test_parse_ingest_bucket_name(unittest.TestCase):
    def setUp(self):
        self.config = load_config(TEST_CONFIG_PATH)

    def test_returns_expected_keys(self):
        bucket_name = "mscape-birm.mscape-ont-prod"
        parsed = parse_ingest_bucket_name(self.config, bucket_name)

        self.assertEqual(
            set(parsed.keys()),
            {"project", "raw_site", "site", "platform", "test_flag", "scope"},
        )
        self.assertEqual(parsed["project"], "mscape")
        self.assertEqual(parsed["raw_site"], "birm.mscape")
        self.assertEqual(parsed["site"], "birm")
        self.assertEqual(parsed["platform"], "ont")
        self.assertEqual(parsed["test_flag"], "prod")
        self.assertEqual(parsed["scope"], "site")

    def test_rejects_non_ingest_bucket(self):
        bucket_name = project_bucket(self.config, "mscape", "published_reads")
        with self.assertRaises(ConfigError):
            parse_ingest_bucket_name(self.config, bucket_name)


if __name__ == "__main__":
    unittest.main()
