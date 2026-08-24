from dataclasses import dataclass, field
import itertools
import json
import os
import regex as re


class ConfigError(ValueError):
    """Raised when the roz config can't be loaded, or a bucket lookup fails against it"""


TEST_FLAGS = ("prod", "test")


_config_cache: dict[str, dict] = {}


def load_config(path: str | None = None) -> dict:
    """Load and cache the roz config from disk

    Args:
        path (str | None): Path to the config JSON file. Defaults to
            $ROZ_CONFIG_JSON if not given.

    Returns:
        dict: The parsed config document

    Raises:
        ConfigError: If no path is available, the file can't be read, or it
            isn't valid JSON
    """
    path = path or os.getenv("ROZ_CONFIG_JSON")

    if not path:
        raise ConfigError(
            "No config path given and ROZ_CONFIG_JSON is not set"
        )

    if path in _config_cache:
        return _config_cache[path]

    try:
        with open(path, "r") as f:
            config = json.load(f)
    except FileNotFoundError as e:
        raise ConfigError(f"Config file not found: {path}") from e
    except json.JSONDecodeError as e:
        raise ConfigError(f"Config file at {path} is not valid JSON: {e}") from e

    _config_cache[path] = config
    return config


def _expand_name_layout(name_layout: str, **kwargs) -> str:
    labels = re.findall(r"{(\w*)}", name_layout)

    try:
        namespace = {label: kwargs[label] for label in labels}
    except KeyError as e:
        raise ConfigError(
            f"Bucket layout {name_layout!r} requires a value for {e}, which was not given"
        ) from e

    return name_layout.format(**namespace)


def project_bucket(config: dict, project: str, bucket: str, **kwargs) -> str:
    """Resolve a project-level bucket name from config

    Args:
        config (dict): The loaded roz config, from load_config()
        project (str): The project the bucket belongs to, e.g. "mscape"
        bucket (str): The project_buckets key to resolve, e.g. "published_reads"
        **kwargs: Any extra values the bucket's name_layout requires (e.g.
            platform, test_flag) beyond `project`, which is filled in automatically

    Returns:
        str: The resolved bucket name

    Raises:
        ConfigError: If the project or bucket key is unknown, or a required
            name_layout placeholder wasn't supplied
    """
    try:
        project_buckets = config["configs"][project]["project_buckets"]
    except KeyError as e:
        raise ConfigError(f"Unknown project: {project!r}") from e

    try:
        bucket_config = project_buckets[bucket]
    except KeyError as e:
        raise ConfigError(
            f"Unknown project bucket {bucket!r} for project {project!r}. "
            f"Available: {sorted(project_buckets)}"
        ) from e

    return _expand_name_layout(bucket_config["name_layout"], project=project, **kwargs)


def site_bucket(config: dict, project: str, site: str, bucket: str, **kwargs) -> str:
    """Resolve a site-level bucket name from config

    Args:
        config (dict): The loaded roz config, from load_config()
        project (str): The project the bucket belongs to, e.g. "mscape"
        site (str): The site the bucket belongs to
        bucket (str): The site_buckets key to resolve, e.g. "results"
        **kwargs: Any extra values the bucket's name_layout requires (e.g.
            platform, test_flag) beyond `project`/`site`, which are filled in
            automatically

    Returns:
        str: The resolved bucket name

    Raises:
        ConfigError: If the project or bucket key is unknown, or a required
            name_layout placeholder wasn't supplied
    """
    try:
        site_buckets = config["configs"][project]["site_buckets"]
    except KeyError as e:
        raise ConfigError(f"Unknown project: {project!r}") from e

    try:
        bucket_config = site_buckets[bucket]
    except KeyError as e:
        raise ConfigError(
            f"Unknown site bucket {bucket!r} for project {project!r}. "
            f"Available: {sorted(site_buckets)}"
        ) from e

    return _expand_name_layout(
        bucket_config["name_layout"], project=project, site=site, **kwargs
    )


def project_bucket_uri(config: dict, project: str, bucket: str, key: str, **kwargs) -> str:
    """Resolve an s3:// URI into a project-level bucket

    Args:
        config (dict): The loaded roz config, from load_config()
        project (str): The project the bucket belongs to, e.g. "mscape"
        bucket (str): The project_buckets key to resolve, e.g. "published_reads"
        key (str): The object key within the bucket
        **kwargs: Any extra values the bucket's name_layout requires

    Returns:
        str: An "s3://bucket/key" URI
    """
    return f"s3://{project_bucket(config, project, bucket, **kwargs)}/{key}"


@dataclass(frozen=True)
class BucketMatch:
    """The result of resolving a bucket name back to the config entry that produces it

    Attributes:
        bucket_name (str): The bucket name that was parsed
        project (str): The project the bucket belongs to, e.g. "mscape"
        bucket (str): The project_buckets/site_buckets key, e.g. "ingest"
        scope (str): Either "project" or "site"
        site (str | None): The exact config `sites` key, for scope="site". None for scope="project"
        platform (str | None): The platform, if the bucket's name_layout includes {platform}
        test_flag (str | None): "prod" or "test", if the bucket's name_layout includes {test_flag}
        fields (dict): Every placeholder value the name_layout was expanded with
    """

    bucket_name: str
    project: str
    bucket: str
    scope: str
    site: str | None
    platform: str | None
    test_flag: str | None
    fields: dict = field(default_factory=dict)


def short_site(site: str) -> str:
    """Derive the short form of a site name used for exchange names and dedup keys

    e.g. "gpha.ukhsa.mscape" -> "ukhsa". Note this is a lossy heuristic that
    can collide between distinct sites sharing a second-to-last dotted
    component - it exists here, centralised, purely to match pre-existing
    behaviour rather than as an endorsement of the scheme.

    Args:
        site (str): The full config `sites` key

    Returns:
        str: The short site name
    """
    return site.split(".")[-2] if "." in site else site


def _label_domain(label: str, project: str, project_config: dict, site: str | None):
    """Return every value a name_layout placeholder can take, or None if it can't be enumerated"""
    if label == "project":
        return [project]
    if label == "site":
        return [site] if site is not None else None
    if label == "platform":
        return list(project_config.get("file_specs", {}).keys())
    if label == "test_flag":
        return list(TEST_FLAGS)
    return None


def _index_layout(
    index: dict, project: str, project_config: dict, bucket: str, bucket_config: dict, scope: str, site: str | None
) -> None:
    name_layout = bucket_config["name_layout"]
    labels = re.findall(r"{(\w*)}", name_layout)

    domains = []
    for label in labels:
        domain = _label_domain(label, project, project_config, site)
        if domain is None:
            raise ConfigError(
                f"Cannot enumerate values for placeholder '{label}' in bucket layout "
                f"{name_layout!r} (project={project!r}, bucket={bucket!r})"
            )
        domains.append(domain)

    for combo in itertools.product(*domains) if domains else [()]:
        fields = dict(zip(labels, combo))
        bucket_name = name_layout.format(**fields)

        match = BucketMatch(
            bucket_name=bucket_name,
            project=project,
            bucket=bucket,
            scope=scope,
            site=fields.get("site"),
            platform=fields.get("platform"),
            test_flag=fields.get("test_flag"),
            fields=fields,
        )

        if bucket_name in index:
            existing = index[bucket_name]
            raise ConfigError(
                f"Bucket name {bucket_name!r} is ambiguous: matches both "
                f"(project={existing.project!r}, bucket={existing.bucket!r}, scope={existing.scope!r}) "
                f"and (project={project!r}, bucket={bucket!r}, scope={scope!r})"
            )

        index[bucket_name] = match


def _build_bucket_index(config: dict) -> dict:
    index: dict = {}

    for project, project_config in config.get("configs", {}).items():
        for bucket, bucket_config in project_config.get("project_buckets", {}).items():
            _index_layout(index, project, project_config, bucket, bucket_config, scope="project", site=None)

        for site in project_config.get("sites", {}):
            for bucket, bucket_config in project_config.get("site_buckets", {}).items():
                _index_layout(index, project, project_config, bucket, bucket_config, scope="site", site=site)

    return index


# Keyed by id(config) rather than the config dict itself (which isn't
# hashable in general) - safe because load_config() caches and returns the
# same long-lived dict object for a given path, which is how every caller
# obtains a config in practice.
_bucket_index_cache: dict[int, dict] = {}


def _get_bucket_index(config: dict) -> dict:
    key = id(config)
    if key not in _bucket_index_cache:
        _bucket_index_cache[key] = _build_bucket_index(config)
    return _bucket_index_cache[key]


def parse_bucket_name(
    config: dict, bucket_name: str, *, bucket: str | None = None, scope: str | None = None
) -> BucketMatch:
    """Resolve a bucket name back to the config entry that produces it

    This is the inverse of project_bucket()/site_bucket(): rather than
    inferring structure from the bucket name's characters (fragile - see the
    positional-split parsing this replaces), it enumerates every bucket name
    the config can produce and looks the input up in that index. This means
    a bucket name can only ever resolve to a real, currently-configured
    bucket, and an ambiguous config (two layouts producing the same name) is
    caught at index-build time rather than on a live notification.

    Args:
        config (dict): The loaded roz config, from load_config()
        bucket_name (str): The bucket name to resolve
        bucket (str | None): If given, require the match's bucket key to equal this
        scope (str | None): If given, require the match's scope ("project" or "site") to equal this

    Returns:
        BucketMatch: The resolved match

    Raises:
        ConfigError: If the bucket name doesn't match any known bucket, or
            doesn't satisfy the `bucket`/`scope` filters
    """
    index = _get_bucket_index(config)

    match = index.get(bucket_name)
    if match is None:
        raise ConfigError(f"Bucket name {bucket_name!r} does not match any known bucket layout")

    if bucket is not None and match.bucket != bucket:
        raise ConfigError(
            f"Bucket name {bucket_name!r} resolved to bucket {match.bucket!r}, expected {bucket!r}"
        )

    if scope is not None and match.scope != scope:
        raise ConfigError(
            f"Bucket name {bucket_name!r} resolved to scope {match.scope!r}, expected {scope!r}"
        )

    return match


def try_parse_bucket_name(config: dict, bucket_name: str, **kwargs) -> BucketMatch | None:
    """Like parse_bucket_name(), but returns None instead of raising on no/bad match

    Args:
        config (dict): The loaded roz config, from load_config()
        bucket_name (str): The bucket name to resolve
        **kwargs: Passed through to parse_bucket_name() (bucket, scope)

    Returns:
        BucketMatch | None: The resolved match, or None
    """
    try:
        return parse_bucket_name(config, bucket_name, **kwargs)
    except ConfigError:
        return None


def parse_ingest_bucket_name(config: dict, bucket_name: str) -> dict:
    """Resolve an ingest bucket name into the fields s3_matcher/s3_onyx_updates need

    Args:
        config (dict): The loaded roz config, from load_config()
        bucket_name (str): The ingest bucket name to resolve

    Returns:
        dict: {"project", "raw_site", "site", "platform", "test_flag", "scope"}.
            For a project-level ingest bucket (no real site involved),
            raw_site/site are "public", matching this codebase's pre-existing
            convention for that case.

    Raises:
        ConfigError: If the bucket name doesn't resolve to an "ingest" bucket
    """
    match = parse_bucket_name(config, bucket_name, bucket="ingest")

    raw_site = match.site or "public"

    return {
        "project": match.project,
        "raw_site": raw_site,
        "site": short_site(raw_site),
        "platform": match.platform,
        "test_flag": match.test_flag,
        "scope": match.scope,
    }
