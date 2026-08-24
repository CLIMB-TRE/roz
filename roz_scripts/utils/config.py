import json
import os
import regex as re


class ConfigError(ValueError):
    """Raised when the roz config can't be loaded, or a bucket lookup fails against it"""


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
