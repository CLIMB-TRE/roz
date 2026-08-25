import boto3
import json
import sys
from botocore.exceptions import ClientError
from botocore.client import Config
import os
import re
import copy
import requests

S3_ENDPOINT_URL = "https://s3.climb.ac.uk"

REQUESTS_TIMEOUT = 30

policy_template = {
    "Version": "2012-10-17",
    "Statement": [],
}

statement_template = {
    "Effect": "Allow",
    "Principal": {"AWS": ["arn:aws:iam:::user/{}"]},  # admin username
    "Action": [],
    "Resource": ["arn:aws:s3:::{}/*"],
}

ssl_only_statement_template = {
    "Sid": "AllowSSLRequestsOnly",
    "Action": "s3:*",
    "Effect": "Deny",
    "Resource": [],
    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
    "Principal": "*",
}

in_actions_template = [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket",
]

# out_actions_template = ["s3:GetObject", "s3:ListBucket"]

admin_bucket_actions_template = [
    "s3:DeleteBucketPolicy",
    "s3:DeleteBucketWebsite",
    "s3:DeleteBucket",
    "s3:GetBucketAcl",
    "s3:GetBucketLogging",
    "s3:GetBucketNotification",
    "s3:GetBucketPolicy",
    "s3:GetBucketTagging",
    "s3:GetBucketVersioning",
    "s3:GetBucketWebsite",
    "s3:GetLifecycleConfiguration",
    "s3:ListBucket",
    "s3:ListAllMyBuckets",
    "s3:ListBucketMultipartUploads",
    "s3:ListBucketVersions",
    "s3:PutBucketLogging",
    "s3:PutBucketNotification",
    "s3:PutBucketPolicy",
    "s3:PutBucketRequestPayment",
    "s3:PutBucketTagging",
    "s3:PutBucketVersioning",
    "s3:PutBucketWebsite",
    "s3:PutBucketAcl",
]

admin_obj_actions_template = [
    "s3:AbortMultipartUpload",
    "s3:CreateBucket",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
    "s3:GetObjectAcl",
    "s3:GetObject",
    "s3:GetObjectVersionAcl",
    "s3:GetObjectVersion",
    "s3:GetObjectVersionTorrent",
    "s3:ListMultipartUploadParts",
    "s3:PutLifecycleConfiguration",
    "s3:PutObjectAcl",
    "s3:PutObjectVersionAcl",
    "s3:RestoreObject",
    "s3:PutObject",
]

perm_map = {
    "get": "s3:GetObject",
    "put": "s3:PutObject",
    "delete": "s3:DeleteObject",
    "list": "s3:ListBucket",
}


def resolve_credentials(
    aws_credentials_dict: dict, project: str, site: str | None
) -> dict:
    """Resolve the credentials to use for a given project/site pair.

    The "admin" identity is a top-level key in aws_credentials_dict rather than
    being nested under a project, so it needs special casing wherever either
    project or site is "admin" (different callers use either convention).

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}
        project (str): The project the credentials belong to, or "admin"
        site (str): The site the credentials belong to, or "admin"

    Returns:
        dict: The credentials dict for the resolved identity
    """
    if site == "admin" or project == "admin":
        return aws_credentials_dict["admin"]

    return aws_credentials_dict[project][site]


def bryn_team_slug(aws_credentials_dict: dict, project: str, site: str) -> str:
    """Derive a site's bryn team slug from its RGW/bryn username

    This must be derived from the site's actual username, NOT the config "site"
    key - the two are not guaranteed to match (a team's RGW username can differ
    from the name used for it in config), and bryn's team-scoped endpoints are
    keyed on the username-derived slug, not the config key.

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}
        project (str): The project the site belongs to
        site (str): The site's config key

    Returns:
        str: The bryn team slug
    """
    username = aws_credentials_dict[project][site]["username"]
    return username[0:16].replace(".", "-")


def get_s3_client(credentials: dict, config: Config | None = None):
    """Construct a boto3 S3 client for the given credentials

    Args:
        credentials (dict): A dict of the form {aws_access_key_id: "", aws_secret_access_key: "", username: ""}
        config (Config, optional): botocore Config to pass through, e.g. for signature_version overrides

    Returns:
        boto3.client: An S3 client
    """
    kwargs = {
        "aws_access_key_id": credentials["aws_access_key_id"],
        "aws_secret_access_key": credentials["aws_secret_access_key"],
        "endpoint_url": S3_ENDPOINT_URL,
    }

    if config is not None:
        kwargs["config"] = config

    return boto3.client("s3", **kwargs)


def get_s3_resource(credentials: dict):
    """Construct a boto3 S3 resource for the given credentials

    Args:
        credentials (dict): A dict of the form {aws_access_key_id: "", aws_secret_access_key: "", username: ""}

    Returns:
        boto3.resource: An S3 resource
    """
    return boto3.resource(
        "s3",
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        endpoint_url=S3_ENDPOINT_URL,
    )


def create_config_map(config_dict: dict) -> dict:
    """Create a map of all the buckets that need to be created for each site and correct permissions

    Args:
        config_dict (dict): The config file as a dictionary

    Returns:
        dict: A dictionary of the form {project: {sites: {site: {in_buckets: []}, policies: {in: [], out: []}, out_buckets: []}}
    """
    config_map = {}

    for project, config in config_dict["configs"].items():
        # Put this in the config file eventually so it can vary on a per-project basis

        project_config: dict = {
            "sites": {site: {"site_buckets": set()} for site in config["sites"]}
        }

        project_config.setdefault("project_buckets", set())

        for bucket, bucket_config in config["project_buckets"].items():
            desired_labels = re.findall(r"{(\w*)}", bucket_config["name_layout"])

            for platform in config["file_specs"].keys():
                for test_flag in ["prod", "test"]:
                    try:
                        namespace = {}

                        # Can't do a dict comp here
                        for label in desired_labels:
                            namespace[label] = locals()[label]

                        bucket_name = bucket_config["name_layout"].format(**namespace)

                        project_config["project_buckets"].add((bucket, bucket_name))

                    except KeyError as e:
                        e.add_note(
                            f"Bucket layout {bucket_config['name_layout']} is invalid"
                        )
                        raise e

        for site in config["sites"]:
            for bucket, bucket_config in config["site_buckets"].items():
                desired_labels = re.findall(r"{(\w*)}", bucket_config["name_layout"])

                for platform in config["file_specs"].keys():
                    for test_flag in ["prod", "test"]:
                        try:
                            namespace = {}

                            # Can't do a dict comp here
                            for label in desired_labels:
                                namespace[label] = locals()[label]

                            bucket_name = bucket_config["name_layout"].format(
                                **namespace
                            )

                            project_config["sites"][site]["site_buckets"].add(
                                (bucket, bucket_name)
                            )

                        except KeyError as e:
                            e.add_note(
                                f"Bucket layout {bucket_config['name_layout']} is invalid"
                            )
                            raise e

        config_map[project] = project_config

    return config_map


def check_project_bucket_exists(
    bucket_name: str, aws_credentials_dict: dict, project: str, site: str
) -> bool:
    """Check if a bucket exists

    Args:
        bucket_name (str): The name of the bucket
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}

    Returns:
        bool: True if the bucket exists, False otherwise
    """
    credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3 = get_s3_resource(credentials)

    bucket = s3.Bucket(bucket_name)  # type: ignore

    if bucket.creation_date:
        return True
    else:
        return False


def can_site_list_objects(
    bucket_name: str, aws_credentials_dict: dict, project: str, site: str
) -> bool:
    """Check if a site can list objects in a bucket

    Args:
        bucket_name (str): name of bucket to check
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        project (str): name of project in question
        site (str): name of site in question

    Raises:
        ValueError: If the bucket does not exist

    Returns:
        bool: True if the site can list objects in the bucket, False otherwise
    """
    site_credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3 = get_s3_client(site_credentials)

    try:
        s3.list_objects_v2(Bucket=bucket_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            raise ValueError(f"Bucket {bucket_name} does not exist")
        else:
            return False


def can_site_get_object(
    bucket_name: str, aws_credentials_dict: dict, project: str, site: str
) -> bool:
    """Check if a site can get an object from a bucket

    Args:
        bucket_name (str): name of bucket to check
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        project (str): name of project in question
        site (str): name of site in question

    Returns:
        bool: True if the site can get an object from the bucket, False otherwise
    """
    site_credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3 = get_s3_client(site_credentials)

    try:
        s3.get_object(Bucket=bucket_name, Key="test")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return True
        else:
            return False
    except Exception:
        return False


def can_site_put_object(
    bucket_name: str, aws_credentials_dict: dict, project: str, site: str
) -> bool:
    """Check if a site can put an object in a bucket

    Args:
        bucket_name (str): name of bucket to check
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        project (str): name of project in question
        site (str): name of site in question

    Raises:
        ValueError: If the bucket does not exist

    Returns:
        bool: True if the site can put an object in the bucket, False otherwise
    """

    site_credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3 = get_s3_client(site_credentials)

    try:
        s3.put_object(Bucket=bucket_name, Key="test", Body=b"test")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            raise ValueError(f"Bucket {bucket_name} does not exist")
        else:
            return False


def can_site_delete_object(
    bucket_name: str, aws_credentials_dict: dict, project: str, site: str
) -> bool:
    """Check if a site can delete an object from a bucket

    Args:
        bucket_name (str): name of bucket to check
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        project (str): name of project in question
        site (str): name of site in question

    Raises:
        ValueError: If the bucket does not exist

    Returns:
        bool: True if the site can delete an object from the bucket, False otherwise
    """
    site_credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3 = get_s3_client(site_credentials)

    try:
        s3.delete_object(Bucket=bucket_name, Key="test")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            raise ValueError(f"Bucket {bucket_name} does not exist")
        elif e.response["Error"]["Code"] == "NoSuchKey":
            return True
        else:
            return False


def put_project_policy(
    bucket_arn: str,
    aws_credentials_dict: dict,
    policy: dict,
    project: str,
    site: str | None,
) -> bool:
    """Put a policy on a bucket

    Args:
        bucket_name (str): name of bucket to put policy on
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        policy (dict): The policy to put on the bucket as a dictionary

    Returns:
        bool: True if the policy was put on the bucket, False otherwise
    """
    credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3 = get_s3_client(credentials)

    if isinstance(policy, dict):
        policy = json.dumps(policy, separators=(",", ":"))  # type: ignore

    try:
        # Retrieve waiter instance that will wait till a specified bucket exists
        s3_bucket_exists_waiter = s3.get_waiter("bucket_exists")

        # Wait till bucket exists
        s3_bucket_exists_waiter.wait(Bucket=bucket_arn)

        s3.put_bucket_policy(Bucket=bucket_arn, Policy=policy)
        return True
    except ClientError as e:
        print(e)
        return False


def generate_site_policy(
    bucket_name: str,
    bucket_arn: str,
    project: str,
    site: str,
    aws_credentials_dict: dict,
    config_dict: dict,
) -> dict:
    """Generate the policy for a site bucket

    Args:
        bucket_name (str): The name of the bucket
        project (str): The project the bucket belongs to
        site (str): The site the bucket belongs to
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}

    Returns:
        dict: The policy as a dictionary
    """
    policy = copy.deepcopy(policy_template)

    site_role = config_dict["configs"][project]["sites"][site]

    site_slug = bryn_team_slug(aws_credentials_dict, project, site)

    # admin_slug = aws_credentials_dict["admin"]["username"][0:16].replace(".", "-")

    # Force SSL only

    ssl_only_statement = copy.deepcopy(ssl_only_statement_template)
    ssl_only_statement["Resource"] = [
        f"arn:aws:s3:::{bucket_arn}/*",
        f"arn:aws:s3:::{bucket_arn}",
    ]

    policy["Statement"].append(ssl_only_statement)

    # Add the admin object permissions statement
    admin_obj_statement = copy.deepcopy(statement_template)

    admin_obj_statement["Principal"]["AWS"] = [
        f"arn:aws:iam:::user/{aws_credentials_dict["admin"]["username"]}"
    ]

    admin_obj_statement["Action"] = admin_obj_actions_template

    admin_obj_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}/*"]

    policy["Statement"].append(admin_obj_statement)

    # Add the admin bucket permissions statement

    admin_bucket_statement = copy.deepcopy(statement_template)

    admin_bucket_statement["Principal"]["AWS"] = [
        f"arn:aws:iam:::user/{aws_credentials_dict["admin"]["username"]}"
    ]

    admin_bucket_statement["Action"] = admin_bucket_actions_template

    admin_bucket_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}"]

    policy["Statement"].append(admin_bucket_statement)

    # Add the site statement
    site_obj_statement = copy.deepcopy(statement_template)

    site_obj_statement["Principal"]["AWS"] = [f"arn:aws:iam:::user/bryn-{site_slug}"]

    site_bucket_statement = copy.deepcopy(statement_template)

    site_bucket_statement["Principal"]["AWS"] = [f"arn:aws:iam:::user/bryn-{site_slug}"]
    try:
        permission_set = config_dict["configs"][project]["site_buckets"][bucket_name][
            "policy"
        ][site_role]
    except KeyError:
        permission_set = None

    if permission_set:

        correct_perms = config_dict["configs"][project]["bucket_policies"][
            permission_set
        ]

        site_obj_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}/*"]

        site_bucket_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}"]

        for perm in correct_perms:
            aws_perm = perm_map[perm]
            if aws_perm in admin_obj_actions_template:
                site_obj_statement["Action"].append(perm_map[perm])

            elif aws_perm in admin_bucket_actions_template:
                site_bucket_statement["Action"].append(perm_map[perm])

        if site_obj_statement["Action"]:
            policy["Statement"].append(site_obj_statement)

        if site_bucket_statement["Action"]:
            policy["Statement"].append(site_bucket_statement)

    return policy


def generate_project_policy(
    bucket_name: str,
    bucket_arn: str,
    project: str,
    config_dict: dict,
    aws_credentials_dict: dict,
) -> dict:
    """Generate the policy for an out bucket

    Args:
        bucket_name (str): The name of the bucket
        bucket_arn (str): The ARN of the bucket
        project (str): The project the bucket belongs to
        config_dict (dict): Dict created from the config JSON
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}

    Returns:
        dict: The policy as a dictionary
    """

    policy = copy.deepcopy(policy_template)

    # admin_slug = aws_credentials_dict["admin"]["username"][0:16].replace(".", "-")

    # Force SSL only
    ssl_only_statement = copy.deepcopy(ssl_only_statement_template)
    ssl_only_statement["Resource"] = [
        f"arn:aws:s3:::{bucket_arn}/*",
        f"arn:aws:s3:::{bucket_arn}",
    ]

    policy["Statement"].append(ssl_only_statement)

    # Add the admin object permissions statement
    admin_obj_statement = copy.deepcopy(statement_template)

    admin_obj_statement["Principal"]["AWS"] = [
        f"arn:aws:iam:::user/{aws_credentials_dict["admin"]["username"]}"
    ]

    admin_obj_statement["Action"] = admin_obj_actions_template

    admin_obj_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}/*"]

    policy["Statement"].append(admin_obj_statement)

    # Add the admin bucket permissions statement

    admin_bucket_statement = copy.deepcopy(statement_template)

    admin_bucket_statement["Principal"]["AWS"] = [
        f"arn:aws:iam:::user/{aws_credentials_dict["admin"]["username"]}"
    ]

    admin_bucket_statement["Action"] = admin_bucket_actions_template

    admin_bucket_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}"]

    policy["Statement"].append(admin_bucket_statement)

    # Sites that resolve to the same permission set (usually because they share
    # a role) are granted identical actions, so they're grouped into a single
    # pair of statements with multiple principals instead of one pair each -
    # this is what keeps the policy from growing linearly with site count.
    bucket_policy_map = config_dict["configs"][project]["project_buckets"][
        bucket_name
    ]["policy"]

    site_arns_by_permission_set = {}

    for site, role in config_dict["configs"][project]["sites"].items():
        if role not in bucket_policy_map:
            continue

        permission_set = bucket_policy_map[role]

        correct_perms = config_dict["configs"][project]["bucket_policies"].get(
            permission_set, []
        )

        if not correct_perms:
            continue

        site_slug = bryn_team_slug(aws_credentials_dict, project, site)

        site_arns_by_permission_set.setdefault(permission_set, []).append(
            f"arn:aws:iam:::user/bryn-{site_slug}"
        )

    for permission_set, site_arns in site_arns_by_permission_set.items():
        correct_perms = config_dict["configs"][project]["bucket_policies"][
            permission_set
        ]

        site_obj_statement = copy.deepcopy(statement_template)
        site_obj_statement["Principal"]["AWS"] = site_arns
        site_obj_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}/*"]

        site_bucket_statement = copy.deepcopy(statement_template)
        site_bucket_statement["Principal"]["AWS"] = site_arns
        site_bucket_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}"]

        for perm in correct_perms:
            aws_perm = perm_map[perm]
            if aws_perm in admin_obj_actions_template:
                site_obj_statement["Action"].append(aws_perm)

            elif aws_perm in admin_bucket_actions_template:
                site_bucket_statement["Action"].append(aws_perm)

        if site_obj_statement["Action"]:
            policy["Statement"].append(site_obj_statement)

        if site_bucket_statement["Action"]:
            policy["Statement"].append(site_bucket_statement)

    return policy


def create_project_bucket(
    bucket_name: str, project: str, site: str, aws_credentials_dict: dict
) -> bool:
    """Create a bucket

    Args:
        bucket_name (str): The name of the bucket
        project (str): The project the bucket belongs to
        site (str): The site the bucket belongs to
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}

    Returns:
        bool: True if the bucket was created, False otherwise
    """
    credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3 = get_s3_client(credentials)

    try:
        s3.create_bucket(Bucket=bucket_name, ACL="private")
        return True
    except ClientError as e:
        print(
            f"Failed to create bucket {bucket_name} - Boto Exception:\n{e}",
            file=sys.stderr,
        )
        return False


def create_site_bucket(
    bucket_arn: str,
    slug: str,
    policy: dict,
) -> bool:
    """Create a bucket via bryn

    Args:
        bucket_name (str): The name of the bucket
        bucket_arn (str): The ARN of the bucket
        project (str): The project the bucket belongs to
        site (str): The site the bucket belongs to
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}
        config_dict (dict): The config json as a dictionary

    Returns:
        bool: True if the bucket was created, False otherwise
    """

    bryn_url = os.getenv("BRYN_API_URL")

    endpoint_url = f"{bryn_url}/admin-api/teams/{slug}/ceph/s3/buckets/"

    headers = {"Authorization": f"token {os.getenv('BRYN_API_TOKEN')}"}

    data = {"name": bucket_arn, "policy": json.dumps(policy, separators=(",", ":"))}

    r = requests.post(endpoint_url, headers=headers, json=data, timeout=REQUESTS_TIMEOUT)

    if r.status_code == 201:
        return True
    else:
        print(
            f"Failed to create bucket {bucket_arn} - Bryn Response:\n{r.text}",
            file=sys.stderr,
        )
        print(f"URL: {r.url}", file=sys.stderr)
        print(f"Request: {r.request}", file=sys.stderr)
        sys.exit(r.status_code)


def put_site_policy(bucket_arn: str, slug: str, policy: dict) -> bool:
    """Put a policy on a bucket via bryn

    Args:
        bucket_arn (str): The ARN of the bucket
        slug (str): The site's bryn team slug - derived from the site's RGW/bryn
            username (aws_credentials_dict[project][site]["username"]), NOT the
            config "site" key, since the two can differ. See create_site_bucket,
            which already gets this right, for the derivation.
        policy (dict): The policy to put on the bucket as a dictionary

    Returns:
        bool: True if the policy was put on the bucket, False otherwise
    """
    bryn_url = os.getenv("BRYN_API_URL")

    endpoint_url = (
        f"{bryn_url}/admin-api/teams/{slug}/ceph/s3/buckets/{bucket_arn}/"
    )

    headers = {"Authorization": f"token {os.getenv('BRYN_API_TOKEN')}"}

    response = requests.patch(
        endpoint_url,
        headers=headers,
        json={"policy": json.dumps(policy, separators=(",", ":"))},
        timeout=REQUESTS_TIMEOUT,
    )

    if response.status_code == 200:
        return True
    else:
        print(
            f"Failed to put policy on bucket {bucket_arn}, Bryn response:\n{response.text}",
            file=sys.stderr,
        )
        return False


def check_site_bucket_exists(bucket_arn: str, slug: str) -> bool:
    """Check if a bucket exists via bryn

    Args:
        bucket_arn (str): The ARN of the bucket
        slug (str): The site's bryn team slug - derived from the site's RGW/bryn
            username (aws_credentials_dict[project][site]["username"]), NOT the
            config "site" key, since the two can differ. See create_site_bucket,
            which already gets this right, for the derivation.

    Returns:
        bool: True if the bucket exists, False otherwise
    """

    bryn_url = os.getenv("BRYN_API_URL")

    endpoint_url = (
        f"{bryn_url}/admin-api/teams/{slug}/ceph/s3/buckets/{bucket_arn}/"
    )

    headers = {"Authorization": f"token {os.getenv('BRYN_API_TOKEN')}"}

    response = requests.get(endpoint_url, headers=headers, timeout=REQUESTS_TIMEOUT)

    if response.status_code == 200:
        return True
    elif response.status_code == 404:
        return False
    else:
        print(
            f"Failed to check if bucket {bucket_arn} exists, Bryn response:\n{response.text}",
            file=sys.stderr,
        )
        print(f"URL: {response.url}", file=sys.stderr)
        print(f"Request: {response.request.body}", file=sys.stderr)
        print(f"Status code: {response.status_code}", file=sys.stderr)
        sys.exit(response.status_code)


def audit_bucket_policy(
    bucket_name: str,
    aws_credentials_dict: dict,
    project: str,
    config_map: dict,
) -> dict:
    """Audit the policy on a bucket

    Args:
        bucket_name (str): The name of the bucket
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        project (str): The project the bucket belongs to
        site (str): The site the bucket belongs to
        config_map (dict): The config map as a dictionary

    Returns:
        dict: A dictionary of the form {site: {list: True, get: True, put: True, delete: True, put_policy: True, delete_policy: True, get_policy: True}}
    """

    to_test = [x for x in config_map[project]["sites"].keys()]
    to_test.append("admin")

    policy_audit: dict = {
        x: {
            "list": None,
            "get": None,
            "put": None,
            "delete": None,
            # "put_policy": None,
            # "delete_policy": None,
        }
        for x in to_test
    }

    for site in to_test:
        policy_audit[site]["list"] = can_site_list_objects(
            bucket_name=bucket_name,
            aws_credentials_dict=aws_credentials_dict,
            project=project,
            site=site,
        )
        policy_audit[site]["get"] = can_site_get_object(
            bucket_name=bucket_name,
            aws_credentials_dict=aws_credentials_dict,
            project=project,
            site=site,
        )
        policy_audit[site]["delete"] = can_site_delete_object(
            bucket_name=bucket_name,
            aws_credentials_dict=aws_credentials_dict,
            project=project,
            site=site,
        )
        policy_audit[site]["put"] = can_site_put_object(
            bucket_name=bucket_name,
            aws_credentials_dict=aws_credentials_dict,
            project=project,
            site=site,
        )
        # policy_audit[site]["put_policy"] = can_site_modify_policy(
        #     bucket_name=bucket_name,
        #     aws_credentials_dict=aws_credentials_dict,
        #     project=project,
        #     site=site,
        # )
        # policy_audit[site]["delete_policy"] = can_site_delete_policy(
        #     bucket_name=bucket_name,
        #     aws_credentials_dict=aws_credentials_dict,
        #     project=project,
        #     site=site,
        # )

    return policy_audit


def check_bucket_exist_and_create(
    aws_credentials_dict: dict,
    config_map: dict,
    config_dict: dict,
    dry_run: bool = False,
) -> None:
    """Check if all specified buckets exist, and if not, create them

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        config_map (dict): The config map as a dictionary

    Raises:
        ValueError: If a bucket cannot be created
    """

    for project, project_config in config_map.items():
        # Create project buckets (made by admin user)
        for bucket, bucket_arn in project_config["project_buckets"]:
            exists = check_project_bucket_exists(
                bucket_arn, aws_credentials_dict, project, "admin"
            )

            if exists:
                print(
                    f"Bucket {bucket_arn} already exists, no need to create",
                    file=sys.stdout,
                )
                continue

            if dry_run:
                print(
                    f"Dry run, bucket {bucket_arn} does not exist, not creating",
                    file=sys.stdout,
                )
                continue

            print(f"Idempotently creating bucket {bucket_arn}", file=sys.stdout)
            create_success = create_project_bucket(
                bucket_name=bucket_arn,
                project=project,
                site="admin",
                aws_credentials_dict=aws_credentials_dict,
            )

            if not create_success:

                raise ValueError(f"Bucket {bucket_arn} could not be created")

        # Create in buckets (made by site user)
        for site, site_config in project_config["sites"].items():
            site_slug = bryn_team_slug(aws_credentials_dict, project, site)

            for bucket, bucket_arn in site_config["site_buckets"]:
                exists = check_site_bucket_exists(bucket_arn=bucket_arn, slug=site_slug)

                if exists:
                    print(
                        f"Bucket {bucket_arn} already exists, no need to create",
                        file=sys.stdout,
                    )
                    continue

                if dry_run:
                    print(
                        f"Dry run, bucket {bucket_arn} does not exist, not creating",
                        file=sys.stdout,
                    )
                    continue

                print(f"Idempotently creating bucket {bucket_arn}", file=sys.stdout)

                policy = generate_site_policy(
                    bucket_name=bucket,
                    bucket_arn=bucket_arn,
                    project=project,
                    site=site,
                    aws_credentials_dict=aws_credentials_dict,
                    config_dict=config_dict,
                )

                create_success = create_site_bucket(
                    bucket_arn=bucket_arn,
                    slug=site_slug,
                    policy=policy,
                )

                if not create_success:
                    raise ValueError(f"Site bucket {bucket_arn} could not be created")


def audit_all_buckets(
    aws_credentials_dict: dict, config_map: dict, dry_run: bool = False
) -> dict:
    """Iterate through all buckets and audit their policies

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        config_map (dict): The config map as a dictionary

    Returns:
        audit_dict (dict): A dictionary of the form {project: {in_buckets: {bucket: {list: True, get: True, put: True, delete: True, put_policy: True, delete_policy: True, get_policy: True}}, out_buckets: {bucket: {list: True, get: True, put: True, delete: True, put_policy: True, delete_policy: True, get_policy: True}}}}
    """

    audit_dict = {
        project: {
            "site_buckets": {x: {} for x in config_map[project]["sites"].keys()},
            "project_buckets": {},
        }
        for project in config_map.keys()
    }

    if dry_run:
        return audit_dict

    for project, project_config in config_map.items():
        # Audit out buckets (made by admin user)
        for bucket, bucket_arn in project_config["project_buckets"]:
            audit_dict[project]["project_buckets"][(bucket, bucket_arn)] = (
                audit_bucket_policy(
                    bucket_name=bucket_arn,
                    aws_credentials_dict=aws_credentials_dict,
                    project=project,
                    config_map=config_map,
                )
            )

        # Audit in buckets (made by site user)
        for site, site_config in project_config["sites"].items():
            for bucket, bucket_arn in site_config["site_buckets"]:
                audit_dict[project]["site_buckets"][site][(bucket, bucket_arn)] = (
                    audit_bucket_policy(
                        bucket_name=bucket_arn,
                        aws_credentials_dict=aws_credentials_dict,
                        project=project,
                        config_map=config_map,
                    )
                )

    return audit_dict


def _site_bucket_expected_perms(
    config_dict: dict, project: str, bucket: str, owner_site: str, audit_site: str
) -> list:
    """Expected permission set for `audit_site` probing a site bucket owned by `owner_site`

    Only the owning site is ever expected to hold any of the configured permissions -
    every other (non-admin) site is expected to hold none, which is itself an
    important invariant this audit checks for.
    """
    if audit_site != owner_site:
        return []

    try:
        site_role = config_dict["configs"][project]["sites"][audit_site]
        permission_set = config_dict["configs"][project]["site_buckets"][bucket][
            "policy"
        ][site_role]
        return config_dict["configs"][project]["bucket_policies"][permission_set]
    except KeyError:
        return []


def _project_bucket_expected_perms(
    config_dict: dict, project: str, bucket: str, audit_site: str
) -> list:
    """Expected permission set for `audit_site` probing a project bucket

    Unlike site buckets, several sites can legitimately share access to a project
    bucket (see the permission-set grouping in generate_project_policy), so there is
    no single "owning" site to gate against here.
    """
    try:
        audit_site_role = config_dict["configs"][project]["sites"][audit_site]
        permission_set = config_dict["configs"][project]["project_buckets"][bucket][
            "policy"
        ][audit_site_role]
        return config_dict["configs"][project]["bucket_policies"][permission_set]
    except KeyError:
        return []


def _record_permission_drift(
    audit_results: dict,
    correct_perms: list,
    audit_site: str,
    bucket_label: str,
    context: str,
    to_fix: set,
    to_fix_value: tuple,
) -> None:
    """Compare probed permissions against the expected set, printing and recording drift

    A result of True for a permission not in correct_perms (and not admin) is an
    unexpected grant (a leak); a result of False for a permission that should be
    granted is a missing grant. Either is drift that needs fixing.
    """
    for permission, result in audit_results.items():
        if result:
            if permission in correct_perms or audit_site == "admin":
                continue
            print(
                f"Incorrect policy for bucket {bucket_label} detected\n{context}, Audit site: {audit_site}, Permission: {permission}, Result: {result}, Correct perms: {correct_perms}",
                file=sys.stdout,
            )
            to_fix.add(to_fix_value)
        else:
            if permission in correct_perms or audit_site == "admin":
                print(
                    f"Missing policy for bucket {bucket_label} detected\n{context}, Audit site: {audit_site}, Permission: {permission}, Result: {result}, Correct perms: {correct_perms}",
                    file=sys.stdout,
                )
                to_fix.add(to_fix_value)


def test_policies(audit_dict: dict, config_dict: dict) -> dict:
    """Test the policies on all buckets and return a dict of buckets that need to be fixed

    Args:
        audit_dict (dict): A dictionary of the form {project: {in_buckets: {bucket: {list: True, get: True, put: True, delete: True, put_policy: True, delete_policy: True, get_policy: True}}, out_buckets: {bucket: {list: True, get: True, put: True, delete: True, put_policy: True, delete_policy: True, get_policy: True}}}}
        config_map (dict): The config map as a dictionary

    Returns:
        dict: A dictionary of the form {in_buckets: [(bucket, project, site)], out_buckets: [(bucket, project)]} indicating which buckets need to be fixed
    """

    to_fix = {"site_buckets": set(), "project_buckets": set()}

    for project, buckets in audit_dict.items():
        for site, site_buckets in buckets["site_buckets"].items():
            for (bucket, bucket_arn), bucket_audit in site_buckets.items():
                for audit_site, audit_results in bucket_audit.items():
                    correct_perms = _site_bucket_expected_perms(
                        config_dict, project, bucket, site, audit_site
                    )

                    _record_permission_drift(
                        audit_results=audit_results,
                        correct_perms=correct_perms,
                        audit_site=audit_site,
                        bucket_label=bucket_arn,
                        context=f"Site: {site}",
                        to_fix=to_fix["site_buckets"],
                        to_fix_value=(bucket, bucket_arn, project, site),
                    )

        for (bucket, bucket_arn), bucket_audit in buckets["project_buckets"].items():
            for audit_site, audit_results in bucket_audit.items():
                correct_perms = _project_bucket_expected_perms(
                    config_dict, project, bucket, audit_site
                )

                _record_permission_drift(
                    audit_results=audit_results,
                    correct_perms=correct_perms,
                    audit_site=audit_site,
                    bucket_label=bucket,
                    context="Project bucket",
                    to_fix=to_fix["project_buckets"],
                    to_fix_value=(bucket, bucket_arn, project),
                )

    return to_fix


def fetch_deployed_policy(bucket_arn: str, aws_credentials_dict: dict) -> dict | None:
    """Fetch the policy document currently deployed on a bucket, admin-credentialed

    Args:
        bucket_arn (str): The name of the bucket
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}

    Returns:
        dict | None: The deployed policy document, or None if the bucket has no policy attached
    """
    admin_credentials = aws_credentials_dict["admin"]

    s3 = get_s3_client(admin_credentials)

    try:
        response = s3.get_bucket_policy(Bucket=bucket_arn)
        return json.loads(response["Policy"])
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
            return None
        raise


def policy_to_grants(policy: dict | None) -> dict:
    """Reduce a policy document to {principal_arn: set(actions)} for its Allow statements

    Deny statements (e.g. the SSL-only statement) are deliberately excluded - this
    describes what is actually granted, not what is restricted.

    Args:
        policy (dict | None): A policy document, or None for "no policy"

    Returns:
        dict: A mapping of principal ARN to the set of actions granted to it
    """
    grants: dict = {}

    if policy is None:
        return grants

    for statement in policy.get("Statement", []):
        if statement.get("Effect") != "Allow":
            continue

        principal = statement.get("Principal")
        if isinstance(principal, dict):
            principals = principal.get("AWS", [])
            if isinstance(principals, str):
                principals = [principals]
        elif isinstance(principal, str):
            principals = [principal]
        else:
            principals = []

        actions = statement.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]

        for arn in principals:
            grants.setdefault(arn, set()).update(actions)

    return grants


def diff_bucket_policy_grants(
    bucket_name: str,
    bucket_arn: str,
    project: str,
    config_dict: dict,
    aws_credentials_dict: dict,
    site: str | None = None,
) -> dict:
    """Compare a bucket's deployed policy grants against what config says they should be

    Report-only: this does not decide whether a bucket needs fixing, it just surfaces
    disagreements between the live policy document and the policy that would be
    generated from config, for the Stage 2 report-only rollout described in the
    policy-check complexity reduction plan.

    Args:
        bucket_name (str): The name of the bucket, as used in config
        bucket_arn (str): The ARN of the bucket
        project (str): The project the bucket belongs to
        config_dict (dict): The config file as a dictionary
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}
        site (str | None): The owning site, for a site bucket; None for a project bucket

    Note on the admin principal: RGW grants the bucket-owning account (admin) implicit
    full access regardless of what the policy document says, so an absent or narrowed
    admin statement has no functional effect - the same reasoning test_policies already
    applies by never checking admin's probed permissions against correct_perms. The
    admin principal is therefore excluded from missing_principals/action_mismatches
    here too, to avoid flagging permanent, non-actionable drift.

    Returns:
        dict: {"missing_principals": [...], "unexpected_principals": [...], "action_mismatches": {principal: {"missing": [...], "extra": [...]}}}
    """
    if site is not None:
        expected_policy = generate_site_policy(
            bucket_name=bucket_name,
            bucket_arn=bucket_arn,
            project=project,
            site=site,
            aws_credentials_dict=aws_credentials_dict,
            config_dict=config_dict,
        )
    else:
        expected_policy = generate_project_policy(
            bucket_name=bucket_name,
            bucket_arn=bucket_arn,
            project=project,
            config_dict=config_dict,
            aws_credentials_dict=aws_credentials_dict,
        )

    deployed_policy = fetch_deployed_policy(bucket_arn, aws_credentials_dict)

    expected_grants = policy_to_grants(expected_policy)
    deployed_grants = policy_to_grants(deployed_policy)

    expected_principals = set(expected_grants)
    deployed_principals = set(deployed_grants)

    admin_arn = f"arn:aws:iam:::user/{aws_credentials_dict['admin']['username']}"

    action_mismatches = {}
    for principal in (expected_principals & deployed_principals) - {admin_arn}:
        missing_actions = expected_grants[principal] - deployed_grants[principal]
        extra_actions = deployed_grants[principal] - expected_grants[principal]
        if missing_actions or extra_actions:
            action_mismatches[principal] = {
                "missing": sorted(missing_actions),
                "extra": sorted(extra_actions),
            }

    return {
        "missing_principals": sorted(
            (expected_principals - deployed_principals) - {admin_arn}
        ),
        "unexpected_principals": sorted(deployed_principals - expected_principals),
        "action_mismatches": action_mismatches,
    }


def audit_policy_diff_report(
    aws_credentials_dict: dict,
    config_map: dict,
    config_dict: dict,
    dry_run: bool = False,
) -> dict:
    """Run the Stage 2 report-only policy-document diff across all buckets

    This runs alongside (not instead of) the existing functional-probe audit
    (audit_all_buckets/test_policies) so that disagreements between the two can be
    observed over several cycles before the audit mechanism itself is switched over.
    It never contributes to a to_fix decision.

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}
        config_map (dict): The config map as a dictionary
        config_dict (dict): The config file as a dictionary
        dry_run (bool, optional): If True, skip live calls and return an empty report

    Returns:
        dict: {"project_buckets": {(bucket, bucket_arn): diff}, "site_buckets": {(site, bucket, bucket_arn): diff}}
    """
    report: dict = {"project_buckets": {}, "site_buckets": {}}

    if dry_run:
        return report

    for project, project_config in config_map.items():
        for bucket, bucket_arn in project_config["project_buckets"]:
            diff = diff_bucket_policy_grants(
                bucket_name=bucket,
                bucket_arn=bucket_arn,
                project=project,
                config_dict=config_dict,
                aws_credentials_dict=aws_credentials_dict,
            )

            if diff["missing_principals"] or diff["unexpected_principals"] or diff["action_mismatches"]:
                print(
                    f"[policy-diff] Disagreement for project bucket {bucket_arn}: {diff}",
                    file=sys.stdout,
                )

            report["project_buckets"][(bucket, bucket_arn)] = diff

        for site, site_config in project_config["sites"].items():
            for bucket, bucket_arn in site_config["site_buckets"]:
                diff = diff_bucket_policy_grants(
                    bucket_name=bucket,
                    bucket_arn=bucket_arn,
                    project=project,
                    config_dict=config_dict,
                    aws_credentials_dict=aws_credentials_dict,
                    site=site,
                )

                if diff["missing_principals"] or diff["unexpected_principals"] or diff["action_mismatches"]:
                    print(
                        f"[policy-diff] Disagreement for site bucket {bucket_arn} (site={site}): {diff}",
                        file=sys.stdout,
                    )

                report["site_buckets"][(site, bucket, bucket_arn)] = diff

    return report


def retest_fixed_buckets(
    to_fix: dict, aws_credentials_dict: dict, config_dict: dict
) -> dict:
    """Re-check only the buckets that were just fixed, not the whole config_map

    Buckets that weren't in to_fix weren't touched by apply_policies, so re-auditing
    them again would just repeat the per-bucket diff/ACL cost for no new information.

    Args:
        to_fix (dict): {"site_buckets": {(bucket, bucket_arn, project, site)}, "project_buckets": {(bucket, bucket_arn, project)}}
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}
        config_dict (dict): The config file as a dictionary

    Returns:
        dict: Same shape as to_fix, containing only the buckets that still show drift
    """
    retest_to_fix = {"site_buckets": set(), "project_buckets": set()}

    for bucket, bucket_arn, project, site in to_fix["site_buckets"]:
        diff = diff_bucket_policy_grants(
            bucket_name=bucket,
            bucket_arn=bucket_arn,
            project=project,
            config_dict=config_dict,
            aws_credentials_dict=aws_credentials_dict,
            site=site,
        )
        acl = audit_bucket_acl(bucket_arn, aws_credentials_dict)

        if _bucket_needs_fix(diff, acl):
            retest_to_fix["site_buckets"].add((bucket, bucket_arn, project, site))

    for bucket, bucket_arn, project in to_fix["project_buckets"]:
        diff = diff_bucket_policy_grants(
            bucket_name=bucket,
            bucket_arn=bucket_arn,
            project=project,
            config_dict=config_dict,
            aws_credentials_dict=aws_credentials_dict,
        )
        acl = audit_bucket_acl(bucket_arn, aws_credentials_dict)

        if _bucket_needs_fix(diff, acl):
            retest_to_fix["project_buckets"].add((bucket, bucket_arn, project))

    return retest_to_fix


PUBLIC_ACL_URIS = (
    "http://acs.amazonaws.com/groups/global/AllUsers",
    "http://acs.amazonaws.com/groups/global/AuthenticatedUsers",
)


def audit_bucket_acl(bucket_arn: str, aws_credentials_dict: dict) -> dict:
    """Check a bucket's ACL for grants a policy document diff can't see

    The policy document only covers explicit statements - bucket ACLs are a
    separate access-control layer that generate_site_policy/generate_project_policy
    never touch, so a public/authenticated-users ACL grant would be invisible to
    diff_bucket_policy_grants even though it's a real access leak.

    Args:
        bucket_arn (str): The ARN of the bucket
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}

    Returns:
        dict: {"unexpected_grants": [{"grantee": ..., "permission": ...}]}
    """
    admin_credentials = aws_credentials_dict["admin"]

    s3 = get_s3_client(admin_credentials)

    response = s3.get_bucket_acl(Bucket=bucket_arn)

    owner_id = response.get("Owner", {}).get("ID")

    unexpected_grants = []
    for grant in response.get("Grants", []):
        grantee = grant.get("Grantee", {})

        if grantee.get("URI") in PUBLIC_ACL_URIS:
            unexpected_grants.append(
                {"grantee": grantee["URI"], "permission": grant.get("Permission")}
            )
        elif grantee.get("ID") and grantee["ID"] != owner_id:
            unexpected_grants.append(
                {"grantee": grantee["ID"], "permission": grant.get("Permission")}
            )

    return {"unexpected_grants": unexpected_grants}


def _bucket_needs_fix(diff: dict, acl: dict) -> bool:
    return bool(
        diff["missing_principals"]
        or diff["unexpected_principals"]
        or diff["action_mismatches"]
        or acl["unexpected_grants"]
    )


def audit_and_test_policies(
    aws_credentials_dict: dict,
    config_map: dict,
    config_dict: dict,
    dry_run: bool = False,
) -> tuple:
    """Stage 3 audit: decide which buckets need fixing from a policy-document diff
    plus an ACL check, instead of the O(buckets x sites x 4 calls) functional-probe
    cross product in audit_all_buckets/test_policies.

    This makes one GetBucketPolicy and one GetBucketAcl call per bucket (both
    admin-credentialed), regardless of how many sites exist - see the plan in the
    policy-check complexity reduction work for the reasoning and trade-offs (a
    document diff can't see RGW enforcement bugs or bypass ACLs on its own, which
    is why the ACL check exists and why run_canary_probes supplements this with a
    small rotating sample of real functional probes).

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}
        config_map (dict): The config map as a dictionary
        config_dict (dict): The config file as a dictionary
        dry_run (bool, optional): If True, skip live calls and return empty results

    Returns:
        tuple: (audit_report, to_fix) where audit_report is {"project_buckets": {(bucket, bucket_arn): {"diff": ..., "acl": ...}}, "site_buckets": {(site, bucket, bucket_arn): {"diff": ..., "acl": ...}}} and to_fix matches the shape test_policies returns: {"site_buckets": {(bucket, bucket_arn, project, site)}, "project_buckets": {(bucket, bucket_arn, project)}}
    """
    audit_report: dict = {"project_buckets": {}, "site_buckets": {}}
    to_fix = {"site_buckets": set(), "project_buckets": set()}

    if dry_run:
        return audit_report, to_fix

    for project, project_config in config_map.items():
        for bucket, bucket_arn in project_config["project_buckets"]:
            diff = diff_bucket_policy_grants(
                bucket_name=bucket,
                bucket_arn=bucket_arn,
                project=project,
                config_dict=config_dict,
                aws_credentials_dict=aws_credentials_dict,
            )
            acl = audit_bucket_acl(bucket_arn, aws_credentials_dict)

            audit_report["project_buckets"][(bucket, bucket_arn)] = {
                "diff": diff,
                "acl": acl,
            }

            if _bucket_needs_fix(diff, acl):
                print(
                    f"Policy drift detected for project bucket {bucket_arn}: {diff}, acl: {acl}",
                    file=sys.stdout,
                )
                to_fix["project_buckets"].add((bucket, bucket_arn, project))

        for site, site_config in project_config["sites"].items():
            for bucket, bucket_arn in site_config["site_buckets"]:
                diff = diff_bucket_policy_grants(
                    bucket_name=bucket,
                    bucket_arn=bucket_arn,
                    project=project,
                    config_dict=config_dict,
                    aws_credentials_dict=aws_credentials_dict,
                    site=site,
                )
                acl = audit_bucket_acl(bucket_arn, aws_credentials_dict)

                audit_report["site_buckets"][(site, bucket, bucket_arn)] = {
                    "diff": diff,
                    "acl": acl,
                }

                if _bucket_needs_fix(diff, acl):
                    print(
                        f"Policy drift detected for site bucket {bucket_arn} (site={site}): {diff}, acl: {acl}",
                        file=sys.stdout,
                    )
                    to_fix["site_buckets"].add((bucket, bucket_arn, project, site))

    return audit_report, to_fix


def _rotating_index(group_key: str, candidates_len: int) -> int:
    """Deterministic index that rotates daily, without needing state persisted between runs"""
    if candidates_len == 0:
        return 0

    import datetime
    import zlib

    day_of_year = datetime.date.today().timetuple().tm_yday
    offset = zlib.crc32(group_key.encode())

    return (day_of_year + offset) % candidates_len


def select_canary_targets(config_map: dict) -> dict:
    """Pick one site bucket per project to fully functionally probe, rotating daily

    Also picks one non-owning site to probe the same bucket with, so each canary
    round checks both a correct positive (owner has access) and a correct negative
    (a non-owner does not) - the two things the retired per-site cross product used
    to check for every site, that this canary now checks for one rotating sample
    instead.

    Args:
        config_map (dict): The config map as a dictionary

    Returns:
        dict: {project: (bucket, bucket_arn, owner_site, other_site | None)}
    """
    targets = {}

    for project, project_config in config_map.items():
        candidates = sorted(
            (bucket, bucket_arn, site)
            for site, site_config in project_config["sites"].items()
            for bucket, bucket_arn in site_config["site_buckets"]
        )

        if not candidates:
            continue

        bucket, bucket_arn, owner_site = candidates[
            _rotating_index(f"{project}-site-bucket", len(candidates))
        ]

        other_sites = sorted(s for s in project_config["sites"] if s != owner_site)
        other_site = None
        if other_sites:
            other_site = other_sites[
                _rotating_index(f"{project}-{bucket_arn}-other-site", len(other_sites))
            ]

        targets[project] = (bucket, bucket_arn, owner_site, other_site)

    return targets


def run_canary_probes(
    aws_credentials_dict: dict, config_map: dict, dry_run: bool = False
) -> dict:
    """Run a reduced functional-probe sanity check alongside the policy-document diff

    For one rotating site bucket per project, this probes both the owning site
    (expected to have access) and one non-owning site (expected not to), so real
    enforcement drift (a Ceph bug, a Deny/Allow ordering issue) that a document diff
    can't see gets caught eventually, at O(projects) cost instead of O(buckets x
    sites). Put/delete are only probed against a "-test" flagged bucket, using the
    same throwaway key as the retired can_site_put_object/can_site_delete_object
    probes, to avoid disturbing real data.

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}, "admin": {...}}
        config_map (dict): The config map as a dictionary
        dry_run (bool, optional): If True, skip live calls and return an empty result

    Returns:
        dict: {project: {"bucket_arn": ..., "owner_site": ..., "probes": {site: {permission: bool}}}}
    """
    results: dict = {}

    if dry_run:
        return results

    targets = select_canary_targets(config_map)

    for project, (_bucket, bucket_arn, owner_site, other_site) in targets.items():
        probes = {}

        for probe_site in (s for s in (owner_site, other_site) if s):
            probe_result = {
                "list": can_site_list_objects(
                    bucket_arn, aws_credentials_dict, project, probe_site
                ),
                "get": can_site_get_object(
                    bucket_arn, aws_credentials_dict, project, probe_site
                ),
            }

            if "-test" in bucket_arn:
                probe_result["put"] = can_site_put_object(
                    bucket_arn, aws_credentials_dict, project, probe_site
                )
                probe_result["delete"] = can_site_delete_object(
                    bucket_arn, aws_credentials_dict, project, probe_site
                )

            probes[probe_site] = probe_result

            expected_access = probe_site == owner_site
            for permission, result in probe_result.items():
                if result != expected_access:
                    print(
                        f"[canary] Unexpected result for site bucket {bucket_arn}: "
                        f"site={probe_site} (owner={owner_site}), permission={permission}, "
                        f"result={result}, expected_access={expected_access}",
                        file=sys.stdout,
                    )

        results[project] = {
            "bucket_arn": bucket_arn,
            "owner_site": owner_site,
            "probes": probes,
        }

    return results


def _json_safe(obj):
    """Recursively convert sets/tuples/tuple-keys into a form json.dumps can handle"""
    if isinstance(obj, dict):
        return {
            ("|".join(str(part) for part in key) if isinstance(key, tuple) else key): _json_safe(value)
            for key, value in obj.items()
        }
    elif isinstance(obj, (set, frozenset)):
        return sorted(_json_safe(item) for item in obj)
    elif isinstance(obj, (list, tuple)):
        return [_json_safe(item) for item in obj]
    else:
        return obj


def apply_policies(
    to_fix: dict,
    aws_credentials_dict: dict,
    config_dict: dict,
    dry_run: bool,
    force: bool,
) -> None:
    """Apply the correct policies to all buckets that need to be fixed

    Args:
        to_fix (dict): A dictionary of the form {in_buckets: [(bucket, project, site)], out_buckets: [(bucket, project)]} indicating which buckets need to be fixed
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        config_map (dict): The config map as a dictionary
    """

    if not force:
        for bucket, bucket_arn, project, site in to_fix["site_buckets"]:
            policy = generate_site_policy(
                bucket_name=bucket,
                bucket_arn=bucket_arn,
                project=project,
                site=site,
                aws_credentials_dict=aws_credentials_dict,
                config_dict=config_dict,
            )

            if not dry_run:
                print(f"Applying policy: {json.dumps(policy)} for bucket {bucket_arn}")
                policy_success = put_site_policy(
                    bucket_arn=bucket_arn,
                    slug=bryn_team_slug(aws_credentials_dict, project, site),
                    policy=policy,
                )

                if not policy_success:
                    print(
                        f"Policy for bucket {bucket_arn} could not be applied",
                        file=sys.stdout,
                    )
            else:
                print(
                    f"Dry run, not applying policy: {json.dumps(policy)} for bucket {bucket_arn}",
                    file=sys.stdout,
                )

        for bucket, bucket_arn, project in to_fix["project_buckets"]:
            policy = generate_project_policy(
                bucket_name=bucket,
                bucket_arn=bucket_arn,
                project=project,
                aws_credentials_dict=aws_credentials_dict,
                config_dict=config_dict,
            )

            if not dry_run:
                print(f"Applying policy: {json.dumps(policy)} for bucket {bucket_arn}")
                policy_success = put_project_policy(
                    bucket_arn=bucket_arn,
                    project="admin",
                    site=None,
                    aws_credentials_dict=aws_credentials_dict,
                    policy=policy,
                )

                if not policy_success:
                    print(
                        f"Policy for bucket {bucket_arn} could not be applied, policy:\n{json.dumps(policy)}",
                        file=sys.stdout,
                    )
            else:
                print(
                    f"Dry run, not applying policy: {json.dumps(policy)} for bucket {bucket_arn}",
                    file=sys.stdout,
                )
    else:
        config_map = create_config_map(config_dict)

        for project, project_config in config_map.items():
            for bucket, bucket_arn in project_config["project_buckets"]:
                policy = generate_project_policy(
                    bucket_name=bucket,
                    bucket_arn=bucket_arn,
                    project=project,
                    aws_credentials_dict=aws_credentials_dict,
                    config_dict=config_dict,
                )

                if not dry_run:
                    policy_success = put_project_policy(
                        bucket_arn=bucket_arn,
                        project="admin",
                        site=None,
                        aws_credentials_dict=aws_credentials_dict,
                        policy=policy,
                    )

                    if not policy_success:
                        print(
                            f"Policy for bucket {bucket_arn} could not be applied",
                            file=sys.stdout,
                        )
                else:
                    print(
                        f"Dry run, not applying policy: {json.dumps(policy)} for bucket {bucket_arn}",
                        file=sys.stdout,
                    )

            for site, site_config in project_config["sites"].items():
                for bucket, bucket_arn in site_config["site_buckets"]:
                    policy = generate_site_policy(
                        bucket_name=bucket,
                        bucket_arn=bucket_arn,
                        project=project,
                        site=site,
                        aws_credentials_dict=aws_credentials_dict,
                        config_dict=config_dict,
                    )

                    if not dry_run:
                        policy_success = put_site_policy(
                            bucket_arn=bucket_arn,
                            slug=bryn_team_slug(aws_credentials_dict, project, site),
                            policy=policy,
                        )

                        if not policy_success:
                            print(
                                f"Policy for bucket {bucket_arn} could not be applied",
                                file=sys.stdout,
                            )
                    else:
                        print(
                            f"Dry run, not applying policy: {json.dumps(policy)} for bucket {bucket_arn}",
                            file=sys.stdout,
                        )


def setup_sns_topic(
    aws_credentials_dict: dict,
    topic_name: str,
    amqp_host: str,
    amqp_user: str,
    amqp_pass: str,
    amqp_exchange: str,
    amqps: bool = False,
    amqp_ca_location: str | None = None,
) -> str:
    """Setup an SNS topic

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        topic_name (str): Name of topic to create
        amqp_host (str): Host address of rmq server
        amqp_user (str): Username to connect to rmq server
        amqp_pass (str): Password to connect to rmq server
        amqp_exchange (str): Exchange to publish messages to
        amqps (bool, optional): Use AMQPS to connect to rmq server. Defaults to False.
        amqp_ca_location (str, optional): Path, on the RGW host(s), to a CA certificate
            RGW should trust when validating the rmq server's certificate (needed when
            that certificate is self-signed). Only meaningful when amqps=True. When not
            provided, certificate verification is disabled rather than left to fail
            against an untrusted self-signed cert.

    Returns:
        str: ARN of created topic
    """

    admin_credentials = aws_credentials_dict["admin"]

    sns_client = boto3.client(
        "sns",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=admin_credentials["aws_access_key_id"],
        aws_secret_access_key=admin_credentials["aws_secret_access_key"],
        config=Config(signature_version="s3"),
    )

    amqp_port = 5671 if amqps else 5672

    protocol = "amqps" if amqps else "amqp"

    # to see the list of available "regions" use:
    # radosgw-admin realm zonegroup list

    # this is standard AWS services call, using custom attributes to add AMQP endpoint information to the topic

    if amqp_user and amqp_pass:
        push_endpoint = f"{protocol}://{amqp_user}:{amqp_pass}@{amqp_host}:{amqp_port}"
    else:
        push_endpoint = f"{protocol}://{amqp_host}:{amqp_port}"

    topic_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": ["sns:GetTopicAttributes"],
                    "Resource": [f"arn:aws:sns:s3::{topic_name}"],
                }
            ],
        }
    )

    attributes = {
        "amqp-exchange": amqp_exchange,
        "push-endpoint": push_endpoint,
        "amqp-ack-level": "broker",
        # Without a CA to check the rmq server's certificate against (e.g. when
        # it's self-signed) there's nothing for verify-ssl=true to validate
        # against, so certificate verification is only enabled once a CA
        # location is actually configured.
        "verify-ssl": "true" if amqp_ca_location else "false",
        "max_retries": "10080",
        "retry_sleep_duration": "60",
        "persistent": "true",
        "Policy": topic_policy,
    }

    if amqp_ca_location:
        attributes["ca-location"] = amqp_ca_location

    resp = sns_client.create_topic(Name=topic_name, Attributes=attributes)

    topic_arn = resp["TopicArn"]

    return topic_arn


def setup_messaging(
    aws_credentials_dict: dict,
    bucket_name: str,
    site: str,
    project: str,
    topic_arn: str,
    amqp_topic: str,
) -> bool:
    """Setup AMQP(S) messaging for a bucket

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        bucket_name (str): Name of bucket to setup messaging for
        topic_arn (str): ARN of previously setup topic to attach to bucket
        amqp_topic (str): Name of topic to create

    Returns:
        bool: True if messaging was setup successfully, False otherwise
    """

    credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3_client = get_s3_client(credentials)

    topic_conf_list = [
        {
            "TopicArn": topic_arn,
            "Events": [
                "s3:ObjectCreated:*",
            ],
            "Id": amqp_topic,  # Id is mandatory!
        },
    ]

    resp = s3_client.put_bucket_notification_configuration(
        Bucket=bucket_name,
        NotificationConfiguration={"TopicConfigurations": topic_conf_list},
    )

    if resp["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return True
    else:
        return False


def test_bucket_messaging(
    aws_credentials_dict: dict,
    bucket_name: str,
    correct_topic: str,
    project: str,
    site: str,
) -> bool:
    """Test if a bucket has the correct messaging setup

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        bucket_name (str): Name of bucket to test
        correct_topic (str): Name of topic that should be attached to bucket

    Returns:
        bool: True if the bucket has the correct messaging setup, False otherwise
    """

    credentials = resolve_credentials(aws_credentials_dict, project, site)

    s3_client = get_s3_client(credentials)

    resp = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)

    if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
        return False

    if "TopicConfigurations" not in resp:
        return False

    if len(resp["TopicConfigurations"]) != 1:
        return False

    if resp["TopicConfigurations"][0]["Id"] != correct_topic:
        return False

    return True


def audit_bucket_messaging(
    aws_credentials_dict: dict,
    config_map: dict,
    config_dict: dict,
    dry_run: bool = False,
) -> list:
    """Audit the messaging setup on all buckets

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        config_map (dict): The config map as a dictionary
        config_dict (dict): The config file as a dictionary

    Returns:
        list: A list of buckets that need to be fixed, of the form [(bucket, bucket_arn)]
    """
    to_fix = []

    if dry_run:
        print("Dry run, not auditing messaging", file=sys.stdout)
        return to_fix

    for project, project_config in config_map.items():
        for bucket, bucket_arn in project_config["project_buckets"]:
            if (
                bucket
                in config_dict["configs"][project]["notification_bucket_configs"].keys()
            ):
                if not test_bucket_messaging(
                    aws_credentials_dict=aws_credentials_dict,
                    bucket_name=bucket_arn,
                    project=project,
                    site="admin",
                    correct_topic=config_dict["configs"][project][
                        "notification_bucket_configs"
                    ][bucket]["rmq_exchange"],
                ):
                    to_fix.append((bucket, bucket_arn, project, "admin"))

        for site, site_config in project_config["sites"].items():
            for bucket, bucket_arn in site_config["site_buckets"]:
                if (
                    bucket
                    in config_dict["configs"][project][
                        "notification_bucket_configs"
                    ].keys()
                ):
                    if not test_bucket_messaging(
                        aws_credentials_dict=aws_credentials_dict,
                        bucket_name=bucket_arn,
                        project=project,
                        site=site,
                        correct_topic=config_dict["configs"][project][
                            "notification_bucket_configs"
                        ][bucket]["rmq_exchange"],
                    ):
                        to_fix.append((bucket, bucket_arn, project, site))

    return to_fix


def run(args):
    if args.setup_messaging:
        for env_var in [
            "AMQP_HOST",
            "AMQP_USER",
            "AMQP_PASS",
            "BRYN_API_TOKEN",
            "BRYN_API_URL",
        ]:
            if env_var not in os.environ.keys():
                print(f"Environment variable {env_var} not set", file=sys.stderr)
                sys.exit(1)

    with open(args.config, "r") as f:
        config_dict = json.load(f)

    with open(args.aws_credentials, "r") as f:
        aws_credentials_dict = json.load(f)

    config_map = create_config_map(config_dict)

    check_bucket_exist_and_create(
        aws_credentials_dict=aws_credentials_dict,
        config_map=config_map,
        config_dict=config_dict,
        dry_run=args.dry_run,
    )
    to_fix = {"site_buckets": False, "project_buckets": False}

    if not args.dry_run:

        if args.legacy_audit:
            # Retired O(buckets x sites x 4 calls) probe-based audit, kept available
            # as a rollback/comparison path - see the policy-check complexity
            # reduction plan for why this was replaced by a policy-document diff.
            audit_dict = audit_all_buckets(
                aws_credentials_dict=aws_credentials_dict, config_map=config_map
            )

            to_fix = test_policies(audit_dict=audit_dict, config_dict=config_dict)
        else:
            policy_audit_report, to_fix = audit_and_test_policies(
                aws_credentials_dict=aws_credentials_dict,
                config_map=config_map,
                config_dict=config_dict,
            )

            canary_results = run_canary_probes(
                aws_credentials_dict=aws_credentials_dict, config_map=config_map
            )

        if args.audit_report:
            report_payload = {"to_fix": _json_safe(to_fix)}

            if args.legacy_audit:
                report_payload["audit_dict"] = _json_safe(audit_dict)
            else:
                report_payload["policy_audit_report"] = _json_safe(policy_audit_report)
                report_payload["canary_results"] = _json_safe(canary_results)

            with open(args.audit_report, "w") as f:
                json.dump(report_payload, f, indent=2)

            print(f"Wrote audit report to {args.audit_report}", file=sys.stdout)

        if (
            not to_fix["site_buckets"] and not to_fix["project_buckets"]
        ) and not args.force:
            print("All buckets have correct policies", file=sys.stdout)
        else:
            apply_policies(
                to_fix=to_fix,
                aws_credentials_dict=aws_credentials_dict,
                config_dict=config_dict,
                dry_run=args.dry_run,
                force=args.force,
            )

            print(
                f"Applied policies to {len(to_fix['site_buckets']) + len(to_fix['project_buckets'])} buckets",
                file=sys.stdout,
            )

    if args.setup_messaging:
        to_setup_messaging = audit_bucket_messaging(
            aws_credentials_dict=aws_credentials_dict,
            config_map=config_map,
            config_dict=config_dict,
            dry_run=args.dry_run,
        )

    # for project, project_config in config_dict["configs"].items():
    if not args.dry_run and args.setup_messaging:
        if not to_setup_messaging:  # type: ignore
            print("All buckets have correct messaging configuration", file=sys.stdout)
        else:
            for bucket, bucket_arn, project, site in to_setup_messaging:
                amqp_host = os.environ["AMQP_HOST"]
                amqp_user = os.environ["AMQP_USER"]
                amqp_pass = os.environ["AMQP_PASS"]
                amqp_ca_location = os.getenv("AMQP_CA_LOCATION")
                notification_bucket_config = config_dict["configs"][project][
                    "notification_bucket_configs"
                ][bucket]
                topic_arn = setup_sns_topic(
                    aws_credentials_dict=aws_credentials_dict,
                    topic_name=notification_bucket_config["rmq_exchange"],
                    amqp_host=amqp_host,
                    amqp_user=amqp_user,
                    amqp_pass=amqp_pass,
                    amqp_exchange=notification_bucket_config["rmq_exchange"],
                    amqps=notification_bucket_config.get("amqps", False),
                    amqp_ca_location=amqp_ca_location,
                )

                success = setup_messaging(
                    aws_credentials_dict=aws_credentials_dict,
                    bucket_name=bucket_arn,
                    project=project,
                    site=site,
                    topic_arn=topic_arn,
                    amqp_topic=notification_bucket_config["rmq_exchange"],
                )
                if success:
                    print(f"Setup messaging for bucket {bucket_arn}", file=sys.stdout)
                else:
                    print(
                        f"Failed to setup messaging for bucket {bucket_arn}",
                        file=sys.stdout,
                    )

            retest_messaging = audit_bucket_messaging(
                aws_credentials_dict=aws_credentials_dict,
                config_map=config_map,
                config_dict=config_dict,
            )

            if retest_messaging:
                failed_buckets = ", ".join(
                    f"{bucket_arn} ({project}/{site})"
                    for _, bucket_arn, project, site in retest_messaging
                )
                print(
                    f"Failed to setup messaging for {len(retest_messaging)} buckets: {failed_buckets}",
                    file=sys.stdout,
                )

    if (to_fix["site_buckets"] or to_fix["project_buckets"]) and not args.dry_run:
        if args.legacy_audit:
            retest_audit_dict = audit_all_buckets(
                aws_credentials_dict=aws_credentials_dict,
                config_map=config_map,
                dry_run=args.dry_run,
            )

            retest_to_fix = test_policies(
                audit_dict=retest_audit_dict, config_dict=config_dict
            )
        else:
            # Only the buckets that were just fixed need re-checking - the rest of
            # config_map wasn't touched by apply_policies.
            retest_to_fix = retest_fixed_buckets(
                to_fix=to_fix,
                aws_credentials_dict=aws_credentials_dict,
                config_dict=config_dict,
            )

        if retest_to_fix["site_buckets"] or retest_to_fix["project_buckets"]:
            print(
                f"Failed to apply policies to {len(retest_to_fix['site_buckets']) + len(retest_to_fix['project_buckets'])} buckets",
                file=sys.stdout,
            )
            print(
                f"Site Buckets to fix: {retest_to_fix['site_buckets']}", file=sys.stdout
            )
            print(
                f"Project Buckets to fix: {retest_to_fix['project_buckets']}",
                file=sys.stdout,
            )

            # varys_client = Varys(
            #     profile="roz",
            #     logfile=os.devnull,
            #     log_level="CRITICAL",
            #     auto_acknowledge=False,
            # )

            # varys_client.send(
            #     message="Bucket controller failed in some manner :(",
            #     exchange="mscape.restricted.announce",
            #     queue_suffix="slack_integration",
            # )
        else:
            print("All buckets have correct policies", file=sys.stdout)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Create buckets and policies for a set of projects"
    )
    parser.add_argument("config", help="The config file to use")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen, but don't actually do it",
    )
    parser.add_argument(
        "--aws-credentials",
        help="The AWS credentials file to use",
        type=str,
    )
    parser.add_argument(
        "--setup-messaging",
        action="store_true",
        help="Whether or not to setup messaging",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Set policies on all buckets regardless of current state",
    )
    parser.add_argument(
        "--audit-report",
        type=str,
        default=None,
        help=(
            "Path to write a JSON audit report to, for the audit mechanism actually "
            "in use this run (policy-document diff + ACL check + canary probes by "
            "default, or the retired per-site functional-probe audit_dict with "
            "--legacy-audit)."
        ),
    )
    parser.add_argument(
        "--legacy-audit",
        action="store_true",
        help=(
            "Use the retired O(buckets x sites x 4 calls) functional-probe audit "
            "(audit_all_buckets/test_policies) instead of the policy-document diff. "
            "Rollback/comparison path only - see the policy-check complexity "
            "reduction plan."
        ),
    )
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
