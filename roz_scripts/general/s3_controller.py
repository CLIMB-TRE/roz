from varys import varys
import boto3
import json
import sys
from botocore.exceptions import ClientError
from botocore.client import Config
import os
import re
import copy


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

in_actions_template = [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket",
]

# out_actions_template = ["s3:GetObject", "s3:ListBucket"]

admin_actions_template = [
    "s3:AbortMultipartUpload",
    "s3:CreateBucket",
    "s3:DeleteBucketPolicy",
    "s3:DeleteBucketWebsite",
    "s3:DeleteBucket",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
    "s3:GetBucketAcl",
    "s3:GetBucketLogging",
    "s3:GetBucketNotification",
    "s3:GetBucketPolicy",
    "s3:GetBucketTagging",
    "s3:GetBucketVersioning",
    "s3:GetBucketWebsite",
    "s3:GetLifecycleConfiguration",
    "s3:GetObjectAcl",
    "s3:GetObject",
    "s3:GetObjectVersionAcl",
    "s3:GetObjectVersion",
    "s3:GetObjectVersionTorrent",
    "s3:ListAllMyBuckets",
    "s3:ListBucketMultipartUploads",
    "s3:ListBucket",
    "s3:ListBucketVersions",
    "s3:ListMultipartUploadParts",
    "s3:PutBucketAcl",
    "s3:PutBucketLogging",
    "s3:PutBucketNotification",
    "s3:PutBucketPolicy",
    "s3:PutBucketRequestPayment",
    "s3:PutBucketTagging",
    "s3:PutBucketVersioning",
    "s3:PutBucketWebsite",
    "s3:PutLifecycleConfiguration",
    "s3:PutObjectAcl",
    "s3:PutObject",
    "s3:PutObjectVersionAcl",
    "s3:RestoreObject",
]

perm_map = {
    "get": "s3:GetObject",
    "put": "s3:PutObject",
    "delete": "s3:DeleteObject",
    "list": "s3:ListBucket",
}


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

        project_config = {
            "sites": {site: {"site_buckets": []} for site in config["sites"]}
        }

        project_config.setdefault("project_buckets", [])

        for bucket, bucket_config in config["project_buckets"].items():
            desired_labels = re.findall("{(\w*)}", bucket_config["name_layout"])

            try:
                namespace = {}

                # Can't do a dict comp here
                for label in desired_labels:
                    namespace[label] = locals()[label]

                bucket_name = bucket_config["name_layout"].format(**namespace)

                project_config["project_buckets"].append((bucket, bucket_name))

            except KeyError as e:
                e.add_note(f"Bucket layout {bucket_config['name_layout']} is invalid")
                raise e

        for site in config["sites"]:
            for bucket, bucket_config in config["site_buckets"].items():
                desired_labels = re.findall("{(\w*)}", bucket_config["name_layout"])

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

                            project_config["sites"][site]["site_buckets"].append(
                                (bucket, bucket_name)
                            )

                        except KeyError as e:
                            e.add_note(
                                f"Bucket layout {bucket_config['name_layout']} is invalid"
                            )
                            raise e

        config_map[project] = project_config

    return config_map


def check_bucket_exists(bucket_name: str, aws_credentials_dict: dict) -> bool:
    """Check if a bucket exists

    Args:
        bucket_name (str): The name of the bucket
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}

    Returns:
        bool: True if the bucket exists, False otherwise
    """

    credentials = aws_credentials_dict["admin"]
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True

    except ClientError:
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
    site_credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=site_credentials["aws_access_key_id"],
        aws_secret_access_key=site_credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

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
    site_credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=site_credentials["aws_access_key_id"],
        aws_secret_access_key=site_credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    try:
        s3.get_object(Bucket=bucket_name, Key="test")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return True
        else:
            return False
    except:
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
    site_credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=site_credentials["aws_access_key_id"],
        aws_secret_access_key=site_credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    admin_s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials_dict["admin"]["aws_access_key_id"],
        aws_secret_access_key=aws_credentials_dict["admin"]["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    try:
        s3.put_object(Bucket=bucket_name, Key="test", Body=b"test")
        admin_s3.delete_object(Bucket=bucket_name, Key="test")
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

    site_credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=site_credentials["aws_access_key_id"],
        aws_secret_access_key=site_credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

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


def can_site_modify_policy(
    bucket_name: str, aws_credentials_dict: dict, project: str, site: str
) -> bool:
    """Check if a site can modify a bucket policy, i.e. get the current policy and put it back

    Args:
        bucket_name (str): name of bucket to check
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        project (str): name of project in question
        site (str): name of site in question

    Returns:
        bool: True if the site can modify a bucket policy, False otherwise
    """
    site_credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=site_credentials["aws_access_key_id"],
        aws_secret_access_key=site_credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    admin_s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials_dict["admin"]["aws_access_key_id"],
        aws_secret_access_key=aws_credentials_dict["admin"]["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    try:
        policy = admin_s3.get_bucket_policy(Bucket=bucket_name)["Policy"]

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
            policy = copy.deepcopy(policy_template)

            statement = copy.deepcopy(statement_template)

            statement["Principal"][
                "AWS"
            ] = f"arn:aws:iam:::user/{aws_credentials_dict['admin']['username']}"

            statement["Action"] = admin_actions_template

            statement["Resource"] = [f"arn:aws:s3:::{bucket_name}/*"]

            policy["Statement"].append(statement)

        elif e.response["Error"]["Code"] == "AccessDenied":
            return False

    if isinstance(policy, str):
        policy = json.loads(policy)

    try:
        s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
        return True
    except ClientError as e:
        return False


def can_site_delete_policy(
    bucket_name: str, aws_credentials_dict: dict, project: str, site: str
) -> bool:
    """Check if a site can delete a bucket policy, i.e. get the current policy, delete it and put it back

    Args:
        bucket_name (str): name of bucket to check
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        project (str): name of project in question
        site (str): name of site in question

    Returns:
        bool: True if the site can delete a bucket policy, False otherwise
    """
    site_credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=site_credentials["aws_access_key_id"],
        aws_secret_access_key=site_credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    admin_s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_credentials_dict["admin"]["aws_access_key_id"],
        aws_secret_access_key=aws_credentials_dict["admin"]["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    try:
        policy = admin_s3.get_bucket_policy(Bucket=bucket_name)["Policy"]

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
            policy = copy.deepcopy(policy_template)

            policy["Statement"] = copy.deepcopy(statement_template)

            policy["Statement"]["Principal"][
                "AWS"
            ] = f"arn:aws:iam:::user/{aws_credentials_dict['admin']['username']}"

            policy["Statement"]["Action"] = admin_actions_template

        elif e.response["Error"]["Code"] == "AccessDenied":
            return False

    if isinstance(policy, str):
        policy = json.loads(policy)

    try:
        s3.delete_bucket_policy(Bucket=bucket_name)
        admin_s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
        return True
    except ClientError as e:
        return False


def can_site_get_policy(
    bucket_name: str, aws_credentials_dict: dict, project: str, site: str
) -> bool:
    site_credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=site_credentials["aws_access_key_id"],
        aws_secret_access_key=site_credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    try:
        s3.get_bucket_policy(Bucket=bucket_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
            return True

        return False


def put_policy(bucket_name: str, aws_credentials_dict: dict, policy: dict) -> bool:
    """Put a policy on a bucket

    Args:
        bucket_name (str): name of bucket to put policy on
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        policy (dict): The policy to put on the bucket as a dictionary

    Returns:
        bool: True if the policy was put on the bucket, False otherwise
    """

    credentials = aws_credentials_dict["admin"]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    if isinstance(policy, dict):
        policy = json.dumps(policy)

    try:
        s3.put_bucket_policy(Bucket=bucket_name, Policy=policy)
        return True
    except ClientError as e:
        return False


def generate_site_policy(
    bucket_name: str,
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

    # Add the admin statement
    admin_statement = copy.deepcopy(statement_template)

    admin_statement["Principal"]["AWS"] = [
        f"arn:aws:iam:::user/{aws_credentials_dict['admin']['username']}"
    ]

    admin_statement["Action"] = admin_actions_template

    policy["Statement"].append(admin_statement)

    # Add the site statement
    site_statement = copy.deepcopy(statement_template)

    site_statement["Principal"]["AWS"] = [
        aws_credentials_dict[project][site]["username"]
    ]

    permission_set = config_dict["configs"][project]["site_buckets"][bucket_name][
        "policy"
    ]

    correct_perms = config_dict["configs"][project]["bucket_policies"][permission_set]

    site_statement["Action"] = [perm_map[x] for x in correct_perms]

    site_statement["Resource"] = [f"arn:aws:s3:::{bucket_name}/*"]

    policy["Statement"].append(site_statement)

    return policy


def generate_project_policy(
    bucket_name: str,
    project: str,
    config_dict: dict,
    aws_credentials_dict: dict,
) -> dict:
    """Generate the policy for an out bucket

    Args:
        bucket_name (str): The name of the bucket
        project (str): The project the bucket belongs to
        config_dict (dict): The config file as a dictionary
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}

    Returns:
        dict: The policy as a dictionary
    """

    policy = copy.deepcopy(policy_template)

    # Add the admin statement
    admin_statement = copy.deepcopy(statement_template)

    admin_statement["Principal"]["AWS"] = [
        f"arn:aws:iam:::user/{aws_credentials_dict['admin']['username']}"
    ]

    admin_statement["Action"] = admin_actions_template

    policy["Statement"].append(admin_statement)

    for site, config in config_dict[project].items():
        # Add the site statement
        site_statement = copy.deepcopy(statement_template)

        site_statement["Principal"]["AWS"] = [
            aws_credentials_dict[project][site]["username"]
        ]

        permission_set = config_dict["configs"][project]["site_buckets"][bucket_name][
            "policy"
        ]

        correct_perms = config_dict["configs"][project]["bucket_policies"][
            permission_set
        ]

        site_statement["Action"] = [perm_map[x] for x in correct_perms]

        policy["Statement"].append(site_statement)

    policy["Resource"] = [f"arn:aws:s3:::{bucket_name}/*"]

    return policy


def create_bucket(
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
    if not site == "admin":
        credentials = aws_credentials_dict[project][site]
    else:
        credentials = aws_credentials_dict["admin"]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    try:
        s3.create_bucket(Bucket=bucket_name, ACL="private")
        return True
    except ClientError as e:
        return False


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

    policy_audit = {
        x: {
            "list": None,
            "get": None,
            "put": None,
            "delete": None,
            "put_policy": None,
            "delete_policy": None,
            "get_policy": None,
        }
        for x in config_map[project]["sites"].keys()
    }

    for site in config_map[project]["sites"].keys():
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
        policy_audit[site]["put_policy"] = can_site_modify_policy(
            bucket_name=bucket_name,
            aws_credentials_dict=aws_credentials_dict,
            project=project,
            site=site,
        )
        policy_audit[site]["delete_policy"] = can_site_delete_policy(
            bucket_name=bucket_name,
            aws_credentials_dict=aws_credentials_dict,
            project=project,
            site=site,
        )
        policy_audit[site]["get_policy"] = can_site_get_policy(
            bucket_name=bucket_name,
            aws_credentials_dict=aws_credentials_dict,
            project=project,
            site=site,
        )

    return policy_audit


def check_bucket_exist_and_create(aws_credentials_dict: dict, config_map: dict) -> None:
    """Check if all specified buckets exist, and if not, create them

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        config_map (dict): The config map as a dictionary

    Raises:
        ValueError: If a bucket cannot be created
    """

    for project, project_config in config_map.items():
        # Create out buckets (made by admin user)
        for bucket, bucket_arn in project_config["project_buckets"]:
            exists = check_bucket_exists(bucket_arn, aws_credentials_dict)

            if exists:
                print(
                    f"Bucket {bucket} already exists, no need to create",
                    file=sys.stdout,
                )
                continue

            print(f"Creating bucket {bucket}", file=sys.stdout)
            create_success = create_bucket(
                bucket_name=bucket_arn,
                project=project,
                site="admin",
                aws_credentials_dict=aws_credentials_dict,
            )

            if not create_success:
                raise ValueError(f"Bucket {bucket} could not be created")

        # Create in buckets (made by site user)
        for site, site_config in project_config["sites"].items():
            for bucket, bucket_arn in site_config["site_buckets"]:
                exists = check_bucket_exists(bucket_arn, aws_credentials_dict)

                if exists:
                    print(
                        f"Bucket {bucket_arn} already exists, no need to create",
                        file=sys.stdout,
                    )
                    continue

                create_success = create_bucket(
                    bucket_name=bucket_arn,
                    project=project,
                    site=site,
                    aws_credentials_dict=aws_credentials_dict,
                )

                if not create_success:
                    raise ValueError(f"Bucket {bucket} could not be created")


def audit_all_buckets(aws_credentials_dict: dict, config_map: dict) -> dict:
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

    for project, project_config in config_map.items():
        # Audit out buckets (made by admin user)
        for bucket, bucket_arn in project_config["project_buckets"]:
            audit_dict[project]["project_buckets"][
                (bucket, bucket_arn)
            ] = audit_bucket_policy(
                bucket_name=bucket_arn,
                aws_credentials_dict=aws_credentials_dict,
                project=project,
                config_map=config_map,
            )

        # Audit in buckets (made by site user)
        for site, site_config in project_config["sites"].items():
            for bucket, bucket_arn in site_config["site_buckets"]:
                audit_dict[project]["site_buckets"][site][
                    (bucket, bucket_arn)
                ] = audit_bucket_policy(
                    bucket_name=bucket_arn,
                    aws_credentials_dict=aws_credentials_dict,
                    project=project,
                    config_map=config_map,
                )

    return audit_dict


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
                    if audit_site == site:
                        permission_set = config_dict["configs"][project][
                            "site_buckets"
                        ][bucket]["policy"]

                        correct_perms = config_dict["configs"][project][
                            "bucket_policies"
                        ][permission_set]
                    else:
                        correct_perms = []

                    for permission, result in audit_results.items():
                        if result:
                            if permission in correct_perms:
                                continue
                            else:
                                print(
                                    f"Incorrect policy for bucket {bucket_arn} detected",
                                    file=sys.stdout,
                                )
                                to_fix["site_buckets"].add(
                                    (bucket, bucket_arn, project, site)
                                )
                        else:
                            if permission in correct_perms:
                                print(
                                    f"Missing policy for bucket {bucket_arn} detected",
                                    file=sys.stdout,
                                )
                                to_fix["site_buckets"].add(
                                    (bucket, bucket_arn, project, site)
                                )
                            else:
                                continue

        for (bucket, bucket_arn), bucket_audit in buckets["project_buckets"].items():
            for audit_site, audit_results in bucket_audit.items():
                permission_set = config_dict["configs"][project]["project_buckets"][
                    bucket
                ]["policy"]

                correct_perms = config_dict["configs"][project]["bucket_policies"][
                    permission_set
                ]

                for permission, result in audit_results.items():
                    if result:
                        if permission in correct_perms:
                            continue
                        else:
                            print(
                                f"Incorrect policy for bucket {bucket} detected",
                                file=sys.stdout,
                            )
                            to_fix["project_buckets"].add((bucket, bucket_arn, project))
                    else:
                        if permission in correct_perms:
                            print(
                                f"Missing policy for bucket {bucket} detected",
                                file=sys.stdout,
                            )
                            to_fix["project_buckets"].add((bucket, bucket_arn, project))
                        else:
                            continue

    return to_fix


def apply_policies(to_fix: dict, aws_credentials_dict: dict, config_map: dict) -> None:
    """Apply the correct policies to all buckets that need to be fixed

    Args:
        to_fix (dict): A dictionary of the form {in_buckets: [(bucket, project, site)], out_buckets: [(bucket, project)]} indicating which buckets need to be fixed
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        config_map (dict): The config map as a dictionary
    """

    for bucket, bucket_arn, project, site in to_fix["site_buckets"]:
        policy = generate_site_policy(
            bucket_name=bucket,
            project=project,
            site=site,
            aws_credentials_dict=aws_credentials_dict,
            config_dict=config_map,
        )
        put_policy(
            bucket_name=bucket, aws_credentials_dict=aws_credentials_dict, policy=policy
        )

    for bucket, bucket_arn, project in to_fix["project_buckets"]:
        policy = generate_project_policy(
            bucket_name=bucket_arn,
            project=project,
            aws_credentials_dict=aws_credentials_dict,
            config_dict=config_map,
        )
        put_policy(
            bucket_name=bucket, aws_credentials_dict=aws_credentials_dict, policy=policy
        )


def setup_sns_topic(
    aws_credentials_dict: dict,
    topic_name: str,
    amqp_host: str,
    amqp_user: str,
    amqp_pass: str,
    amqp_exchange: str,
    amqps: bool = False,
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

    Returns:
        str: ARN of created topic
    """

    sns_client = boto3.client(
        "sns",
        endpoint_url="https://s3.climb.ac.uk",
        aws_access_key_id=aws_credentials_dict["admin"]["aws_access_key_id"],
        aws_secret_access_key=aws_credentials_dict["admin"]["aws_secret_access_key"],
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

    attributes = {
        "push-endpoint": push_endpoint,
        "amqp-exchange": amqp_exchange,
        "amqp-ack-level": "broker",
        "verify-ssl": "false",
    }

    resp = sns_client.create_topic(Name=topic_name, Attributes=attributes)

    topic_arn = resp["TopicArn"]

    return topic_arn


def setup_messaging(
    aws_credentials_dict: dict,
    bucket_name: str,
    topic_arn: str,
    amqp_topic: str,
):
    """Setup AMQP(S) messaging for a bucket

    Args:
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        bucket_name (str): Name of bucket to setup messaging for
        topic_arn (str): ARN of previously setup topic to attach to bucket
        amqp_topic (str): Name of topic to create

    """
    s3_client = boto3.client(
        "s3",
        endpoint_url="https://s3.climb.ac.uk",
        aws_access_key_id=aws_credentials_dict["admin"]["aws_access_key_id"],
        aws_secret_access_key=aws_credentials_dict["admin"]["aws_secret_access_key"],
    )

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


def run(args):
    with open(args.config, "r") as f:
        config_dict = json.load(f)

    with open(args.aws_credentials, "r") as f:
        aws_credentials_dict = json.load(f)

    config_map = create_config_map(config_dict)

    check_bucket_exist_and_create(
        aws_credentials_dict=aws_credentials_dict,
        config_map=config_map,
        config_dict=config_dict,
    )

    audit_dict = audit_all_buckets(
        aws_credentials_dict=aws_credentials_dict, config_map=config_map
    )

    to_fix = test_policies(audit_dict=audit_dict, config_map=config_map)

    if args.dry_run:
        print("Dry run, not applying policies", file=sys.stdout)
        print(
            f"Would apply policies to {len(to_fix['in_buckets'])} in buckets and {len(to_fix['out_buckets'])} out buckets",
            file=sys.stdout,
        )
    else:
        apply_policies(
            to_fix=to_fix,
            aws_credentials_dict=aws_credentials_dict,
            config_map=config_map,
        )
        print(
            f"Applied policies to {len(to_fix['in_buckets'])} in buckets and {len(to_fix['out_buckets'])} out buckets",
            file=sys.stdout,
        )

        retest_audit_dict = audit_all_buckets(
            aws_credentials_dict=aws_credentials_dict, config_map=config_map
        )

        retest_to_fix = test_policies(
            audit_dict=retest_audit_dict, config_map=config_map
        )

        if (
            len(retest_to_fix["in_buckets"]) == 0
            and len(retest_to_fix["out_buckets"]) == 0
        ):
            print("All policies applied successfully", file=sys.stdout)
        else:
            print("Policies not applied successfully", file=sys.stdout)
            print(
                "Buckets which still appear to have incorrect policies:",
                file=sys.stdout,
            )
            for bucket, project, site in retest_to_fix["in_buckets"]:
                print(f"{bucket} in {project} at {site}", file=sys.stdout)
            for bucket, project in retest_to_fix["out_buckets"]:
                print(f"{bucket} in {project}", file=sys.stdout)

            varys_client = varys(
                profile="roz",
                logfile=os.devnull,
                log_level="CRITICAL",
                auto_acknowledge=False,
            )


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

    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
