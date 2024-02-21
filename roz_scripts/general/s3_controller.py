from varys import Varys
import boto3
import json
import sys
from botocore.exceptions import ClientError
from botocore.client import Config
import os
import re
import copy
import requests

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
    if site == "admin":
        credentials = aws_credentials_dict["admin"]
    else:
        credentials = aws_credentials_dict[project][site]

    s3 = boto3.resource(
        "s3",
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    bucket = s3.Bucket(bucket_name)

    if bucket.creation_date:
        return True
    else:
        return False

    # try:
    #     s3_client.head_bucket(Bucket=bucket_name)
    #     return True

    # except ClientError:
    #     return False


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
    if site == "admin":
        site_credentials = aws_credentials_dict["admin"]
    else:
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
    if site == "admin":
        site_credentials = aws_credentials_dict["admin"]
    else:
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

    if site == "admin":
        site_credentials = aws_credentials_dict["admin"]
    else:
        site_credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=site_credentials["aws_access_key_id"],
        aws_secret_access_key=site_credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

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
    if site == "admin":
        site_credentials = aws_credentials_dict["admin"]
    else:
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


# def can_site_modify_policy(
#     bucket_name: str, aws_credentials_dict: dict, project: str, site: str
# ) -> bool:
#     """Check if a site can modify a bucket policy, i.e. get the current policy and put it back

#     Args:
#         bucket_name (str): name of bucket to check
#         aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
#         project (str): name of project in question
#         site (str): name of site in question

#     Returns:
#         bool: True if the site can modify a bucket policy, False otherwise
#     """
#     site_credentials = aws_credentials_dict[project][site]

#     s3 = boto3.client(
#         "s3",
#         aws_access_key_id=site_credentials["aws_access_key_id"],
#         aws_secret_access_key=site_credentials["aws_secret_access_key"],
#         endpoint_url="https://s3.climb.ac.uk",
#     )

#     admin_s3 = boto3.client(
#         "s3",
#         aws_access_key_id=aws_credentials_dict["admin"]["aws_access_key_id"],
#         aws_secret_access_key=aws_credentials_dict["admin"]["aws_secret_access_key"],
#         endpoint_url="https://s3.climb.ac.uk",
#     )

#     try:
#         policy = admin_s3.get_bucket_policy(Bucket=bucket_name)["Policy"]

#     except ClientError as e:
#         if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
#             policy = copy.deepcopy(policy_template)

#             # Add the admin object permissions statement
#             admin_obj_statement = copy.deepcopy(statement_template)

#             admin_obj_statement["Principal"]["AWS"] = [
#                 f"arn:aws:iam:::user/{aws_credentials_dict['admin']['username']}"
#             ]

#             admin_obj_statement["Action"] = admin_obj_actions_template

#             admin_obj_statement["Resource"] = [f"arn:aws:s3:::{bucket_name}/*"]

#             policy["Statement"].append(admin_obj_statement)

#             # Add the admin bucket permissions statement

#             admin_bucket_statement = copy.deepcopy(statement_template)

#             admin_bucket_statement["Principal"]["AWS"] = [
#                 f"arn:aws:iam:::user/{admin_slug}"
#             ]

#             admin_bucket_statement["Action"] = admin_bucket_actions_template

#             admin_bucket_statement["Resource"] = [f"arn:aws:s3:::{bucket_name}"]

#             policy["Statement"].append(admin_bucket_statement)

#         elif e.response["Error"]["Code"] == "AccessDenied":
#             return False

#     if isinstance(policy, str):
#         policy = json.loads(policy)

#     try:
#         s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
#         return True
#     except ClientError as e:
#         return False


# def can_site_delete_policy(
#     bucket_name: str, aws_credentials_dict: dict, project: str, site: str
# ) -> bool:
#     """Check if a site can delete a bucket policy, i.e. get the current policy, delete it and put it back

#     Args:
#         bucket_name (str): name of bucket to check
#         aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
#         project (str): name of project in question
#         site (str): name of site in question

#     Returns:
#         bool: True if the site can delete a bucket policy, False otherwise
#     """
#     site_credentials = aws_credentials_dict[project][site]

#     s3 = boto3.client(
#         "s3",
#         aws_access_key_id=site_credentials["aws_access_key_id"],
#         aws_secret_access_key=site_credentials["aws_secret_access_key"],
#         endpoint_url="https://s3.climb.ac.uk",
#     )

#     admin_s3 = boto3.client(
#         "s3",
#         aws_access_key_id=aws_credentials_dict["admin"]["aws_access_key_id"],
#         aws_secret_access_key=aws_credentials_dict["admin"]["aws_secret_access_key"],
#         endpoint_url="https://s3.climb.ac.uk",
#     )

#     try:
#         policy = admin_s3.get_bucket_policy(Bucket=bucket_name)["Policy"]

#     except ClientError as e:
#         if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
#             policy = copy.deepcopy(policy_template)

#             # Add the admin object permissions statement
#             admin_obj_statement = copy.deepcopy(statement_template)

#             admin_obj_statement["Principal"]["AWS"] = [
#                 f"arn:aws:iam:::user/{aws_credentials_dict['admin']['username']}"
#             ]

#             admin_obj_statement["Action"] = admin_obj_actions_template

#             admin_obj_statement["Resource"] = [f"arn:aws:s3:::{bucket_name}/*"]

#             policy["Statement"].append(admin_obj_statement)

#             # Add the admin bucket permissions statement

#             admin_bucket_statement = copy.deepcopy(statement_template)

#             admin_bucket_statement["Principal"]["AWS"] = [
#                 f"arn:aws:iam:::user/{aws_credentials_dict['admin']['username']}"
#             ]

#             admin_bucket_statement["Action"] = admin_bucket_actions_template

#             admin_bucket_statement["Resource"] = [f"arn:aws:s3:::{bucket_name}"]

#             policy["Statement"].append(admin_bucket_statement)

#         elif e.response["Error"]["Code"] == "AccessDenied":
#             return False

#     if isinstance(policy, str):
#         policy = json.loads(policy)

#     try:
#         s3.delete_bucket_policy(Bucket=bucket_name)
#         admin_s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
#         return True
#     except ClientError as e:
#         return False


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


def put_project_policy(
    bucket_arn: str, aws_credentials_dict: dict, policy: dict, project: str, site: str
) -> bool:
    """Put a policy on a bucket

    Args:
        bucket_name (str): name of bucket to put policy on
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        policy (dict): The policy to put on the bucket as a dictionary

    Returns:
        bool: True if the policy was put on the bucket, False otherwise
    """
    if project == "admin":
        credentials = aws_credentials_dict["admin"]

    else:
        credentials = aws_credentials_dict[project][site]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials["aws_access_key_id"],
        aws_secret_access_key=credentials["aws_secret_access_key"],
        endpoint_url="https://s3.climb.ac.uk",
    )

    if isinstance(policy, dict):
        policy = json.dumps(policy)

    try:
        # Retrieve waiter instance that will wait till a specified bucket exists
        s3_bucket_exists_waiter = s3.get_waiter("bucket_exists")

        # Wait till bucket exists
        s3_bucket_exists_waiter.wait(Bucket=bucket_arn)

        resp = s3.put_bucket_policy(Bucket=bucket_arn, Policy=policy)
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

    site_slug = aws_credentials_dict[project][site]["username"][0:16].replace(".", "-")

    admin_slug = aws_credentials_dict["admin"]["username"][0:16].replace(".", "-")

    # Add the admin object permissions statement
    admin_obj_statement = copy.deepcopy(statement_template)

    admin_obj_statement["Principal"]["AWS"] = [f"arn:aws:iam:::user/{admin_slug}"]

    admin_obj_statement["Action"] = admin_obj_actions_template

    admin_obj_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}/*"]

    policy["Statement"].append(admin_obj_statement)

    # Add the admin bucket permissions statement

    admin_bucket_statement = copy.deepcopy(statement_template)

    admin_bucket_statement["Principal"]["AWS"] = [f"arn:aws:iam:::user/{admin_slug}"]

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

    admin_slug = aws_credentials_dict["admin"]["username"][0:16].replace(".", "-")

    # Add the admin object permissions statement
    admin_obj_statement = copy.deepcopy(statement_template)

    admin_obj_statement["Principal"]["AWS"] = [f"arn:aws:iam:::user/{admin_slug}"]

    admin_obj_statement["Action"] = admin_obj_actions_template

    admin_obj_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}/*"]

    policy["Statement"].append(admin_obj_statement)

    # Add the admin bucket permissions statement

    admin_bucket_statement = copy.deepcopy(statement_template)

    admin_bucket_statement["Principal"]["AWS"] = [f"arn:aws:iam:::user/{admin_slug}"]

    admin_bucket_statement["Action"] = admin_bucket_actions_template

    admin_bucket_statement["Resource"] = [f"arn:aws:s3:::{bucket_arn}"]

    policy["Statement"].append(admin_bucket_statement)

    for site, role in config_dict["configs"][project]["sites"].items():
        if (
            role
            not in config_dict["configs"][project]["project_buckets"][bucket_name][
                "policy"
            ]
        ):
            continue

        # Add the site statement
        site_obj_statement = copy.deepcopy(statement_template)

        site_slug = aws_credentials_dict[project][site]["username"][0:16].replace(
            ".", "-"
        )

        site_obj_statement["Principal"]["AWS"] = [
            f"arn:aws:iam:::user/bryn-{site_slug}"
        ]

        site_bucket_statement = copy.deepcopy(statement_template)

        site_bucket_statement["Principal"]["AWS"] = [
            f"arn:aws:iam:::user/bryn-{site_slug}"
        ]

        try:
            permission_set = config_dict["configs"][project]["project_buckets"][
                bucket_name
            ]["policy"][role]

            correct_perms = config_dict["configs"][project]["bucket_policies"][
                permission_set
            ]
        except KeyError:
            correct_perms = []

        if not correct_perms:
            continue

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
        print(
            f"Failed to create bucket {bucket_name} - Boto Exception:\n{e}",
            file=sys.stderr,
        )
        return False


def create_site_bucket(
    bucket_arn: str,
    site: str,
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

    site_slug = site[0:16].replace(".", "-")

    endpoint_url = (
        f"https://bryn-staging.climb.ac.uk/admin-api/teams/{site_slug}/ceph/s3/buckets/"
    )

    headers = {"Authorization": f"token {os.getenv('BRYN_API_TOKEN')}"}

    data = {"name": bucket_arn, "policy": json.dumps(policy)}

    r = requests.post(endpoint_url, headers=headers, json=data)

    if r.status_code == 201:
        return True
    else:
        print(
            f"Failed to create bucket {bucket_arn} - Bryn Response:\n{r.json()}",
            file=sys.stderr,
        )
        return False


def put_site_policy(bucket_arn: str, site: str, policy: dict) -> bool:
    """Put a policy on a bucket via bryn

    Args:
        bucket_arn (str): The ARN of the bucket
        site (str): The site the bucket belongs to
        policy (dict): The policy to put on the bucket as a dictionary

    Returns:
        bool: True if the policy was put on the bucket, False otherwise
    """
    site_slug = site[0:16].replace(".", "-")

    endpoint_url = f"https://bryn-staging.climb.ac.uk/admin-api/teams/{site_slug}/ceph/s3/buckets/{bucket_arn}/"

    headers = {"Authorization": f"token {os.getenv('BRYN_API_TOKEN')}"}

    response = requests.patch(
        endpoint_url, headers=headers, json={"policy": json.dumps(policy)}
    )

    if response.status_code == 200:
        return True
    else:
        print(
            f"Failed to put policy on bucket {bucket_arn}, Bryn response:\n{response.json()}",
            file=sys.stderr,
        )
        return False


def check_site_bucket_exists(bucket_arn: str, site: str) -> bool:
    """Check if a bucket exists via bryn

    Args:
        bucket_arn (str): The ARN of the bucket
        site (str): The site the bucket belongs to

    Returns:
        bool: True if the bucket exists, False otherwise
    """

    site_slug = site[0:16].replace(".", "-")

    endpoint_url = f"https://bryn-staging.climb.ac.uk/admin-api/teams/{site_slug}/ceph/s3/buckets/{bucket_arn}/"

    headers = {"Authorization": f"token {os.getenv('BRYN_API_TOKEN')}"}

    response = requests.get(endpoint_url, headers=headers)

    if response.status_code == 200:
        return True
    else:
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

    to_test = [x for x in config_map[project]["sites"].keys()]
    to_test.append("admin")

    policy_audit = {
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
                print(f"Dry run, not creating bucket: {bucket_arn}", file=sys.stdout)
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
            for bucket, bucket_arn in site_config["site_buckets"]:
                exists = check_site_bucket_exists(bucket_arn=bucket_arn, site=site)

                if dry_run:
                    print(
                        f"Dry run, not creating bucket: {bucket_arn}", file=sys.stdout
                    )
                    continue

                if exists:
                    print(
                        f"Bucket {bucket_arn} already exists, no need to create",
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
                    site=site,
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
                    try:
                        site_role = config_dict["configs"][project]["sites"][audit_site]
                        if audit_site == site:
                            permission_set = config_dict["configs"][project][
                                "site_buckets"
                            ][bucket]["policy"][site_role]

                            correct_perms = config_dict["configs"][project][
                                "bucket_policies"
                            ][permission_set]
                        else:
                            correct_perms = []
                    except KeyError:
                        correct_perms = []

                    for permission, result in audit_results.items():
                        if result:
                            if permission in correct_perms or audit_site == "admin":
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
                            if permission in correct_perms or audit_site == "admin":
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
                try:
                    audit_site_role = config_dict["configs"][project]["sites"][
                        audit_site
                    ]
                    permission_set = config_dict["configs"][project]["project_buckets"][
                        bucket
                    ]["policy"][audit_site_role]

                    correct_perms = config_dict["configs"][project]["bucket_policies"][
                        permission_set
                    ]
                except KeyError:
                    correct_perms = []

                for permission, result in audit_results.items():
                    if result:
                        if permission in correct_perms or audit_site == "admin":
                            continue
                        else:
                            print(
                                f"Incorrect policy for bucket {bucket} detected",
                                file=sys.stdout,
                            )
                            to_fix["project_buckets"].add((bucket, bucket_arn, project))
                    else:
                        if permission in correct_perms or audit_site == "admin":
                            print(
                                f"Missing policy for bucket {bucket} detected",
                                file=sys.stdout,
                            )
                            to_fix["project_buckets"].add((bucket, bucket_arn, project))
                        else:
                            continue

    return to_fix


def apply_policies(
    to_fix: dict, aws_credentials_dict: dict, config_dict: dict, dry_run: bool
) -> list:
    """Apply the correct policies to all buckets that need to be fixed

    Args:
        to_fix (dict): A dictionary of the form {in_buckets: [(bucket, project, site)], out_buckets: [(bucket, project)]} indicating which buckets need to be fixed
        aws_credentials_dict (dict): A dictionary of the form {project: {site: {aws_access_key_id: "", aws_secret_access_key: "", username: ""}}}
        config_map (dict): The config map as a dictionary
    """

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
            policy_success = put_site_policy(
                bucket_arn=bucket_arn,
                site=site,
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

    topic_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": ["sns:GetTopicAttributes"],
                    "Resource": [f"arn:aws:sns:s3::{amqp_exchange}"],
                }
            ],
        }
    )

    attributes = {
        "push-endpoint": push_endpoint,
        "amqp-exchange": amqp_exchange,
        "amqp-ack-level": "broker",
        "verify-ssl": "false",
        "max_retries": "20",
        "retry_sleep_duration": "60",
        "persistent": "false",
        "Policy": topic_policy,
    }

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

    s3_client = boto3.client(
        "s3",
        endpoint_url="https://s3.climb.ac.uk",
        aws_access_key_id=aws_credentials_dict[project][site]["aws_access_key_id"],
        aws_secret_access_key=aws_credentials_dict[project][site][
            "aws_secret_access_key"
        ],
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

    if site == "admin":
        s3_client = boto3.client(
            "s3",
            endpoint_url="https://s3.climb.ac.uk",
            aws_access_key_id=aws_credentials_dict["admin"]["aws_access_key_id"],
            aws_secret_access_key=aws_credentials_dict["admin"][
                "aws_secret_access_key"
            ],
        )
    else:
        s3_client = boto3.client(
            "s3",
            endpoint_url="https://s3.climb.ac.uk",
            aws_access_key_id=aws_credentials_dict[project][site]["aws_access_key_id"],
            aws_secret_access_key=aws_credentials_dict[project][site][
                "aws_secret_access_key"
            ],
        )

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
        for env_var in ["AMQP_HOST", "AMQP_USER", "AMQP_PASS", "BRYN_API_TOKEN"]:
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
        audit_dict = audit_all_buckets(
            aws_credentials_dict=aws_credentials_dict, config_map=config_map
        )

        to_fix = test_policies(audit_dict=audit_dict, config_dict=config_dict)

        if not to_fix["site_buckets"] and not to_fix["project_buckets"]:
            print("All buckets have correct policies", file=sys.stdout)
        else:
            apply_policies(
                to_fix=to_fix,
                aws_credentials_dict=aws_credentials_dict,
                config_dict=config_dict,
                dry_run=args.dry_run,
            )

            print(
                f"Applied policies to {len(to_fix['site_buckets']) + len(to_fix['project_buckets'])} buckets",
                file=sys.stdout,
            )

    to_setup_messaging = audit_bucket_messaging(
        aws_credentials_dict=aws_credentials_dict,
        config_map=config_map,
        config_dict=config_dict,
        dry_run=args.dry_run,
    )

    if not to_setup_messaging:
        print("All buckets have correct messaging configuration", file=sys.stdout)

    # for project, project_config in config_dict["configs"].items():
    if not args.dry_run and args.setup_messaging:
        for bucket, bucket_arn, project, site in to_setup_messaging:
            amqp_host = os.getenv("AMQP_HOST")
            amqp_user = os.getenv("AMQP_USER")
            amqp_pass = os.getenv("AMQP_PASS")
            topic_arn = setup_sns_topic(
                aws_credentials_dict=aws_credentials_dict,
                topic_name=config_dict["configs"][project][
                    "notification_bucket_configs"
                ][bucket]["rmq_exchange"],
                amqp_host=amqp_host,
                amqp_user=amqp_user,
                amqp_pass=amqp_pass,
                amqp_exchange=config_dict["configs"][project][
                    "notification_bucket_configs"
                ][bucket]["rmq_exchange"],
                amqps=True,
            )

            success = setup_messaging(
                aws_credentials_dict=aws_credentials_dict,
                bucket_name=bucket_arn,
                project=project,
                site=site,
                topic_arn=topic_arn,
                amqp_topic=config_dict["configs"][project][
                    "notification_bucket_configs"
                ][bucket]["rmq_exchange"],
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
            print(
                f"Failed to setup messaging for {len(retest_messaging)} buckets in project: {project}",
                file=sys.stdout,
            )

    if (to_fix["site_buckets"] or to_fix["project_buckets"]) and not args.dry_run:
        retest_audit_dict = audit_all_buckets(
            aws_credentials_dict=aws_credentials_dict,
            config_map=config_map,
            dry_run=args.dry_run,
        )

        retest_to_fix = test_policies(
            audit_dict=retest_audit_dict, config_dict=config_dict
        )

        if retest_to_fix["site_buckets"] or retest_to_fix["project_buckets"]:
            print(
                f"Failed to apply policies to {len(retest_to_fix['site_buckets']) + len(retest_to_fix['project_buckets'])} buckets",
                file=sys.stdout,
            )

            varys_client = Varys(
                profile="roz",
                logfile=os.devnull,
                log_level="CRITICAL",
                auto_acknowledge=False,
            )

            varys_client.send(
                message="Bucket controller failed in some manner :(",
                exchange="mscape.restricted.announce",
                queue_suffix="slack_integration",
            )
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
    args = parser.parse_args()

    run(args)


if __name__ == "__main__":
    main()
