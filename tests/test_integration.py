import unittest
from unittest.mock import Mock, mock_open, patch, MagicMock, call

from roz_scripts import (
    s3_matcher,
    ingest,
    mscape_ingest_validation,
    utils,
    pathsafe_validation,
)

from onyx.exceptions import (
    OnyxRequestError,
    OnyxConnectionError,
    OnyxServerError,
    OnyxConfigError,
    OnyxClientError,
)

from types import SimpleNamespace
import multiprocessing as mp
import time
import os
import json
from varys import varys
from moto import mock_s3
from moto.server import ThreadedMotoServer
import boto3
import uuid
import pika
import copy

DIR = os.path.dirname(__file__)
S3_MATCHER_LOG_FILENAME = os.path.join(DIR, "s3_matcher.log")
ROZ_INGEST_LOG_FILENAME = os.path.join(DIR, "ingest.log")
MSCAPE_VALIDATION_LOG_FILENAME = os.path.join(DIR, "mscape_validation.log")
PATHSAFE_VALIDATION_LOG_FILENAME = os.path.join(DIR, "pathsafe_validation.log")
TEST_MESSAGE_LOG_FILENAME = os.path.join(DIR, "test_messages.log")

TEST_CSV_FILENAME = os.path.join(DIR, "test.csv")

VARYS_CFG_PATH = os.path.join(DIR, "varys_cfg.json")
TEXT = "Hello, world!"

example_csv_msg = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "ceph:s3",
            "awsRegion": "",
            "eventTime": "2023-10-10T06:39:35.470367Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "testuser"},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {
                "x-amz-request-id": "testdata",
                "x-amz-id-2": "testdata",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "inbound.s3",
                "bucket": {
                    "name": "mscape-birm-ont-prod",
                    "ownerIdentity": {"principalId": "testuser"},
                    "arn": "arn:aws:s3:::mscape-birm-ont-prod",
                    "id": "testdata",
                },
                "object": {
                    "key": "mscape.sample-test.run-test.csv",
                    "size": 275,
                    "eTag": "7022ea6a3adb39323b5039c1d6587d08",
                    "versionId": "",
                    "sequencer": "testdata",
                    "metadata": [
                        {"key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD"},
                        {"key": "x-amz-date", "val": "testdata"},
                    ],
                    "tags": [],
                },
            },
            "eventId": "testdata",
            "opaqueData": "",
        }
    ]
}

example_csv_msg_2 = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "ceph:s3",
            "awsRegion": "",
            "eventTime": "2023-10-10T06:39:35.470367Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "testuser"},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {
                "x-amz-request-id": "testdata",
                "x-amz-id-2": "testdata",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "inbound.s3",
                "bucket": {
                    "name": "mscape-birm-ont-prod",
                    "ownerIdentity": {"principalId": "testuser"},
                    "arn": "arn:aws:s3:::mscape-birm-ont-prod",
                    "id": "testdata",
                },
                "object": {
                    "key": "mscape.sample-test.run-test.csv",
                    "size": 275,
                    "eTag": "29d33a6a67446891caf00d228b954ba7",
                    "versionId": "",
                    "sequencer": "testdata",
                    "metadata": [
                        {"key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD"},
                        {"key": "x-amz-date", "val": "testdata"},
                    ],
                    "tags": [],
                },
            },
            "eventId": "testdata",
            "opaqueData": "",
        }
    ]
}

example_fastq_msg = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "ceph:s3",
            "awsRegion": "",
            "eventTime": "2023-10-10T06:39:35.470367Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "testuser"},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {
                "x-amz-request-id": "testdata",
                "x-amz-id-2": "testdata",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "inbound.s3",
                "bucket": {
                    "name": "mscape-birm-ont-prod",
                    "ownerIdentity": {"principalId": "testuser"},
                    "arn": "arn:aws:s3:::mscape-birm-ont-prod",
                    "id": "testdata",
                },
                "object": {
                    "key": "mscape.sample-test.run-test.fastq.gz",
                    "size": 123123123,
                    "eTag": "179d94f8cd22896c2a80a9a7c98463d2-21",
                    "versionId": "",
                    "sequencer": "testdata",
                    "metadata": [
                        {"key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD"},
                        {"key": "x-amz-date", "val": "testdata"},
                    ],
                    "tags": [],
                },
            },
            "eventId": "testdata",
            "opaqueData": "",
        }
    ]
}

incorrect_fastq_msg = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "ceph:s3",
            "awsRegion": "",
            "eventTime": "2023-10-10T06:39:35.470367Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": "testuser"},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {
                "x-amz-request-id": "testdata",
                "x-amz-id-2": "testdata",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "inbound.s3",
                "bucket": {
                    "name": "mscape-birm-ont-prod",
                    "ownerIdentity": {"principalId": "testuser"},
                    "arn": "arn:aws:s3:::mscape-birm-ont-prod",
                    "id": "testdata",
                },
                "object": {
                    "key": "mscape.sample-test-2.run-test.fastq.gz",
                    "size": 123123123,
                    "eTag": "179d94f8cd22896c2a80a9a7c98463d2-21",
                    "versionId": "",
                    "sequencer": "testdata",
                    "metadata": [
                        {"key": "x-amz-content-sha256", "val": "UNSIGNED-PAYLOAD"},
                        {"key": "x-amz-date", "val": "testdata"},
                    ],
                    "tags": [],
                },
            },
            "eventId": "testdata",
            "opaqueData": "",
        }
    ]
}

example_match_message = {
    "uuid": "42c3796d-d767-4293-97a8-c4906bb5cca8",
    "payload_version": 1,
    "site": "birm",
    "uploaders": ["testuser"],
    "match_timestamp": 1697036668222422871,
    "artifact": "mscape.sample-test.run-test",
    "sample_id": "sample-test",
    "run_id": "run-test",
    "project": "mscape",
    "platform": "ont",
    "files": {
        ".fastq.gz": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "mscapeple-test.run-test.fastq.gz",
        },
        ".csv": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.csv",
            "etag": "7022ea6a3adb39323b5039c1d6587d08",
            "key": "mscape.sample-test.run-test.csv",
        },
    },
    "test_flag": False,
}

example_mismatch_match_message = {
    "uuid": "42c3796d-d767-4293-97a8-c4906bb5cca8",
    "payload_version": 1,
    "site": "birm",
    "uploaders": ["testuser"],
    "match_timestamp": 1697036668222422871,
    "artifact": "mscape.sample-test-2.run-test-2",
    "sample_id": "sample-test-2",
    "run_id": "run-test-2",
    "project": "mscape",
    "platform": "ont",
    "files": {
        ".fastq.gz": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test-2.run-test-2.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "mscape.sample-test-2.run-test-2.fastq.gz",
        },
        ".csv": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.csv",
            "etag": "7022ea6a3adb39323b5039c1d6587d08",
            "key": "mscape.sample-test.run-test.csv",
        },
    },
    "test_flag": False,
}

example_validator_message = {
    "uuid": "b7a4bf27-9305-40e4-9b6b-ed4eb8f5dca6",
    "artifact": "mscape.sample-test.run-test",
    "sample_id": "sample-test",
    "run_id": "run-test",
    "project": "mscape",
    "uploaders": ["mscape-testuser"],
    "platform": "ont",
    "ingest_timestamp": 1694780451766213337,
    "climb_id": False,
    "site": "birm",
    "created": False,
    "ingested": False,
    "files": {
        ".fastq.gz": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "mscape.sample-test.run-test.fastq.gz",
        },
        ".csv": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.csv",
            "etag": "7022ea6a3adb39323b5039c1d6587d08",
            "key": "mscape.sample-test.run-test.csv",
        },
    },
    "onyx_test_status_code": 201,
    "onyx_test_create_errors": {},
    "onyx_test_create_status": True,
    "validate": True,
    "onyx_status_code": False,
    "onyx_errors": {},
    "onyx_create_status": False,
    "ingest_errors": [],
    "test_flag": False,
    "test_ingest_result": False,
}

example_pathsafe_validator_message = {
    "uuid": "b7a4bf27-9305-40e4-9b6b-ed4eb8f5dca6",
    "artifact": "pathsafetest.sample-test.run-test",
    "sample_id": "sample-test",
    "run_id": "run-test",
    "project": "pathsafetest",
    "uploaders": ["mscape-testuser"],
    "platform": "illumina",
    "ingest_timestamp": 1694780451766213337,
    "climb_id": False,
    "site": "birm",
    "created": False,
    "ingested": False,
    "files": {
        ".1.fastq.gz": {
            "uri": "s3://pathsafetest-birm-illumina-prod/pathsafetest.sample-test.run-test.1.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "pathsafetest.sample-test.run-test.1.fastq.gz",
        },
        ".2.fastq.gz": {
            "uri": "s3://pathsafetest-birm-illumina-prod/pathsafetest.sample-test.run-test.2.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "pathsafetest.sample-test.run-test.2.fastq.gz",
        },
        ".csv": {
            "uri": "s3://pathsafetest-birm-illumina-prod/pathsafetest.sample-test.run-test.csv",
            "etag": "7022ea6a3adb39323b5039c1d6587d08",
            "key": "pathsafetest.sample-test.run-test.csv",
        },
    },
    "onyx_test_status_code": 201,
    "onyx_test_create_errors": {},
    "onyx_test_create_status": True,
    "validate": True,
    "onyx_status_code": False,
    "onyx_errors": {},
    "onyx_create_status": False,
    "ingest_errors": [],
    "test_flag": False,
    "test_ingest_result": False,
}

example_pathsafe_test_validator_message = {
    "uuid": "b7a4bf27-9305-40e4-9b6b-ed4eb8f5dca6",
    "artifact": "pathsafetest.sample-test.run-test",
    "sample_id": "sample-test",
    "run_id": "run-test",
    "project": "pathsafetest",
    "uploaders": ["mscape-testuser"],
    "platform": "illumina",
    "ingest_timestamp": 1694780451766213337,
    "climb_id": False,
    "site": "birm",
    "created": False,
    "ingested": False,
    "files": {
        ".1.fastq.gz": {
            "uri": "s3://pathsafetest-birm-illumina-prod/pathsafetest.sample-test.run-test.1.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "pathsafetest.sample-test.run-test.1.fastq.gz",
        },
        ".2.fastq.gz": {
            "uri": "s3://pathsafetest-birm-illumina-prod/pathsafetest.sample-test.run-test.2.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "pathsafetest.sample-test.run-test.2.fastq.gz",
        },
        ".csv": {
            "uri": "s3://pathsafetest-birm-illumina-prod/pathsafetest.sample-test.run-test.csv",
            "etag": "7022ea6a3adb39323b5039c1d6587d08",
            "key": "pathsafetest.sample-test.run-test.csv",
        },
    },
    "onyx_test_status_code": 201,
    "onyx_test_create_errors": {},
    "onyx_test_create_status": True,
    "validate": True,
    "onyx_status_code": False,
    "onyx_errors": {},
    "onyx_create_status": False,
    "ingest_errors": [],
    "test_flag": True,
    "test_ingest_result": False,
}

example_test_validator_message = {
    "uuid": "b7a4bf27-9305-40e4-9b6b-ed4eb8f5dca6",
    "artifact": "mscape.sample-test.run-test",
    "sample_id": "sample-test",
    "run_id": "run-test",
    "project": "mscape",
    "uploaders": ["mscape-testuser"],
    "platform": "ont",
    "ingest_timestamp": 1694780451766213337,
    "climb_id": False,
    "site": "birm",
    "created": False,
    "ingested": False,
    "files": {
        ".fastq.gz": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.fastq.gz",
            "etag": "179d94f8cd22896c2a80a9a7c98463d2-21",
            "key": "mscape.sample-test.run-test.fastq.gz",
        },
        ".csv": {
            "uri": "s3://mscape-birm-ont-prod/mscape.sample-test.run-test.csv",
            "etag": "7022ea6a3adb39323b5039c1d6587d08",
            "key": "mscape.sample-test.run-test.csv",
        },
    },
    "onyx_test_status_code": 201,
    "onyx_test_create_errors": {},
    "onyx_test_create_status": True,
    "validate": True,
    "onyx_status_code": False,
    "onyx_errors": {},
    "onyx_create_status": False,
    "ingest_errors": [],
    "test_flag": True,
    "test_ingest_result": False,
}

example_execution_trace = """task_id	hash	native_id	name	status	exit	submit	duration	realtime	%cpu	peak_rss	peak_vmem	rchar	wchar
1	c8/ff67a6	nf-c8ff67a652a85eda6287589d2e7cd49f	ingest:get_params_and_versions:get_params	COMPLETED	0	2023-12-14 21:27:52.842	13.2s	84ms	55.8%	3.3 MB	5.6 MB	65.2 KB	4.9 KB
3	80/6be72c	nf-806be72cd3bb8ac04caa16b2e232e757	ingest:get_params_and_versions:get_versions	COMPLETED	0	2023-12-14 21:27:53.449	14.6s	1.8s	97.2%	43 MB	56.7 MB	13.8 MB	9 KB
2	35/232dc9	nf-35232dc9a9711f2747073e1d90020b81	ingest:preprocess:fastp_single (1)	COMPLETED	0	2023-12-14 21:32:18.624	6m 44s	6m 31s	456.6%	2.4 GB	2.9 GB	31.5 GB	26.3 GB
5	09/84c320	nf-0984c3209aef763c49d95a9c3de6957b	ingest:classify_and_report:qc_checks:read_stats (1)	COMPLETED	0	2023-12-14 21:39:08.654	4m 22s	4m 14s	93.9%	6.1 MB	11.3 MB	8.8 GB	17.8 GB
6	88/d6fa5c	nf-88d6fa5cc7eca3ec17af559611b8063c	ingest:classify_and_report:qc_checks:publish_stats (1)	COMPLETED	0	2023-12-14 21:43:41.168	14.8s	35ms	125.5%	0	0	70 KB	213 B
4	2f/3f5e13	nf-2f3f5e13dea535095f6e6a236b560080	ingest:classify_and_report:kraken_classify:run_kraken_and_bracken:kraken2_client (1)	COMPLETED	0	2023-12-14 21:39:08.773	9m 38s	9m 25s	61.1%	4.6 GB	5.2 GB	8.8 GB	2.2 GB
8	51/d29ab2	nf-51d29ab2012d75109e2ea08723d726d4	ingest:classify_and_report:kraken_classify:run_kraken_and_bracken:kraken_to_json (1)	COMPLETED	0	2023-12-14 21:48:52.545	19.5s	4.7s	190.3%	702.3 MB	1.8 GB	417.9 MB	1.8 MB
7	fa/2551d1	nf-fa2551d15cd1c1b13a810293a1ecd13c	ingest:extract_all:extract_taxa:split_kreport (1)	COMPLETED	0	2023-12-14 21:48:52.762	17.2s	0ms	88.0%	3 MB	5.4 MB	1.8 MB	804.6 KB
10	b2/7766a1	nf-b27766a1c07e7be17d969689a0a33e91	ingest:extract_all:extract_fractions:extract_virus_and_unclassified (1)	COMPLETED	0	2023-12-14 21:48:52.654	2m 28s	2m 20s	97.1%	730.3 MB	738.3 MB	11.4 GB	1.6 MB
13	08/dc8a41	nf-08dc8a4161e92d4c883b47822b63cc33	ingest:extract_all:extract_taxa:extract_taxa_reads (2)	FAILED	3	2023-12-14 21:49:17.974	2m 25s	2m 12s	-	-	-	-	-
14	e3/eb8b5b	nf-e3eb8b5b9d3d3221b72f75fc036e4ca1	ingest:extract_all:extract_taxa:extract_taxa_reads (3)	COMPLETED	0	2023-12-14 21:49:18.074	2m 30s	2m 15s	99.1%	603.5 MB	613.6 MB	11.2 GB	52.5 KB
16	a6/b5fecf	nf-a6b5fecfd116ba580f7635ea6b71aebb	ingest:extract_all:extract_taxa:bgzip_extracted_taxa (1)	COMPLETED	0	2023-12-14 21:51:51.875	7.1s	138ms	66.8%	3.2 MB	5.6 MB	162.4 KB	26.7 KB
15	03/8a312f	nf-038a312f81e661f14c9559f165393ea6	ingest:extract_all:extract_taxa:extract_taxa_reads (4)	COMPLETED	0	2023-12-14 21:49:18.170	4m 5s	3m 51s	100.0%	1.4 GB	1.4 GB	11.2 GB	1.7 GB
17	71/814115	nf-71814115b27f7b8fc9499f8f392b9eef	ingest:extract_all:extract_taxa:bgzip_extracted_taxa (2)	COMPLETED	0	2023-12-14 21:53:28.562	32.4s	24.6s	416.4%	13.4 MB	586.6 MB	1.7 GB	908.5 MB
9	78/1c46e3	nf-781c46e36e7bcb6fcd36f1ce88e58f8c	ingest:classify_and_report:check_hcid_status:check_hcid (1)	COMPLETED	0	2023-12-14 21:48:52.868	16m 28s	16m 11s	99.4%	392.4 MB	613.5 MB	4.3 GB	2.2 KB
18	79/46778a	nf-7946778a7caee2e4e3269b71a898d09e	ingest:classify_and_report:generate_report:make_report (1)	COMPLETED	0	2023-12-14 22:05:26.866	33.1s	18.4s	99.0%	188.8 MB	202.1 MB	344.7 MB	315.6 KB
12	c7/041109	nf-c7041109325a3a30579502b4605a86e4	ingest:extract_all:extract_taxa:extract_taxa_reads (1)	COMPLETED	0	2023-12-14 21:49:17.848	24m 42s	24m 28s	100.3%	18.1 GB	18.1 GB	11.2 GB	18.6 GB
11	cb/7c562d	nf-cb7c562da605ac9820b9a60f1f86c755	ingest:extract_all:extract_fractions:extract_dehumanized (1)	COMPLETED	0	2023-12-14 21:48:52.345	25m 10s	24m 55s	99.0%	600.3 MB	609.5 MB	11.4 GB	17.2 GB
20	c1/d5b2c4	nf-c1d5b2c4709000759e76fbfb765f0b41	ingest:extract_all:extract_taxa:merge_read_summary (1)	COMPLETED	0	2023-12-14 22:14:12.519	13.5s	82ms	79.3%	3.3 MB	5.6 MB	77.4 KB	16 KB
23	4f/a59b6e	nf-4fa59b6e01b05128c3ac44a633729437	ingest:extract_all:extract_fractions:merge_read_summary (1)	COMPLETED	0	2023-12-14 22:14:12.959	16s	54ms	77.2%	3.2 MB	5.6 MB	65.7 KB	874 B
22	55/6248c5	nf-556248c5bf00e5694d83b81b36eb019d	ingest:extract_all:extract_fractions:bgzip_extracted_taxa (2)	COMPLETED	0	2023-12-14 22:14:13.061	1m 38s	181ms	87.9%	3.1 MB	5.6 MB	1.7 MB	838.3 KB
21	19/d1c288	nf-19d1c28844e360082ae73ef34ee95b52	ingest:extract_all:extract_fractions:bgzip_extracted_taxa (1)	COMPLETED	0	2023-12-14 22:14:12.919	2m 41s	2m 32s	497.5%	13.1 MB	522.5 MB	17.2 GB	8.7 GB
19	8c/292cce	nf-8c292cce85d72effaa636db6ad0098ab	ingest:extract_all:extract_taxa:bgzip_extracted_taxa (3)	COMPLETED	0	2023-12-14 22:14:12.692	3m 34s	3m 21s	476.5%	13.3 MB	585.3 MB	18.6 GB	9.4 GB
"""

example_execution_trace_human = """task_id	hash	native_id	name	status	exit	submit	duration	realtime	%cpu	peak_rss	peak_vmem	rchar	wchar
1	c8/ff67a6	nf-c8ff67a652a85eda6287589d2e7cd49f	ingest:get_params_and_versions:get_params	COMPLETED	0	2023-12-14 21:27:52.842	13.2s	84ms	55.8%	3.3 MB	5.6 MB	65.2 KB	4.9 KB
3	80/6be72c	nf-806be72cd3bb8ac04caa16b2e232e757	ingest:get_params_and_versions:get_versions	COMPLETED	0	2023-12-14 21:27:53.449	14.6s	1.8s	97.2%	43 MB	56.7 MB	13.8 MB	9 KB
2	35/232dc9	nf-35232dc9a9711f2747073e1d90020b81	ingest:preprocess:fastp_single (1)	COMPLETED	0	2023-12-14 21:32:18.624	6m 44s	6m 31s	456.6%	2.4 GB	2.9 GB	31.5 GB	26.3 GB
5	09/84c320	nf-0984c3209aef763c49d95a9c3de6957b	ingest:classify_and_report:qc_checks:read_stats (1)	COMPLETED	0	2023-12-14 21:39:08.654	4m 22s	4m 14s	93.9%	6.1 MB	11.3 MB	8.8 GB	17.8 GB
6	88/d6fa5c	nf-88d6fa5cc7eca3ec17af559611b8063c	ingest:classify_and_report:qc_checks:publish_stats (1)	COMPLETED	0	2023-12-14 21:43:41.168	14.8s	35ms	125.5%	0	0	70 KB	213 B
4	2f/3f5e13	nf-2f3f5e13dea535095f6e6a236b560080	ingest:classify_and_report:kraken_classify:run_kraken_and_bracken:kraken2_client (1)	COMPLETED	0	2023-12-14 21:39:08.773	9m 38s	9m 25s	61.1%	4.6 GB	5.2 GB	8.8 GB	2.2 GB
8	51/d29ab2	nf-51d29ab2012d75109e2ea08723d726d4	ingest:classify_and_report:kraken_classify:run_kraken_and_bracken:kraken_to_json (1)	COMPLETED	0	2023-12-14 21:48:52.545	19.5s	4.7s	190.3%	702.3 MB	1.8 GB	417.9 MB	1.8 MB
7	fa/2551d1	nf-fa2551d15cd1c1b13a810293a1ecd13c	ingest:extract_all:extract_taxa:split_kreport (1)	COMPLETED	0	2023-12-14 21:48:52.762	17.2s	0ms	88.0%	3 MB	5.4 MB	1.8 MB	804.6 KB
10	b2/7766a1	nf-b27766a1c07e7be17d969689a0a33e91	ingest:extract_all:extract_fractions:extract_virus_and_unclassified (1)	COMPLETED	0	2023-12-14 21:48:52.654	2m 28s	2m 20s	97.1%	730.3 MB	738.3 MB	11.4 GB	1.6 MB
13	08/dc8a41	nf-08dc8a4161e92d4c883b47822b63cc33	ingest:extract_all:extract_taxa:extract_taxa_reads (2)	FAILED	3	2023-12-14 21:49:17.974	2m 25s	2m 12s	-	-	-	-	-
14	e3/eb8b5b	nf-e3eb8b5b9d3d3221b72f75fc036e4ca1	ingest:extract_all:extract_taxa:extract_taxa_reads (3)	COMPLETED	0	2023-12-14 21:49:18.074	2m 30s	2m 15s	99.1%	603.5 MB	613.6 MB	11.2 GB	52.5 KB
16	a6/b5fecf	nf-a6b5fecfd116ba580f7635ea6b71aebb	ingest:extract_all:extract_taxa:bgzip_extracted_taxa (1)	COMPLETED	0	2023-12-14 21:51:51.875	7.1s	138ms	66.8%	3.2 MB	5.6 MB	162.4 KB	26.7 KB
15	03/8a312f	nf-038a312f81e661f14c9559f165393ea6	ingest:extract_all:extract_taxa:extract_taxa_reads (4)	COMPLETED	0	2023-12-14 21:49:18.170	4m 5s	3m 51s	100.0%	1.4 GB	1.4 GB	11.2 GB	1.7 GB
17	71/814115	nf-71814115b27f7b8fc9499f8f392b9eef	ingest:extract_all:extract_taxa:bgzip_extracted_taxa (2)	COMPLETED	0	2023-12-14 21:53:28.562	32.4s	24.6s	416.4%	13.4 MB	586.6 MB	1.7 GB	908.5 MB
9	78/1c46e3	nf-781c46e36e7bcb6fcd36f1ce88e58f8c	ingest:classify_and_report:check_hcid_status:check_hcid (1)	COMPLETED	0	2023-12-14 21:48:52.868	16m 28s	16m 11s	99.4%	392.4 MB	613.5 MB	4.3 GB	2.2 KB
18	79/46778a	nf-7946778a7caee2e4e3269b71a898d09e	ingest:classify_and_report:generate_report:make_report (1)	COMPLETED	0	2023-12-14 22:05:26.866	33.1s	18.4s	99.0%	188.8 MB	202.1 MB	344.7 MB	315.6 KB
12	c7/041109	nf-c7041109325a3a30579502b4605a86e4	ingest:extract_all:extract_taxa:extract_taxa_reads (1)	FAILED	2	2023-12-14 21:49:17.848	24m 42s	24m 28s	100.3%	18.1 GB	18.1 GB	11.2 GB	18.6 GB
11	cb/7c562d	nf-cb7c562da605ac9820b9a60f1f86c755	ingest:extract_all:extract_fractions:extract_dehumanized (1)	COMPLETED	0	2023-12-14 21:48:52.345	25m 10s	24m 55s	99.0%	600.3 MB	609.5 MB	11.4 GB	17.2 GB
20	c1/d5b2c4	nf-c1d5b2c4709000759e76fbfb765f0b41	ingest:extract_all:extract_taxa:merge_read_summary (1)	COMPLETED	0	2023-12-14 22:14:12.519	13.5s	82ms	79.3%	3.3 MB	5.6 MB	77.4 KB	16 KB
23	4f/a59b6e	nf-4fa59b6e01b05128c3ac44a633729437	ingest:extract_all:extract_fractions:merge_read_summary (1)	COMPLETED	0	2023-12-14 22:14:12.959	16s	54ms	77.2%	3.2 MB	5.6 MB	65.7 KB	874 B
22	55/6248c5	nf-556248c5bf00e5694d83b81b36eb019d	ingest:extract_all:extract_fractions:bgzip_extracted_taxa (2)	COMPLETED	0	2023-12-14 22:14:13.061	1m 38s	181ms	87.9%	3.1 MB	5.6 MB	1.7 MB	838.3 KB
21	19/d1c288	nf-19d1c28844e360082ae73ef34ee95b52	ingest:extract_all:extract_fractions:bgzip_extracted_taxa (1)	COMPLETED	0	2023-12-14 22:14:12.919	2m 41s	2m 32s	497.5%	13.1 MB	522.5 MB	17.2 GB	8.7 GB
19	8c/292cce	nf-8c292cce85d72effaa636db6ad0098ab	ingest:extract_all:extract_taxa:bgzip_extracted_taxa (3)	COMPLETED	0	2023-12-14 22:14:12.692	3m 34s	3m 21s	476.5%	13.3 MB	585.3 MB	18.6 GB	9.4 GB
"""

example_reads_summary = [
    {
        "human_readable": "Pseudomonas",
        "taxon": "286",
        "tax_level": "G",
        "filenames": ["286.fastq"],
        "qc_metrics": {
            "num_reads": 20188,
            "avg_qual": 37.19427382603527,
            "mean_len": 249.46433524866256,
        },
    }
]

example_params = {
    "database_set": "PlusPF",
}

example_k2_out = {
    "0": {
        "percentage": 3.36,
        "count": 50615,
        "count_descendants": 50615,
        "raw_rank": "U",
        "rank": "U",
        "name": "unclassified",
        "taxid": "0",
    }
}


class MockResponse:
    def __init__(self, status_code, json_data=None, ok=True):
        self.status_code = status_code
        self.json_data = json_data
        self.ok = ok

    def json(self):
        return self.json_data


class Test_S3_matcher(unittest.TestCase):
    def setUp(self):
        self.server = ThreadedMotoServer()
        self.server.start()

        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        os.environ["UNIT_TESTING"] = "True"

        config = {
            "version": "0.1",
            "profiles": {
                "roz": {
                    "username": "guest",
                    "password": "guest",
                    "amqp_url": "127.0.0.1",
                    "port": 5672,
                    "use_tls": False,
                }
            },
        }

        with open(VARYS_CFG_PATH, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        os.environ["VARYS_CFG"] = VARYS_CFG_PATH
        os.environ["S3_MATCHER_LOG"] = S3_MATCHER_LOG_FILENAME
        os.environ["INGEST_LOG_LEVEL"] = "DEBUG"
        os.environ["ROZ_CONFIG_JSON"] = "config/config.json"
        os.environ["ONYX_ROZ_PASSWORD"] = "password"
        os.environ["ROZ_INGEST_LOG"] = ROZ_INGEST_LOG_FILENAME

        self.varys_client = varys("roz", TEST_MESSAGE_LOG_FILENAME)

        self.s3_matcher_process = mp.Process(target=s3_matcher.main)
        self.s3_matcher_process.start()
        # Annoying but required so that the matcher can make the huge number of S3 calls it needs to make when it starts
        time.sleep(2)

    def tearDown(self):
        self.varys_client.close()
        self.s3_matcher_process.kill()
        self.server.stop()

        credentials = pika.PlainCredentials("guest", "guest")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", credentials=credentials)
        )
        channel = connection.channel()

        channel.queue_delete(queue="inbound.s3")
        channel.queue_delete(queue="inbound.matched")

        del os.environ["UNIT_TESTING"]

        connection.close()
        time.sleep(1)

    def test_s3_successful_match(self):
        self.varys_client.send(
            example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )
        self.varys_client.send(
            example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )

        message = self.varys_client.receive(
            exchange="inbound.matched",
            queue_suffix="s3_matcher",
            timeout=20,
        )

        self.assertIsNotNone(message)
        message_dict = json.loads(message.body)

        self.assertEqual(message_dict["sample_id"], "sample-test")
        self.assertEqual(message_dict["artifact"], "mscape.sample-test.run-test")
        self.assertEqual(message_dict["run_id"], "run-test")
        self.assertEqual(message_dict["project"], "mscape")
        self.assertEqual(message_dict["platform"], "ont")
        self.assertEqual(message_dict["site"], "birm")
        self.assertEqual(message_dict["uploaders"], ["testuser"])
        self.assertEqual(
            message_dict["files"][".csv"]["key"],
            "mscape.sample-test.run-test.csv",
        )
        self.assertEqual(
            message_dict["files"][".fastq.gz"]["key"],
            "mscape.sample-test.run-test.fastq.gz",
        )
        self.assertTrue(uuid.UUID(message_dict["uuid"], version=4))

    def test_s3_incorrect_match(self):
        self.varys_client.send(
            example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )
        self.varys_client.send(
            incorrect_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )

        message = self.varys_client.receive(
            exchange="inbound.matched",
            queue_suffix="s3_matcher",
            timeout=10,
        )
        self.assertIsNone(message)

    def test_s3_updated_csv(self):
        self.varys_client.send(
            example_csv_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )
        self.varys_client.send(
            example_fastq_msg, exchange="inbound.s3", queue_suffix="s3_matcher"
        )

        message = self.varys_client.receive(
            exchange="inbound.matched",
            queue_suffix="s3_matcher",
            timeout=30,
        )

        self.assertIsNotNone(message)

        self.varys_client.send(
            example_csv_msg_2, exchange="inbound.s3", queue_suffix="s3_matcher"
        )

        message_2 = self.varys_client.receive(
            exchange="inbound.matched",
            queue_suffix="s3_matcher",
            timeout=30,
        )

        self.assertIsNotNone(message_2)

        message_dict = json.loads(message_2.body)

        self.assertEqual(message_dict["sample_id"], "sample-test")
        self.assertEqual(message_dict["artifact"], "mscape.sample-test.run-test")
        self.assertEqual(message_dict["run_id"], "run-test")
        self.assertEqual(message_dict["project"], "mscape")
        self.assertEqual(message_dict["platform"], "ont")
        self.assertEqual(message_dict["site"], "birm")
        self.assertEqual(message_dict["uploaders"], ["testuser"])
        self.assertEqual(
            message_dict["files"][".csv"]["key"],
            "mscape.sample-test.run-test.csv",
        )
        self.assertEqual(
            message_dict["files"][".fastq.gz"]["key"],
            "mscape.sample-test.run-test.fastq.gz",
        )
        self.assertTrue(uuid.UUID(message_dict["uuid"], version=4))


class Test_ingest(unittest.TestCase):
    def setUp(self):
        self.server = ThreadedMotoServer()
        self.server.start()

        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["ONYX_DOMAIN"] = "testing"
        os.environ["ONYX_TOKEN"] = "testing"
        os.environ["UNIT_TESTING"] = "True"

        self.s3_client = boto3.client("s3", endpoint_url="http://localhost:5000")
        self.s3_client.create_bucket(Bucket="mscape-birm-ont-prod")

        with open(TEST_CSV_FILENAME, "w") as f:
            f.write("sample_id,run_id,project,platform,site\n")
            f.write("sample-test,run-test,mscape,ont,birm")

        self.s3_client.upload_file(
            TEST_CSV_FILENAME,
            "mscape-birm-ont-prod",
            "mscape.sample-test.run-test.csv",
        )

        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        csv_etag = resp["ETag"].replace('"', "").replace('"', "")

        example_match_message["files"][".csv"]["etag"] = csv_etag.replace('"', "")
        example_mismatch_match_message["files"][".csv"]["etag"] = csv_etag.replace(
            '"', ""
        )

        config = {
            "version": "0.1",
            "profiles": {
                "roz": {
                    "username": "guest",
                    "password": "guest",
                    "amqp_url": "127.0.0.1",
                    "port": 5672,
                    "use_tls": False,
                }
            },
        }

        with open(VARYS_CFG_PATH, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        os.environ["VARYS_CFG"] = VARYS_CFG_PATH
        os.environ["S3_MATCHER_LOG"] = ROZ_INGEST_LOG_FILENAME
        os.environ["INGEST_LOG_LEVEL"] = "DEBUG"
        os.environ["ROZ_CONFIG_JSON"] = "config/config.json"
        os.environ["ONYX_ROZ_PASSWORD"] = "password"
        os.environ["ROZ_INGEST_LOG"] = ROZ_INGEST_LOG_FILENAME

        self.varys_client = varys("roz", TEST_MESSAGE_LOG_FILENAME)

    def tearDown(self):
        self.varys_client.close()
        self.server.stop()
        self.ingest_process.kill()

        credentials = pika.PlainCredentials("guest", "guest")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", credentials=credentials)
        )
        channel = connection.channel()

        os.remove(TEST_CSV_FILENAME)

        del os.environ["UNIT_TESTING"]

        channel.queue_delete(queue="inbound.matched")
        channel.queue_delete(queue="inbound.to_validate.mscape")

        connection.close()
        time.sleep(1)

    def test_ingest_successful(self):
        with patch("roz_scripts.utils.utils.OnyxClient") as mock_client:
            mock_client.return_value.__enter__.return_value.csv_create.return_value = {}

            self.ingest_process = mp.Process(target=ingest.main)
            self.ingest_process.start()

            time.sleep(1)

            test_message = copy.deepcopy(example_match_message)

            self.varys_client.send(
                test_message,
                exchange="inbound.matched",
                queue_suffix="s3_matcher",
            )

            message = self.varys_client.receive(
                exchange="inbound.to_validate.mscape",
                queue_suffix="ingest",
                timeout=10,
            )

            self.assertIsNotNone(message)

            message_dict = json.loads(message.body)

            self.assertEqual(message_dict["sample_id"], "sample-test")
            self.assertEqual(message_dict["artifact"], "mscape.sample-test.run-test")
            self.assertEqual(message_dict["run_id"], "run-test")
            self.assertEqual(message_dict["project"], "mscape")
            self.assertEqual(message_dict["platform"], "ont")
            self.assertEqual(message_dict["site"], "birm")
            self.assertEqual(message_dict["uploaders"], ["testuser"])
            self.assertEqual(
                message_dict["files"][".csv"]["key"],
                "mscape.sample-test.run-test.csv",
            )
            self.assertTrue(message_dict["validate"])
            self.assertTrue(message_dict["onyx_test_create_status"])
            self.assertNotIn("climb_id", message_dict.keys())
            self.assertFalse(message_dict["test_flag"])
            self.assertTrue(uuid.UUID(message_dict["uuid"], version=4))


class Test_mscape_validator(unittest.TestCase):
    def setUp(self):
        self.server = ThreadedMotoServer()
        self.server.start()

        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        os.environ["ONYX_DOMAIN"] = "testing"
        os.environ["ONYX_USERNAME"] = "testing"
        os.environ["ONYX_PASSWORD"] = "testing"
        os.environ["SCYLLA_K2_DB_PATH"] = "/test/path/pluspf"
        os.environ["SCYLLA_K2_DB_DATE"] = "2024-01-01"
        os.environ["SCYLLA_TAXONOMY_PATH"] = "/test/path/taxonomy"
        os.environ["SCYLLA_TAXONOMY_DATE"] = "2024-01-01"

        os.environ["UNIT_TESTING"] = "True"

        self.s3_client = boto3.client("s3", endpoint_url="http://localhost:5000")
        self.s3_client.create_bucket(Bucket="mscape-birm-ont-prod")
        self.s3_client.create_bucket(Bucket="mscape-published-reads")
        self.s3_client.create_bucket(Bucket="mscape-published-reports")
        self.s3_client.create_bucket(Bucket="mscape-published-taxon-reports")
        self.s3_client.create_bucket(Bucket="mscape-published-binned-reads")
        self.s3_client.create_bucket(Bucket="mscape-published-read-fractions")

        with open(TEST_CSV_FILENAME, "w") as f:
            f.write("sample_id,run_id,project,platform,site\n")
            f.write("sample-test,run-test,mscape,ont,birm")

        self.s3_client.upload_file(
            TEST_CSV_FILENAME,
            "mscape-birm-ont-prod",
            "mscape.sample-test.run-test.csv",
        )

        resp = self.s3_client.head_object(
            Bucket="mscape-birm-ont-prod",
            Key="mscape.sample-test.run-test.csv",
        )

        self.log = utils.init_logger(
            "mscape.ingest", MSCAPE_VALIDATION_LOG_FILENAME, "DEBUG"
        )

        csv_etag = resp["ETag"].replace('"', "")

        example_validator_message["files"][".csv"]["etag"] = csv_etag

        config = {
            "version": "0.1",
            "profiles": {
                "roz": {
                    "username": "guest",
                    "password": "guest",
                    "amqp_url": "127.0.0.1",
                    "port": 5672,
                    "use_tls": False,
                }
            },
        }

        with open(VARYS_CFG_PATH, "w") as f:
            json.dump(config, f, ensure_ascii=False)

        os.environ["VARYS_CFG"] = VARYS_CFG_PATH
        os.environ["S3_MATCHER_LOG"] = ROZ_INGEST_LOG_FILENAME
        os.environ["INGEST_LOG_LEVEL"] = "DEBUG"
        os.environ["ROZ_CONFIG_JSON"] = "config/config.json"
        os.environ["ONYX_DOMAIN"] = "domain"
        os.environ["ONYX_TOKEN"] = "testing"

        os.environ["ROZ_INGEST_LOG"] = ROZ_INGEST_LOG_FILENAME

        self.varys_client = varys("roz", TEST_MESSAGE_LOG_FILENAME)

    def tearDown(self):
        credentials = pika.PlainCredentials("guest", "guest")

        connection = pika.BlockingConnection(
            pika.ConnectionParameters("localhost", credentials=credentials)
        )
        channel = connection.channel()

        channel.queue_delete(queue="inbound.to_validate.mscape")
        channel.queue_delete(queue="inbound.new_artifact.mscape")
        channel.queue_delete(queue="inbound.results.mscape.birm")

        connection.close()

        os.remove(TEST_CSV_FILENAME)

        self.server.stop()
        self.varys_client.close()

        del os.environ["UNIT_TESTING"]

        time.sleep(1)

    def test_validator_successful(self):
        with (
            patch("roz_scripts.utils.utils.pipeline") as mock_pipeline,
            patch("roz_scripts.utils.utils.OnyxClient") as mock_client,
        ):
            mock_pipeline.return_value.execute.return_value = (
                0,
                "test_stdout",
                "test_stderr",
            )

            mock_pipeline.return_value.cleanup.return_value = (
                0,
                "test_stdout",
                "test_stderr",
            )
            mock_pipeline.return_value.cmd.return_value = "Hello pytest :)"

            mock_client.return_value.__enter__.return_value.update.return_value = {}

            mock_client.return_value.__enter__.return_value.csv_create.return_value = {
                "climb_id": "test_climb_id",
                "sample_id": "sample-test",
                "run_id": "run-test",
            }

            mock_client.return_value.__enter__.return_value.identify = Mock(
                side_effect=OnyxRequestError(
                    message="test identify exception",
                    response=MockResponse(
                        status_code=404,
                        json_data={
                            "data": [],
                            "messages": {"sample_id": "Test sample_id error handling"},
                        },
                    ),
                )
            )

            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ()
            )

            result_path = os.path.join(DIR, example_validator_message["uuid"])
            preprocess_path = os.path.join(result_path, "preprocess")
            classifications_path = os.path.join(result_path, "classifications")
            pipeline_info_path = os.path.join(result_path, "pipeline_info")
            binned_reads_path = os.path.join(result_path, "reads_by_taxa")
            read_fraction_path = os.path.join(result_path, "read_fractions")

            os.makedirs(preprocess_path, exist_ok=True)
            os.makedirs(classifications_path, exist_ok=True)
            os.makedirs(pipeline_info_path, exist_ok=True)
            os.makedirs(binned_reads_path, exist_ok=True)
            os.makedirs(read_fraction_path, exist_ok=True)

            open(
                os.path.join(
                    preprocess_path,
                    f"{example_validator_message['uuid']}.fastp.fastq.gz",
                ),
                "w",
            ).close()
            open(os.path.join(read_fraction_path, "dehumanised.fastq.gz"), "w").close()
            open(os.path.join(read_fraction_path, "viral.fastq.gz"), "w").close()
            open(os.path.join(read_fraction_path, "unclassified.fastq.gz"), "w").close()
            open(
                os.path.join(read_fraction_path, "viral_and_unclassified.fastq.gz"), "w"
            ).close()
            open(
                os.path.join(classifications_path, "PlusPF.kraken_report.txt"), "w"
            ).close()
            open(os.path.join(binned_reads_path, "286.fastq.gz"), "w").close()
            open(
                os.path.join(
                    result_path, f"{example_validator_message['uuid']}_report.html"
                ),
                "w",
            ).close()

            with open(
                os.path.join(
                    pipeline_info_path,
                    f"execution_trace_{example_validator_message['uuid']}.txt",
                ),
                "w",
            ) as f:
                f.write(example_execution_trace)

            with open(
                os.path.join(
                    pipeline_info_path,
                    f"params_{example_validator_message['uuid']}.log",
                ),
                "w",
            ) as f:
                f.write(json.dumps(example_params))

            with open(
                os.path.join(classifications_path, "PlusPF.kraken_report.json"), "w"
            ) as f:
                f.write(json.dumps(example_k2_out))

            with open(
                os.path.join(binned_reads_path, "reads_summary_combined.json"), "w"
            ) as f:
                json.dump(example_reads_summary, f)

            args = SimpleNamespace(
                logfile=MSCAPE_VALIDATION_LOG_FILENAME,
                log_level="DEBUG",
                nxf_executable="test",
                config="test",
                k2_host="test",
                result_dir=DIR,
                n_workers=2,
            )

            pipeline = utils.pipeline(
                pipe="test",
                nxf_executable="test",
                config="test",
            )

            test_message = copy.deepcopy(example_validator_message)

            in_message = SimpleNamespace(body=json.dumps(test_message))

            Success, alert, payload, message = mscape_ingest_validation.validate(
                in_message, args, pipeline
            )

            print(payload)

            self.assertTrue(Success)
            self.assertFalse(alert)

            self.assertTrue(uuid.UUID(payload["uuid"], version=4))
            self.assertEqual(
                payload["artifact"],
                "mscape.sample-test.run-test",
            )
            self.assertEqual(payload["project"], "mscape")
            self.assertEqual(payload["site"], "birm")
            self.assertEqual(payload["platform"], "ont")
            self.assertEqual(payload["climb_id"], "test_climb_id")
            self.assertEqual(payload["created"], True)
            self.assertEqual(payload["published"], True)
            self.assertEqual(payload["onyx_create_status"], True)
            self.assertEqual(payload["test_flag"], False)

            published_reads_contents = self.s3_client.list_objects(
                Bucket="mscape-published-reads"
            )
            self.assertEqual(
                published_reads_contents["Contents"][0]["Key"], "test_climb_id.fastq.gz"
            )

            published_reports_contents = self.s3_client.list_objects(
                Bucket="mscape-published-reports"
            )
            self.assertEqual(
                published_reports_contents["Contents"][0]["Key"],
                "test_climb_id_scylla_report.html",
            )

            published_taxon_reports_contents = self.s3_client.list_objects(
                Bucket="mscape-published-taxon-reports"
            )
            self.assertIn(
                "test_climb_id/test_climb_id_PlusPF.kraken_report.txt",
                [x["Key"] for x in published_taxon_reports_contents["Contents"]],
            )

            published_binned_reads_contents = self.s3_client.list_objects(
                Bucket="mscape-published-binned-reads"
            )
            self.assertEqual(
                published_binned_reads_contents["Contents"][0]["Key"],
                "test_climb_id/test_climb_id_286.fastq.gz",
            )

    def test_too_much_human(self):
        with (
            patch("roz_scripts.utils.utils.pipeline") as mock_pipeline,
            patch("roz_scripts.utils.utils.OnyxClient") as mock_client,
        ):
            mock_pipeline.return_value.execute.return_value = (
                0,
                "test_stdout",
                "test_stderr",
            )

            mock_pipeline.return_value.cleanup.return_value = (
                0,
                "test_stdout",
                "test_stderr",
            )
            mock_pipeline.return_value.cmd.return_value = "Hello pytest :)"

            mock_client.return_value.__enter__.return_value.update.return_value = {}

            mock_client.return_value.__enter__.return_value.csv_create.return_value = {}

            result_path = os.path.join(DIR, example_validator_message["uuid"])
            preprocess_path = os.path.join(result_path, "preprocess")
            classifications_path = os.path.join(result_path, "classifications")
            pipeline_info_path = os.path.join(result_path, "pipeline_info")
            binned_reads_path = os.path.join(result_path, "reads_by_taxa")
            read_fraction_path = os.path.join(result_path, "read_fractions")

            os.makedirs(preprocess_path, exist_ok=True)
            os.makedirs(classifications_path, exist_ok=True)
            os.makedirs(pipeline_info_path, exist_ok=True)
            os.makedirs(binned_reads_path, exist_ok=True)
            os.makedirs(read_fraction_path, exist_ok=True)

            open(os.path.join(read_fraction_path, "dehumanised.fastq.gz"), "w").close()
            open(os.path.join(read_fraction_path, "viral.fastq.gz"), "w").close()
            open(os.path.join(read_fraction_path, "unclassified.fastq.gz"), "w").close()
            open(
                os.path.join(read_fraction_path, "viral_and_unclassified.fastq.gz"), "w"
            ).close()

            open(
                os.path.join(
                    preprocess_path,
                    f"{example_validator_message['uuid']}.fastp.fastq.gz",
                ),
                "w",
            ).close()
            open(
                os.path.join(classifications_path, "PlusPF.kraken_report.txt"), "w"
            ).close()
            open(
                os.path.join(
                    result_path, f"{example_validator_message['uuid']}_report.html"
                ),
                "w",
            ).close()

            with open(
                os.path.join(
                    pipeline_info_path,
                    f"execution_trace_{example_validator_message['uuid']}.txt",
                ),
                "w",
            ) as f:
                f.write(example_execution_trace_human)

            with open(
                os.path.join(
                    pipeline_info_path,
                    f"params_{example_validator_message['uuid']}.log",
                ),
                "w",
            ) as f:
                f.write(json.dumps(example_params))

            with open(
                os.path.join(classifications_path, "PlusPF.kraken_report.json"), "w"
            ) as f:
                f.write(json.dumps(example_k2_out))

            with open(
                os.path.join(binned_reads_path, "reads_summary_combined.json"), "w"
            ) as f:
                json.dump(example_reads_summary, f)

            args = SimpleNamespace(
                logfile=MSCAPE_VALIDATION_LOG_FILENAME,
                log_level="DEBUG",
                nxf_executable="test",
                config="test",
                k2_host="test",
                result_dir=DIR,
                n_workers=2,
            )

            pipeline = utils.pipeline(
                pipe="test",
                nxf_executable="test",
                config="test",
            )

            test_message = copy.deepcopy(example_validator_message)

            in_message = SimpleNamespace(body=json.dumps(test_message))

            Success, alert, payload, message = mscape_ingest_validation.validate(
                in_message, args, pipeline
            )

            self.assertFalse(Success)
            self.assertFalse(alert)

            self.assertIn(
                "Human reads detected above rejection threshold, please ensure pre-upload dehumanisation has been performed properly",
                payload["ingest_errors"],
            )

            self.assertFalse(payload["created"])
            self.assertFalse(payload["ingested"])
            self.assertFalse(payload["onyx_create_status"])
            self.assertFalse(payload["climb_id"])

            published_reads_contents = self.s3_client.list_objects(
                Bucket="mscape-published-reads"
            )
            print(published_reads_contents)
            self.assertNotIn("Contents", published_reads_contents.keys())

            published_reports_contents = self.s3_client.list_objects(
                Bucket="mscape-published-reports"
            )
            print(published_reports_contents)
            self.assertNotIn("Contents", published_reports_contents.keys())

            published_taxon_reports_contents = self.s3_client.list_objects(
                Bucket="mscape-published-taxon-reports"
            )
            print(published_taxon_reports_contents)
            self.assertNotIn("Contents", published_taxon_reports_contents.keys())

            published_binned_reads_contents = self.s3_client.list_objects(
                Bucket="mscape-published-binned-reads"
            )
            print(published_binned_reads_contents)
            self.assertNotIn("Contents", published_binned_reads_contents.keys())

    def test_successful_test(self):
        with (
            patch("roz_scripts.utils.utils.pipeline") as mock_pipeline,
            patch("roz_scripts.utils.utils.OnyxClient") as mock_client,
        ):
            test_message = copy.deepcopy(example_test_validator_message)

            test_message["uuid"] = "test_successful_test"

            mock_pipeline.return_value.execute.return_value = (
                0,
                "test_stdout",
                "test_stderr",
            )

            mock_pipeline.return_value.cleanup.return_value = (
                0,
                "test_stdout",
                "test_stderr",
            )
            mock_pipeline.return_value.cmd.return_value = "Hello pytest :)"

            mock_client.return_value.__enter__.return_value.update.return_value = {}

            mock_client.return_value.__enter__.return_value.csv_create.return_value = {
                "data": {"climb_id": "test_climb_id"}
            }

            mock_client.return_value.__enter__.return_value.filter.return_value = iter(
                ()
            )

            result_path = os.path.join(DIR, test_message["uuid"])
            preprocess_path = os.path.join(result_path, "preprocess")
            classifications_path = os.path.join(result_path, "classifications")
            pipeline_info_path = os.path.join(result_path, "pipeline_info")
            binned_reads_path = os.path.join(result_path, "reads_by_taxa")

            os.makedirs(preprocess_path, exist_ok=True)
            os.makedirs(classifications_path, exist_ok=True)
            os.makedirs(pipeline_info_path, exist_ok=True)
            os.makedirs(binned_reads_path, exist_ok=True)

            open(
                os.path.join(
                    preprocess_path,
                    f"{test_message['uuid']}.fastp.fastq.gz",
                ),
                "w",
            ).close()
            open(
                os.path.join(classifications_path, "PlusPF.kraken_report.txt"), "w"
            ).close()
            open(
                os.path.join(result_path, f"{test_message['uuid']}_report.html"),
                "w",
            ).close()

            with open(
                os.path.join(
                    pipeline_info_path,
                    f"execution_trace_{test_message['uuid']}.txt",
                ),
                "w",
            ) as f:
                f.write(example_execution_trace)

            with open(
                os.path.join(binned_reads_path, "reads_summary_combined.json"), "w"
            ) as f:
                json.dump(example_reads_summary, f)

            args = SimpleNamespace(
                logfile=MSCAPE_VALIDATION_LOG_FILENAME,
                log_level="DEBUG",
                nxf_executable="test",
                config="test",
                k2_host="test",
                result_dir=DIR,
                n_workers=2,
            )

            pipeline = utils.pipeline(
                pipe="test",
                nxf_executable="test",
                config="test",
            )

            in_message = SimpleNamespace(body=json.dumps(test_message))

            Success, alert, payload, message = mscape_ingest_validation.validate(
                in_message, args, pipeline
            )

            print(payload)

            self.assertTrue(Success)
            self.assertFalse(alert)

            self.assertFalse(payload["created"])
            self.assertFalse(payload["ingested"])
            self.assertFalse(payload["onyx_create_status"])
            self.assertFalse(payload["climb_id"])
            self.assertTrue(payload["test_ingest_result"])
            self.assertFalse(payload["ingest_errors"])

            published_reads_contents = self.s3_client.list_objects(
                Bucket="mscape-published-reads"
            )
            print(published_reads_contents)
            self.assertNotIn("Contents", published_reads_contents.keys())

            published_reports_contents = self.s3_client.list_objects(
                Bucket="mscape-published-reports"
            )
            print(published_reports_contents)
            self.assertNotIn("Contents", published_reports_contents.keys())

            published_taxon_reports_contents = self.s3_client.list_objects(
                Bucket="mscape-published-taxon-reports"
            )
            print(published_taxon_reports_contents)
            self.assertNotIn("Contents", published_taxon_reports_contents.keys())

            published_binned_reads_contents = self.s3_client.list_objects(
                Bucket="mscape-published-binned-reads"
            )
            print(published_binned_reads_contents)
            self.assertNotIn("Contents", published_binned_reads_contents.keys())

    def test_onyx_fail(self):
        with (
            patch("roz_scripts.utils.utils.pipeline") as mock_pipeline,
            patch("roz_scripts.utils.utils.OnyxClient") as mock_client,
        ):
            mock_pipeline.return_value.execute.return_value = (
                0,
                "test_stdout",
                "test_stderr",
            )

            mock_pipeline.return_value.cleanup.return_value = (
                0,
                "test_stdout",
                "test_stderr",
            )
            mock_pipeline.return_value.cmd.return_value = "Hello pytest :)"

            mock_client.return_value.__enter__.return_value.csv_create = Mock(
                side_effect=OnyxRequestError(
                    message="test csv_create exception",
                    response=MockResponse(
                        status_code=400,
                        json_data={
                            "data": [],
                            "messages": {
                                "sample_id": ["Test sample_id error handling"]
                            },
                        },
                    ),
                )
            )

            mock_client.return_value.__enter__.return_value.filter = Mock(
                side_effect=[
                    iter(()),
                    iter(
                        ({"yeet": "yeet", "climb_id": "test_id", "is_published": True},)
                    ),
                    iter(
                        ({"yeet": "yeet", "climb_id": "test_id", "is_published": True},)
                    ),
                ]
            )

            mock_client.return_value.__enter__.return_value.identify.return_value = {
                "field": "sample_id",
                "value": "hidden-value",
                "identifier": "S-1234567890",
            }

            # mock_client.return_value.__enter__.return_value.filter.return_value = iter(
            #     ()
            # )

            result_path = os.path.join(DIR, example_validator_message["uuid"])
            preprocess_path = os.path.join(result_path, "preprocess")
            classifications_path = os.path.join(result_path, "classifications")
            pipeline_info_path = os.path.join(result_path, "pipeline_info")
            binned_reads_path = os.path.join(result_path, "reads_by_taxa")

            os.makedirs(preprocess_path, exist_ok=True)
            os.makedirs(classifications_path, exist_ok=True)
            os.makedirs(pipeline_info_path, exist_ok=True)
            os.makedirs(binned_reads_path, exist_ok=True)

            open(
                os.path.join(
                    preprocess_path,
                    f"{example_validator_message['uuid']}.fastp.fastq.gz",
                ),
                "w",
            ).close()
            open(
                os.path.join(classifications_path, "PlusPF.kraken_report.txt"), "w"
            ).close()
            open(
                os.path.join(
                    result_path, f"{example_validator_message['uuid']}_report.html"
                ),
                "w",
            ).close()

            with open(
                os.path.join(
                    pipeline_info_path,
                    f"execution_trace_{example_validator_message['uuid']}.txt",
                ),
                "w",
            ) as f:
                f.write(example_execution_trace)

            with open(
                os.path.join(binned_reads_path, "reads_summary_combined.json"), "w"
            ) as f:
                json.dump(example_reads_summary, f)

            args = SimpleNamespace(
                logfile=MSCAPE_VALIDATION_LOG_FILENAME,
                log_level="DEBUG",
                nxf_executable="test",
                nxf_config="test",
                k2_host="test",
                result_dir=DIR,
                n_workers=2,
            )

            pipeline = utils.pipeline(
                pipe="test",
                config="test",
                nxf_executable="test",
            )

            test_message = copy.deepcopy(example_validator_message)

            in_message = SimpleNamespace(body=json.dumps(test_message))

            Success, alert, payload, message = mscape_ingest_validation.validate(
                in_message, args, pipeline
            )

            print(payload)

            self.assertFalse(Success)
            self.assertFalse(alert)

            self.assertFalse(payload["created"])
            self.assertFalse(payload["ingested"])
            self.assertFalse(payload["onyx_create_status"])
            self.assertFalse(payload["climb_id"])
            self.assertFalse(payload["test_ingest_result"])

            self.assertIn(
                "Test sample_id error handling",
                payload["onyx_create_errors"]["sample_id"],
            )
            self.assertFalse(payload["onyx_create_status"])

            published_reads_contents = self.s3_client.list_objects(
                Bucket="mscape-published-reads"
            )
            self.assertNotIn("Contents", published_reads_contents.keys())

            published_reports_contents = self.s3_client.list_objects(
                Bucket="mscape-published-reports"
            )
            self.assertNotIn("Contents", published_reports_contents.keys())

            published_taxon_reports_contents = self.s3_client.list_objects(
                Bucket="mscape-published-taxon-reports"
            )
            self.assertNotIn("Contents", published_taxon_reports_contents.keys())

            published_binned_reads_contents = self.s3_client.list_objects(
                Bucket="mscape-published-binned-reads"
            )
            self.assertNotIn("Contents", published_binned_reads_contents.keys())


# class Test_pathsafe_validator(unittest.TestCase):
#     def setUp(self):
#         self.server = ThreadedMotoServer()
#         self.server.start()

#         os.environ["AWS_ACCESS_KEY_ID"] = "testing"
#         os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
#         os.environ["AWS_SECURITY_TOKEN"] = "testing"
#         os.environ["AWS_SESSION_TOKEN"] = "testing"
#         os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

#         os.environ["UNIT_TESTING"] = "True"

#         self.s3_client = boto3.client("s3", endpoint_url="http://localhost:5000")
#         self.s3_client.create_bucket(Bucket="pathsafetest-birm-illumina-prod")
#         self.s3_client.create_bucket(Bucket="pathsafetest-published-assembly")

#         with open(TEST_CSV_FILENAME, "w") as f:
#             f.write("sample_id,run_id,project,platform,site\n")
#             f.write("sample-test,run-test,pathsafetest,ont,birm")

#         self.s3_client.upload_file(
#             TEST_CSV_FILENAME,
#             "pathsafetest-birm-illumina-prod",
#             "pathsafetest.sample-test.run-test.csv",
#         )

#         resp = self.s3_client.head_object(
#             Bucket="pathsafetest-birm-illumina-prod",
#             Key="pathsafetest.sample-test.run-test.csv",
#         )

#         self.log = utils.init_logger(
#             "pathsafe.validate", PATHSAFE_VALIDATION_LOG_FILENAME, "DEBUG"
#         )

#         csv_etag = resp["ETag"].replace('"', "")

#         example_pathsafe_validator_message["files"][".csv"]["etag"] = csv_etag
#         example_pathsafe_test_validator_message["files"][".csv"]["etag"] = csv_etag

#         config = {
#             "version": "0.1",
#             "profiles": {
#                 "roz": {
#                     "username": "guest",
#                     "password": "guest",
#                     "amqp_url": "127.0.0.1",
#                     "port": 5672,
#                 }
#             },
#         }

#         with open(VARYS_CFG_PATH, "w") as f:
#             json.dump(config, f, ensure_ascii=False)

#         os.environ["VARYS_CFG"] = VARYS_CFG_PATH
#         os.environ["S3_MATCHER_LOG"] = ROZ_INGEST_LOG_FILENAME
#         os.environ["INGEST_LOG_LEVEL"] = "DEBUG"
#         os.environ["ROZ_CONFIG_JSON"] = "config/config.json"
#         os.environ["ONYX_ROZ_PASSWORD"] = "password"
#         os.environ["ROZ_INGEST_LOG"] = ROZ_INGEST_LOG_FILENAME

#         self.varys_client = varys("roz", TEST_MESSAGE_LOG_FILENAME)

#     def tearDown(self):
#         credentials = pika.PlainCredentials("guest", "guest")

#         connection = pika.BlockingConnection(
#             pika.ConnectionParameters("localhost", credentials=credentials)
#         )
#         channel = connection.channel()

#         channel.queue_delete(queue="inbound.to_validate.pathsafetest")
#         channel.queue_delete(queue="inbound.new_artifact.pathsafe")
#         channel.queue_delete(queue="inbound.results.pathsafe.birm")

#         connection.close()

#         os.remove(TEST_CSV_FILENAME)

#         self.server.stop()
#         self.varys_client.close()
#         time.sleep(1)

#     def test_validator_successful(self):
#         with (
#             patch("roz_scripts.pathsafe_validation.pipeline") as mock_pipeline,
#             patch("roz_scripts.pathsafe_validation.OnyxClient") as mock_local_client,
#             patch("roz_scripts.utils.utils.OnyxClient") as mock_util_client,
#             patch("roz_scripts.pathsafe_validation.requests.post") as mock_requests,
#         ):
#             mock_pipeline.return_value.execute.return_value = (
#                 0,
#                 "test_stdout",
#                 "test_stderr",
#             )

#             mock_pipeline.return_value.cleanup.return_value = (
#                 0,
#                 "test_stdout",
#                 "test_stderr",
#             )

#             mock_requests.return_value = MockResponse(
#                 status_code=201, json_data={"id": "test_pwid"}
#             )

#             mock_pipeline.return_value.cmd.return_value.__str__ = "Hello pytest :)"

#             mock_util_client.return_value.__enter__.return_value._update.return_value = MockResponse(
#                 status_code=200
#             )

#             mock_util_client.return_value.__enter__.return_value._csv_create.return_value.__next__.return_value = MockResponse(
#                 status_code=201, json_data={"data": {"climb_id": "test_climb_id"}}
#             )

#             mock_local_client.return_value.__enter__.return_value.get.return_value = {
#                 "hello": "goodbye"
#             }

#             result_path = os.path.join(DIR, example_pathsafe_validator_message["uuid"])
#             pipeline_info_path = os.path.join(result_path, "pipeline_info")
#             assembly_path = os.path.join(result_path, "assembly")

#             os.makedirs(assembly_path, exist_ok=True)
#             os.makedirs(pipeline_info_path, exist_ok=True)

#             open(
#                 os.path.join(
#                     assembly_path,
#                     f"{example_pathsafe_validator_message['uuid']}.result.fasta",
#                 ),
#                 "w",
#             ).close()

#             with open(
#                 os.path.join(
#                     pipeline_info_path,
#                     f"execution_trace_{example_pathsafe_validator_message['uuid']}.txt",
#                 ),
#                 "w",
#             ) as f:
#                 f.write(example_execution_trace)

#             args = SimpleNamespace(
#                 logfile=PATHSAFE_VALIDATION_LOG_FILENAME,
#                 log_level="DEBUG",
#                 nxf_executable="test",
#                 config="test",
#                 k2_host="test",
#                 result_dir=DIR,
#                 n_workers=2,
#             )

#             pipeline = pathsafe_validation.pipeline(
#                 pipe="test",
#                 config="test",
#                 nxf_executable="test",
#                 config="test",
#                 k2_host="test",
#                 result_dir=DIR,
#                 n_workers=2,
#             )

#             in_message = SimpleNamespace(
#                 body=json.dumps(example_pathsafe_validator_message)
#             )

#             Success, payload, message = pathsafe_validation.validate(
#                 in_message, args, pipeline
#             )

#             self.assertTrue(Success)

#             self.assertTrue(uuid.UUID(payload["uuid"], version=4))
#             self.assertEqual(
#                 payload["artifact"],
#                 "pathsafetest.sample-test.run-test",
#             )
#             self.assertEqual(payload["project"], "pathsafetest")
#             self.assertEqual(payload["site"], "birm")
#             self.assertEqual(payload["platform"], "illumina")
#             self.assertEqual(payload["climb_id"], "test_climb_id")
#             self.assertEqual(payload["created"], True)
#             self.assertEqual(payload["ingested"], True)
#             self.assertEqual(payload["onyx_test_status_code"], 201)
#             self.assertEqual(payload["onyx_test_create_status"], True)
#             self.assertEqual(payload["onyx_status_code"], 201)
#             self.assertEqual(payload["onyx_create_status"], True)
#             self.assertEqual(payload["test_flag"], False)
#             self.assertEqual(payload["ingest_errors"], [])

#             published_reads_contents = self.s3_client.list_objects(
#                 Bucket="pathsafetest-published-assembly"
#             )
#             self.assertEqual(
#                 published_reads_contents["Contents"][0]["Key"],
#                 "test_climb_id.assembly.fasta",
#             )

#             resp = requests.get(payload["assembly_presigned_url"])

#             self.assertTrue(resp.ok)

#     def test_successful_test(self):
#         with (
#             patch("roz_scripts.pathsafe_validation.pipeline") as mock_pipeline,
#             patch("roz_scripts.pathsafe_validation.OnyxClient") as mock_local_client,
#             patch("roz_scripts.utils.utils.OnyxClient") as mock_util_client,
#             patch("roz_scripts.pathsafe_validation.requests.post") as mock_requests,
#         ):
#             mock_pipeline.return_value.execute.return_value = (
#                 0,
#                 "test_stdout",
#                 "test_stderr",
#             )

#             mock_pipeline.return_value.cleanup.return_value = (
#                 0,
#                 "test_stdout",
#                 "test_stderr",
#             )

#             mock_requests.return_value = MockResponse(
#                 status_code=201, json_data={"id": "test_pwid"}
#             )

#             mock_pipeline.return_value.cmd.return_value.__str__.return_value = (
#                 "Hello pytest :)"
#             )

#             mock_util_client.return_value.__enter__.return_value._update.return_value = MockResponse(
#                 status_code=200
#             )

#             mock_util_client.return_value.__enter__.return_value._csv_create.return_value.__next__.return_value = MockResponse(
#                 status_code=201, json_data={"data": {"climb_id": "test_climb_id"}}
#             )

#             mock_local_client.return_value.__enter__.return_value.get.return_value = {
#                 "hello": "goodbye"
#             }

#             result_path = os.path.join(
#                 DIR, example_pathsafe_test_validator_message["uuid"]
#             )
#             pipeline_info_path = os.path.join(result_path, "pipeline_info")
#             assembly_path = os.path.join(result_path, "assembly")

#             os.makedirs(assembly_path, exist_ok=True)
#             os.makedirs(pipeline_info_path, exist_ok=True)

#             open(
#                 os.path.join(
#                     assembly_path,
#                     f"{example_pathsafe_test_validator_message['uuid']}.result.fasta",
#                 ),
#                 "w",
#             ).close()

#             with open(
#                 os.path.join(
#                     pipeline_info_path,
#                     f"execution_trace_{example_pathsafe_test_validator_message['uuid']}.txt",
#                 ),
#                 "w",
#             ) as f:
#                 f.write(example_execution_trace)

#             args = SimpleNamespace(
#                 logfile=PATHSAFE_VALIDATION_LOG_FILENAME,
#                 log_level="DEBUG",
#                 nxf_executable="test",
#                 config="test",
#                 k2_host="test",
#                 result_dir=DIR,
#                 n_workers=2,
#             )

#             pipeline = pathsafe_validation.pipeline(
#                 pipe="test",
#                 config="test",
#                 nxf_executable="test",
#                 config="test",
#                 k2_host="test",
#                 result_dir=DIR,
#                 n_workers=2,
#             )

#             in_message = SimpleNamespace(
#                 body=json.dumps(example_pathsafe_test_validator_message)
#             )

#             Success, payload, message = pathsafe_validation.validate(
#                 in_message, args, pipeline
#             )

#             self.assertFalse(Success)

#             self.assertFalse(payload["created"])
#             self.assertFalse(payload["ingested"])
#             self.assertFalse(payload["onyx_create_status"])
#             self.assertFalse(payload["climb_id"])
#             self.assertTrue(payload["test_ingest_result"])
#             self.assertFalse(payload["ingest_errors"])

#             published_reads_contents = self.s3_client.list_objects(
#                 Bucket="pathsafetest-published-assembly"
#             )
#             self.assertNotIn("Contents", published_reads_contents.keys())
#             self.assertNotIn("assembly_presigned_url", payload.keys())

#     def test_onyx_fail(self):
#         with (
#             patch("roz_scripts.pathsafe_validation.pipeline") as mock_pipeline,
#             patch("roz_scripts.pathsafe_validation.OnyxClient") as mock_local_client,
#             patch("roz_scripts.utils.utils.OnyxClient") as mock_util_client,
#             patch("roz_scripts.pathsafe_validation.requests.post") as mock_requests,
#         ):
#             mock_pipeline.return_value.execute.return_value = (
#                 0,
#                 "test_stdout",
#                 "test_stderr",
#             )

#             mock_pipeline.return_value.cleanup.return_value = (
#                 0,
#                 "test_stdout",
#                 "test_stderr",
#             )

#             mock_requests.return_value = MockResponse(
#                 status_code=201, json_data={"id": "test_pwid"}
#             )
#             mock_pipeline.return_value.cmd.return_value.__str__ = "Hello pytest :)"

#             mock_util_client.return_value.__enter__.return_value._update.return_value = MockResponse(
#                 status_code=400
#             )

#             mock_local_client.return_value.__enter__.return_value.get.return_value = {
#                 "hello": "goodbye"
#             }

#             mock_util_client.return_value.__enter__.return_value._csv_create.return_value.__next__.return_value = MockResponse(
#                 status_code=400,
#                 json_data={
#                     "data": [],
#                     "messages": {"sample_id": "Test sample_id error handling"},
#                 },
#                 ok=False,
#             )

#             result_path = os.path.join(DIR, example_pathsafe_validator_message["uuid"])
#             pipeline_info_path = os.path.join(result_path, "pipeline_info")
#             assembly_path = os.path.join(result_path, "assembly")

#             os.makedirs(assembly_path, exist_ok=True)
#             os.makedirs(pipeline_info_path, exist_ok=True)

#             open(
#                 os.path.join(
#                     assembly_path,
#                     f"{example_pathsafe_validator_message['uuid']}.result.fasta",
#                 ),
#                 "w",
#             ).close()

#             with open(
#                 os.path.join(
#                     pipeline_info_path,
#                     f"execution_trace_{example_pathsafe_validator_message['uuid']}.txt",
#                 ),
#                 "w",
#             ) as f:
#                 f.write(example_execution_trace)

#             args = SimpleNamespace(
#                 logfile=PATHSAFE_VALIDATION_LOG_FILENAME,
#                 log_level="DEBUG",
#                 nxf_executable="test",
#                 nxf_config="test",
#                 k2_host="test",
#                 result_dir=DIR,
#                 n_workers=2,
#             )

#             pipeline = pathsafe_validation.pipeline(
#                 pipe="test",
#                 nxf_executable="test",
#                 config="test",
#                 k2_host="test",
#                 result_dir=DIR,
#                 n_workers=2,
#             )

#             in_message = SimpleNamespace(
#                 body=json.dumps(example_pathsafe_validator_message)
#             )

#             Success, payload, message = pathsafe_validation.validate(
#                 in_message, args, pipeline
#             )

#             self.assertFalse(Success)

#             self.assertFalse(payload["created"])
#             self.assertFalse(payload["ingested"])
#             self.assertFalse(payload["onyx_create_status"])
#             self.assertFalse(payload["climb_id"])
#             self.assertFalse(payload["test_ingest_result"])

#             self.assertIn(
#                 "Test sample_id error handling",
#                 payload["onyx_errors"]["sample_id"],
#             )
#             self.assertFalse(payload["onyx_create_status"])
#             self.assertEqual(payload["onyx_status_code"], 400)

#             published_reads_contents = self.s3_client.list_objects(
#                 Bucket="pathsafetest-published-assembly"
#             )
#             self.assertNotIn("Contents", published_reads_contents.keys())
#             self.assertNotIn("assembly_presigned_url", payload.keys())
