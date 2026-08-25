import argparse
import csv
import json
import os
import sys
import tempfile
import time
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, call, mock_open, patch

from onyx.exceptions import (
    OnyxClientError,
    OnyxConfigError,
    OnyxConnectionError,
    OnyxRequestError,
    OnyxServerError,
)

from roz_scripts.mscape.chimera_runner import (
    chimera_worker_pool_handler,
    create_samplesheet,
    handle_alignment_report,
    handle_sylph_report,
    onyx_get_metadata,
    process_record,
    push_bam_file,
    push_chimera_report,
    ret_0_parser,
    run,
)
from roz_scripts.utils.utils import PodResources


def setUpModule():
    os.environ.update({
        "ONYX_DOMAIN": "https://test.onyx",
        "ONYX_TOKEN": "testtoken",
        "VARYS_CFG": "/tmp/varys.cfg",
        "AWS_ACCESS_KEY_ID": "test-key-id",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "NXF_WORK": "/tmp/nxf_work",
        "NXF_HOME": "/tmp/nxf_home",
    })

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TRACE_HEADER = "task_id\thash\tnative_id\tname\tstatus\texit\tsubmit\tduration\trealtime\t%cpu\tpeak_rss\tpeak_vmem\trchar\twchar\n"


def make_trace_row(process, tag, exit_code):
    return f"1\tabc123\t-\t{process} {tag}\tCOMPLETED\t{exit_code}\t-\t-\t-\t-\t-\t-\t-\t-\n"


def write_trace_file(path, rows):
    with open(path, "w") as fh:
        fh.write(TRACE_HEADER)
        for row in rows:
            fh.write(row)


def make_payload(uuid="test-uuid-1234", climb_id="CLIMB001", match_uuid="match-uuid-5678"):
    return {
        "uuid": uuid,
        "climb_id": climb_id,
        "match_uuid": match_uuid,
        "project": "mscape",
    }


def make_message(payload=None):
    if payload is None:
        payload = make_payload()
    msg = MagicMock()
    msg.body = json.dumps(payload)
    return msg


def make_config():
    return {
        "configs": {
            "mscape": {
                "project_buckets": {
                    "chimera_bams": {"name_layout": "{project}-fake-chimera-bams"},
                    "chimera_reports": {"name_layout": "{project}-fake-chimera-reports"},
                }
            }
        }
    }


def make_args(**kwargs):
    defaults = dict(
        project="mscape",
        outdir=Path("/tmp/chimera_out"),
        chimera_release="main",
        nxf_config="/tmp/nxf.config",
        nxf_image="quay.io/climb-tre/nextflow:25.04.8",
        mm2_index=Path("/tmp/mm2.idx"),
        bwa_index_prefix=Path("/tmp/bwa"),
        sylph_db=Path("/tmp/sylph.db"),
        sylph_taxdb=Path("/tmp/sylph.tax"),
        database_metadata=Path("/tmp/meta.tsv"),
        alignment_db_version="v1.0",
        sylph_db_version="v2.0",
        logfile=None,
        log_level="DEBUG",
        n_workers=3,
        chimera_timeout=3600,
        nxf_pod_resources=PodResources(),
        config=make_config(),
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def make_metadata(climb_id="CLIMB001"):
    return {
        "climb_id": climb_id,
        "platform": "ont",
        "human_filtered_reads_1": "s3://bucket/reads.fastq.gz",
        "human_filtered_reads_2": "s3://bucket/reads2.fastq.gz",
    }


SYLPH_TSV_HEADER = "\t".join([
    "tax_id", "human_readable", "taxon_string", "contig_id", "Contig_name",
    "Taxonomic_abundance", "Sequence_abundance", "Adjusted_ANI", "ANI_5-95_percentile",
    "Eff_cov", "Lambda_5-95_percentile", "Median_cov", "Mean_cov_geq1",
    "Containment_ind", "Naive_ANI", "kmers_reassigned",
])

SYLPH_TSV_ROW = "\t".join([
    "1234", "Homo sapiens", "k__Bacteria", "GCF_001", "contig_1",
    "0.5", "0.4", "99.5", "99.0-100.0",
    "10.0", "9.5-10.5", "5.0", "6.0",
    "0.9", "99.2", "100",
])


# ---------------------------------------------------------------------------
# onyx_get_metadata
# ---------------------------------------------------------------------------

class TestOnyxGetMetadata(unittest.TestCase):
    def setUp(self):
        os.environ["ONYX_DOMAIN"] = "https://test.onyx"
        os.environ["ONYX_TOKEN"] = "testtoken"
        self.args = make_args()
        self.log = MagicMock()

    @patch("roz_scripts.mscape.chimera_runner.OnyxClient")
    @patch("roz_scripts.mscape.chimera_runner.OnyxConfig")
    def test_success_returns_record(self, mock_config, mock_client_cls):
        expected_record = {"climb_id": "CLIMB001", "platform": "ont"}
        mock_client = MagicMock()
        mock_client.get.return_value = expected_record
        mock_client_cls.return_value.__enter__.return_value = mock_client

        result = onyx_get_metadata(self.args, "CLIMB001", self.log)

        self.assertEqual(result, expected_record)
        mock_client.get.assert_called_once_with(project="mscape", climb_id="CLIMB001")

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.OnyxClient")
    @patch("roz_scripts.mscape.chimera_runner.OnyxConfig")
    def test_connection_error_retries_then_succeeds(self, mock_config, mock_client_cls, mock_sleep):
        expected_record = {"climb_id": "CLIMB001"}
        mock_client = MagicMock()
        mock_client.get.side_effect = [
            OnyxConnectionError("timeout"),
            OnyxConnectionError("timeout"),
            expected_record,
        ]
        mock_client_cls.return_value.__enter__.return_value = mock_client

        result = onyx_get_metadata(self.args, "CLIMB001", self.log)

        self.assertEqual(result, expected_record)
        self.assertEqual(mock_client.get.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.OnyxClient")
    @patch("roz_scripts.mscape.chimera_runner.OnyxConfig")
    def test_connection_error_max_retries_raises(self, mock_config, mock_client_cls, mock_sleep):
        mock_client = MagicMock()
        mock_client.get.side_effect = OnyxConnectionError("persistent failure")
        mock_client_cls.return_value.__enter__.return_value = mock_client

        with self.assertRaises(OnyxConnectionError):
            onyx_get_metadata(self.args, "CLIMB001", self.log)

        self.assertEqual(mock_client.get.call_count, 4)  # 3 retries + initial attempt

    @patch("roz_scripts.mscape.chimera_runner.OnyxClient")
    @patch("roz_scripts.mscape.chimera_runner.OnyxConfig")
    def test_server_error_raises_immediately(self, mock_config, mock_client_cls):
        mock_client = MagicMock()
        mock_client.get.side_effect = OnyxServerError("500", MagicMock())
        mock_client_cls.return_value.__enter__.return_value = mock_client

        with self.assertRaises(OnyxServerError):
            onyx_get_metadata(self.args, "CLIMB001", self.log)

        mock_client.get.assert_called_once()

    @patch("roz_scripts.mscape.chimera_runner.OnyxClient")
    @patch("roz_scripts.mscape.chimera_runner.OnyxConfig")
    def test_client_error_raises(self, mock_config, mock_client_cls):
        mock_client = MagicMock()
        mock_client.get.side_effect = OnyxClientError("bad request")
        mock_client_cls.return_value.__enter__.return_value = mock_client

        with self.assertRaises(OnyxClientError):
            onyx_get_metadata(self.args, "CLIMB001", self.log)

    @patch("roz_scripts.mscape.chimera_runner.OnyxClient")
    @patch("roz_scripts.mscape.chimera_runner.OnyxConfig")
    def test_request_error_raises(self, mock_config, mock_client_cls):
        mock_client = MagicMock()
        mock_client.get.side_effect = OnyxRequestError("not found", MagicMock())
        mock_client_cls.return_value.__enter__.return_value = mock_client

        with self.assertRaises(OnyxRequestError):
            onyx_get_metadata(self.args, "CLIMB001", self.log)


# ---------------------------------------------------------------------------
# ret_0_parser
# ---------------------------------------------------------------------------

class TestRet0Parser(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.pipeline_info = os.path.join(self.tmpdir.name, "pipeline_info")
        os.makedirs(self.pipeline_info)
        self.log = MagicMock()
        self.payload = make_payload()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _write_trace(self, filename, rows):
        path = os.path.join(self.pipeline_info, filename)
        write_trace_file(path, rows)
        return path

    def test_all_processes_exit_0_returns_no_fail(self):
        self._write_trace(
            "execution_trace_2024-01-01.txt",
            [
                make_trace_row("PROCESS_A", "tag1", "0"),
                make_trace_row("PROCESS_B", "tag2", "0"),
            ],
        )

        ingest_fail, result_payload = ret_0_parser(
            log=self.log,
            payload=self.payload,
            result_path=self.tmpdir.name,
        )

        self.assertFalse(ingest_fail)
        self.assertEqual(result_payload, self.payload)

    def test_sylph_taxonomy_exit_2_adds_no_hits_info(self):
        self._write_trace(
            "execution_trace.txt",
            [
                make_trace_row("PROCESS_SYLPH_TAXONOMY", "tag1", "2"),
            ],
        )

        ingest_fail, result_payload = ret_0_parser(
            log=self.log,
            payload=self.payload,
            result_path=self.tmpdir.name,
        )

        self.assertFalse(ingest_fail)
        self.assertIn("chimera_info", result_payload)
        self.assertEqual(
            result_payload["chimera_info"]["SYLPH_TAXONOMY"]["status"], "no_hits"
        )

    def test_unexpected_non_zero_exit_raises(self):
        self._write_trace(
            "execution_trace.txt",
            [make_trace_row("PROCESS_ALIGN", "tag1", "1")],
        )

        with self.assertRaises(Exception):
            ret_0_parser(
                log=self.log,
                payload=self.payload,
                result_path=self.tmpdir.name,
            )

    def test_no_trace_file_raises(self):
        # No trace files written — should raise
        with self.assertRaises(Exception):
            ret_0_parser(
                log=self.log,
                payload=self.payload,
                result_path=self.tmpdir.name,
            )

    def test_uses_most_recent_trace_file(self):
        older = self._write_trace(
            "execution_trace_1.txt",
            [make_trace_row("PROCESS_A", "tag1", "1")],  # would fail
        )
        newer = self._write_trace(
            "execution_trace_2.txt",
            [make_trace_row("PROCESS_A", "tag1", "0")],  # succeeds
        )
        # Make newer file appear more recent
        os.utime(older, (time.time() - 100, time.time() - 100))
        os.utime(newer, (time.time(), time.time()))

        ingest_fail, _ = ret_0_parser(
            log=self.log,
            payload=self.payload,
            result_path=self.tmpdir.name,
        )

        self.assertFalse(ingest_fail)

    def test_ingest_fail_true_propagates(self):
        self._write_trace(
            "execution_trace.txt",
            [make_trace_row("PROCESS_A", "tag1", "0")],
        )

        ingest_fail, _ = ret_0_parser(
            log=self.log,
            payload=self.payload,
            result_path=self.tmpdir.name,
            ingest_fail=True,
        )

        self.assertTrue(ingest_fail)


# ---------------------------------------------------------------------------
# create_samplesheet
# ---------------------------------------------------------------------------

class TestCreateSamplesheet(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_creates_csv_with_correct_headers(self):
        out_path = Path(os.path.join(self.tmpdir.name, "samplesheet.csv"))
        create_samplesheet([], out_path)

        with open(out_path) as fh:
            reader = csv.DictReader(fh)
            self.assertEqual(reader.fieldnames, ["sample", "platform", "fastq_1", "fastq_2"])

    def test_writes_single_record_correctly(self):
        metadata = [
            {
                "climb_id": "CLIMB001",
                "platform": "ont",
                "human_filtered_reads_1": "s3://bucket/r1.fastq.gz",
                "human_filtered_reads_2": "s3://bucket/r2.fastq.gz",
            }
        ]
        out_path = Path(os.path.join(self.tmpdir.name, "samplesheet.csv"))
        create_samplesheet(metadata, out_path)

        with open(out_path) as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sample"], "CLIMB001")
        self.assertEqual(rows[0]["platform"], "ont")
        self.assertEqual(rows[0]["fastq_1"], "s3://bucket/r1.fastq.gz")
        self.assertEqual(rows[0]["fastq_2"], "s3://bucket/r2.fastq.gz")

    def test_writes_multiple_records(self):
        metadata = [
            {
                "climb_id": f"CLIMB00{i}",
                "platform": "illumina",
                "human_filtered_reads_1": f"s3://bucket/r{i}_1.fastq.gz",
                "human_filtered_reads_2": f"s3://bucket/r{i}_2.fastq.gz",
            }
            for i in range(3)
        ]
        out_path = Path(os.path.join(self.tmpdir.name, "samplesheet.csv"))
        create_samplesheet(metadata, out_path)

        with open(out_path) as fh:
            rows = list(csv.DictReader(fh))

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[2]["sample"], "CLIMB002")


# ---------------------------------------------------------------------------
# handle_alignment_report
# ---------------------------------------------------------------------------

class TestHandleAlignmentReport(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.log = MagicMock()
        self.payload = make_payload()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _write_alignment_tsv(self, filename, rows):
        path = os.path.join(self.tmpdir.name, filename)
        with open(path, "w") as fh:
            writer = csv.DictWriter(fh, fieldnames=["reference", "mapped_reads"], delimiter="\t")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return path

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    def test_success_clears_then_updates(self, mock_onyx_update):
        mock_onyx_update.return_value = (False, False, self.payload)
        path = self._write_alignment_tsv(
            "alignment_report.tsv",
            [{"reference": "ref1", "mapped_reads": "100"}],
        )

        result = handle_alignment_report(path, self.payload, self.log)

        self.assertTrue(result)
        # First call: clear old alignment_results
        clear_call = mock_onyx_update.call_args_list[0]
        self.assertIsNone(clear_call.kwargs["fields"])
        self.assertEqual(clear_call.kwargs["clear_fields"], ["alignment_results"])

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    def test_clear_failure_returns_false(self, mock_onyx_update):
        # clear step fails
        mock_onyx_update.return_value = (True, False, self.payload)
        path = self._write_alignment_tsv(
            "alignment_report.tsv",
            [{"reference": "ref1", "mapped_reads": "100"}],
        )

        result = handle_alignment_report(path, self.payload, self.log)

        self.assertFalse(result)
        mock_onyx_update.assert_called_once()  # only the clear call was made

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    def test_update_failure_returns_false(self, mock_onyx_update):
        # clear succeeds, update fails
        mock_onyx_update.side_effect = [
            (False, False, self.payload),  # clear
            (True, False, self.payload),   # update
        ]
        path = self._write_alignment_tsv(
            "alignment_report.tsv",
            [{"reference": "ref1", "mapped_reads": "100"}],
        )

        result = handle_alignment_report(path, self.payload, self.log)

        self.assertFalse(result)

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    def test_batches_large_input(self, mock_onyx_update):
        mock_onyx_update.return_value = (False, False, self.payload)
        rows = [{"reference": f"ref{i}", "mapped_reads": str(i)} for i in range(250)]
        path = self._write_alignment_tsv("alignment_report.tsv", rows)

        result = handle_alignment_report(path, self.payload, self.log)

        self.assertTrue(result)
        # clear + 3 batches of 100 (100 + 100 + 50)
        self.assertEqual(mock_onyx_update.call_count, 4)


# ---------------------------------------------------------------------------
# handle_sylph_report
# ---------------------------------------------------------------------------

class TestHandleSylphReport(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.log = MagicMock()
        self.payload = make_payload()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _write_sylph_tsv(self, filename, rows=None):
        if rows is None:
            rows = [SYLPH_TSV_ROW]
        path = os.path.join(self.tmpdir.name, filename)
        with open(path, "w") as fh:
            fh.write(SYLPH_TSV_HEADER + "\n")
            for row in rows:
                fh.write(row + "\n")
        return path

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    def test_success_clears_then_updates(self, mock_onyx_update):
        mock_onyx_update.return_value = (False, False, self.payload)
        path = self._write_sylph_tsv("sylph_report.tsv")

        result = handle_sylph_report(path, self.payload, self.log)

        self.assertTrue(result)
        clear_call = mock_onyx_update.call_args_list[0]
        self.assertIsNone(clear_call.kwargs["fields"])
        self.assertEqual(clear_call.kwargs["clear_fields"], ["sylph_results"])

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    def test_maps_fields_correctly(self, mock_onyx_update):
        mock_onyx_update.return_value = (False, False, self.payload)
        path = self._write_sylph_tsv("sylph_report.tsv")

        handle_sylph_report(path, self.payload, self.log)

        # Second call is the batch update with mapped rows
        update_call = mock_onyx_update.call_args_list[1]
        batch = update_call.kwargs["fields"]["sylph_results"]
        self.assertEqual(len(batch), 1)
        row = batch[0]
        self.assertEqual(row["taxon_id"], "1234")
        self.assertEqual(row["human_readable"], "Homo sapiens")
        self.assertEqual(row["gtdb_taxon_string"], "k__Bacteria")
        self.assertEqual(row["adjusted_ani"], "99.5")

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    def test_clear_failure_returns_false(self, mock_onyx_update):
        mock_onyx_update.return_value = (True, False, self.payload)
        path = self._write_sylph_tsv("sylph_report.tsv")

        result = handle_sylph_report(path, self.payload, self.log)

        self.assertFalse(result)
        mock_onyx_update.assert_called_once()

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    def test_update_failure_returns_false(self, mock_onyx_update):
        mock_onyx_update.side_effect = [
            (False, False, self.payload),  # clear
            (True, False, self.payload),   # update
        ]
        path = self._write_sylph_tsv("sylph_report.tsv")

        result = handle_sylph_report(path, self.payload, self.log)

        self.assertFalse(result)


# ---------------------------------------------------------------------------
# push_bam_file
# ---------------------------------------------------------------------------

class TestPushBamFile(unittest.TestCase):
    def setUp(self):
        self.log = MagicMock()
        self.payload = make_payload()

    @patch("roz_scripts.mscape.chimera_runner.boto3.client")
    def test_success_returns_correct_s3_uri(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        uri = push_bam_file("/tmp/CLIMB001.bam", self.payload, self.log, make_config())

        self.assertEqual(uri, "s3://mscape-fake-chimera-bams/CLIMB001.chimera.bam")
        mock_s3.upload_file.assert_called_once_with(
            "/tmp/CLIMB001.bam",
            "mscape-fake-chimera-bams",
            "CLIMB001.chimera.bam",
        )

    @patch("roz_scripts.mscape.chimera_runner.boto3.client")
    def test_upload_failure_raises(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = Exception("S3 upload failed")
        mock_boto_client.return_value = mock_s3

        with self.assertRaises(Exception, msg="S3 upload failed"):
            push_bam_file("/tmp/CLIMB001.bam", self.payload, self.log, make_config())

    @patch("roz_scripts.mscape.chimera_runner.boto3.client")
    def test_uses_climb_id_in_key(self, mock_boto_client):
        payload = make_payload(climb_id="CLIMB999")
        mock_boto_client.return_value = MagicMock()

        uri = push_bam_file("/tmp/CLIMB999.bam", payload, self.log, make_config())

        self.assertIn("CLIMB999", uri)


# ---------------------------------------------------------------------------
# push_chimera_report
# ---------------------------------------------------------------------------

class TestPushChimeraReport(unittest.TestCase):
    def setUp(self):
        self.log = MagicMock()
        self.payload = make_payload()

    @patch("roz_scripts.mscape.chimera_runner.boto3.client")
    def test_success_uploads_canonical_and_versioned_copies(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        uri = push_chimera_report(
            "/tmp/CLIMB001.alignment_report.tsv",
            "alignment_report.tsv",
            "v1.0",
            self.payload,
            self.log,
            make_config(),
        )

        self.assertEqual(
            uri, "s3://mscape-fake-chimera-reports/CLIMB001.alignment_report.tsv"
        )
        mock_s3.upload_file.assert_any_call(
            "/tmp/CLIMB001.alignment_report.tsv",
            "mscape-fake-chimera-reports",
            "CLIMB001.alignment_report.tsv",
        )
        mock_s3.upload_file.assert_any_call(
            "/tmp/CLIMB001.alignment_report.tsv",
            "mscape-fake-chimera-reports",
            "CLIMB001.v1.0.alignment_report.tsv",
        )
        self.assertEqual(mock_s3.upload_file.call_count, 2)

    @patch("roz_scripts.mscape.chimera_runner.boto3.client")
    def test_no_db_version_raises_and_uploads_nothing(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3

        with self.assertRaises(ValueError):
            push_chimera_report(
                "/tmp/CLIMB001.sylph_taxonomy_report.tsv",
                "sylph_taxonomy_report.tsv",
                None,
                self.payload,
                self.log,
                make_config(),
            )

        mock_s3.upload_file.assert_not_called()

    @patch("roz_scripts.mscape.chimera_runner.boto3.client")
    def test_upload_failure_raises(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = Exception("S3 upload failed")
        mock_boto_client.return_value = mock_s3

        with self.assertRaises(Exception, msg="S3 upload failed"):
            push_chimera_report(
                "/tmp/CLIMB001.alignment_report.tsv",
                "alignment_report.tsv",
                "v1.0",
                self.payload,
                self.log,
                make_config(),
            )


# ---------------------------------------------------------------------------
# process_record — pool worker (replaces the old inline pipeline block)
# ---------------------------------------------------------------------------

class TestProcessRecord(unittest.TestCase):
    """process_record() is the pool worker that runs in a separate process.
    It must never touch varys directly (ack/nack/publish) - it only ever
    returns a result tuple for the handler's callback to act on, since a
    worker process can be killed independently of the main process."""

    def setUp(self):
        chmod_patcher = patch("pathlib.Path.chmod")
        self.mock_chmod = chmod_patcher.start()
        self.addCleanup(chmod_patcher.stop)

        mkdir_patcher = patch("pathlib.Path.mkdir")
        self.mock_mkdir = mkdir_patcher.start()
        self.addCleanup(mkdir_patcher.stop)

    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_metadata_failure_returns_failure_result(self, mock_get_metadata, mock_heartbeat_cls):
        mock_get_metadata.return_value = False
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()

        success, timed_out, result_payload, match_uuid, is_rerun, out_msg = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertFalse(success)
        self.assertFalse(timed_out)
        self.assertEqual(match_uuid, payload["match_uuid"])
        self.assertIs(out_msg, msg)
        pipe.execute.assert_not_called()

    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_pipeline_nonzero_rc_returns_failure(
        self, mock_get_metadata, mock_create_ss, mock_heartbeat_cls
    ):
        mock_get_metadata.return_value = make_metadata()
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 1

        success, timed_out, *_ = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertFalse(success)
        self.assertFalse(timed_out)

    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_chimera_timeout_arg_is_passed_to_pipeline_execute(
        self, mock_get_metadata, mock_create_ss, mock_heartbeat_cls
    ):
        mock_get_metadata.return_value = make_metadata()
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 0

        process_record(
            message=msg,
            args=make_args(chimera_timeout=1800),
            chimera_pipe=pipe,
            namespace="ns",
        )

        _, kwargs = pipe.execute.call_args
        self.assertEqual(kwargs["timeout"], 1800)

    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_pipeline_timeout_rc_124_marks_timed_out(
        self, mock_get_metadata, mock_create_ss, mock_heartbeat_cls
    ):
        """rc 124 is pipeline.execute()'s signal for a hard k8s-job timeout -
        this must be distinguished from other failures so the handler can
        alert specifically on repeated timeouts."""
        mock_get_metadata.return_value = make_metadata()
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 124

        success, timed_out, *_ = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertFalse(success)
        self.assertTrue(timed_out)

    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_ret_0_parser_ingest_fail_returns_failure(
        self, mock_get_metadata, mock_create_ss, mock_ret_0, mock_heartbeat_cls
    ):
        mock_get_metadata.return_value = make_metadata()
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 0
        mock_ret_0.return_value = (True, payload)

        success, timed_out, *_ = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertFalse(success)
        self.assertFalse(timed_out)

    @patch("os.path.exists")
    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_alignment_report_missing_returns_failure(
        self, mock_get_metadata, mock_create_ss, mock_ret_0, mock_heartbeat_cls, mock_exists
    ):
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 0
        mock_get_metadata.return_value = make_metadata()
        mock_ret_0.return_value = (False, payload)
        mock_exists.return_value = False

        success, timed_out, *_ = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertFalse(success)

    @patch("roz_scripts.mscape.chimera_runner.push_chimera_report")
    @patch("os.path.exists")
    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.handle_alignment_report")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_sylph_report_missing_without_chimera_info_returns_failure(
        self, mock_get_metadata, mock_create_ss, mock_ret_0, mock_handle_align,
        mock_heartbeat_cls, mock_exists, mock_push_report
    ):
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 0
        mock_get_metadata.return_value = make_metadata()
        mock_ret_0.return_value = (False, payload)
        mock_handle_align.return_value = True
        mock_exists.side_effect = lambda p: "alignment_report" in str(p)

        success, timed_out, *_ = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertFalse(success)

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    @patch("roz_scripts.mscape.chimera_runner.push_bam_file")
    @patch("roz_scripts.mscape.chimera_runner.push_chimera_report")
    @patch("os.path.exists")
    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.handle_alignment_report")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_sylph_report_missing_with_no_hits_does_not_fail(
        self, mock_get_metadata, mock_create_ss, mock_ret_0, mock_handle_align,
        mock_heartbeat_cls, mock_exists, mock_push_report, mock_push_bam, mock_onyx_update
    ):
        """When SYLPH_TAXONOMY has no_hits status the missing sylph report is expected."""
        payload = make_payload()
        payload_with_no_hits = {
            **payload,
            "chimera_info": {"SYLPH_TAXONOMY": {"status": "no_hits"}},
        }
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 0
        mock_get_metadata.return_value = make_metadata()
        mock_ret_0.return_value = (False, payload_with_no_hits)
        mock_handle_align.return_value = True
        mock_exists.side_effect = lambda p: "sylph_taxonomy" not in str(p)
        mock_push_bam.return_value = "s3://mscape-chimera-bams/CLIMB001.chimera.bam"
        mock_onyx_update.return_value = (False, False, payload_with_no_hits)

        success, timed_out, *_ = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertTrue(success)

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    @patch("roz_scripts.mscape.chimera_runner.push_bam_file")
    @patch("roz_scripts.mscape.chimera_runner.push_chimera_report")
    @patch("os.path.exists")
    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.handle_sylph_report")
    @patch("roz_scripts.mscape.chimera_runner.handle_alignment_report")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_full_success_returns_success_result(
        self, mock_get_metadata, mock_create_ss, mock_ret_0, mock_handle_align, mock_handle_sylph,
        mock_heartbeat_cls, mock_exists, mock_push_report, mock_push_bam, mock_onyx_update
    ):
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 0
        mock_get_metadata.return_value = make_metadata()
        mock_ret_0.return_value = (False, payload)
        mock_exists.return_value = True
        mock_handle_align.return_value = True
        mock_handle_sylph.return_value = True
        mock_push_bam.return_value = "s3://mscape-chimera-bams/CLIMB001.chimera.bam"
        mock_onyx_update.return_value = (False, False, payload)

        success, timed_out, result_payload, match_uuid, is_rerun, out_msg = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns", is_rerun=True
        )

        self.assertTrue(success)
        self.assertFalse(timed_out)
        self.assertEqual(match_uuid, payload["match_uuid"])
        self.assertTrue(is_rerun)
        self.assertIs(out_msg, msg)

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    @patch("roz_scripts.mscape.chimera_runner.push_bam_file")
    @patch("roz_scripts.mscape.chimera_runner.push_chimera_report")
    @patch("os.path.exists")
    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.handle_sylph_report")
    @patch("roz_scripts.mscape.chimera_runner.handle_alignment_report")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_onyx_update_failure_after_bam_upload_returns_failure(
        self, mock_get_metadata, mock_create_ss, mock_ret_0, mock_handle_align, mock_handle_sylph,
        mock_heartbeat_cls, mock_exists, mock_push_report, mock_push_bam, mock_onyx_update
    ):
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()
        pipe.execute.return_value = 0
        mock_get_metadata.return_value = make_metadata()
        mock_ret_0.return_value = (False, payload)
        mock_exists.return_value = True
        mock_handle_align.return_value = True
        mock_handle_sylph.return_value = True
        mock_push_bam.return_value = "s3://mscape-chimera-bams/CLIMB001.chimera.bam"
        mock_onyx_update.return_value = (True, False, payload)  # update fails

        success, timed_out, *_ = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertFalse(success)

    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_unhandled_exception_returns_failure_not_raised(
        self, mock_get_metadata, mock_heartbeat_cls
    ):
        """A worker process must never propagate an exception out of
        process_record() - apply_async's error_callback path throws away
        the message reference in mp.Pool, so an escaped exception here would
        leave a message neither acked nor nacked."""
        mock_get_metadata.side_effect = RuntimeError("boom")
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()

        success, timed_out, *_ = process_record(
            message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns"
        )

        self.assertFalse(success)
        self.assertFalse(timed_out)

    @patch("roz_scripts.mscape.chimera_runner.JobHeartbeat")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    def test_job_heartbeat_cleared_on_every_path(self, mock_get_metadata, mock_heartbeat_cls):
        mock_get_metadata.return_value = False
        payload = make_payload()
        msg = make_message(payload)
        pipe = MagicMock()

        process_record(message=msg, args=make_args(), chimera_pipe=pipe, namespace="ns")

        mock_heartbeat_cls.return_value.clear.assert_called_once()


# ---------------------------------------------------------------------------
# chimera_worker_pool_handler
# ---------------------------------------------------------------------------

class TestChimeraWorkerPoolHandlerInit(unittest.TestCase):
    @patch("roz_scripts.mscape.chimera_runner.mp.get_context")
    def test_init_creates_pool_with_correct_worker_count(self, mock_get_context):
        mock_pool_cls = mock_get_context.return_value.Pool
        log = MagicMock()
        varys = MagicMock()

        handler = chimera_worker_pool_handler(
            workers=4, logger=log, varys_client=varys, project="mscape", health=MagicMock()
        )

        mock_get_context.assert_called_once_with("fork")
        mock_pool_cls.assert_called_once_with(processes=4)
        self.assertEqual(handler._project, "mscape")
        self.assertEqual(handler.in_flight(), 0)


class TestChimeraWorkerPoolHandlerSubmitJob(unittest.TestCase):
    def setUp(self):
        with patch("roz_scripts.mscape.chimera_runner.mp.get_context"):
            self.handler = chimera_worker_pool_handler(
                workers=2,
                logger=MagicMock(),
                varys_client=MagicMock(),
                project="mscape",
                health=MagicMock(),
            )
        self.message = make_message()
        self.args = make_args()
        self.chimera_pipe = MagicMock()

    def test_submit_job_calls_apply_async_with_correct_kwargs(self):
        self.handler.submit_job(
            self.message, self.args, self.chimera_pipe, namespace="ns", is_rerun=True
        )

        self.handler.worker_pool.apply_async.assert_called_once()
        _, kwargs = self.handler.worker_pool.apply_async.call_args
        self.assertIs(kwargs["func"], process_record)
        self.assertEqual(kwargs["kwds"]["message"], self.message)
        self.assertEqual(kwargs["kwds"]["args"], self.args)
        self.assertEqual(kwargs["kwds"]["chimera_pipe"], self.chimera_pipe)
        self.assertEqual(kwargs["kwds"]["namespace"], "ns")
        self.assertTrue(kwargs["kwds"]["is_rerun"])
        self.assertEqual(kwargs["callback"], self.handler.callback)

    def test_submit_job_is_rerun_defaults_to_false(self):
        self.handler.submit_job(self.message, self.args, self.chimera_pipe, namespace="ns")

        _, kwargs = self.handler.worker_pool.apply_async.call_args
        self.assertFalse(kwargs["kwds"]["is_rerun"])

    def test_submit_job_increments_in_flight(self):
        self.handler.submit_job(self.message, self.args, self.chimera_pipe, namespace="ns")

        self.assertEqual(self.handler.in_flight(), 1)

    def test_malformed_message_nacks_with_requeue_and_does_not_submit(self):
        """Never dead-letter: even a malformed body must be requeued (not
        dropped), since manual intervention is required, not silent loss."""
        bad_msg = MagicMock()
        bad_msg.body = "not json"

        self.handler.submit_job(bad_msg, self.args, self.chimera_pipe, namespace="ns")

        self.handler.worker_pool.apply_async.assert_not_called()
        self.handler._varys_client.nack_message.assert_called_once_with(bad_msg)
        self.assertEqual(self.handler.in_flight(), 0)


class TestChimeraWorkerPoolHandlerCallback(unittest.TestCase):
    def setUp(self):
        with patch("roz_scripts.mscape.chimera_runner.mp.get_context"):
            self.handler = chimera_worker_pool_handler(
                workers=2,
                logger=MagicMock(),
                varys_client=MagicMock(),
                project="mscape",
                health=MagicMock(),
            )
        self.message = make_message()
        self.payload = make_payload()
        self.handler._in_flight = 1

    def test_callback_success_acknowledges_and_sends_downstream(self):
        self.handler.callback((True, False, self.payload, "match-uuid-5678", False, self.message))

        self.handler._varys_client.acknowledge_message.assert_called_once_with(self.message)
        self.handler._varys_client.send.assert_called_once_with(
            message=self.payload,
            exchange="downstream-chimera-mscape",
            queue_suffix="chimera",
        )

    def test_callback_success_rerun_sends_to_rerun_exchange(self):
        self.handler.callback((True, False, self.payload, "match-uuid-5678", True, self.message))

        self.handler._varys_client.send.assert_called_once_with(
            message=self.payload,
            exchange="downstream-chimera_rerun-mscape",
            queue_suffix="chimera",
        )

    def test_callback_success_clears_job_and_decrements_in_flight(self):
        self.handler.callback((True, False, self.payload, "match-uuid-5678", False, self.message))

        self.handler._health.clear_job.assert_called_once_with("match-uuid-5678")
        self.assertEqual(self.handler.in_flight(), 0)

    def test_callback_failure_nacks_without_dropping(self):
        self.handler.callback((False, False, self.payload, "match-uuid-5678", False, self.message))

        self.handler._varys_client.nack_message.assert_called_once_with(self.message)
        self.handler._varys_client.acknowledge_message.assert_not_called()

    def test_callback_failure_never_dead_letters_even_after_many_failures(self):
        """Regression test for the explicit product decision: unlike mscape's
        capped rerun-retry behaviour, chimera must never pass requeue=False -
        every message here is vital and requires manual intervention rather
        than being dropped."""
        for _ in range(20):
            self.handler._in_flight = 1
            self.handler.callback(
                (False, False, self.payload, "match-uuid-5678", False, self.message)
            )

        for call_args in self.handler._varys_client.nack_message.call_args_list:
            self.assertNotIn("requeue", call_args.kwargs)
            self.assertEqual(call_args.args, (self.message,))

    def test_callback_failure_sends_alert_at_fifth_failure(self):
        for _ in range(5):
            self.handler._in_flight = 1
            self.handler.callback(
                (False, False, self.payload, "match-uuid-5678", False, self.message)
            )

        alert_calls = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
        ]
        self.assertEqual(len(alert_calls), 1)

    def test_callback_timeout_sends_alert_only_from_second_timeout(self):
        self.handler._in_flight = 1
        self.handler.callback((False, True, self.payload, "match-uuid-5678", False, self.message))
        alerts_after_first = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
        ]
        self.assertEqual(len(alerts_after_first), 0)

        self.handler._in_flight = 1
        self.handler.callback((False, True, self.payload, "match-uuid-5678", False, self.message))
        alerts_after_second = [
            c for c in self.handler._varys_client.send.call_args_list
            if c.kwargs.get("queue_suffix") == "alert"
        ]
        self.assertEqual(len(alerts_after_second), 1)

    def test_callback_success_resets_failure_and_timeout_counters(self):
        self.handler._in_flight = 1
        self.handler.callback((False, True, self.payload, "match-uuid-5678", False, self.message))
        self.assertIn("match-uuid-5678", self.handler._timeout_log)
        self.assertIn("match-uuid-5678", self.handler._failure_log)

        self.handler._in_flight = 1
        self.handler.callback((True, False, self.payload, "match-uuid-5678", False, self.message))
        self.assertNotIn("match-uuid-5678", self.handler._timeout_log)
        self.assertNotIn("match-uuid-5678", self.handler._failure_log)


class TestChimeraWorkerPoolHandlerErrorCallback(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp_dir.cleanup)

        from roz_scripts.utils.health import HealthState
        self.health = HealthState(self._tmp_dir.name)

        with patch("roz_scripts.mscape.chimera_runner.mp.get_context"):
            self.handler = chimera_worker_pool_handler(
                workers=2,
                logger=MagicMock(),
                varys_client=MagicMock(),
                project="mscape",
                health=self.health,
            )
        self.message = make_message()
        self.handler._in_flight = 1

    def test_error_callback_nacks_the_specific_message(self):
        self.handler.error_callback(self.message, Exception("worker exploded"))

        self.handler._varys_client.nack_message.assert_called_once_with(self.message)

    def test_error_callback_sends_dead_worker_message(self):
        exc = Exception("worker exploded")
        self.handler.error_callback(self.message, exc)

        self.handler._varys_client.send.assert_any_call(
            message=f"mscape chimera worker failed with unhandled exception: {exc}",
            exchange="mscape-restricted-announce",
            queue_suffix="dead_worker",
        )

    def test_error_callback_decrements_in_flight(self):
        self.handler.error_callback(self.message, Exception("boom"))

        self.assertEqual(self.handler.in_flight(), 0)

    def test_single_error_does_not_mark_health_fatal(self):
        """A single crashed job must not restart the whole pod - other
        in-flight jobs would be killed with it."""
        self.handler.error_callback(self.message, Exception("boom"))

        fatal_path = Path(self._tmp_dir.name) / "fatal"
        self.assertFalse(fatal_path.exists())

    def test_three_consecutive_errors_marks_health_fatal(self):
        for _ in range(3):
            self.handler._in_flight = 1
            self.handler.error_callback(self.message, Exception("boom"))

        fatal_path = Path(self._tmp_dir.name) / "fatal"
        self.assertTrue(fatal_path.exists())

    def test_success_between_errors_resets_consecutive_count(self):
        self.handler.error_callback(self.message, Exception("boom"))

        self.handler._in_flight = 1
        self.handler.callback(
            (True, False, make_payload(), "match-uuid-5678", False, self.message)
        )

        self.handler._in_flight = 1
        self.handler.error_callback(self.message, Exception("boom again"))
        self.handler._in_flight = 1
        self.handler.error_callback(self.message, Exception("boom thrice"))

        # Only 2 consecutive since the intervening success reset the counter
        fatal_path = Path(self._tmp_dir.name) / "fatal"
        self.assertFalse(fatal_path.exists())


# ---------------------------------------------------------------------------
# run() — thin receive loop: message prioritisation + submission
# ---------------------------------------------------------------------------

class TestRunMessagePrioritisation(unittest.TestCase):
    """The loop's only job now is to pick a message and hand it to the
    worker pool handler - pipeline execution lives in process_record()."""

    def setUp(self):
        patcher = patch(
            "roz_scripts.mscape.chimera_runner.get_pod_namespace",
            return_value="climb-gre-test",
        )
        self.mock_get_pod_namespace = patcher.start()
        self.addCleanup(patcher.stop)

    def _make_handler_mock(self, mock_handler_cls):
        mock_handler = mock_handler_cls.return_value
        mock_handler.in_flight.return_value = 0
        return mock_handler

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.chimera_worker_pool_handler")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_no_messages_sleeps(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_handler_cls, mock_sleep
    ):
        mock_handler = self._make_handler_mock(mock_handler_cls)
        mock_varys = mock_varys_cls.return_value
        call_count = [0]

        def receive_side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                return None
            raise KeyboardInterrupt()

        mock_varys.receive.side_effect = receive_side_effect

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_sleep.assert_any_call(60)
        mock_handler.submit_job.assert_not_called()

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.chimera_worker_pool_handler")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_priority_nacks_rerun_and_submits_priority(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_handler_cls, mock_sleep
    ):
        mock_handler = self._make_handler_mock(mock_handler_cls)
        payload = make_payload()
        priority_msg = make_message(payload)
        rerun_msg = make_message(
            make_payload(uuid="rerun-uuid", climb_id="CLIMB002", match_uuid="rerun-match")
        )
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = [priority_msg, rerun_msg, KeyboardInterrupt()]

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_called_once_with(rerun_msg)
        mock_handler.submit_job.assert_called_once()
        _, kwargs = mock_handler.submit_job.call_args
        self.assertEqual(kwargs["message"], priority_msg)
        self.assertFalse(kwargs["is_rerun"])

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.chimera_worker_pool_handler")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_only_priority_no_nack(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_handler_cls, mock_sleep
    ):
        mock_handler = self._make_handler_mock(mock_handler_cls)
        payload = make_payload()
        priority_msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = [priority_msg, None, KeyboardInterrupt()]

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_not_called()
        mock_handler.submit_job.assert_called_once()

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.chimera_worker_pool_handler")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_only_rerun_submitted_with_is_rerun_true(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_handler_cls, mock_sleep
    ):
        mock_handler = self._make_handler_mock(mock_handler_cls)
        payload = make_payload()
        rerun_msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = [None, rerun_msg, KeyboardInterrupt()]

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_not_called()
        mock_handler.submit_job.assert_called_once()
        _, kwargs = mock_handler.submit_job.call_args
        self.assertEqual(kwargs["message"], rerun_msg)
        self.assertTrue(kwargs["is_rerun"])

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.chimera_worker_pool_handler")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_receive_paused_while_pool_saturated(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_handler_cls, mock_sleep
    ):
        """When the pool is at capacity the loop must not call receive() at
        all - this is the backpressure mechanism, avoiding mscape's flaw of
        doubling the effective in-flight budget across two consumers."""
        mock_handler = mock_handler_cls.return_value
        mock_handler.in_flight.side_effect = [3, 3, 0]
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            run(make_args(n_workers=3))

        mock_varys.receive.assert_called_once()
        self.assertEqual(mock_sleep.call_args_list[0], call(5))
        self.assertEqual(mock_sleep.call_args_list[1], call(5))


class TestRunCrashHandling(unittest.TestCase):
    """Regression test: the top-level except block must not reference
    health/varys_client before they're guaranteed to be bound, or a crash
    during setup gets masked by an UnboundLocalError."""

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.chimera_worker_pool_handler")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.get_pod_namespace", return_value="ns")
    def test_init_logger_failure_propagates_cleanly(
        self, mock_get_pod_namespace, mock_varys_cls, mock_pipeline_cls, mock_handler_cls, mock_sleep
    ):
        with patch(
            "roz_scripts.mscape.chimera_runner.init_logger",
            side_effect=RuntimeError("logger init failed"),
        ):
            with self.assertRaises(RuntimeError):
                run(make_args())
