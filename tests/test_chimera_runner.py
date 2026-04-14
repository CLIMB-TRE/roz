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
    create_samplesheet,
    handle_alignment_report,
    handle_sylph_report,
    onyx_get_metadata,
    push_bam_file,
    ret_0_parser,
    run,
)

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

        uri = push_bam_file("/tmp/CLIMB001.bam", self.payload, self.log)

        self.assertEqual(uri, "s3://mscape-chimera-bams/CLIMB001.chimera.bam")
        mock_s3.upload_file.assert_called_once_with(
            "/tmp/CLIMB001.bam",
            "mscape-chimera-bams",
            "CLIMB001.chimera.bam",
        )

    @patch("roz_scripts.mscape.chimera_runner.boto3.client")
    def test_upload_failure_raises(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_s3.upload_file.side_effect = Exception("S3 upload failed")
        mock_boto_client.return_value = mock_s3

        with self.assertRaises(Exception, msg="S3 upload failed"):
            push_bam_file("/tmp/CLIMB001.bam", self.payload, self.log)

    @patch("roz_scripts.mscape.chimera_runner.boto3.client")
    def test_uses_climb_id_in_key(self, mock_boto_client):
        payload = make_payload(climb_id="CLIMB999")
        mock_boto_client.return_value = MagicMock()

        uri = push_bam_file("/tmp/CLIMB999.bam", payload, self.log)

        self.assertIn("CLIMB999", uri)


# ---------------------------------------------------------------------------
# run() — message prioritisation and pipeline flow
# ---------------------------------------------------------------------------

class TestRunMessagePrioritisation(unittest.TestCase):
    """Tests for the priority-over-rerun message selection in the run() loop."""

    def _make_run_mocks(
        self,
        mock_init_logger,
        mock_varys_cls,
        mock_pipeline_cls,
        priority_msg,
        rerun_msg,
        extra_side_effects=None,
    ):
        mock_varys = mock_varys_cls.return_value
        mock_pipe = mock_pipeline_cls.return_value
        mock_init_logger.return_value = MagicMock()

        # Build receive side_effect: [iter1-priority, iter1-rerun, iter2-priority (raise)]
        third = KeyboardInterrupt() if extra_side_effects is None else extra_side_effects[0]
        mock_varys.receive.side_effect = [priority_msg, rerun_msg, third]

        return mock_varys, mock_pipe

    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_no_messages_sleeps(self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_sleep):
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

    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_priority_nacks_rerun(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_sleep, mock_get_metadata
    ):
        payload = make_payload()
        priority_msg = make_message(payload)
        rerun_msg = make_message(make_payload(uuid="rerun-uuid", climb_id="CLIMB002", match_uuid="rerun-match"))
        mock_varys = mock_varys_cls.return_value

        mock_varys.receive.side_effect = [priority_msg, rerun_msg, KeyboardInterrupt()]
        # Stop further processing after metadata fetch
        mock_get_metadata.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_called_once_with(rerun_msg)

    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_only_priority_no_nack(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_sleep, mock_get_metadata
    ):
        payload = make_payload()
        priority_msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value

        mock_varys.receive.side_effect = [priority_msg, None, KeyboardInterrupt()]
        mock_get_metadata.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_not_called()

    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_only_rerun_submitted(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_sleep, mock_get_metadata
    ):
        payload = make_payload()
        rerun_msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value

        mock_varys.receive.side_effect = [None, rerun_msg, KeyboardInterrupt()]
        mock_get_metadata.side_effect = KeyboardInterrupt()

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        # No nack for the rerun message itself
        mock_varys.nack_message.assert_not_called()


class TestRunPipelineFlow(unittest.TestCase):
    """Tests for the pipeline execution flow inside run()."""

    def _base_mocks(self, priority_msg, rerun_msg=None):
        """Return a context-manager patch stack covering all external dependencies."""
        # We'll apply patches manually per test to keep flexibility
        pass

    def _receive_side_effects(self, priority_msg, rerun_msg=None):
        return [priority_msg, rerun_msg, KeyboardInterrupt()]

    @patch("sys.exit")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_get_metadata_false_calls_sys_exit(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_get_metadata, mock_sleep, mock_exit
    ):
        payload = make_payload()
        msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = self._receive_side_effects(msg)
        # onyx_get_metadata returns falsy to trigger sys.exit
        mock_get_metadata.return_value = False
        mock_exit.side_effect = SystemExit(1)

        with self.assertRaises(SystemExit):
            run(make_args())

        mock_exit.assert_called_once_with(1)

    @patch("pathlib.Path.mkdir")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_pipeline_failure_nacks_message(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_get_metadata,
        mock_create_ss, mock_sleep, mock_mkdir
    ):
        payload = make_payload()
        msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = self._receive_side_effects(msg)
        mock_get_metadata.return_value = make_metadata()
        mock_pipeline_cls.return_value.execute.side_effect = [
            1,                  # pipeline fails with rc=1
            KeyboardInterrupt(),# second iteration breaks loop
        ]

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_called_once_with(msg)
        mock_varys.acknowledge_message.assert_not_called()

    @patch("pathlib.Path.mkdir")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_ret_0_parser_fail_nacks_message(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_get_metadata,
        mock_create_ss, mock_ret_0, mock_sleep, mock_mkdir
    ):
        payload = make_payload()
        msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = self._receive_side_effects(msg)
        mock_get_metadata.return_value = make_metadata()
        mock_pipeline_cls.return_value.execute.side_effect = [0, KeyboardInterrupt()]
        mock_ret_0.return_value = (True, payload)  # ingest_fail=True

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_called_once_with(msg)

    @patch("os.path.exists")
    @patch("pathlib.Path.mkdir")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_alignment_report_missing_nacks(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_get_metadata,
        mock_create_ss, mock_ret_0, mock_sleep, mock_mkdir, mock_exists
    ):
        payload = make_payload()
        msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = self._receive_side_effects(msg)
        mock_get_metadata.return_value = make_metadata()
        mock_pipeline_cls.return_value.execute.side_effect = [0, KeyboardInterrupt()]
        mock_ret_0.return_value = (False, payload)
        mock_exists.return_value = False  # alignment report not found

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_called_with(msg)
        mock_varys.acknowledge_message.assert_not_called()

    @patch("os.path.exists")
    @patch("pathlib.Path.mkdir")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.handle_alignment_report")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_sylph_report_missing_without_chimera_info_nacks(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_get_metadata,
        mock_create_ss, mock_handle_align, mock_ret_0, mock_sleep, mock_mkdir, mock_exists
    ):
        payload = make_payload()
        msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = self._receive_side_effects(msg)
        mock_get_metadata.return_value = make_metadata()
        mock_pipeline_cls.return_value.execute.side_effect = [0, KeyboardInterrupt()]
        mock_ret_0.return_value = (False, payload)
        mock_handle_align.return_value = True
        # alignment report exists, sylph report does not
        mock_exists.side_effect = lambda p: "alignment_report" in str(p)

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_called_with(msg)

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    @patch("roz_scripts.mscape.chimera_runner.push_bam_file")
    @patch("os.path.exists")
    @patch("pathlib.Path.mkdir")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.handle_alignment_report")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_sylph_report_missing_with_no_hits_does_not_nack(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_get_metadata,
        mock_create_ss, mock_handle_align, mock_ret_0, mock_sleep, mock_mkdir, mock_exists,
        mock_push_bam, mock_onyx_update
    ):
        """When SYLPH_TAXONOMY has no_hits status the missing sylph report is expected — no nack."""
        payload = make_payload()
        # ret_0_parser sets chimera_info to indicate no hits
        payload_with_no_hits = {
            **payload,
            "chimera_info": {"SYLPH_TAXONOMY": {"status": "no_hits"}},
        }
        msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = self._receive_side_effects(msg)
        mock_get_metadata.return_value = make_metadata()
        mock_pipeline_cls.return_value.execute.side_effect = [0, KeyboardInterrupt()]
        mock_ret_0.return_value = (False, payload_with_no_hits)
        mock_handle_align.return_value = True
        # alignment report and BAM exist; sylph report does not
        mock_exists.side_effect = lambda p: "sylph_taxonomy" not in str(p)
        mock_push_bam.return_value = "s3://mscape-chimera-bams/CLIMB001.chimera.bam"
        mock_onyx_update.return_value = (False, False, payload_with_no_hits)

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        # nack_message should NOT be called because the missing sylph report was expected
        nack_calls = [c for c in mock_varys.nack_message.call_args_list if c == call(msg)]
        self.assertEqual(len(nack_calls), 0)

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    @patch("roz_scripts.mscape.chimera_runner.push_bam_file")
    @patch("roz_scripts.mscape.chimera_runner.handle_sylph_report")
    @patch("roz_scripts.mscape.chimera_runner.handle_alignment_report")
    @patch("os.path.exists")
    @patch("pathlib.Path.mkdir")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_full_success_acknowledges_and_sends_downstream(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_get_metadata,
        mock_create_ss, mock_ret_0, mock_sleep, mock_mkdir, mock_exists,
        mock_handle_align, mock_handle_sylph, mock_push_bam, mock_onyx_update
    ):
        payload = make_payload()
        msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = self._receive_side_effects(msg)
        mock_get_metadata.return_value = make_metadata()
        mock_pipeline_cls.return_value.execute.side_effect = [0, KeyboardInterrupt()]
        mock_ret_0.return_value = (False, payload)
        mock_exists.return_value = True
        mock_handle_align.return_value = True
        mock_handle_sylph.return_value = True
        mock_push_bam.return_value = "s3://mscape-chimera-bams/CLIMB001.chimera.bam"
        mock_onyx_update.return_value = (False, False, payload)

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.acknowledge_message.assert_called_once_with(msg)
        mock_varys.send.assert_called_once_with(
            message=payload,
            exchange="downstream-chimera-mscape",
            queue_suffix="chimera",
        )

    @patch("roz_scripts.mscape.chimera_runner.onyx_update")
    @patch("roz_scripts.mscape.chimera_runner.push_bam_file")
    @patch("roz_scripts.mscape.chimera_runner.handle_sylph_report")
    @patch("roz_scripts.mscape.chimera_runner.handle_alignment_report")
    @patch("os.path.exists")
    @patch("pathlib.Path.mkdir")
    @patch("time.sleep")
    @patch("roz_scripts.mscape.chimera_runner.ret_0_parser")
    @patch("roz_scripts.mscape.chimera_runner.create_samplesheet")
    @patch("roz_scripts.mscape.chimera_runner.onyx_get_metadata")
    @patch("roz_scripts.mscape.chimera_runner.pipeline")
    @patch("roz_scripts.mscape.chimera_runner.Varys")
    @patch("roz_scripts.mscape.chimera_runner.init_logger")
    def test_onyx_update_failure_after_bam_upload_nacks(
        self, mock_logger, mock_varys_cls, mock_pipeline_cls, mock_get_metadata,
        mock_create_ss, mock_ret_0, mock_sleep, mock_mkdir, mock_exists,
        mock_handle_align, mock_handle_sylph, mock_push_bam, mock_onyx_update
    ):
        payload = make_payload()
        msg = make_message(payload)
        mock_varys = mock_varys_cls.return_value
        mock_varys.receive.side_effect = self._receive_side_effects(msg)
        mock_get_metadata.return_value = make_metadata()
        mock_pipeline_cls.return_value.execute.side_effect = [0, KeyboardInterrupt()]
        mock_ret_0.return_value = (False, payload)
        mock_exists.return_value = True
        mock_handle_align.return_value = True
        mock_handle_sylph.return_value = True
        mock_push_bam.return_value = "s3://mscape-chimera-bams/CLIMB001.chimera.bam"
        mock_onyx_update.return_value = (True, False, payload)  # update fails

        with self.assertRaises(KeyboardInterrupt):
            run(make_args())

        mock_varys.nack_message.assert_called_with(msg)
        mock_varys.acknowledge_message.assert_not_called()
