import logging
import pytest
from unittest.mock import Mock, patch
from roz_scripts.mscape.mscape_ingest_validation import onyx_submission


class MockResponse:
    def __init__(self, status_code, json_data=None):
        self.status_code = status_code
        self.json_data = json_data

    def json(self):
        return self.json_data


def test_onyx_submission_success():
    mock_logger = Mock(spec=logging.Logger)
    mock_varys_client = Mock()

    mock_csv_file = Mock()
    mock_s3_to_fh = Mock(return_value=mock_csv_file)

    mock_onyx_client = Mock()
    mock_onyx_client.csv_create.return_value = iter(
        [MockResponse(status_code=201, json_data={"data": {"cid": "test_cid"}})]
    )

    mock_utils = Mock()
    mock_utils.s3_to_fh = mock_s3_to_fh

    with patch("onyx.OnyxClient", return_value=mock_onyx_client), patch(
        "utils.utils", mock_utils
    ):
        payload = {
            "artifact": "test_artifact",
            "project": "test_project",
            "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
            "uuid": "test_uuid",
            "site_code": "test_site",
        }

        result = onyx_submission(mock_logger, payload, mock_varys_client)

        assert result[0] is True
        assert result[1]["onyx_create_status"] is True
        assert result[1]["created"] is True
        assert result[1]["cid"] == "test_cid"

        mock_logger.info.assert_called_with(
            "Received match for artifact: test_artifact, now attempting to create record in Onyx"
        )
        mock_onyx_client.csv_create.assert_called_with(
            "test_project", csv_file=mock_csv_file
        )
        mock_varys_client.send.assert_not_called()  # In this case, ingest_fail is False, so no message should be sent


def test_onyx_submission_internal_server_error():
    mock_logger = Mock(spec=logging.Logger)
    mock_varys_client = Mock()

    mock_csv_file = Mock()
    mock_s3_to_fh = Mock(return_value=mock_csv_file)

    mock_onyx_client = Mock()
    mock_onyx_client.csv_create.return_value = iter([MockResponse(status_code=500)])

    mock_utils = Mock()
    mock_utils.s3_to_fh = mock_s3_to_fh

    with patch("your_module.OnyxClient", return_value=mock_onyx_client), patch(
        "your_module.utils", mock_utils
    ):
        payload = {
            "artifact": "test_artifact",
            "project": "test_project",
            "files": {".csv": {"uri": "test_uri", "etag": "test_etag"}},
            "uuid": "test_uuid",
            "site_code": "test_site",
        }

        result = onyx_submission(mock_logger, payload, mock_varys_client)

        assert result[0] is True
        assert "onyx_client_errors" in result[1]["onyx_errors"]
        assert (
            "Onyx internal server error"
            in result[1]["onyx_errors"]["onyx_client_errors"]
        )
        mock_varys_client.send.assert_called_with(
            message=payload,
            exchange="inbound.results.mscape.test_site",
            queue_suffix="validator",
        )


if __name__ == "__main__":
    pytest.main()
