import pytest
import mappy


@pytest.fixture
def config():
    return {
        "bam": {
            "allowed_refs": ["CANONICAL_MPX"],
            "check_dehumanised": True,
            "require_sorted": True,
            "require_primertrimmed": False,
            "size_limit_gb": 2,
            "require_samtools_quickcheck": True,
        }
    }

