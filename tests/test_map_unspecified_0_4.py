import json
import os
import pytest

from bodspipelines.infrastructure.utils import map_unspecified

@pytest.fixture
def repex_json_data_gleif_deletion():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/bods_repex_deletion.json", "r") as read_file:
        return json.load(read_file)

def test_map_unspecified(repex_json_data_gleif_deletion):
    """Test transform pipeline stage on relationship update when lei updated"""

    mapped = map_unspecified(repex_json_data_gleif_deletion[-1])

    print(json.dumps(mapped, indent=2))

    assert False
