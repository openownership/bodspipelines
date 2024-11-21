import json
import os
import pytest

from .config import set_environment_variables
from .run_pipeline import run_transform_pipeline

# Setup environment variables
set_environment_variables()

@pytest.fixture(scope="module")
def wait_for_es(module_scoped_container_getter):
    service = module_scoped_container_getter.get("bods_ingester_gleif_es").network_info[0]
    os.environ['ELASTICSEARCH_HOST'] = service.hostname
    os.environ['ELASTICSEARCH_PORT'] = service.host_port
    return service

@pytest.fixture
def rr_json_data_gleif_issued():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/relationship_issued.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_json_data_gleif_updated():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/lei_updated.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_gleif_replaced():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/repex_replace_repex.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_gleif_deletion():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/repex_deletion.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_json_data_gleif_issued():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/lei_issued.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_json_data_gleif_retired_gc():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/lei_retired_gc.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_json_data_gleif_retired():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/lei_retired.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_gleif_deletion():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/relationship_deletion.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_gleif_replaced():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/relationship_replace_relationship.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_gleif_updated():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/relationship_updated.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_gleif_issued():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/repex_issued.json", "r") as read_file:
        return json.load(read_file)

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, rr_json_data_gleif_issued):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(rr_json_data_gleif_issued)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 4

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, lei_json_data_gleif_updated):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(lei_json_data_gleif_updated)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 4


#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, repex_json_data_gleif_deletion):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(repex_json_data_gleif_deletion)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 4

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, repex_json_data_gleif_replaced):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(repex_json_data_gleif_replaced)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, lei_json_data_gleif_issued):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(lei_json_data_gleif_issued)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

# Test data broken
#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, lei_json_data_gleif_retired_gc):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(lei_json_data_gleif_retired_gc)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, lei_json_data_gleif_retired):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(lei_json_data_gleif_retired)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, rr_json_data_gleif_deletion):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(rr_json_data_gleif_deletion)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, rr_json_data_gleif_replaced):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(rr_json_data_gleif_replaced)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

#@pytest.mark.asyncio
#@pytest.mark.order(1)
#async def test_multiple_updates7(wait_for_es, rr_json_data_gleif_updated):
#    """Test transform pipeline stage on relationship update when lei updated"""
#
#    output_stream = await run_transform_pipeline(rr_json_data_gleif_updated)
#
#    print(json.dumps(output_stream, indent=2))
#
#    assert len(output_stream) == 0

@pytest.mark.asyncio
@pytest.mark.order(1)
async def test_multiple_updates7(wait_for_es, repex_json_data_gleif_issued):
    """Test transform pipeline stage on relationship update when lei updated"""

    output_stream = await run_transform_pipeline(repex_json_data_gleif_issued)

    print(json.dumps(output_stream, indent=2))

    assert len(output_stream) == 0
