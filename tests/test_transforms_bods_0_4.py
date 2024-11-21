import pytest
import os
import json

from bodspipelines.infrastructure.bods.transforms import (transform_entity, transform_relationship,
                                                          transform_exception)
from bodspipelines.pipelines.gleif.sources import GLEIFSource

@pytest.fixture
def lei_json_data_gleif_issued():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/lei_issued.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_gleif_issued():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/relationship_issued.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_gleif_issued():
    """GLEIF LEI Record data"""
    with open("tests/fixtures/bods_0_4/repex_issued.json", "r") as read_file:
        return json.load(read_file)

def test_lei_issued_transform(lei_json_data_gleif_issued):
    print(lei_json_data_gleif_issued)
    source = GLEIFSource()
    status = 'new'
    bods_statement = transform_entity(source, lei_json_data_gleif_issued[0], status)
    print(json.dumps(bods_statement, indent=2))
    assert False

def test_rr_issued_transform(rr_json_data_gleif_issued):
    print(rr_json_data_gleif_issued)
    source = GLEIFSource()
    status = 'new'
    bods_statement = transform_relationship(source, rr_json_data_gleif_issued[0], status)
    print(json.dumps(bods_statement, indent=2))
    assert False

def test_repex_issued_transform(repex_json_data_gleif_issued):
    print(repex_json_data_gleif_issued)
    source = GLEIFSource()
    status = 'new'
    bods_statement = transform_exception(source, repex_json_data_gleif_issued[-1], status)
    print(json.dumps(bods_statement, indent=2))
    assert False
