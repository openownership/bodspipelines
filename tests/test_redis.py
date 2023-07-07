import os
import sys
import time
import json
from unittest.mock import patch, Mock
import pytest

from redis import RedisError

from bodspipelines.infrastructure.storage import Storage
from bodspipelines.infrastructure.clients.redis_client import RedisClient
from bodspipelines.pipelines.gleif.indexes import (lei_properties, rr_properties, repex_properties,
                                          match_lei, match_rr, match_repex,
                                          id_lei, id_rr, id_repex)

index_properties = {"lei": {"properties": lei_properties, "match": match_lei, "id": id_lei},
                    "rr": {"properties": rr_properties, "match": match_rr, "id": id_rr},
                    "repex": {"properties": repex_properties, "match": match_repex, "id": id_repex}}

def set_environment_variables():
    os.environ['ELASTICSEARCH_PROTOCOL'] = 'http'
    os.environ['ELASTICSEARCH_HOST'] = 'localhost'
    os.environ['ELASTICSEARCH_PORT'] = '9876'
    os.environ['ELASTICSEARCH_PASSWORD'] = '********'

@pytest.fixture
def lei_item():
    """Example LEI-CDF v3.1 data"""
    return {'LEI': '097900BICQ0000135514',
          'Entity': {'LegalName': 'Ing. Magdaléna Beňo Frackowiak ZARIA TRAVEL',
                     'TransliteratedOtherEntityNames': {'TransliteratedOtherEntityName': 'ING MAGDALENA BENO FRACKOWIAK ZARIA TRAVEL'},
                     'LegalAddress': {'FirstAddressLine': 'Partizánska Ľupča 708', 'City': 'Partizánska Ľupča', 'Country': 'SK', 'PostalCode': '032 15'},
                     'HeadquartersAddress': {'FirstAddressLine': 'Partizánska Ľupča 708', 'City': 'Partizánska Ľupča', 'Country': 'SK', 'PostalCode': '032 15'},
                     'RegistrationAuthority': {'RegistrationAuthorityID': 'RA000670', 'RegistrationAuthorityEntityID': '43846696'},
                     'LegalJurisdiction': 'SK',
                     'EntityCategory': 'SOLE_PROPRIETOR',
                     'LegalForm': {'EntityLegalFormCode': 'C4PZ'},
                     'EntityStatus': 'ACTIVE',
                     'EntityCreationDate': '2007-11-15T08:00:00+01:00'},
          'Registration': {'InitialRegistrationDate': '2018-02-16T00:00:00+01:00',
                           'LastUpdateDate': '2023-01-10T08:30:56.044+01:00',
                           'RegistrationStatus': 'ISSUED',
                           'NextRenewalDate': '2024-02-16T00:00:00+01:00',
                           'ManagingLOU': '097900BEFH0000000217',
                           'ValidationSources': 'FULLY_CORROBORATED',
                           'ValidationAuthority': {'ValidationAuthorityID': 'RA000670', 'ValidationAuthorityEntityID': '43846696'}}}

@pytest.fixture
def lei_list():
    return ['097900BICQ0000135514', '097900BICQ0000135515', '097900BICQ0000135516', '097900BICQ0000135517', '097900BICQ0000135518',
            '097900BICQ0000135519', '097900BICQ0000135520', '097900BICQ0000135521', '097900BICQ0000135522', '097900BICQ0000135523']

def test_lei_storage_get(lei_item):
    """Test getting a new LEI-CDF v3.1 record in redis"""
    with patch('bodspipelines.infrastructure.clients.redis_client.Redis') as mock_rd:
        mock_rd.return_value.get.return_value = json.dumps(lei_item).encode("utf-8")
        set_environment_variables()
        storage = Storage(storage=RedisClient(indexes=index_properties))
        assert storage.get(lei_item["LEI"]) == lei_item


def test_lei_storage_new(lei_item):
    """Test storing a new LEI-CDF v3.1 record in redis"""
    with patch('bodspipelines.infrastructure.clients.redis_client.Redis') as mock_rd:
        mock_rd.return_value.get.side_effect = RedisError()
        set_environment_variables()
        storage = Storage(storage=RedisClient(indexes=index_properties))
        assert storage.process(lei_item, 'lei') == lei_item


def test_lei_storage_existing(lei_item):
    """Test trying to store LEI-CDF v3.1 record which is already in redis"""
    with patch('bodspipelines.infrastructure.clients.redis_client.Redis') as mock_rd:
        mock_rd.return_value.get.return_value = json.dumps(lei_item).encode("utf-8")
        set_environment_variables()
        storage = Storage(storage=RedisClient(indexes=index_properties))
        assert storage.process(lei_item, 'lei') == False


def test_lei_storage_batch_new(lei_item, lei_list):
    """Test storing a batch of new LEI-CDF v3.1 records in redis"""
    with patch('bodspipelines.infrastructure.clients.redis_client.Redis') as mock_rd:
        def build_stream(lei_item, lei_list):
            for lei in lei_list:
                item = lei_item.copy()
                item['LEI'] = lei
                yield item
        mock_rd.return_value.pipeline.return_value.execute.return_value = [True for result in build_stream(lei_item, lei_list)]
        set_environment_variables()
        storage = Storage(storage=RedisClient(indexes=index_properties))
        for item in storage.process_batch(build_stream(lei_item, lei_list), "lei"):
            assert item["LEI"] in lei_list


def test_lei_storage_batch_existing(lei_item, lei_list):
    """Test storing a batch of existing LEI-CDF v3.1 records in redis"""
    with patch('bodspipelines.infrastructure.clients.redis_client.Redis') as mock_rd:
        def build_stream(lei_item, lei_list):
            for lei in lei_list:
                item = lei_item.copy()
                item['LEI'] = lei
                yield item
        mock_rd.return_value.pipeline.return_value.execute.return_value = [False for result in build_stream(lei_item, lei_list)]
        set_environment_variables()
        storage = Storage(storage=RedisClient(indexes=index_properties))
        for item in storage.process_batch(build_stream(lei_item, lei_list), "lei"):
            assert False, "Error: New record found"
