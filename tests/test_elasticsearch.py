import os
import sys
import time
import json
from unittest.mock import patch, Mock
import pytest

from bodspipelines.infrastructure.storage import ElasticStorage
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
    return ['001GPB6A9XPE8XJICC14', '004L5FPTUREIWK9T2N63', '00EHHQ2ZHDCFXJCPCL46', '00GBW0Z2GYIER7DHDS71', '00KLB2PFTM3060S2N216',
            '00QDBXDXLLF3W3JJJO36', '00TR8NKAEL48RGTZEW89', '00TV1D5YIV5IDUGWBW29', '00W0SLGGVF0QQ5Q36N03', '00X5RQKJQQJFFX0WPA53',
            '1595D0QCK7Y15293JK84', '213800FERQ5LE3H7WJ58', '213800BJPX8V9HVY1Y11']


@pytest.fixture
def json_data():
    with open("tests/fixtures/lei-data.json", "r") as read_file:
        return json.load(read_file)

def test_lei_storage_new(lei_item):
    """Test storing a new LEI-CDF v3.1 record in elasticsearch"""
    with patch('bodspipelines.infrastructure.clients.elasticsearch_client.Elasticsearch') as mock_es:
        mock_es.return_value.search.return_value = {"hits": {"total": {"value": 1, "relation": "eq"}, "hits": []}}
        set_environment_variables()
        storage = ElasticStorage(indexes=index_properties)
        assert storage.process(lei_item, 'lei') == lei_item


def test_lei_storage_existing(lei_item):
    """Test trying to store LEI-CDF v3.1 record which is already in elasticsearch"""
    with patch('bodspipelines.infrastructure.clients.elasticsearch_client.Elasticsearch') as mock_es:
        mock_es.return_value.search.return_value = {"hits": {"total": {"value": 1, "relation": "eq"}, "hits": [lei_item]}}
        set_environment_variables()
        storage = ElasticStorage(indexes=index_properties)
        assert storage.process(lei_item, 'lei') == False


def test_lei_bulk_storage_new(lei_list, json_data):
    """Test ingest pipeline stage on LEI-CDF v3.1 records"""
    with patch('bodspipelines.infrastructure.clients.elasticsearch_client.streaming_bulk') as mock_sb:
        mock_sb.return_value = iter([(True, {'create': {'_id': lei}}) for lei in lei_list])
        set_environment_variables()
        storage = ElasticStorage(indexes=index_properties)
        def json_stream():
            for d in json_data:
                yield d
        count = 0
        for result in storage.process_batch(json_stream(), 'lei'):
            assert result == json_data[count]
            count += 1
        assert count == 13


def test_lei_bulk_storage_existing(lei_list, json_data):
    """Test ingest pipeline stage on LEI-CDF v3.1 records"""
    with patch('bodspipelines.infrastructure.clients.elasticsearch_client.streaming_bulk') as mock_sb:
        mock_sb.return_value = iter([(False, {'create': {'_id': lei}}) for lei in lei_list])
        set_environment_variables()
        storage = ElasticStorage(indexes=index_properties)
        def json_stream():
            for d in json_data:
                yield d
        count = 0
        for result in storage.process_batch(json_stream(), 'lei'):
            count += 1
        assert count == 0
