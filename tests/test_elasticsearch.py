import os
import sys
import time
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
