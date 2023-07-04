import datetime
import pytz
from collections import OrderedDict
from unittest.mock import patch, Mock
import pytest

from bodspipelines.infrastructure.processing.api_data import APIData


def test_gleif_api():
    """Test API accessing Global Legal Entity Identifier Foundation (GLEIF) API"""
    with patch('bodspipelines.infrastructure.processing.api_data.requests.get') as mock_get:
        mock_get.return_value = Mock(status_code = 200)
        mock_get.return_value.json.return_value = {'links': {'first': 'https://api.gleif.org/api/v1/lei-records?page%5Bnumber%5D=1&page%5Bsize%5D=1'},
                                                   'data': [{'attributes': {'lei': '213800AA33UZLIAHLB74'}}]}
        api_data = APIData("GLEIF API Download",
                       ["https://api.gleif.org/api/v1/lei-records"],
                       headers={'Accept': 'application/vnd.api+json'},
                       params={},
                       paging_names={"pagesize": "page[size]", "page": "page[number]"},
                       pagesize=1,
                       data_element='data')
        api_data.streaming = False
        for lei_item in api_data.process():
            assert len(lei_item['attributes']['lei']) == 20
        if not 'lei_item' in locals():
            raise AssertionError("No statements produced")


def test_gleif_api_update():
    """Test API accessing Global Legal Entity Identifier Foundation (GLEIF) API update"""
    with patch('bodspipelines.infrastructure.processing.api_data.requests.get') as mock_get:
        mock_get.return_value = Mock(status_code = 200)
        mock_get.return_value.json.return_value = {'links': {'first': 'https://api.gleif.org/api/v1/lei-records?filter%5Bregistration.lastUpdateDate%5D=%3E2023-05-01&page%5Bnumber%5D=1&page%5Bsize%5D=1'},
                                                   'data': [{'attributes': {'lei': '213800AA33UZLIAHLB74',
                                                                            'registration': {'lastUpdateDate': '2023-05-08T18:33:06Z'}}}]}
        api_data = APIData("GLEIF API Download (Update)",
                       ["https://api.gleif.org/api/v1/lei-records"],
                       headers={'Accept': 'application/vnd.api+json'},
                       params={"filter[registration.lastUpdateDate]": ["=>", "2023-05-01"]},
                       paging_names={"pagesize": "page[size]", "page": "page[number]"},
                       pagesize=1,
                       data_element='data')
        api_data.streaming = False
        for lei_item in api_data.process():
            print(lei_item)
            assert len(lei_item['attributes']['lei']) == 20
            assert datetime.datetime.strptime(lei_item['attributes']['registration']['lastUpdateDate'], "%Y-%m-%dT%H:%M:%SZ") > datetime.datetime(2023, 5, 1, 0, 0)
        if not 'lei_item' in locals():
            raise AssertionError("No statements produced")
