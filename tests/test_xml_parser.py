import os
import sys
import time
import datetime
from pathlib import Path
import json
from unittest.mock import patch, Mock
import pytest

from bodspipelines.infrastructure.processing.xml_data import XMLData

def validate_datetime(d):
    try:
        datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S%z')
        return True
    except ValueError:
        return False


def validate_date_now(d):
    return d == datetime.date.today().strftime('%Y-%m-%d')


@pytest.fixture
def lei_list():
    return ['001GPB6A9XPE8XJICC14', '004L5FPTUREIWK9T2N63', '00EHHQ2ZHDCFXJCPCL46', '00GBW0Z2GYIER7DHDS71', '00KLB2PFTM3060S2N216',
            '00QDBXDXLLF3W3JJJO36', '00TR8NKAEL48RGTZEW89', '00TV1D5YIV5IDUGWBW29', '00W0SLGGVF0QQ5Q36N03', '00X5RQKJQQJFFX0WPA53']


@pytest.fixture
def lei_xml_data_file():
    return Path("tests/fixtures/lei-data.xml")


@pytest.fixture
def rr_xml_data_file():
    return Path("tests/fixtures/rr-data.xml")


@pytest.fixture
def repex_xml_data_file():
    return Path("tests/fixtures/repex-data.xml")


def test_lei_xml_parser(lei_xml_data_file):
    xml_parser = XMLData(item_tag="LEIRecord",
                         namespace={"lei": "http://www.gleif.org/data/schema/leidata/2016"},
                         filter=['NextVersion', 'Extension'])
    data = xml_parser.process(lei_xml_data_file)
    count = 0
    for item in data:
        if count == 0:
            assert item['LEI'] == '001GPB6A9XPE8XJICC14'
            assert item['Entity']['LegalName'] == 'Fidelity Advisor Leveraged Company Stock Fund'
            assert item['Entity']['OtherEntityNames'] == [{'type': 'PREVIOUS_LEGAL_NAME', 
                'OtherEntityName': 'FIDELITY ADVISOR SERIES I - Fidelity Advisor Leveraged Company Stock Fund'}]
                #'FIDELITY ADVISOR SERIES I - Fidelity Advisor Leveraged Company Stock Fund']
            assert item['Entity']['LegalAddress'] == {'FirstAddressLine': '245 SUMMER STREET', 'City': 'BOSTON', 'Region': 'US-MA',
                                                  'Country': 'US', 'PostalCode': '02210'}
            assert item['Entity']['HeadquartersAddress'] == {'FirstAddressLine': 'C/O Fidelity Management & Research Company LLC',
                                                         'City': 'Boston', 'Region': 'US-MA', 'Country': 'US', 'PostalCode': '02210'} 
            assert item['Entity']['RegistrationAuthority'] == {'RegistrationAuthorityID': 'RA000665', 'RegistrationAuthorityEntityID': 'S000005113'}
            assert item['Entity']['LegalJurisdiction'] == 'US-MA'
            assert item['Entity']['EntityCategory'] == 'FUND' 
            assert item['Entity']['LegalForm'] == {'EntityLegalFormCode': '8888', 'OtherLegalForm': 'FUND'}
            assert item['Entity']['EntityStatus'] == 'ACTIVE'
            assert item['Entity']['EntityCreationDate'] == '2012-11-29T00:00:00.000Z'
            assert item['Registration']['InitialRegistrationDate'] == '2012-11-29T16:33:00.000Z'
            assert item['Registration']['LastUpdateDate'] == '2023-05-18T15:41:20.212Z'
            assert item['Registration']['RegistrationStatus'] == 'ISSUED'
            assert item['Registration']['NextRenewalDate'] == '2024-05-18T15:48:53.604Z'
            assert item['Registration']['ManagingLOU'] == 'EVK05KS7XY1DEII3R011'
            assert item['Registration']['ValidationSources'] == 'FULLY_CORROBORATED'
            assert item['Registration']['ValidationAuthority'] == {'ValidationAuthorityID': 'RA000665', 'ValidationAuthorityEntityID': 'S000005113'}
        count += 1
    assert count == 10


def test_rr_xml_parser(rr_xml_data_file):
    xml_parser = XMLData(item_tag="RelationshipRecord",
                                    namespace={"rr": "http://www.gleif.org/data/schema/rr/2016"},
                                    filter=['Extension'])
    data = xml_parser.process(rr_xml_data_file)
    count = 0
    for item in data:
        if count == 0:
            print(item)
            assert item['Relationship']['StartNode'] == {'NodeID': '001GPB6A9XPE8XJICC14', 'NodeIDType': 'LEI'}
            assert item['Relationship']['EndNode'] == {'NodeID': '5493001Z012YSB2A0K51', 'NodeIDType': 'LEI'}
            assert item['Relationship']['RelationshipType'] == 'IS_FUND-MANAGED_BY'
            assert item['Relationship']['RelationshipPeriods'] == [{'StartDate': '2012-11-29T00:00:00.000Z', 'PeriodType': 'RELATIONSHIP_PERIOD'}]
            assert item['Relationship']['RelationshipStatus'] == 'ACTIVE'
            assert item['Registration']['InitialRegistrationDate'] == '2022-11-14T09:59:48.000Z'
            assert item['Registration']['LastUpdateDate'] == '2023-05-18T15:41:20.212Z'
            assert item['Registration']['RegistrationStatus'] == 'PUBLISHED'
            assert item['Registration']['NextRenewalDate'] == '2024-05-18T15:48:53.604Z'
            assert item['Registration']['ManagingLOU'] == 'EVK05KS7XY1DEII3R011'
            assert item['Registration']['ValidationSources'] == 'FULLY_CORROBORATED'
            assert item['Registration']['ValidationDocuments'] == 'SUPPORTING_DOCUMENTS'
            assert item['Registration']['ValidationReference'] == 'https://www.sec.gov/Archives/edgar/data/722574/000072257422000127/filing4699.htm'
        count += 1
    assert count == 10


def test_repex_xml_parser(repex_xml_data_file):
    xml_parser = XMLData(item_tag="Exception",
                         namespace={"repex": "http://www.gleif.org/data/schema/repex/2016"},
                         filter=['NextVersion', 'Extension'])
    data = xml_parser.process(repex_xml_data_file)
    count = 0
    for item in data:
        if count == 0:
            print(item)
            assert item['LEI'] == '001GPB6A9XPE8XJICC14'
            assert item['ExceptionCategory'] == 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT'
            assert item['ExceptionReason'] == 'NO_KNOWN_PERSON'
        count += 1
    assert count == 10
