import os
import sys
import time
import datetime
import json
from pathlib import Path
import json
from unittest.mock import patch, Mock
import asyncio
import pytest

from bodspipelines.infrastructure.processing.xml_data import XMLData

def validate_datetime(d):
    """Test is valid datetime"""
    try:
        datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S%z')
        return True
    except ValueError:
        return False


def validate_date_now(d):
    """Test is today's date"""
    return d == datetime.date.today().strftime('%Y-%m-%d')


@pytest.fixture
def lei_list():
    """List entity LEIs"""
    return ['001GPB6A9XPE8XJICC14', '004L5FPTUREIWK9T2N63', '00EHHQ2ZHDCFXJCPCL46', '00GBW0Z2GYIER7DHDS71', '00KLB2PFTM3060S2N216',
            '00QDBXDXLLF3W3JJJO36', '00TR8NKAEL48RGTZEW89', '00TV1D5YIV5IDUGWBW29', '00W0SLGGVF0QQ5Q36N03', '00X5RQKJQQJFFX0WPA53']


@pytest.fixture
def lei_xml_data_file():
    """GLEIF LEI XML data"""
    return Path("tests/fixtures/lei-data.xml")


@pytest.fixture
def rr_xml_data_file():
    """GLEIF RR XML data"""
    return Path("tests/fixtures/rr-data.xml")


@pytest.fixture
def repex_xml_data_file():
    """GLEIF Repex XML data"""
    return Path("tests/fixtures/repex-data.xml")


@pytest.mark.asyncio
async def test_lei_xml_parser(lei_xml_data_file):
    """Test XML parser on GLEIF LEI data"""
    xml_parser = XMLData(item_tag="LEIRecord",
                         namespace={"lei": "http://www.gleif.org/data/schema/leidata/2016"},
                         filter=['NextVersion', 'Extension'])
    data = xml_parser.process(lei_xml_data_file)
    count = 0
    async for header, item in data:
        if count == 0:
            assert item['LEI'] == '001GPB6A9XPE8XJICC14'
            assert item['Entity']['LegalName'] == 'Fidelity Advisor Leveraged Company Stock Fund'
            assert item['Entity']['OtherEntityNames'] == [{'type': 'PREVIOUS_LEGAL_NAME', 
                'OtherEntityName': 'FIDELITY ADVISOR SERIES I - Fidelity Advisor Leveraged Company Stock Fund'}]
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
        elif count == 10:
            assert item['LEI'] == '1595D0QCK7Y15293JK84'
            assert item['Entity']['LegalName'] == 'GALAPAGOS CONSERVATION TRUST'
            assert item['Entity']['LegalAddress'] == {'FirstAddressLine': '7-14 Great Dover Street', 'City': 'London', 'Country': 'GB', 'PostalCode': 'SE1 4YR'}
            assert item['Entity']['HeadquartersAddress'] == {'FirstAddressLine': '7-14 Great Dover Street', 'City': 'London', 'Country': 'GB', 
                                                             'PostalCode': 'SE1 4YR'}
            assert item['Entity']['RegistrationAuthority'] == {'RegistrationAuthorityID': 'RA000585', 'RegistrationAuthorityEntityID': '03004112'}
            assert item['Entity']['LegalJurisdiction'] == 'GB'
            assert item['Entity']['EntityCategory'] == 'GENERAL'
            assert item['Entity']['LegalForm'] == {'EntityLegalFormCode': 'G12F'}
            assert item['Entity']['EntityStatus'] == 'ACTIVE'
            assert item['Entity']['EntityCreationDate'] == '1994-12-21T00:00:00+01:00'
            assert item['Registration']['InitialRegistrationDate'] == '2023-02-13T22:13:11+01:00'
            assert item['Registration']['LastUpdateDate'] == '2023-03-10T13:08:56+01:00'
            assert item['Registration']['RegistrationStatus'] == 'ISSUED'
            assert item['Registration']['NextRenewalDate'] == '2024-02-13T22:13:11+01:00'
            assert item['Registration']['ManagingLOU'] == '98450045AN5EB5FDC780'
            assert item['Registration']['ValidationSources'] == 'FULLY_CORROBORATED'
            assert item['Registration']['ValidationAuthority'] == {'ValidationAuthorityID': 'RA000585', 'ValidationAuthorityEntityID': '03004112'}
            assert item['Registration']['OtherValidationAuthorities'] == [{'ValidationAuthorityID': 'RA000589', 'ValidationAuthorityEntityID': '1043470'}]
        elif count == 11:
            assert item['LEI'] == '213800FERQ5LE3H7WJ58' 
            assert item['Entity']['LegalName'] == 'DENTSU INTERNATIONAL LIMITED'
            assert item['Entity']['OtherEntityNames'] == [{'type': 'PREVIOUS_LEGAL_NAME', 'OtherEntityName': 'DENTSU AEGIS NETWORK LTD.'}, 
                                                          {'type': 'PREVIOUS_LEGAL_NAME', 'OtherEntityName': 'AEGIS GROUP PLC'}]
            assert item['Entity']['LegalAddress'] == {'FirstAddressLine': '10 TRITON STREET', 'AdditionalAddressLine': "REGENT'S PLACE", 
                                                      'City': 'LONDON', 'Region': 'GB-LND', 'Country': 'GB', 'PostalCode': 'NW1 3BF'} 
            assert item['Entity']['HeadquartersAddress'] == {'FirstAddressLine': '10 TRITON STREET', 'AdditionalAddressLine': "REGENT'S PLACE", 
                                                             'City': 'LONDON', 'Region': 'GB-LND', 'Country': 'GB', 'PostalCode': 'NW1 3BF'}
            assert item['Entity']['RegistrationAuthority'] == {'RegistrationAuthorityID': 'RA000585', 'RegistrationAuthorityEntityID': '01403668'}
            assert item['Entity']['LegalJurisdiction'] == 'GB'
            assert item['Entity']['EntityCategory'] == 'GENERAL'
            assert item['Entity']['LegalForm'] == {'EntityLegalFormCode': 'H0PO'}
            assert item['Entity']['EntityStatus'] == 'ACTIVE'
            assert item['Entity']['EntityCreationDate'] == '1978-12-05T00:00:00Z'
            assert item['Registration']['InitialRegistrationDate'] == '2014-02-10T00:00:00Z'
            assert item['Registration']['LastUpdateDate'] == '2023-02-02T09:07:52.390Z'
            assert item['Registration']['RegistrationStatus'] == 'ISSUED'
            assert item['Registration']['NextRenewalDate'] == '2024-02-17T00:00:00Z'
            assert item['Registration']['ManagingLOU'] == '213800WAVVOPS85N2205'
            assert item['Registration']['ValidationSources'] == 'FULLY_CORROBORATED'
            assert item['Registration']['ValidationAuthority'] == {'ValidationAuthorityID': 'RA000585', 'ValidationAuthorityEntityID': '01403668'}
        elif count == 12:
            assert item['LEI'] == '213800BJPX8V9HVY1Y11'
            assert item['Entity']['LegalName'] == 'Swedeit Italian Aktiebolag'
            assert item['Entity']['LegalAddress'] == {'FirstAddressLine': 'C/O Anita Lindberg', 'MailRouting': 'C/O Anita Lindberg', 
                                                      'AdditionalAddressLine': 'Fortgatan 11', 'City': 'Västra Frölunda', 'Region': 'SE-O', 
                                                      'Country': 'SE', 'PostalCode': '426 76'}
            assert item['Entity']['HeadquartersAddress'] == {'FirstAddressLine': 'C/O Anita Lindberg', 'MailRouting': 'C/O Anita Lindberg', 
                                                             'AdditionalAddressLine': 'Fortgatan 11', 'City': 'Västra Frölunda', 'Region': 'SE-O', 
                                                             'Country': 'SE', 'PostalCode': '426 76'}
            assert {'FirstAddressLine': 'C/O Anita Lindberg', 'MailRouting': 'C/O Anita Lindberg', 'AdditionalAddressLine': 'Fortgatan 11', 
                    'City': 'Vastra Frolunda', 'Region': 'SE-O', 'Country': 'SE', 'PostalCode': '426 76', 
                    'type': 'AUTO_ASCII_TRANSLITERATED_LEGAL_ADDRESS'} in item['Entity']['TransliteratedOtherAddresses']
            assert {'FirstAddressLine': 'C/O Anita Lindberg', 'MailRouting': 'C/O Anita Lindberg', 'AdditionalAddressLine': 'Fortgatan 11', 
                    'City': 'Vastra Frolunda', 'Region': 'SE-O', 'Country': 'SE', 'PostalCode': '426 76', 
                    'type': 'AUTO_ASCII_TRANSLITERATED_HEADQUARTERS_ADDRESS'} in item['Entity']['TransliteratedOtherAddresses'] 
            assert item['Entity']['RegistrationAuthority'] == {'RegistrationAuthorityID': 'RA000544', 'RegistrationAuthorityEntityID': '556543-1193'}
            assert item['Entity']['LegalJurisdiction'] == 'SE'
            assert item['Entity']['EntityCategory'] == 'GENERAL'
            assert item['Entity']['LegalForm'] == {'EntityLegalFormCode': 'XJHM'}
            assert item['Entity']['EntityStatus'] == 'ACTIVE'
            assert item['Entity']['EntityCreationDate'] == '1997-06-05T02:00:00+02:00'
            assert item['Registration']['InitialRegistrationDate'] == '2014-04-09T00:00:00Z'
            assert item['Registration']['LastUpdateDate'] == '2023-04-25T13:18:00Z'
            assert item['Registration']['RegistrationStatus'] == 'ISSUED'
            assert item['Registration']['NextRenewalDate'] == '2024-05-12T06:59:39Z'
            assert item['Registration']['ManagingLOU'] == '549300O897ZC5H7CY412'
            assert item['Registration']['ValidationSources'] == 'FULLY_CORROBORATED'
            assert item['Registration']['ValidationAuthority'] == {'ValidationAuthorityID': 'RA000544', 'ValidationAuthorityEntityID': '556543-1193'}
        count += 1
    assert count == 13


@pytest.mark.asyncio
async def test_rr_xml_parser(rr_xml_data_file):
    """Test XML parser on GLEIF RR data"""
    xml_parser = XMLData(item_tag="RelationshipRecord",
                                    namespace={"rr": "http://www.gleif.org/data/schema/rr/2016"},
                                    filter=['Extension'])
    data = xml_parser.process(rr_xml_data_file)
    count = 0
    async for header, item in data:
        if count == 0:
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


@pytest.mark.asyncio
async def test_repex_xml_parser(repex_xml_data_file):
    """Test XML parser on GLEIF Repex data"""
    xml_parser = XMLData(item_tag="Exception",
                         namespace={"repex": "http://www.gleif.org/data/schema/repex/2016"},
                         filter=['NextVersion', 'Extension'])
    data = xml_parser.process(repex_xml_data_file)
    count = 0
    async for header, item in data:
        if count == 0:
            print(item)
            assert item['LEI'] == '001GPB6A9XPE8XJICC14'
            assert item['ExceptionCategory'] == 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT'
            assert item['ExceptionReason'] == 'NO_KNOWN_PERSON'
        count += 1
    assert count == 10
