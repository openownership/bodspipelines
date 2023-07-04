import pytest

import datetime

from bodspipelines.pipelines.gleif.transforms import Gleif2Bods, generate_statement_id

def validate_datetime(d):
    try:
        datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S%z')
        return True
    except ValueError:
        return False

def validate_date_now(d):
    return d == datetime.date.today().strftime('%Y-%m-%d')

def test_generate_statement_id():
    """Test the deterministic generation of statement IDs"""
    assert generate_statement_id('2138008UKA1QH5L5XM10','entity') == '9edfc53c-34c7-4e3f-b427-bbbf65806dd3'

@pytest.fixture
def lei_data():
    """Example LEI-CDF v3.1 data"""
    return {'LEI': '097900CAKA0000022867',
            'Entity': {'LegalName': 'Avanea GreenTech j.s.a.',
                       'OtherEntityNames': ['IPM GF XVIII j.s.a.'],
                       'TransliteratedOtherEntityNames': ['Avanea GreenTech j.s.a.'],
                       'LegalAddress': {'FirstAddressLine': 'Mostová 4', 'City': 'Bratislava - mestská časť Staré Mesto', 'Country': 'SK', 'PostalCode': '811 02'}, 
                       'HeadquartersAddress': {'FirstAddressLine': 'Mostová 4', 'City': 'Bratislava - mestská časť Staré Mesto', 'Country': 'SK', 'PostalCode': '811 02'}, 
                       'RegistrationAuthority': {'RegistrationAuthorityID': 'RA000526', 'RegistrationAuthorityEntityID': '53539516'}, 
                       'LegalJurisdiction': 'SK',
                       'EntityCategory': 'GENERAL',
                       'LegalForm': {'EntityLegalFormCode': 'D18J'},
                       'EntityStatus': 'ACTIVE',
                       'EntityCreationDate': '2021-01-09T08:00:00+01:00',
                       'LegalEntityEvents': [{'LegalEntityEventType': 'CHANGE_HQ_ADDRESS',
                                              'LegalEntityEventEffectiveDate': '2021-07-30T08:00:00+02:00',
                                              'LegalEntityEventRecordedDate': '2022-10-25T00:00:00+02:00',
                                              'ValidationDocuments': 'SUPPORTING_DOCUMENTS'},
                                             {'LegalEntityEventType': 'CHANGE_LEGAL_ADDRESS',
                                              'LegalEntityEventEffectiveDate': '2021-07-30T08:00:00+02:00',
                                              'LegalEntityEventRecordedDate': '2022-10-25T00:00:00+02:00',
                                              'ValidationDocuments': 'SUPPORTING_DOCUMENTS'},
                                             {'LegalEntityEventType': 'CHANGE_LEGAL_NAME',
                                              'LegalEntityEventEffectiveDate': '2021-07-30T08:00:00+02:00',
                                              'LegalEntityEventRecordedDate': '2022-10-25T00:00:00+02:00',
                                              'ValidationDocuments': 'SUPPORTING_DOCUMENTS'}]},
            'Registration': {'InitialRegistrationDate': '2021-03-14T00:00:00+01:00',
                             'LastUpdateDate': '2022-10-25T08:00:54.268+02:00',
                             'RegistrationStatus': 'ISSUED',
                             'NextRenewalDate': '2023-10-25T00:00:00+02:00',
                             'ManagingLOU': '097900BEFH0000000217',
                             'ValidationSources': 'FULLY_CORROBORATED',
                             'ValidationAuthority': {'ValidationAuthorityID': 'RA000526', 'ValidationAuthorityEntityID': '53539516'}}}

@pytest.fixture
def rr_data():
    """Example RR-CDF v2.1 data"""
    return {'Relationship': {'StartNode': {'NodeID': '213800WJNSJKGNHNUX19', 'NodeIDType': 'LEI'},
                           'EndNode': {'NodeID': '549300JB5JF78ZEGHF43', 'NodeIDType': 'LEI'},
                           'RelationshipType': 'IS_ULTIMATELY_CONSOLIDATED_BY',
                           'RelationshipPeriods': [{'StartDate': '2018-03-23T00:00:00Z',
                                                    'EndDate': '2018-08-03T20:10:29.927Z',
                                                    'PeriodType': 'ACCOUNTING_PERIOD'},
                                                   {'StartDate': '2018-03-23T00:00:00Z',
                                                    'EndDate': '2018-08-03T20:10:29.927Z',
                                                    'PeriodType': 'RELATIONSHIP_PERIOD'}],
                           'RelationshipStatus': 'NULL',
                           'RelationshipQualifiers': [{'QualifierDimension': 'ACCOUNTING_STANDARD', 'QualifierCategory': 'IFRS'}]},
          'Registration': {'InitialRegistrationDate': '2018-03-23T00:00:00Z',
                           'LastUpdateDate': '2018-03-23T14:07:34.397Z',
                           'RegistrationStatus': 'ANNULLED',
                           'NextRenewalDate': '2019-03-23T00:00:00Z',
                           'ManagingLOU': '213800WAVVOPS85N2205',
                           'ValidationSources': 'ENTITY_SUPPLIED_ONLY',
                           'ValidationDocuments': 'OTHER_OFFICIAL_DOCUMENTS'}}

@pytest.fixture
def repex_data_no_known_person():
    """Example NO_KNOWN_PERSON Reporting Exception"""
    return {'LEI': '029200013A5N6ZD0F605',
            'ExceptionCategory': 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT',
            'ExceptionReason': 'NO_KNOWN_PERSON',
            'NextVersion': '',
            'Extension': ''}

@pytest.fixture
def repex_data_non_consolidating():
    """Example NON_CONSOLIDATING Reporting Exception"""
    return {'LEI': '2138008UKA1QH5L5XM10',
            'ExceptionCategory': 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT',
            'ExceptionReason': 'NON_CONSOLIDATING'}

@pytest.fixture
def repex_data_natural_persons():
    """Example NATURAL_PERSONS Reporting Exception"""
    return {'LEI': '213800RJPV1SI7G2HW19',
            'ExceptionCategory': 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT',
            'ExceptionReason': 'NATURAL_PERSONS'}

@pytest.fixture
def repex_data_non_public():
    """Example NON_PUBLIC Reporting Exception"""
    return {'LEI': '213800ZSKA23GF6L3F24',
            'ExceptionCategory': 'ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT',
            'ExceptionReason': 'NON_PUBLIC'}

@pytest.fixture
def repex_data_no_lei():
    """Example NO_LEI Reporting Exception"""
    return {'LEI': '213800QTG16IUUZ6WD32',
            'ExceptionCategory': 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT',
            'ExceptionReason': 'NO_LEI'}

def test_lei_transform(lei_data):
    """Test transformation of LEI-CDF v3.1 data to BODS entity statement"""
    transform = Gleif2Bods()
    for bods_data in transform.process(lei_data, 'lei'):
        print(bods_data)
        assert bods_data['statementID'] == '659b3c3a-3ac8-b5bd-6480-e56dff141fa9'
        assert bods_data['statementType'] == 'entityStatement'
        assert bods_data['statementDate'] == '2022-10-25'
        assert bods_data['entityType'] == 'registeredEntity'
        assert bods_data['name'] == 'Avanea GreenTech j.s.a.'
        assert bods_data['incorporatedInJurisdiction'] == {'name': 'Slovakia', 'code': 'SK'}
        assert bods_data['identifiers'] == [{'id': '097900CAKA0000022867',
                                             'scheme': 'XI-LEI',
                                             'schemeName': 'Global Legal Entity Identifier Index'}, 
                                            {'id': '53539516', 'schemeName': 'RA000526'}]
        assert bods_data['foundingDate'] == '2021-01-09T08:00:00+01:00'
        assert bods_data['addresses'] == [{'type': 'registered', 'address': 'Mostová 4, Bratislava - mestská časť Staré Mesto', 'postCode': '811 02', 'country': 'SK'}, 
                                      {'type': 'business', 'address': 'Mostová 4, Bratislava - mestská časť Staré Mesto', 'postCode': '811 02', 'country': 'SK'}] 
        assert validate_date_now(bods_data['publicationDetails']['publicationDate'])
        assert bods_data['publicationDetails']['bodsVersion'] == '0.2'
        assert bods_data['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
        assert bods_data['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register',
                                                                'url': 'https://register.openownership.org'}
        assert bods_data['source'] == {'type': ['officialRegister', 'verified'], 'description': 'GLEIF'}
    if not 'bods_data' in locals():
        raise Exception("No statements produced")

def test_rr_transform(rr_data):
    """Test transformation of RR-CDF v2.1 data to BODS ownership or control statement"""
    transform = Gleif2Bods()
    for bods_data in transform.process(rr_data, 'rr'):
        assert bods_data['statementID'] == '7dc21488-bc9e-407c-293b-05578875b12f'
        assert bods_data['statementType'] == 'ownershipOrControlStatement'
        assert bods_data['statementDate'] == '2018-03-23'
        assert bods_data['subject'] == {'describedByEntityStatement': 'c892c662-2904-0c7d-5978-07108d102c33'}
        assert bods_data['interestedParty'] == {'describedByEntityStatement': '7d72f25e-910f-1f28-714d-7a761cbbc5de'}
        assert bods_data['interests'] == [{'type': 'other-influence-or-control',
                                       'interestLevel': 'indirect',
                                       'beneficialOwnershipOrControl': False,
                                       'startDate': '2018-03-23T00:00:00Z'}]
        assert validate_date_now(bods_data['publicationDetails']['publicationDate'])
        assert bods_data['publicationDetails']['bodsVersion'] == '0.2'
        assert bods_data['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
        assert bods_data['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register', 
                                                                'url': 'https://register.openownership.org'}
        assert bods_data['source'] == {'type': ['officialRegister'], 'description': 'GLEIF'}
    if not 'bods_data' in locals():
        raise Exception("No statements produced")

def test_repex_transform_non_consolidating(repex_data_non_consolidating):
    """Test transformation of NON_CONSOLIDATING Reporting Exception data to BODS statements"""
    transform = Gleif2Bods()
    for bods_data in transform.process(repex_data_non_consolidating, 'repex'):
        print(bods_data)
        if bods_data['statementType'] == 'entityStatement':
            assert bods_data['statementID'] == '27b6f7b1-4e16-618c-9a19-3a7ad016ee91'
            assert bods_data['unspecifiedEntityDetails'] == {'reason': 'interested-party-exempt-from-disclosure',
                                                             'description': 'From LEI ExemptionReason `NON_CONSOLIDATING`. The legal entity or entities are not obliged to provide consolidated accounts in relation to the entity they control.'}
            assert bods_data['source'] == {'type': ['officialRegister'], 'description': 'GLEIF'}
            assert bods_data['entityType'] == 'unknownEntity'
            statementID = bods_data['statementID']
        elif bods_data['statementType'] == 'ownershipOrControlStatement':
            assert bods_data['statementID'] == '47ab1442-5eb7-4d09-5745-e6330b89e6ed'
            assert bods_data['subject'] == {'describedByEntityStatement': '5cc33d9b-ee78-a7e1-de3a-8082f06c4798'}
            assert bods_data['interestedParty'] == {'describedByEntityStatement': statementID}
            assert bods_data['interests'] == [{'type': 'other-influence-or-control',
                                               'interestLevel': 'direct',
                                               'beneficialOwnershipOrControl': False,
                                               'details': 'A controlling interest.'}]
            assert validate_date_now(bods_data['publicationDetails']['publicationDate'])
            assert bods_data['publicationDetails']['bodsVersion'] == '0.2'
            assert bods_data['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
            assert bods_data['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register', 'url': 'https://register.openownership.org'}
            assert bods_data['source'] == {'type': ['officialRegister'], 'description': 'GLEIF'}
        else:
            raise Exception(f"Incorrect statement type produced: {bods_data['statementType']}")
    if not 'bods_data' in locals():
        raise Exception("No statements produced")

def test_repex_transform_natural_persons(repex_data_natural_persons):
    """Test transformation of NATURAL_PERSONS Reporting Exception data to BODS statements"""
    transform = Gleif2Bods()
    for bods_data in transform.process(repex_data_natural_persons, 'repex'):
        print(bods_data)
        if bods_data['statementType'] == 'personStatement':
            assert bods_data['statementID'] == '12186e42-9384-8a9e-f7b3-279a399770fe'
            assert bods_data['unspecifiedPersonDetails'] == {'reason': 'interested-party-exempt-from-disclosure',
                                                             'description': 'From LEI ExemptionReason `NATURAL_PERSONS`. An unknown natural person or persons controls an entity.'}
            assert bods_data['source'] == {'type': ['officialRegister'], 'description': 'GLEIF'}
            assert bods_data['personType'] == 'unknownPerson'
            statementID = bods_data['statementID']
        elif bods_data['statementType'] == 'ownershipOrControlStatement':
            assert bods_data['statementID'] == 'd897e79f-6f03-f800-6fd0-495298cbea6a'
            assert bods_data['subject'] == {'describedByEntityStatement': 'e579dc8d-8f7d-90bc-099a-38fa175bd494'}
            assert bods_data['interestedParty'] == {'describedByPersonStatement': statementID}
            assert bods_data['interests'] == [{'type': 'other-influence-or-control',
                                               'interestLevel': 'direct',
                                               'beneficialOwnershipOrControl': False,
                                               'details': 'A controlling interest.'}]
            assert validate_date_now(bods_data['publicationDetails']['publicationDate'])
            assert bods_data['publicationDetails']['bodsVersion'] == '0.2'
            assert bods_data['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
            assert bods_data['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register',
                                                                     'url': 'https://register.openownership.org'}
            assert bods_data['source'] == {'type': ['officialRegister'], 'description': 'GLEIF'}
        else:
            raise Exception(f"Incorrect statement type produced: {bods_data['statementType']}")
    if not 'bods_data' in locals():
        raise Exception("No statements produced")

def test_repex_transform_non_public(repex_data_non_public):
    """Test transformation of NON_PUBLIC Reporting Exception data to BODS statements"""
    transform = Gleif2Bods()
    for bods_data in transform.process(repex_data_non_public, 'repex'):
        print(bods_data)
        if bods_data['statementType'] in ('entityStatement', 'personStatement'):
            assert bods_data['statementID'] == '428aca2a-b344-78a8-4450-6cc726f26ecf'
            if bods_data['statementType'] == 'entityStatement':
                unspecified_type = 'unspecifiedEntityDetails'
            else:
                unspecified_type = 'unspecifiedPersonDetails'
            assert bods_data[unspecified_type] == {'reason': 'interested-party-exempt-from-disclosure',
                                                   'description': 'From LEI ExemptionReason `NON_PUBLIC` or related deprecated values. The legal entity’s relationship information with an entity it controls is non-public. There are therefore obstacles to releasing this information.'}
            assert validate_date_now(bods_data['publicationDetails']['publicationDate'])
            assert bods_data['publicationDetails']['bodsVersion'] == '0.2'
            assert bods_data['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
            assert bods_data['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register', 
                                                                    'url': 'https://register.openownership.org'}
            assert bods_data['source'] == {'type': ['officialRegister'], 'description': 'GLEIF'}
            if bods_data['statementType'] == 'entityStatement':
                assert bods_data['entityType'] == 'unknownEntity'
            else:
                assert bods_data['entityType'] == 'unknownPerson'
            statementID = bods_data['statementID']
        elif bods_data['statementType'] == 'ownershipOrControlStatement':
            assert bods_data['statementID'] == '984398d2-adf1-9111-f461-62f3101ff41d'
            assert bods_data['subject'] == {'describedByEntityStatement': 'fc0205b0-dcf7-cedd-c071-fbd4708149f0'}
            assert bods_data['interestedParty'] == {'describedByEntityStatement': statementID}
            assert bods_data['interests'] == [{'type': 'other-influence-or-control',
                                               'interestLevel': 'indirect',
                                               'beneficialOwnershipOrControl': False,
                                               'details': 'A controlling interest.'}]
            assert validate_date_now(bods_data['publicationDetails']['publicationDate'])
            assert bods_data['publicationDetails']['bodsVersion'] == '0.2'
            assert bods_data['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
            assert bods_data['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register',
                                                                    'url': 'https://register.openownership.org'}
            assert bods_data['source'] == {'type': ['officialRegister'], 'description': 'GLEIF'}
        else:
            raise Exception(f"Incorrect statement type produced: {bods_data['statementType']}")
    if not 'bods_data' in locals():
        raise Exception("No statements produced")
