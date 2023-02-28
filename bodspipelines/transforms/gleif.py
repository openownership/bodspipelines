import random
import hashlib
import uuid
import datetime
import pytz
from typing import List, Union
from dataclasses import dataclass

def format_address(address_type, address):
    """Format address structure"""
    address_string = ", ".join([address['FirstAddressLine'], address['City']])
    return {'type': address_type,'address': address_string, 'postCode': address['PostalCode'], 'country': address['Country']}

def generate_statement_id(name, role, version=None):
    """Generate statement ID deterministically"""
    if version:
        seed = '-'.join([name, role, version])
    else:
        seed = '-'.join([name, role])
    m = hashlib.md5()
    m.update(seed.encode('utf-8'))
    return str(uuid.UUID(m.hexdigest()))

def format_datetime(d):
    """Format datetime in ISO 8601"""
    if d[-1] == "Z":
        return datetime.datetime.fromisoformat(d[:-1]).isoformat(timespec='seconds') + "+00:00"
    else:
        return datetime.datetime.fromisoformat(d).isoformat(timespec='seconds')

def current_datetime_iso():
    """Generate current datetime in ISO 8601"""
    return datetime.datetime.now(pytz.timezone('Europe/London')).isoformat(timespec='seconds')

def publication_details():
    """Generate publication details"""
    return {'publicationDate': current_datetime_iso(), # TODO: fix publication date
            'bodsVersion': "0.2",
            'license': "https://register.openownership.org/terms-and-conditions",
            'publisher': {"name": "OpenOwnership Register",
                          "url": "https://register.openownership.org"}}

def transform_lei(data):
    """Transform LEI-CDF v3.1 data to BODS statement"""
    statementID = generate_statement_id(data['LEI'], 'entityStatement')
    statementType = 'entityStatement'
    statementDate = format_datetime(data['Registration']['LastUpdateDate'])
    entityType = 'registeredEntity'
    name = data['Entity']['LegalName']
    jurisdiction = data['Entity']['LegalJurisdiction']
    identifiers = [{'id': data['LEI'], 'scheme':'XI-LEI', 'schemeName':'Global Legal Entity Identifier Index'}]
    foundingDate = data['Entity']['EntityCreationDate']
    registeredAddress = format_address('registered', data['Entity']['LegalAddress'])
    businessAddress = format_address('business', data['Entity']['HeadquartersAddress'])
    sourceType = ['officialRegister'] if not data['Registration']['ValidationSources'] == 'FULLY_CORROBORATED' else ['officialRegister', 'verified']
    sourceDescription = 'GLEIF'
    out = {'statementID': statementID,
    'statementType': statementType,
    'statementDate': statementDate,
    'entityType': entityType,
    'name': name,
    'jurisdiction': jurisdiction,
    'identifiers': identifiers,
    'foundingDate': foundingDate,
    'addresses': [registeredAddress,businessAddress],
    'publicationDetails': publication_details(),
    'source': {'type':sourceType,'description':sourceDescription}}
    return out

def interest_level(relationship_type, default):
    """Calculate interest level"""
    if relationship_type == "IS_ULTIMATELY_CONSOLIDATED_BY":
        return "indirect"
    elif relationship_type == "IS_DIRECTLY_CONSOLIDATED_BY":
        return "direct"
    else:
        return default # Other options in data

def transform_rr(data):
    """Transform RR-CDF v2.1 data to BODS statement"""
    statementID = generate_statement_id(data['Relationship']['StartNode']['NodeID'] + 
                                        data['Relationship']['EndNode']['NodeID'] +
                                        data['Relationship']['RelationshipType'], 'ownershipOrControlStatement')
    statementType = 'ownershipOrControlStatement'
    statementDate = format_datetime(data['Registration']['LastUpdateDate'])
    subjectDescribedByEntityStatement = generate_statement_id(data['Relationship']['EndNode']['NodeID'], 'entityStatement')
    interestedPartyDescribedByEntityStatement = generate_statement_id(data['Relationship']['StartNode']['NodeID'], 'entityStatement')
    interestType = 'otherInfluenceOrControl'
    interestLevel = interest_level(data['Relationship']['RelationshipType'], 'unknown')
    periods = data['Relationship']['RelationshipPeriods']
    interestStartDate = False
    for period in periods:
        if period['PeriodType'] == "RELATIONSHIP_PERIOD":
            interestStartDate = period['StartDate']
        else:
            start_date = period['StartDate']
    if not interestStartDate: interestStartDate = start_date
    beneficialOwnershipOrControl = False
    sourceType = ['officialRegister'] if not data['Registration']['ValidationSources'] == 'FULLY_CORROBORATED' else ['officialRegister', 'verified']
    sourceDescription = 'GLEIF'
    out = {'statementID': statementID,
           'statementType':statementType,
           'statementDate':statementDate,
           'subject':{'describedByEntityStatement': subjectDescribedByEntityStatement},
           'interestedParty':{'describedByEntityStatement': interestedPartyDescribedByEntityStatement},
           'interests':[{'type': interestType,
                  'interestLevel': interestLevel,
                  'beneficialOwnershipOrControl': beneficialOwnershipOrControl,
                  'startDate': interestStartDate}],
           'publicationDetails': publication_details(),
           'source':{'type': sourceType, 'description': sourceDescription}}
    return out

def transform_repex_entity(data, description, person=False):
    """Transform Reporting Exception to Entity/Person statement"""
    if person:
        statementType = 'personStatement'
        personType = 'unknownPerson'
    else:
        statementType = 'entityStatement'
        entityType = "unknownEntity"
    statementID = generate_statement_id(data['LEI'] + data['ExceptionCategory'] + data['ExceptionReason'], statementType)
    #isComponent = 'false' Required?
    unspecified_reason = 'interested-party-exempt-from-disclosure'
    unspecified_description = description
    sourceType = ['officialRegister']
    sourceDescription = 'GLEIF'
    out = {'statementID': statementID,
           'statementType': statementType,
           'unspecifiedPersonDetails': {'reason': unspecified_reason, 'description': unspecified_description},
           'publicationDetails': publication_details(),
           'source':{'type':sourceType,'description':sourceDescription}}
    if person:
        out['personType'] = personType
    else:
        out['entityType'] = entityType
    return out, statementID

def transform_repex_ooc(data, interested=None, person=False):
    """Transform Reporting Exception to Ownership or Control statement"""
    statementID = generate_statement_id(data['LEI'] + data['ExceptionCategory'] + data['ExceptionReason'], 'ownershipOrControlStatement')
    statementType = 'ownershipOrControlStatement'
    subjectDescribedByEntityStatement = generate_statement_id(data['LEI'], 'entityStatement')
    if interested:
        interestedParty = interested
    else:
        interestedParty = data['ExceptionReason']
    interestType = 'unknownInterest'
    if data['ExceptionCategory'] == "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT":
        interestLevel = 'indirect'
    elif data['ExceptionCategory'] == "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT":
        interestLevel = 'direct'
    else:
        interestLevel = 'unknown'
    sourceType = ['officialRegister']
    sourceDescription = 'GLEIF'
    if interested:
        if person:
            interestedParty = {'describedByPersonStatement': interestedParty}
        else:
            interestedParty = {'describedByEntityStatement': interestedParty}
    else:
        interestedParty = {'unspecified': interestedParty}
    out = {'statementID': statementID,
           'statementType':statementType,
           'subject': {'describedByEntityStatement': subjectDescribedByEntityStatement},
           'interestedParty': {'unspecified': interestedParty},
           'interests':[{'type': interestType,
                         'interestLevel': interestLevel,
                         'beneficialOwnershipOrControl': False,
                         'details': "A controlling interest."}],
           'publicationDetails': publication_details(),
           'source':{'type': sourceType, 'description': sourceDescription}}
    return out, statementID

def transform_repex_no_lei(data):
    """Transform NO_LEI Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, interested=statement_id)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NO_LEI`. This parent legal entity does not consent to obtain an LEI or to authorize its “child entity” to obtain an LEI on its behalf.")
        yield statement

def transform_repex_natural_persons(data):
    """Transform NATURAL_PERSONS Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, interested=statement_id, person=True)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NATURAL_PERSONS`. An unknown natural person or persons controls an entity.", person=True)
        yield statement

def transform_repex_non_consolidating(data):
    """Transform NON_CONSOLIDATING Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, interested=statement_id)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NON_CONSOLIDATING`. The legal entity or entities are not obliged to provide consolidated accounts in relation to the entity they control.")
        yield statement

def transform_repex_non_public(data):
    """Transform NON_PUBLIC Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, interested=statement_id)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NON_PUBLIC` or related deprecated values. The legal entity’s relationship information with an entity it controls is non-public. There are therefore obstacles to releasing this information.")
        yield statement

def transform_repex(data):
    """Transform Reporting Exceptions to BODS statements"""
    print(data['ExceptionReason'])
    if data['ExceptionReason'] == "NO_LEI":
        print("Got here!")
        for statement in transform_repex_no_lei(data):
            yield statement
    elif data['ExceptionReason'] == "NATURAL_PERSONS":
        for statement in transform_repex_natural_persons(data):
            yield statement
    elif data['ExceptionReason'] == "NON_CONSOLIDATING":
        for statement in transform_repex_non_consolidating(data):
            yield statement
    elif data['ExceptionReason'] in ('NON_PUBLIC', 'BINDING_LEGAL_COMMITMENTS', 'LEGAL_OBSTACLES',
                                     'DISCLOSURE_DETRIMENTAL', 'DETRIMENT_NOT_EXCLUDED', 'DETRIMENT_NOT_EXCLUDED'):
        for statement in transform_repex_non_public(data):
            yield statement

@dataclass
class Gleif2Bods:
    """Data processor definition class"""
    #name: str

    def process(self, item, item_type):
        if item_type == 'lei2':
            yield transform_lei(item)
        elif item_type == 'rr':
            yield transform_rr(item)
        elif item_type == 'repex':
            yield from transform_repex(item)

