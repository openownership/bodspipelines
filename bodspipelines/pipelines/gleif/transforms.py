import random
import hashlib
import uuid
import datetime
import dateutil.parser
import pytz
from typing import List, Union
import pycountry

def format_address(address_type, address):
    """Format address structure"""
    address_string = ", ".join([address['FirstAddressLine'], address['City']])
    out = {'type': address_type,'address': address_string, 'country': address['Country']}
    if 'PostalCode' in address: out['postCode'] = address['PostalCode']
    return out
def generate_statement_id(name, role, version=None):
    """Generate statement ID deterministically"""
    if version:
        seed = '-'.join([name, role, version])
    else:
        seed = '-'.join([name, role])
    m = hashlib.md5()
    m.update(seed.encode('utf-8'))
    return str(uuid.UUID(m.hexdigest()))

def format_date(d):
    """Format date in ISO 8601"""
    return dateutil.parser.isoparse(d).strftime("%Y-%m-%d") #.isoformat(timespec='seconds')

def current_date_iso():
    """Generate current date in ISO 8601"""
    return datetime.datetime.now(pytz.timezone('Europe/London')).strftime("%Y-%m-%d") #.isoformat(timespec='seconds')

def publication_details():
    """Generate publication details"""
    return {'publicationDate': current_date_iso(), # TODO: fix publication date
            'bodsVersion': "0.2",
            'license': "https://register.openownership.org/terms-and-conditions",
            'publisher': {"name": "OpenOwnership Register",
                          "url": "https://register.openownership.org"}}

def transform_lei(data):
    """Transform LEI-CDF v3.1 data to BODS statement"""
    statementID = generate_statement_id(data['LEI'], 'entityStatement')
    statementType = 'entityStatement'
    statementDate = format_date(data['Registration']['LastUpdateDate'])
    entityType = 'registeredEntity'
    name = data['Entity']['LegalName']
    try:
        country = pycountry.countries.get(alpha_2=data['Entity']['LegalJurisdiction']).name
    except AttributeError:
        country = data['Entity']['LegalJurisdiction']
    jurisdiction = {'name': country, 'code': data['Entity']['LegalJurisdiction']}
    identifiers = [{'id': data['LEI'], 'scheme':'XI-LEI', 'schemeName':'Global Legal Entity Identifier Index'}]
    if 'RegistrationAuthority' in data['Entity']:
        authority = {}
        if 'RegistrationAuthorityEntityID' in data['Entity']['RegistrationAuthority']: 
            authority['id'] = data['Entity']['RegistrationAuthority']['RegistrationAuthorityEntityID']
        if 'RegistrationAuthorityID' in data['Entity']['RegistrationAuthority']:
            authority['schemeName'] = data['Entity']['RegistrationAuthority']['RegistrationAuthorityID']
        if authority: identifiers.append(authority)
        #identifiers.append({'id': data['Entity']['RegistrationAuthority']['RegistrationAuthorityEntityID'],
        #                    'schemeName': data['Entity']['RegistrationAuthority']['RegistrationAuthorityID']})
    #foundingDate = data['Entity']['EntityCreationDate']
    registeredAddress = format_address('registered', data['Entity']['LegalAddress'])
    businessAddress = format_address('business', data['Entity']['HeadquartersAddress'])
    if 'ValidationSources' in data['Registration']:
        sourceType = ['officialRegister'] if not data['Registration']['ValidationSources'] == 'FULLY_CORROBORATED' else ['officialRegister', 'verified']
    else:
        sourceType = ['officialRegister']
    sourceDescription = 'GLEIF'
    out = {'statementID': statementID,
    'statementType': statementType,
    'statementDate': statementDate,
    'entityType': entityType,
    'name': name,
    'incorporatedInJurisdiction': jurisdiction,
    'identifiers': identifiers,
    #'foundingDate': foundingDate,
    'addresses': [registeredAddress,businessAddress],
    'publicationDetails': publication_details(),
    'source': {'type':sourceType,'description':sourceDescription}}
    if 'EntityCreationDate' in data['Entity']: out['foundingDate'] = data['Entity']['EntityCreationDate']
    return out

def interest_level(relationship_type, default):
    """Calculate interest level"""
    if relationship_type == "IS_ULTIMATELY_CONSOLIDATED_BY":
        return "indirect"
    elif relationship_type in ("IS_DIRECTLY_CONSOLIDATED_BY", "IS_INTERNATIONAL_BRANCH_OF", "IS_FUND-MANAGED_BY", "IS_SUBFUND_OF", "IS_FEEDER_TO"):
        return "direct"
    else:
        return default # Other options in data

def transform_rr(data):
    """Transform RR-CDF v2.1 data to BODS statement"""
    statementID = generate_statement_id(data['Relationship']['StartNode']['NodeID'] + 
                                        data['Relationship']['EndNode']['NodeID'] +
                                        data['Relationship']['RelationshipType'], 'ownershipOrControlStatement')
    statementType = 'ownershipOrControlStatement'
    statementDate = format_date(data['Registration']['LastUpdateDate'])
    subjectDescribedByEntityStatement = generate_statement_id(data['Relationship']['EndNode']['NodeID'], 'entityStatement')
    interestedPartyDescribedByEntityStatement = generate_statement_id(data['Relationship']['StartNode']['NodeID'], 'entityStatement')
    #interestType = 'otherInfluenceOrControl'
    interestType = 'other-influence-or-control'
    interestLevel = interest_level(data['Relationship']['RelationshipType'], 'unknown')
    #periods = data['Relationship']['RelationshipPeriods']
    interestStartDate = False
    start_date = False
    if 'RelationshipPeriods' in data['Relationship']:
        periods = data['Relationship']['RelationshipPeriods']
        for period in periods:
            if 'StartDate' in period and 'PeriodType' in period:
                if period['PeriodType'] == "RELATIONSHIP_PERIOD":
                    interestStartDate = period['StartDate']
                else:
                    start_date = period['StartDate']
    if not start_date:
        if not interestStartDate: interestStartDate = ""
    else:
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
           'publicationDetails': publication_details(),
           'source':{'type':sourceType,'description':sourceDescription}}
    if person:
        out['personType'] = personType
        out['unspecifiedPersonDetails'] = {'reason': unspecified_reason, 'description': unspecified_description}
    else:
        out['entityType'] = entityType
        out['unspecifiedEntityDetails'] = {'reason': unspecified_reason, 'description': unspecified_description}
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
    #interestType = 'unknownInterest'
    interestType = 'other-influence-or-control'
    annotation = {'motivation': 'commenting',
                  'description': "The nature of this interest is unknown",
                  'statementPointerTarget': "/interests/0/type",
                  'creationDate': current_date_iso(),
                  'createdBy': {'name': 'Open Ownership',
                                'uri': "https://www.openownership.org"}}
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
        interestedParty = {'unspecified': {'reason': interestedParty}}
    out = {'statementID': statementID,
           'statementType':statementType,
           'subject': {'describedByEntityStatement': subjectDescribedByEntityStatement},
           'interestedParty': interestedParty,
           'interests':[{'type': interestType,
                         'interestLevel': interestLevel,
                         'beneficialOwnershipOrControl': False,
                         'details': "A controlling interest."}],
           'publicationDetails': publication_details(),
           'source':{'type': sourceType, 'description': sourceDescription},
           'annotations': [annotation]}
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
    #print(data['ExceptionReason'])
    if data['ExceptionReason'] == "NO_LEI":
        #print("Got here!")
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

class Gleif2Bods:
    """Data processor definition class"""
    def __init__(self, identify=None):
        """Initial setup"""
        self.identify = identify

    def process(self, item, item_type):
        if self.identify: item_type = self.identify(item)
        if item_type == 'lei':
            yield transform_lei(item)
        elif item_type == 'rr':
            yield transform_rr(item)
        elif item_type == 'repex':
            yield from transform_repex(item)

