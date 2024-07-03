import random
import hashlib
import uuid
import datetime
import dateutil.parser
import pytz
from typing import List, Union
import pycountry

from bodspipelines.infrastructure.caching import cached
from bodspipelines.infrastructure.utils import format_date, current_date_iso, generate_statement_id
from .annotations import (add_lei_annotation, add_rr_annotation_status,
                          add_repex_annotation_reason, add_repex_ooc_annotation)
from .indexes import id_lei as entity_id
from .indexes import id_rr as rr_id
from .indexes import id_repex as repex_id

#def entity_id(data):
#    """Create ID for entity"""
#    return f"{data['LEI']}_{data['Registration']['LastUpdateDate']}"

#def rr_id(data):
#    """Create ID for relationship"""
#    return f"{data['Relationship']['StartNode']['NodeID']}_{data['Relationship']['EndNode']['NodeID']}_{data['Relationship']['RelationshipType']}_{data['Registration']['LastUpdateDate']}"

#def repex_id(data):
#    """Create ID for reporting exception"""
#    if 'ExceptionReference' in data:
#        return f"{data['LEI']}_{data['ExceptionCategory']}_{data['ExceptionReason']}_{data['ExceptionReference']}"
#    else:
#        return f"{data['LEI']}_{data['ExceptionCategory']}_{data['ExceptionReason']}_None"

#async def subject_id(lei):
#    async def get_item(x):
#        return x
#    get_item.__self__ = None
#    data = await cached(get_item, lei, "latest", batch=False)
#    return data['statement_id']

def format_address(address_type, address):
    """Format address structure"""
    address_string = ", ".join([address['FirstAddressLine'], address['City']])
    out = {'type': address_type,'address': address_string, 'country': address['Country']}
    if 'PostalCode' in address: out['postCode'] = address['PostalCode']
    return out

def publication_details():
    """Generate publication details"""
    return {'publicationDate': current_date_iso(), # TODO: fix publication date
            'bodsVersion': "0.2",
            'license': "https://register.openownership.org/terms-and-conditions",
            'publisher': {"name": "OpenOwnership Register",
                          "url": "https://register.openownership.org"}}

def jurisdiction_name(data):
    try:
        if "-" in data['Entity']['LegalJurisdiction']:
            subdivision = pycountry.subdivisions.get(code=data['Entity']['LegalJurisdiction'])
            name = f"{subdivision.name}, {subdivision.country.name}"
        else:
            name = pycountry.countries.get(alpha_2=data['Entity']['LegalJurisdiction']).name
    except AttributeError:
        name = data['Entity']['LegalJurisdiction']
    return name

def transform_lei(data):
    """Transform LEI-CDF v3.1 data to BODS statement"""
    #print("Transforming LEI:")
    statementID = generate_statement_id(entity_id(data), 'entityStatement')
    statementType = 'entityStatement'
    statementDate = format_date(data['Registration']['LastUpdateDate'])
    entityType = 'registeredEntity'
    name = data['Entity']['LegalName']
    country = jurisdiction_name(data)
    #try:
    #    country = pycountry.countries.get(alpha_2=data['Entity']['LegalJurisdiction']).name
    #except AttributeError:
    #    country = data['Entity']['LegalJurisdiction']
    jurisdiction = {'name': country, 'code': data['Entity']['LegalJurisdiction']}
    identifiers = [{'id': data['LEI'], 'scheme':'XI-LEI', 'schemeName':'Global Legal Entity Identifier Index'}]
    if 'RegistrationAuthority' in data['Entity']:
        authority = {}
        if 'RegistrationAuthorityEntityID' in data['Entity']['RegistrationAuthority']: 
            authority['id'] = data['Entity']['RegistrationAuthority']['RegistrationAuthorityEntityID']
        if 'RegistrationAuthorityID' in data['Entity']['RegistrationAuthority']:
            authority['schemeName'] = data['Entity']['RegistrationAuthority']['RegistrationAuthorityID']
        if authority: identifiers.append(authority)
    registeredAddress = format_address('registered', data['Entity']['LegalAddress'])
    businessAddress = format_address('business', data['Entity']['HeadquartersAddress'])
    if 'ValidationSources' in data['Registration']:
        sourceType = ['officialRegister'] if not data['Registration']['ValidationSources'] == 'FULLY_CORROBORATED' else ['officialRegister', 'verified']
    else:
        sourceType = ['officialRegister']
    sourceDescription = 'GLEIF'
    annotations = []
    add_lei_annotation(annotations, data['LEI'], data["Registration"]["RegistrationStatus"])
    out = {'statementID': statementID,
    'statementType': statementType,
    'statementDate': statementDate,
    'entityType': entityType,
    'name': name,
    'incorporatedInJurisdiction': jurisdiction,
    'identifiers': identifiers,
    'addresses': [registeredAddress,businessAddress],
    'annotations': annotations,
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

def calc_statement_id(lei, mapping):
    """Calculate statementID for lei using mapping if available"""
    #print("calc_statement_id:", lei, mapping)
    if lei in mapping:
        return mapping[lei]
    else:
        return generate_statement_id(lei, 'entityStatement')

def transform_rr(data, mapping):
    """Transform RR-CDF v2.1 data to BODS statement"""
    statementID = generate_statement_id(rr_id(data), 'ownershipOrControlStatement')
    statementType = 'ownershipOrControlStatement'
    statementDate = format_date(data['Registration']['LastUpdateDate'])
    subjectDescribedByEntityStatement = calc_statement_id(data['Relationship']['StartNode']['NodeID'], mapping)
    interestedPartyDescribedByEntityStatement = calc_statement_id(data['Relationship']['EndNode']['NodeID'], mapping)
    interestType = 'other-influence-or-control'
    #interestLevel = interest_level(data['Relationship']['RelationshipType'], 'unknown')
    interestLevel = "unknown"
    #periods = data['Relationship']['RelationshipPeriods']
    interestStartDate = False
    interestDetails = f"LEI RelationshipType: {data['Relationship']['RelationshipType']}"
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
    annotations = []
    add_rr_annotation_status(annotations,
                             data['Relationship']['StartNode']['NodeID'],
                             data['Relationship']['EndNode']['NodeID'])
    out = {'statementID': statementID,
           'statementType':statementType,
           'statementDate':statementDate,
           'subject':{'describedByEntityStatement': subjectDescribedByEntityStatement},
           'interestedParty':{'describedByEntityStatement': interestedPartyDescribedByEntityStatement},
           'interests':[{'type': interestType,
                  'interestLevel': interestLevel,
                  'beneficialOwnershipOrControl': beneficialOwnershipOrControl,
                  'startDate': interestStartDate,
                  'details': interestDetails}],
           'annotations': annotations,
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
    statementID = generate_statement_id(repex_id(data), statementType)
    statementDate = format_date(data['ContentDate'])
    #isComponent = 'false' Required?
    unspecified_reason = 'interested-party-exempt-from-disclosure'
    if "ExceptionReference" in data:
        unspecified_description = f"{description} ExemptionReference provided: {data['ExceptionReference']}"
    else:
        unspecified_description = description
    sourceType = ['officialRegister']
    sourceDescription = 'GLEIF'
    annotations = []
    add_repex_annotation_reason(annotations, data["ExceptionReason"], data["LEI"])
    out = {'statementID': statementID,
           'statementType': statementType,
           'statementDate': statementDate,
           'annotations': annotations,
           'publicationDetails': publication_details(),
           'source':{'type':sourceType,'description':sourceDescription}}
    if person:
        out['personType'] = personType
        out['unspecifiedPersonDetails'] = {'reason': unspecified_reason, 'description': unspecified_description}
    else:
        out['entityType'] = entityType
        out['unspecifiedEntityDetails'] = {'reason': unspecified_reason, 'description': unspecified_description}
    return out, statementID

def transform_repex_ooc(data, mapping, interested=None, person=False):
    """Transform Reporting Exception to Ownership or Control statement"""
    statementID = generate_statement_id(repex_id(data), 'ownershipOrControlStatement')
    statementType = 'ownershipOrControlStatement'
    statementDate = format_date(data['ContentDate'])
    #subjectDescribedByEntityStatement = generate_statement_id(data['LEI'], 'entityStatement')
    subjectDescribedByEntityStatement = calc_statement_id(data['LEI'], mapping)
    if interested:
        interestedParty = interested
    else:
        interestedParty = data['ExceptionReason']
    interestType = 'other-influence-or-control'
    annotations = []
    add_repex_ooc_annotation(annotations)
    add_repex_annotation_reason(annotations, data["ExceptionReason"], data["LEI"])
    if data['ExceptionCategory'] == "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT":
        interestLevel = 'indirect'
    elif data['ExceptionCategory'] == "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT":
        interestLevel = 'direct'
    else:
        interestLevel = 'unknown'
    interestDetails = f"LEI ExceptionCategory: {data['ExceptionCategory']}"
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
           'statementDate': statementDate,
           'subject': {'describedByEntityStatement': subjectDescribedByEntityStatement},
           'interestedParty': interestedParty,
           'interests':[{'type': interestType,
                         'interestLevel': interestLevel,
                         'beneficialOwnershipOrControl': False,
                         'details': "A controlling interest."}],
           'annotations': annotations,
           'publicationDetails': publication_details(),
           'source':{'type': sourceType, 'description': sourceDescription}}
    return out, statementID

def transform_repex_no_lei(data, mapping):
    """Transform NO_LEI Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, mapping, interested=statement_id)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NO_LEI`. This parent legal entity does not consent to obtain an LEI or to authorize its “child entity” to obtain an LEI on its behalf.")
        yield statement

def transform_repex_natural_persons(data, mapping):
    """Transform NATURAL_PERSONS Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, mapping, interested=statement_id, person=True)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NATURAL_PERSONS`. An unknown natural person or persons controls an entity.", person=True)
        yield statement

def transform_repex_non_consolidating(data, mapping):
    """Transform NON_CONSOLIDATING Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, mapping, interested=statement_id)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NON_CONSOLIDATING`. The legal entity or entities are not obliged to provide consolidated accounts in relation to the entity they control.")
        yield statement

def transform_repex_non_public(data, mapping):
    """Transform NON_PUBLIC Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, mapping, interested=statement_id)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NON_PUBLIC` or related deprecated values. The legal entity’s relationship information with an entity it controls is non-public. There are therefore obstacles to releasing this information.")
        yield statement

def transform_repex_no_known(data, mapping):
    """Transform NO_KNOWN_PERSON Reporting Exception"""
    for func in (transform_repex_entity, transform_repex_ooc):
        if func == transform_repex_ooc:
            statement, statement_id = func(data, mapping, interested=statement_id, person=True)
        else:
            statement, statement_id = func(data, "From LEI ExemptionReason `NO_KNOWN_PERSON`. There is no known person(s) controlling the entity.", person=True)
        yield statement

def transform_repex(data, mapping):
    """Transform Reporting Exceptions to BODS statements"""
    if data['ExceptionReason'] == "NO_LEI":
        for statement in transform_repex_no_lei(data, mapping):
            yield statement
    elif data['ExceptionReason'] == "NATURAL_PERSONS":
        for statement in transform_repex_natural_persons(data, mapping):
            yield statement
    elif data['ExceptionReason'] == "NON_CONSOLIDATING":
        for statement in transform_repex_non_consolidating(data, mapping):
            yield statement
    elif data['ExceptionReason'] in ('NON_PUBLIC', 'BINDING_LEGAL_COMMITMENTS', 'LEGAL_OBSTACLES',
                                     'DISCLOSURE_DETRIMENTAL', 'DETRIMENT_NOT_EXCLUDED', 'CONSENT_NOT_OBTAINED'):
        for statement in transform_repex_non_public(data, mapping):
            yield statement
    elif data['ExceptionReason'] == "NO_KNOWN_PERSON":
        for statement in transform_repex_no_known(data, mapping):
            yield statement

class Gleif2Bods:
    """Data processor definition class"""
    def __init__(self, identify=None):
        """Initial setup"""
        self.identify = identify

    async def process(self, item, item_type, header, mapping={}, updates=False):
        """Process item"""
        if self.identify: item_type = self.identify(item)
        #print("Gleif2Bods:", item_type)
        if item_type == 'lei':
            yield transform_lei(item)
        elif item_type == 'rr':
            yield transform_rr(item, mapping)
        elif item_type == 'repex':
            for statement in transform_repex(item, mapping):
                yield statement

class AddContentDate:
    """Data processor to add ContentDate"""
    def __init__(self, identify=None):
        """Initial setup"""
        self.identify = identify

    async def process(self, item, item_type, header, mapping={}, updates=False):
        """Process item"""
        if self.identify: item_type = self.identify(item)
        if item_type == 'repex':
            item["ContentDate"] = header["ContentDate"]
        yield item

class RemoveEmptyExtension:
    """Data processor to remove empty Extension"""
    def __init__(self, identify=None):
        """Initial setup"""
        self.identify = identify

    async def process(self, item, item_type, header, mapping={}, updates=False):
        """Process item"""
        if self.identify: item_type = self.identify(item)
        if item_type == 'repex':
            if "Extension" in item and not isinstance(item["Extension"], dict):
                del item["Extension"]
        yield item
