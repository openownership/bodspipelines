import pycountry

from bodspipelines.infrastructure.utils import format_date, current_date_iso, generate_statement_id
from bodspipelines.infrastructure.bods.annotations import add_entity_annotation

def add_address_part(address_str, address_part):
    """Add address part to string"""
    if address_str:
        address_str = f"{address_str}, {address_part}"
    else:
        address_str = address_part
    return address_str

def build_name(data, name_type):
    name = {}
    if isinstance(data, dict) and "fullname" in data:
        name = {}
        name["type"] = name_type
        name["fullName"] = data["fullname"]
        if data["fullname"]:
            name["familyName"] = data["surname"] if "surname" in data else data["surname"].split()[-1]
            name["givenName"] = data["firstname"] if "firstname" in data else data["fullname"].split()[0]
        else:
            name["familyName"] = data["surname"] if "surname" in data else ""
            name["givenName"] = data["firstname"] if "firstname" in data else ""
        #name["patronymicName"] =
        return name
    else:
        return None

def build_address_string(address):
    """Build address string"""
    address_str = ""
    if "address1" in address:
        address_str = add_address_part(address_str, address["address1"])
    if "address2" in address:
        address_str = add_address_part(address_str, address["address2"])
    if "city" in address:
        address_str = add_address_part(address_str, address["city"])
    if "region" in address:
        if len(address["region"]) == 5 and "-" in address["region"]:
            try:
                subdivision = pycountry.subdivisions.get(code=address["region"])
                name = subdivision.name
            except AttributeError:
                name = address["region"]
        else:
            name = address["region"]
        address_str = add_address_part(address_str, name)
    return address_str

def format_address(address_type, address):
    """Format address structure"""
    #print("Address:", address)
    if not address:
        return None
    address_string = build_address_string(address)
    if len(address['country']) == 2:
        try:
            country_name = pycountry.countries.get(alpha_2=address['country']).name
            country_code = address['country']
        except AttributeError:
            country_name = address['country']
            country_code = ""
    else:
        country_name = address['country']
        country_code = ""
    out = {'type': address_type,
           'address': address_string,
           'country': {"name": country_name,
                       "code": country_code}}
    if 'postcode' in address: out['postCode'] = address['postcode']
    return out

def build_addresses(registered, business):
    """Build addresses object"""
    addresses = []
    if registered: addresses.append(registered)
    if business: addresses.append(business)
    return addresses

def publication_details():
    """Generate publication details"""
    return {'publicationDate': current_date_iso(), # TODO: fix publication date
            'bodsVersion': "0.4",
            'license': "https://creativecommons.org/publicdomain/zero/1.0/",
            'publisher': {"name": "Open Ownership",
                          "url": "https://www.openownership.org"}}

def jurisdiction_name(jurisdiction):
    """Get juristriction name"""
    try:
        if "-" in jurisdiction:
            subdivision = pycountry.subdivisions.get(code=jurisdiction)
            name = f"{subdivision.name}, {subdivision.country.name}"
        else:
            name = pycountry.countries.get(alpha_2=jurisdiction).name
    except AttributeError:
        name = jurisdiction
    return name

def country_from_code(code):
    """Get Country object from code"""
    if code == "XK":
        name = "Kosova"
    elif code == "XX":
        name = "Stateless"
    else:
        name = pycountry.countries.get(alpha_2=code).name
    return {"name": name, "code": code}

def data_source(data, source):
    """Build data source"""
    sourceType = source.source_type(data)
    sourceDescription = source.source_description
    sourceURL = source.source_url
    return {"type": sourceType,
            "assertedBy": [{"name": sourceDescription}],
            "url": sourceURL}

def record_status(record_id, _):
    """new updated closed"""

def transform_entity(source, data, record_status):
    """Transform into BODS v0.4 entity"""
    recordID = source.record_id(data, 'entity')
    declarationSubject = source.declaration_subject(data)
    updated = source.item_updated(data)
    statementDate = format_date(updated)
    statementID = generate_statement_id(f"{recordID}-{updated}", 'entityStatement')
    recordType = 'entity'
    recordStatus = record_status
    entityType = 'registeredEntity'
    name = source.name(data, 'entity')
    country = jurisdiction_name(source.jurisdiction(data))
    jurisdiction = {'name': country, 'code': source.jurisdiction(data)}
    identifiers = [{'id': source.identifier(data),
                    'scheme': source.scheme,
                    'schemeName': source.scheme_name}]
    identifiers += source.additional_identifiers(data)
    registeredAddress = format_address('registered', source.registered_address(data))
    businessAddress = format_address('business', source.business_address(data))
    creation_date = source.creation_date(data)
    creation = format_date(creation_date) if creation_date else None
    source_data = data_source(data, source)
    annotations = []
    source_status = source.status(data)
    entity_name = source.entity_name
    add_entity_annotation(annotations, entity_name, source_status)
    statement = {"statementId": statementID,
                 "declarationSubject": declarationSubject,
                 "statementDate": statementDate,
                 "recordId": recordID,
                 "recordStatus": recordStatus,
                 "recordType": recordType,
                 "recordDetails": {
                     "isComponent": False,
                     "entityType": {
                         "type": entityType
                     },
                     #"unspecifiedEntityDetails": ,
                     "name": name,
                     "alternateNames": [],
                     "jurisdiction": jurisdiction,
                     "identifiers": identifiers,
                     "foundingDate": creation,
                     #"dissolutionDate": ,
                     "addresses": build_addresses(registeredAddress, businessAddress)
                     #"uri": ,
                     #"publicListing": ,
                     #"formedByStatute": ,
                     },
                 'annotations': annotations,
                 'publicationDetails': publication_details(),
                 'source': source_data
                 }
    return statement

def transform_person(source, data, record_status):
    """Transform into BODS v0.4 person"""
    #print("Building person")
    recordID = source.record_id(data, 'person')
    declarationSubject = source.declaration_subject(data)
    updated = source.item_updated(data)
    statementDate = format_date(updated)
    statementID = generate_statement_id(f"{recordID}-{updated}", 'personStatement')
    recordType = 'person'
    recordStatus = record_status
    entityType = 'registeredEntity'
    name = build_name(source.name(data, 'person'), 'legal')
    country = jurisdiction_name(source.jurisdiction(data))
    jurisdiction = {'name': country, 'code': source.jurisdiction(data)}
    identifier = source.person_identifier(data)
    if identifier:
        identifiers = [{'id': source.identifier(data),
                    'scheme': source.scheme,
                    'schemeName': source.scheme_name}]
    else:
        identifiers = []
    identifiers += source.additional_identifiers(data)
    registeredAddress = format_address('registered', source.registered_address(data))
    nationalities = source.person_nationalities(data)
    placeOfBirthAddress = source.person_place_of_birth(data)
    birthDate = source.person_birth_date(data)
    deathDate = source.person_death_date(data)
    taxResidencies = source.person_tax_residency(data)
    source_data = data_source(data, source)
    annotations = []
    #source_status = source.status(data)
    #add_entity_annotation(annotations, entity_name, source_status)
    statement = {"statementId": statementID,
                 "declarationSubject": declarationSubject,
                 "statementDate": statementDate,
                 "recordId": recordID,
                 "recordStatus": recordStatus,
                 "recordType": recordType,
                 "recordDetails": {
                     "isComponent": False,
                     "personType": source.person_type(data),
                     #"unspecifiedPersonDetails":
                     "names": [name] if name else [],
                     "identifiers": identifiers,
                     "nationalities": [country_from_code(code) for code in nationalities],
                     "taxResidencies": [country_from_code(code) for code in taxResidencies],
                     "addresses": build_addresses(registeredAddress, None),
                     #"politicalExposure": 
                     },
                 'annotations': annotations,
                 'publicationDetails': publication_details(),
                 'source': source_data
                 }
    if placeOfBirthAddress:
        statement["recordDetails"]["placeOfBirthAddress"] = placeOfBirthAddress
    if birthDate:
        statement["recordDetails"]["birthDate"] = birthDate
    if deathDate:
        statement["recordDetails"]["deathDate"] =deathDate
    return statement

def build_interest(source, data, data_type):
    if data_type == "relationship":
        return {
            "directOrIndirect": source.interest_level(data),
            "type": "otherInfluenceOrControl",
            "beneficialOwnershipOrControl": False,
            "startDate": source.interest_start_date(data),
            "details": source.interest_details(data)
           }
    else:
        return {
            "directOrIndirect": source.interest_level(data),
            "type": "otherInfluenceOrControl",
            "beneficialOwnershipOrControl": False,
            "details": source.interest_details(data)
           }

def transform_relationship(source, data, record_status):
    """Transform into BODS v0.4 relationship"""
    #print("Building relationship")
    recordID = source.record_id(data, 'relationship')
    declarationSubject = source.declaration_subject(data)
    updated = source.item_updated(data)
    statementDate = format_date(updated)
    statementID = generate_statement_id(f"{recordID}-{updated}", 'relationshipStatement')
    recordType = 'relationship'
    recordStatus = record_status
    subject = source.relationship_subject(data)
    interestedParty = source.relationship_interested_party(data)
    interest = build_interest(source, data, "relationship")
    source_data = data_source(data, source)
    annotations = []
    statement = {"statementId": statementID,
                 "declarationSubject": declarationSubject,
                 "statementDate": statementDate,
                 "recordId": recordID,
                 "recordStatus": recordStatus,
                 "recordType": recordType,
                 "recordDetails": {
                     "subject": subject,
                     "interestedParty": interestedParty,
                     "interests": [interest],
                     "isComponent": False
                     },
                 'annotations': annotations,
                 'publicationDetails': publication_details(),
                 'source': source_data
                 }
    return statement

def transform_exception(source, data, record_status):
    """Transform exception into BODS v0.4 relationship"""
    recordID = source.record_id(data, 'relationship')
    declarationSubject = source.declaration_subject(data)
    updated = source.item_updated(data)
    statementDate = format_date(updated)
    statementID = generate_statement_id(f"{recordID}-{updated}", 'relationshipStatement')
    recordType = 'relationship'
    recordStatus = record_status
    subject = source.relationship_subject(data)
    interestedParty = source.relationship_interested_party(data)
    interest = build_interest(source, data, "exception")
    source_data = data_source(data, source)
    annotations = []
    statement = {"statementId": statementID,
                 "declarationSubject": declarationSubject,
                 "statementDate": statementDate,
                 "recordId": recordID,
                 "recordStatus": recordStatus,
                 "recordType": recordType,
                 "recordDetails": {
                     "subject": subject,
                     "interestedParty": interestedParty,
                     "interests": [interest],
                     "isComponent": False
                     },
                 'annotations': annotations,
                 'publicationDetails': publication_details(),
                 'source': source_data
                 }
    return statement

def transform_item(source, item, status):
    if not source.skip_item(item):
        item_type = source.identify_item(item)
        if item_type == 'entity':
            yield transform_entity(source, item, status)
        elif item_type == 'relationship':
            interested = source.create_interested_party(item)
            #print("create_interested_party:", interested)
            if interested == "person":
                yield transform_person(source, item, status)
            elif interested == "entity":
                yield transform_entity(source, item, status)
            yield transform_relationship(source, item, status)
        elif item_type == 'exception':
            yield transform_exception(source, item, status)
    #else:
    #    yield None

class BodsTransforms:
    """Data processor definition class"""
    def __init__(self, identify=None):
        """Initial setup"""
        self.identify = identify

    async def process(self, item, item_type, header, mapping={}, updates=False):
        """Process item"""
        if self.identify: item_type = self.identify(item)
        #print("Gleif2Bods:", item_type)
        if item_type == 'entity':
            yield transform_entity(item)
        elif item_type == 'relationship':
            create_interested_party
            yield transform_relationship(item, mapping)
        elif item_type == 'exception':
            for statement in transform_repex(item, mapping):
                yield statement
