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

def build_names(name, data):
    """Build givenName/familyName from data"""
    if "surname" in data:
        name["familyName"] = data["surname"]
    elif "fullname" in data:
        name["familyName"] = data["fullname"].split()[0]
    else:
        name["familyName"] = ""
    given = []
    for part in ("firstname", "middlename"):
        if part in data:
            given.append(data[part])
    if given:
        name["givenName"] = " ".join(given)
    elif "fullname" in data:
        name["givenName"] = " ".join(data["fullname"].split()[:-1])
    else:
        name["givenName"] = ""

def build_name(data, name_type):
    """Build name structure from data"""
    name = {}
    if isinstance(data, dict) and data:
        name = {}
        name["type"] = name_type
        name["fullName"] = data["fullname"] if "fullname" in data else ""
        build_names(name, data)
        #if data["fullname"]:
        #    name["familyName"] = data["surname"] if "surname" in data else data["surname"].split()[-1]
        #    name["givenName"] = data["firstname"] if "firstname" in data else data["fullname"].split()[0]
        #else:
        #    name["familyName"] = data["surname"] if "surname" in data else ""
        #    name["givenName"] = data["firstname"] if "firstname" in data else ""
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
    if "type" in address: address_type = address["type"]
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
    source_retrieved = source.retrived_date(data)
    return {"type": sourceType,
            "assertedBy": [sourceDescription],
            "url": sourceURL,
            #"retrievedAt": source_retrieved
           }

def record_status(record_id, _):
    """new updated closed"""

def transform_entity(source, data, record_status):
    """Transform into BODS v0.4 entity"""
    recordID = source.record_id(data, 'entity')
    declarationSubject = source.declaration_subject(data)
    updated = source.item_updated(data)
    statementDate = format_date(updated)
    statementID = generate_statement_id(source.statement_id(data, 'entity'),
                                        'entityStatement')
    recordType = 'entity'
    recordStatus = record_status
    entityType = 'registeredEntity'
    entity_details = source.entity_details(data)
    name = source.name(data, 'entity')
    alternate_names = source.alternate_names(data, 'entity')
    jurisdiction_country = source.jurisdiction(data)
    if jurisdiction_country:
        jurisdiction = {'name': jurisdiction_name(jurisdiction_country),
                        'code': jurisdiction_country}
    else:
        jurisdiction = {}
    scheme_identifier = source.identifier(data, 'entity')
    #scheme_url = source.scheme_url(data)
    if scheme_identifier:
        scheme, scheme_name, scheme_url = source.scheme(data, 'entity')
        identifier = {'id': scheme_identifier,
                      'scheme': scheme,
                      'schemeName': scheme_name}
        if scheme_url: identifier['uri'] = scheme_url
        identifiers = [identifier]
    else:
        identifiers = []
    identifiers += source.additional_identifiers(data)
    registeredAddress = format_address('registered', source.registered_address(data))
    businessAddress = format_address('business', source.business_address(data))
    creation_date = source.creation_date(data)
    creation = format_date(creation_date) if creation_date else None
    dissolution_date = source.dissolution_date(data)
    dissolution = format_date(dissolution_date) if dissolution_date else None
    has_public_listing = source.has_public_listing(data)
    source_data = data_source(data, source)
    annotations = []
    entity_status = source.entity_status(data)
    registration_status = source.registration_status(data)
    entity_name = source.entity_name
    entity_link = source.item_link(data, 'entity')
    if registration_status:
        add_entity_annotation(annotations, entity_name, "Registration Status", registration_status, entity_link)
    if entity_status:
        add_entity_annotation(annotations, entity_name, "Entity Status", entity_status, entity_link)
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
                     "alternateNames": alternate_names,
                     "jurisdiction": jurisdiction,
                     "identifiers": identifiers,
                     "foundingDate": creation,
                     "addresses": build_addresses(registeredAddress, businessAddress),
                     #"formedByStatute": ,
                     },
                 'annotations': annotations,
                 'publicationDetails': publication_details(),
                 'source': source_data
                 }
    if entity_details: statement["recordDetails"]["entityType"]["details"] = entity_details
    if dissolution: statement["recordDetails"]["dissolutionDate"] = dissolution
    if entity_link: statement["recordDetails"]["uri"] = entity_link
    if not has_public_listing is None:
        statement["recordDetails"]["publicListing"] = {"hasPublicListing": has_public_listing}
    return statement

def transform_person(source, data, record_status):
    """Transform into BODS v0.4 person"""
    #print("Building person")
    recordID = source.record_id(data, 'person')
    declarationSubject = source.declaration_subject(data)
    updated = source.item_updated(data)
    statementDate = format_date(updated)
    statementID = generate_statement_id(source.statement_id(data, 'person'),
                                        'personStatement')
    recordType = 'person'
    recordStatus = record_status
    entityType = 'registeredEntity'
    name = build_name(source.name(data, 'person'), 'legal')
    #country = jurisdiction_name(source.jurisdiction(data))
    #jurisdiction = {'name': country, 'code': source.jurisdiction(data)}
    #identifier = source.person_identifier(data)
    scheme_identifier = source.identifier(data, 'person')
    if scheme_identifier:
        scheme, scheme_name, scheme_url = source.scheme(data, 'person')
        identifier = {'id': scheme_identifier,
                      'scheme': scheme,
                      'schemeName': scheme_name}
        if scheme_url: identifier['uri'] = scheme_url
        identifiers = [identifier]
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

def build_interests(source, data, data_type):
    if data_type == "relationship":
        #print("Interests:", source.create_interested_party(data), data)
        interests = []
        interest_data = source.interest_types(data)
        for interest_type in interest_data:
            interest = {
                "directOrIndirect": source.interest_level(data),
                "type": interest_type,
                "beneficialOwnershipOrControl": True if source.create_interested_party(data) == "person" else False,
                "startDate": source.interest_start_date(data),
                "details": source.interest_details(data)
                }
            if any([interest_data[interest_type][val_name] for val_name in interest_data[interest_type]]):
                interest["share"] = {}
            for val_name in interest_data[interest_type]:
                if not interest_data[interest_type][val_name] is None:
                    interest["share"][val_name] = interest_data[interest_type][val_name]
            interests.append(interest)
        return interests
    else:
        return [{
            "directOrIndirect": source.interest_level(data),
            "type": "otherInfluenceOrControl",
            "beneficialOwnershipOrControl": False,
            "details": source.interest_details(data)
           }]

def transform_relationship(source, data, record_status):
    """Transform into BODS v0.4 relationship"""
    #print("Building relationship")
    recordID = source.record_id(data, 'relationship')
    declarationSubject = source.declaration_subject(data)
    updated = source.item_updated(data)
    statementDate = format_date(updated)
    statementID = generate_statement_id(source.statement_id(data, 'relationship'),
                                        'relationshipStatement')
    recordType = 'relationship'
    recordStatus = record_status
    subject = source.relationship_subject(data)
    interestedParty = source.relationship_interested_party(data)
    interests = build_interests(source, data, "relationship")
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
                     "interests": interests,
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
    statementID = generate_statement_id(source.statement_id(data, 'exception'),
                                        'relationshipStatement')
    recordType = 'relationship'
    recordStatus = record_status
    subject = source.relationship_subject(data)
    interestedParty = source.relationship_interested_party(data)
    interests = build_interests(source, data, "exception")
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
                     "interests": interests,
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
