# BODS 0.4 Elasticsearch Properties

# Entity Type
entity_type = {'type': 'object',
               'properties': {'type': {'type': 'text'},
                              'subtype': {'type': 'text'},
                              'details': {'type': 'text'}}}

# Unspecified or unknown person or entity
unspecified_details = {'type': 'object',
                       'properties': {'reason': {'type': 'text'},
                                      'description': {'type': 'text'}}}

# Jurisdiction
jurisdiction = {'type': 'object',
                        'properties': {'name': {'type': 'text'},
                                       'code': {'type': 'text'}}}

# Identifier
identifier = {'type': 'object',
                      'properties': {'id': {'type': 'text'},
                                     'scheme': {'type': 'text'},
                                     'schemeName': {'type': 'text'},
                                     'uri': {'type': 'text'}}}
# Country
country = {'type': 'object',
                   'properties': {'name': {'type': 'text'},
                                  'code': {'type': 'text'}}}

# Address
address = {'type': 'object',
                   'properties': {'type': {'type': 'text'},
                                  'address': {'type': 'text'},
                                  'postCode': {'type': 'text'},
                                  'country': country}}

# Security
security = {'type': 'object',
                    'properties': {'idScheme': {'type': 'text'},
                                   'idstring': {'type': 'text'},
                                   'ticker': {'type': 'text'}}}

# Securities Listing
securities_listing = {'type': 'object',
                      'properties': {'marketIdentifierCode': {'type': 'text'},
                                     'operatingMarketIdentifierCode': {'type': 'text'},
                                     'stockExchangeJurisdiction': {'type': 'text'},
                                     'stockExchangeName': {'type': 'text'},
                                     'security': security}}

# Public Listing
public_listing = {'type': 'object',
                   'properties': {'hasPublicListing': {"type": "boolean"},
                                  'companyFilingsURLs': {'type': 'text'},
                                  'securitiesListings': securities_listing}}

# Formed by Statute
formed_by_statute = {'type': 'object',
                     'properties': {'name': {'type': 'text'},
                                    'date': {'type': 'text'}}}

# Entity Record Details
entity_record_details = {'type': 'object',
                         'properties': {'isComponent': {"type": "boolean"},
                                        'entityType': entity_type,
                                        'unspecifiedEntityDetails': unspecified_details,
                                        'name': {'type': 'text'},
                                        'alternateNames': {'type': 'text'},
                                        'jurisdiction': jurisdiction,
                                        'identifiers': identifier,
                                        'foundingDate': {'type': 'text'},
                                        'dissolutionDate': {'type': 'text'},
                                        'addresses': address,
                                        'uri': {'type': 'text'},
                                        'publicListing': public_listing,
                                        'formedByStatute': formed_by_statute}}

# Created By
created_by = {'type': 'object',
                      'properties': {'name': {'type': 'text'},
                                     'uri': {'type': 'text'}}}

# Annotation
annotation = {'type': 'object',
                      'properties': {'statementPointerTarget': {'type': 'text'},
                                     'creationDate': {'type': 'text'},
                                     'createdBy': created_by,
                                     'motivation': {'type': 'text'},
                                     'description': {'type': 'text'},
                                     'transformedContent': {'type': 'text'},
                                     'url': {'type': 'text'}}}

# Publisher
publisher = {'type': 'object',
                     'properties': {'name': {'type': 'text'},
                                    'url': {'type': 'text'}}}

# Publication Details
publication_details = {'type': 'object',
                       'properties': {'publicationDate': {'type': 'text'},
                                      'bodsVersion': {'type': 'text'},
                                      'license': {'type': 'text'},
                                      'publisher': publisher}}

# Asserted By
asserted_by = {'type': 'object',
               'properties': {'name': {'type': 'text'},
                              'uri': {'type': 'text'}}}

# Source
source = {'type': 'object',
          'properties': {'type': {'type': 'text'},
                         'description': {'type': 'text'},
                         'url': {'type': 'text'},
                         'retrievedAt': {'type': 'text'},
                         'assertedBy': asserted_by}}

# Name
person_name = {'type': 'object',
               'properties': {'type': {'type': 'text'},
                              'fullName': {'type': 'text'},
                              'familyName': {'type': 'text'},
                              'givenName': {'type': 'text'},
                              'patronymicName': {'type': 'text'}}}

# PEP Status Details
pep_status_details = {'type': 'object',
                      'properties': {'reason': {'type': 'text'},
                                     'missingInfoReason': {'type': 'text'},
                                     'jurisdiction': jurisdiction,
                                     'startDate': {'type': 'text'},
                                     'endDate': {'type': 'text'},
                                     'source': source}}

# Political Exposure
political_exposure = {'type': 'object',
                      'properties': {'status': {'type': 'text'},
                                     'details': pep_status_details}}

# Person Record Details
person_record_details = {'type': 'object',
                         'properties': {'isComponent': {"type": "boolean"},
                                        'personType': {'type': 'text'},
                                        'unspecifiedPersonDetails': unspecified_details,
                                        'names': person_name,
                                        'identifiers': identifier,
                                        'nationalities': country,
                                        'placeOfBirth': address,
                                        'birthDate': {'type': 'text'},
                                        'deathDate': {'type': 'text'},
                                        'taxResidencies': country,
                                        'addresses': address,
                                        'politicalExposure': political_exposure}}

# Percentage Share
percentage_share = {'type': 'object',
                    'properties': {'exact': {"type": "float"},
                                   'maximum': {"type": "float"},
                                   'minimum': {"type": "float"},
                                   'exclusiveMinimum': {"type": "float"},
                                   'exclusiveMaximum': {"type": "float"}}}

# Interest
interest = {'type': 'object',
            'properties': {'type': {'type': 'text'},
                           'directOrIndirect': {'type': 'text'},
                           'beneficialOwnershipOrControl': {"type": "boolean"},
                           'details': {'type': 'text'},
                           'share': percentage_share,
                           'startDate': {'type': 'text'},
                           'endDate': {'type': 'text'}}}

# Relationship Record Details
relationship_record_details = {'type': 'object',
                               'properties': {'isComponent': {"type": "boolean"},
                               'componentRecords': {'type': 'text'},
                               'subject': {'type': 'text'},
                               'subject_unspecified': unspecified_details, # Handle unspecified subject
                               'interestedParty': {'type': 'text'},
                               'interestedParty_unspecified': unspecified_details, # Handle unspecified interested
                               'interests': interest}}

# BODS Entity Statement
entity_statement_properties = {'statementId': {'type': 'text'},
                               'statementDate': {'type': 'text'},
                               'declaration': {'type': 'text'},
                               'declarationSubject': {'type': 'text'},
                               'recordId': {'type': 'text'},
                               'recordType': {'type': 'text'},
                               'recordStatus': {'type': 'text'},
                               'recordDetails': entity_record_details,
                               'annotations': annotation,
                               'publicationDetails': publication_details,
                               'source': source}

# BODS Person Statement
person_statement_properties = {'statementId': {'type': 'text'},
                               'statementDate': {'type': 'text'},
                               'declaration': {'type': 'text'},
                               'declarationSubject': {'type': 'text'},
                               'recordId': {'type': 'text'},
                               'recordType': {'type': 'text'},
                               'recordStatus': {'type': 'text'},
                               'recordDetails': person_record_details,
                               'annotations': annotation,
                               'publicationDetails': publication_details,
                               'source': source}

# BODS Relationship Statement
relationship_statement_properties = {'statementId': {'type': 'text'},
                               'statementDate': {'type': 'text'},
                               'declaration': {'type': 'text'},
                               'declarationSubject': {'type': 'text'},
                               'recordId': {'type': 'text'},
                               'recordType': {'type': 'text'},
                               'recordStatus': {'type': 'text'},
                               'recordDetails': relationship_record_details,
                               'annotations': annotation,
                               'publicationDetails': publication_details,
                               'source': source}


# Additional indexes for managing updates
latest_properties = {'latest_id': {'type': 'text'},
                     'statement_id': {'type': 'text'},
                     'record_id': {'type': 'text'}}

references_properties = {'statement_id': {'type': 'text'},
                         'references_id': {'type': 'object',
                                          'properties': {'statement_id': {'type': 'text'},
                                                         'latest_id': {'type': 'text'}}}
                         }
#updates_properties = {'referencing_id': {'type': 'text'},
#                      'old_statement_id': {'type': 'text'},
#                      'new_statement_id': {'type': 'text'}}
updates_properties = {'referencing_id': {'type': 'text'},
                      'latest_id': {'type': 'text'},
                      'updates': {'type': 'object',
                                          'properties': {'old_statement_id': {'type': 'text'},
                                                         'new_statement_id': {'type': 'text'}}}
                                 #{'type': 'text'}
                     }

exceptions_properties = {'latest_id': {'type': 'text'},
                         'statement_id': {'type': 'text'},
                         'other_id': {'type': 'text'},
                         'reason': {'type': 'text'},
                         'reference': {'type': 'text'},
                         'entity_type': {'type': 'text'}}

# Properties for logging pipeline runs
pipeline_run_properties = {'stage_name': {'type': 'text'},
                           'start_timestamp': {"type": "text"},
                           'end_timestamp': {"type": "text"}}

# Latest record
record_properties = {'record_id': {'type': 'text'},
                     'latest_record_id': {'type': 'text'},
                     'record_status': {'type': 'text'}}

# Records to close
closed_properties = {'statement_id': {'type': 'text'},
                     'record_id': {'type': 'text'},
                     'statement_date': {'type': 'text'}}

def match_entity(item):
    return {"match": {"statementId": item["statementId"]}}

def match_person(item):
    return {"match": {"statementId": item["statementId"]}}

def match_relationship(item):
    return {"match": {"statementId": item["statementId"]}}

def match_latest(item):
    return {"match": {"latest_id": item["latest_id"]}}

def match_references(item):
    return {"match": {"statement_id": item["statement_id"]}}

def match_updates(item):
    return {"match": {"old_statement_id": item["old_statement_id"]}}

def match_exceptions(item):
    return {"match": {"latest_id": item["latest_id"]}}

def match_run(item):
    return {"match": {"end_timestamp": item["end_timestamp"]}}

def match_record(item):
    return {"match": {"record_id": item["record_id"]}}

def match_closed(item):
    return {"match": {"statement_id": item["statement_id"]}}

def id_entity(item):
    return item["statementId"]

def id_person(item):
    return item["statementId"]

def id_relationship(item):
    return item["statementId"]

def id_latest(item):
    return item["latest_id"]

def id_references(item):
    return item["statement_id"]

def id_updates(item):
    return item["referencing_id"]

def id_exceptions(item):
    return item["latest_id"]

def id_run(item):
    return item["end_timestamp"]

def id_record(item):
    return item["record_id"]

def id_closed(item):
    return item["statement_id"]

# Elasticsearch indexes for BODS data
bods_index_properties = {"entity": {"properties": entity_statement_properties,
                                    "match": match_entity,
                                    "id": id_entity},
                         "person": {"properties": person_statement_properties,
                                    "match": match_person,
                                    "id": id_person},
                         "relationship": {"properties": relationship_statement_properties,
                                          "match": match_relationship,
                                          "id": id_relationship},
                         "latest": {"properties": latest_properties, "match": match_latest, "id": id_latest},
                         "references": {"properties": references_properties, "match": match_references, "id": id_references},
                         "updates": {"properties": updates_properties, "match": match_updates, "id": id_updates},
                         "exceptions": {"properties": exceptions_properties, "match": match_exceptions, "id": id_exceptions},
                         "runs": {"properties": pipeline_run_properties, "match": match_run, "id": id_run},
                         "records": {"properties": record_properties, "match": match_record, "id": id_record},
                         "closed": {"properties": closed_properties, "match": match_closed, "id": id_closed}}
