# BODS Entity Statement Elasticsearch Properties
entity_statement_properties = {'statementID': {'type': 'text'},
                               'statementType': {'type': 'text'},
                               'statementDate': {'type': 'text'},
                               'entityType': {'type': 'text'},
                               'name': {'type': 'text'},
                               'isComponent': {"type": "boolean"},
                               'incorporatedInJurisdiction': {'type': 'object',
                                                              'properties': {'name': {'type': 'text'},
                                                                             'code': {'type': 'text'}}},
                               'identifiers': {'type': 'object',
                                               'properties': {'id': {'type': 'text'},
                                                              'scheme':  {'type': 'text'},
                                                              'schemeName':  {'type': 'text'}}},
                               'foundingDate': {'type': 'text'},
                               'addresses': {'type': 'object',
                                             'properties': {'type': {'type': 'text'},
                                                            'address': {'type': 'text'},
                                                            'postCode': {'type': 'text'},
                                                            'country': {'type': 'text'}}},
                               'unspecifiedEntityDetails': {'type': 'object',
                                                            'properties': {'reason': {'type': 'text'},
                                                                           'description': {'type': 'text'}}},
                               'publicationDetails': {'type': 'object',
                                                      'properties': {'publicationDate': {'type': 'text'},
                                                                     'bodsVersion': {'type': 'text'},
                                                                     'license': {'type': 'text'},
                                                                     'publisher': {'type': 'object',
                                                                                   'properties': {'name': {'type': 'text'},
                                                                                                  'url': {'type': 'text'}}}}},
                               'source': {'type': 'object',
                                          'properties': {'type': {'type': 'text'},
                                                         'description': {'type': 'text'}}},
                               'annotations': {'type': 'object',
                                               'properties': {'motivation': {'type': 'text'},
                                                              'description': {'type': 'text'},
                                                              'statementPointerTarget': {'type': 'text'},
                                                              'creationDate': {'type': 'text'},
                                                              'createdBy': {'type': 'object',
                                                                            'properties': {'name': {'type': 'text'},
                                                                                           'uri': {'type': 'text'}}}}},
                               'replacesStatements': {'type': 'text'}
                               }


# BODS Entity Statement Elasticsearch Properties
person_statement_properties = {'statementID': {'type': 'text'},
                               'statementType': {'type': 'text'},
                               'statementDate': {'type': 'text'},
                               'personType': {'type': 'text'},
                               'isComponent': {"type": "boolean"},
                               'unspecifiedPersonDetails': {'type': 'object',
                                                            'properties': {'reason': {'type': 'text'},
                                                                           'description': {'type': 'text'}}},
                               'publicationDetails': {'type': 'object',
                                                      'properties': {'publicationDate': {'type': 'text'},
                                                                     'bodsVersion': {'type': 'text'},
                                                                     'license': {'type': 'text'},
                                                                     'publisher': {'type': 'object',
                                                                                   'properties': {'name': {'type': 'text'},
                                                                                                  'url': {'type': 'text'}}}}},
                               'source': {'type': 'object',
                                          'properties': {'type': {'type': 'text'},
                                                         'description': {'type': 'text'}}},
                               'annotations': {'type': 'object',
                                               'properties': {'motivation': {'type': 'text'},
                                                              'description': {'type': 'text'},
                                                              'statementPointerTarget': {'type': 'text'},
                                                              'creationDate': {'type': 'text'},
                                                              'createdBy': {'type': 'object',
                                                                            'properties': {'name': {'type': 'text'},
                                                                                           'uri': {'type': 'text'}}}}},
                               'replacesStatements': {'type': 'text'}
                               }

# BODS Ownership Or Control Statement 
ownership_statement_properties = {'statementID': {'type': 'text'},
                                  'statementType': {'type': 'text'},
                                  'statementDate': {'type': 'text'},
                                  'isComponent': {"type": "boolean"},
                                  'subject': {'type': 'object',
                                              'properties': {'describedByEntityStatement': {'type': 'text'}}},
                                  'interestedParty': {'type': 'object',
                                                      'properties': {'describedByEntityStatement': {'type': 'text'},
                                                                     'describedByPersonStatement': {'type': 'text'},
                                                                     'unspecified': {'type': 'object',
                                                                                     'properties': {'reason': {'type': 'text'}}}}},
                                  'interests': {'type': 'object',
                                                'properties': {'type': {'type': 'text'},
                                                               'interestLevel': {'type': 'text'},
                                                               'beneficialOwnershipOrControl': {'type': 'text'},
                                                               'details': {'type': 'text'},
                                                               'startDate': {'type': 'text'}}},
                                  'publicationDetails': {'type': 'object',
                                                         'properties': {'publicationDate': {'type': 'text'},
                                                                        'bodsVersion': {'type': 'text'},
                                                                        'license': {'type': 'text'},
                                                                        'publisher': {'type': 'object',
                                                                                   'properties': {'name': {'type': 'text'},
                                                                                                  'url': {'type': 'text'}}}}},
                                  'source': {'type': 'object',
                                          'properties': {'type': {'type': 'text'},
                                                         'description': {'type': 'text'}}},
                                  'annotations': {'type': 'object',
                                                  'properties': {'motivation': {'type': 'text'},
                                                                 'description': {'type': 'text'},
                                                                 'statementPointerTarget': {'type': 'text'},
                                                                 'creationDate': {'type': 'text'},
                                                                 'createdBy': {'type': 'object',
                                                                               'properties': {'name': {'type': 'text'},
                                                                                              'uri': {'type': 'text'}}}}},
                                  'replacesStatements': {'type': 'text'}
                                  }


# Additional indexes for managing updates
latest_properties = {'latest_id': {'type': 'text'},
                     'statement_id': {'type': 'text'},
                     'reason': {'type': 'text'}}
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

def match_entity(item):
    return {"match": {"statementID": item["statementID"]}}

def match_person(item):
    return {"match": {"statementID": item["statementID"]}}

def match_ownership(item):
    return {"match": {"statementID": item["statementID"]}}

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

def id_entity(item):
    return item["statementID"]

def id_person(item):
    return item["statementID"]

def id_ownership(item):
    return item["statementID"]

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

# Elasticsearch indexes for BODS data
bods_index_properties = {"entity": {"properties": entity_statement_properties, "match": match_entity, "id": id_entity},
                         "person": {"properties": person_statement_properties, "match": match_person, "id": id_person},
                         "ownership": {"properties": ownership_statement_properties, "match": match_ownership, "id": id_ownership},
                         "latest": {"properties": latest_properties, "match": match_latest, "id": id_latest},
                         "references": {"properties": references_properties, "match": match_references, "id": id_references},
                         "updates": {"properties": updates_properties, "match": match_updates, "id": id_updates},
                         "exceptions": {"properties": exceptions_properties, "match": match_exceptions, "id": id_exceptions},
                         "runs": {"properties": pipeline_run_properties, "match": match_run, "id": id_run}}
