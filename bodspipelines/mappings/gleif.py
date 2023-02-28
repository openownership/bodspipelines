# GLEIF LEI Elasticsearch Properties
lei_properties = {'LEI': {'type': 'text'},
              'Entity': {'type': 'object',
                         'properties': {'LegalName': {'type': 'text'},
                                        'TransliteratedOtherEntityNames': {'type': 'object',
                                                                           'properties': {'TransliteratedOtherEntityName': {'type': 'text'}}},
                                        'LegalAddress': {'type': 'object',
                                                         'properties': {'FirstAddressLine': {'type': 'text'},
                                                                        'AdditionalAddressLine': {'type': 'text'},
                                                                         'City': {'type': 'text'},
                                                                         'Region': {'type': 'text'},
                                                                         'Country': {'type': 'text'},
                                                                         'PostalCode': {'type': 'text'}}},
                                        'HeadquartersAddress': {'type': 'object',
                                                                'properties': {'FirstAddressLine': {'type': 'text'},
                                                                               'AdditionalAddressLine': {'type': 'text'},
                                                                               'City': {'type': 'text'},
                                                                               'Region': {'type': 'text'},
                                                                               'Country': {'type': 'text'},
                                                                               'PostalCode': {'type': 'text'}}},
                                        'RegistrationAuthority': {'type': 'object',
                                                                  'properties': {'RegistrationAuthorityID': {'type': 'text'},
                                                                                 'RegistrationAuthorityEntityID': {'type': 'text'}}},
                                        'LegalJurisdiction': {'type': 'text'},
                                        'EntityCategory': {'type': 'text'},
                                        'EntityCreationDate': {'type': 'text'},
                                        'LegalForm': {'type': 'object',
                                                      'properties': {'EntityLegalFormCode': {'type': 'text'},
                                                                     'OtherLegalForm': {'type': 'text'}}},
                                        'EntityStatus': {'type': 'text'}}},
              'Registration': {'type': 'object',
                               'properties': {'InitialRegistrationDate': {'type': 'text'},
                                              'LastUpdateDate': {'type': 'text'},
                                              'RegistrationStatus': {'type': 'text'},
                                              'NextRenewalDate': {'type': 'text'},
                                              'ManagingLOU': {'type': 'text'},
                                              'ValidationSources': {'type': 'text'},
                                              'ValidationAuthority': {'type': 'object',
                                                                      'properties': {'ValidationAuthorityID': {'type': 'text'},
                                                                                     'ValidationAuthorityEntityID': {'type': 'text'}}}}}}

rr_properties = {'Relationship': {'type': 'object', 
                                   'properties': {'StartNode': {'type': 'object', 
                                                                'properties': {'NodeID': {'type': 'text'}, 
                                                                               'NodeIDType': {'type': 'text'}}}, 
                                                  'EndNode': {'type': 'object', 
                                                              'properties': {'NodeID': {'type': 'text'}, 
                                                                             'NodeIDType': {'type': 'text'}}}, 
                                                  'RelationshipType': {'type': 'text'}, 
                                                  'RelationshipPeriods': {'type': 'object',
                                                                          'properties': {'StartDate': {'type': 'text'}, 
                                                                                         'EndDate': {'type': 'text'}, 
                                                                                         'PeriodType': {'type': 'text'}}}, 
                                                  'RelationshipStatus': {'type': 'text'}, 
                                                  'RelationshipQualifiers': {'type': 'object',
                                                                             'properties': {'QualifierDimension': {'type': 'text'}, 
                                                                                            'QualifierCategory': {'type': 'text'}}}}}, 
                  'Registration': {'type': 'object', 
                                   'properties': {'InitialRegistrationDate': {'type': 'text'}, 
                                                  'LastUpdateDate': {'type': 'text'}, 
                                                  'RegistrationStatus': {'type': 'text'}, 
                                                  'NextRenewalDate': {'type': 'text'}, 
                                                  'ManagingLOU': {'type': 'text'}, 
                                                  'ValidationSources': {'type': 'text'}, 
                                                  'ValidationDocuments': {'type': 'text'}, 
                                                  'ValidationReference': {'type': 'text'}}}}

repex_properties = {'LEI': {'type': 'text'}, 
                    'ExceptionCategory': {'type': 'text'}, 
                    'ExceptionReason': {'type': 'text'}}

def match_lei(item):
    return {"match": {"LEI": item["LEI"]}}

def match_rr(item):
    return {'bool': {'must': [{"match": {'Relationship.StartNode.NodeID': item['Relationship']['StartNode']['NodeID']}}, 
                              {"match": {'Relationship.EndNode.NodeID': item['Relationship']['EndNode']['NodeID']}}, 
                              {"match": {'Relationship.RelationshipType': item['Relationship']['RelationshipType']}}]}}
#{"bool": {"must": [{"term": {'Relationship.StartNode.NodeID': item['Relationship']['StartNode']['NodeID']}},
#                              {"term": {'Relationship.EndNode.NodeID': item['Relationship']['EndNode']['NodeID']}},
#                              {"term": {'Relationship.RelationshipType': item['Relationship']['RelationshipType']}}]}}

def match_repex(item):
    return {'bool': {'must': [{"match": {'LEI': item["LEI"]}},
                              {"match": {'ExceptionCategory': item["ExceptionCategory"]}}, 
                              {"match": {'ExceptionReason': item["ExceptionReason"]}}]}}
#{"bool": {"must": [{"term": {'ExceptionCategory': item["ExceptionCategory"]}}, 
#                              {"term": {'ExceptionReason': item["ExceptionReason"]}}, 
#                              {"term": {'LEI': item["LEI"]}}]}}
