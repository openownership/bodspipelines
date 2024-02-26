import hashlib

# GLEIF LEI Elasticsearch Properties
lei_properties = {'LEI': {'type': 'text'},
              'Entity': {'type': 'object',
                         'properties': {'LegalName': {'type': 'text'},
                                        'OtherEntityNames': #{'type': 'text'},
                                                            {'type': 'object',
                                                             'properties': {'OtherEntityName': {'type': 'text'},
                                                                            'type': {'type': 'text'}}},
                                        'TransliteratedOtherEntityNames': {'type': 'object',
                                                                           'properties': {'TransliteratedOtherEntityName': {'type': 'text'},
                                                                                          'type': {'type': 'text'}}},
                                        'LegalAddress': {'type': 'object',
                                                         'properties': {'FirstAddressLine': {'type': 'text'},
                                                                        'AdditionalAddressLine': {'type': 'text'},
                                                                        'AddressNumber': {'type': 'text'},
                                                                        'AddressNumberWithinBuilding': {'type': 'text'},
                                                                        'MailRouting': {'type': 'text'},
                                                                         'City': {'type': 'text'},
                                                                         'Region': {'type': 'text'},
                                                                         'Country': {'type': 'text'},
                                                                         'PostalCode': {'type': 'text'}}},
                                        'HeadquartersAddress': {'type': 'object',
                                                                'properties': {'FirstAddressLine': {'type': 'text'},
                                                                               'AdditionalAddressLine': {'type': 'text'},
                                                                               'AddressNumber': {'type': 'text'},
                                                                               'AddressNumberWithinBuilding': {'type': 'text'},
                                                                               'MailRouting': {'type': 'text'},
                                                                               'City': {'type': 'text'},
                                                                               'Region': {'type': 'text'},
                                                                               'Country': {'type': 'text'},
                                                                               'PostalCode': {'type': 'text'}}},
                                        'OtherAddresses': {'type': 'object',
                                                                'properties': {'FirstAddressLine': {'type': 'text'},
                                                                               'AdditionalAddressLine': {'type': 'text'},
                                                                               'AddressNumber': {'type': 'text'},
                                                                               'AddressNumberWithinBuilding': {'type': 'text'},
                                                                               'MailRouting': {'type': 'text'},
                                                                               'City': {'type': 'text'},
                                                                               'Region': {'type': 'text'},
                                                                               'Country': {'type': 'text'},
                                                                               'PostalCode': {'type': 'text'},
                                                                               'type': {'type': 'text'}}},
                                        'TransliteratedOtherAddresses': {'type': 'object',
                                                                'properties': {'FirstAddressLine': {'type': 'text'},
                                                                               'AdditionalAddressLine': {'type': 'text'},
                                                                               'AddressNumber': {'type': 'text'},
                                                                               'AddressNumberWithinBuilding': {'type': 'text'},
                                                                               'MailRouting': {'type': 'text'},
                                                                               'City': {'type': 'text'},
                                                                               'Region': {'type': 'text'},
                                                                               'Country': {'type': 'text'},
                                                                               'PostalCode': {'type': 'text'},
                                                                               'type': {'type': 'text'}}},
                                        'RegistrationAuthority': {'type': 'object',
                                                                  'properties': {'RegistrationAuthorityID': {'type': 'text'},
                                                                                 'RegistrationAuthorityEntityID': {'type': 'text'},
                                                                                 'OtherRegistrationAuthorityID': {'type': 'text'}}},
                                        'LegalJurisdiction': {'type': 'text'},
                                        'EntityCategory': {'type': 'text'},
                                        'EntitySubCategory': {'type': 'text'},
                                        'EntityCreationDate': {'type': 'text'},
                                        'LegalForm': {'type': 'object',
                                                      'properties': {'EntityLegalFormCode': {'type': 'text'},
                                                                     'OtherLegalForm': {'type': 'text'}}},
                                        'SuccessorEntity': {'type': 'object',
                                                      'properties': {'SuccessorLEI': {'type': 'text'},
                                                                     'SuccessorEntityName': {'type': 'text'}}},
                                        'LegalEntityEvents': {'type': 'object',
                                                      'properties': {'LegalEntityEventType': {'type': 'text'},
                                                                     'LegalEntityEventEffectiveDate': {'type': 'text'},
                                                                     'LegalEntityEventRecordedDate': {'type': 'text'},
                                                                     'ValidationDocuments': {'type': 'text'},
                                                                     'ValidationReference': {'type': 'text'},
                                                                     'AffectedFields': {'type': 'text'}}},
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
                                                                                     'OtherValidationAuthorityID': {'type': 'text'},
                                                                                     'ValidationAuthorityEntityID': {'type': 'text'}}},
                                              'OtherValidationAuthorities': {'type': 'object',
                                                                                     'properties': {'ValidationAuthorityID': {'type': 'text'},
                                                                                                    'OtherValidationAuthorityID': {'type': 'text'},
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
                                                                                            'QualifierCategory': {'type': 'text'}}},
                                                  'RelationshipQuantifiers': {'type': 'object',
                                                                              'properties': {'MeasurementMethod': {'type': 'text'},
                                                                                             'QuantifierAmount': {'type': 'text'},
                                                                                             'QuantifierUnits': {'type': 'text'}}}}},
                  'Registration': {'type': 'object', 
                                   'properties': {'InitialRegistrationDate': {'type': 'text'}, 
                                                  'LastUpdateDate': {'type': 'text'}, 
                                                  'RegistrationStatus': {'type': 'text'}, 
                                                  'NextRenewalDate': {'type': 'text'}, 
                                                  'ManagingLOU': {'type': 'text'}, 
                                                  'ValidationSources': {'type': 'text'}, 
                                                  'ValidationDocuments': {'type': 'text'}, 
                                                  'ValidationReference': {'type': 'text'},
                                                  'ValidationAuthority': {'type': 'object',
                                                                          'properties': {'ValidationAuthorityID': {'type': 'text'}, 
                                                                                         'OtherValidationAuthorityID': {'type': 'text'}, 
                                                                                         'ValidationAuthorityEntityID': {'type': 'text'}}}
                                                  }},
                   'Extension': {'type': 'object',
                                 'properties': {'Deletion': {'type': 'object',
                                                             'properties': {'DeletedAt': {'type': 'text'}}}}}
                   }

repex_properties = {'LEI': {'type': 'text'}, 
                    'ExceptionCategory': {'type': 'text'}, 
                    'ExceptionReason': {'type': 'text'},
                    'ExceptionReference': {'type': 'text'},
                    'ContentDate': {'type': 'text'},
                    'Extension': {'type': 'object',
                                  'properties': {'Deletion': {'type': 'object',
                                                              'properties': {'DeletedAt': {'type': 'text'}}}}}
                   }

def match_lei(item):
    return {"match": {"LEI": item["LEI"]}}

def match_rr(item):
    return {'bool': {'must': [{"match": {'Relationship.StartNode.NodeID': item['Relationship']['StartNode']['NodeID']}}, 
                              {"match": {'Relationship.EndNode.NodeID': item['Relationship']['EndNode']['NodeID']}}, 
                              {"match": {'Relationship.RelationshipType': item['Relationship']['RelationshipType']}}]}}

def match_repex(item):
    return {'bool': {'must': [{"match": {'LEI': item["LEI"]}},
                              {"match": {'ExceptionCategory': item["ExceptionCategory"]}}, 
                              {"match": {'ExceptionReason': item["ExceptionReason"]}}]}}

def id_lei(item):
    return f"{item['LEI']}_{item['Registration']['LastUpdateDate']}"

def id_rr(item):
    return f"{item['Relationship']['StartNode']['NodeID']}_{item['Relationship']['EndNode']['NodeID']}_{item['Relationship']['RelationshipType']}_{item['Registration']['LastUpdateDate']}"

def id_repex(item):
    if "ExceptionReference" in item:
        ref_hash = hashlib.sha256(bytes(item['ExceptionReference'], 'utf8')).hexdigest()
        item_id = f"{item['LEI']}_{item['ExceptionCategory']}_{item['ExceptionReason']}_{ref_hash}_{item['ContentDate']}"
    else:
        item_id = f"{item['LEI']}_{item['ExceptionCategory']}_{item['ExceptionReason']}_None_{item['ContentDate']}"
    #print(item_id, len(item_id), item)
    return item_id
