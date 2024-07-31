from bodspipelines.infrastructure.utils import (current_date_iso, generate_statement_id,
                                                random_string, format_date)
from .annotations import (add_lei_annotation, add_repex_annotation_changed,
                          add_repex_annotation_replaced, add_repex_ooc_annotation, add_rr_annotation_deleted,
                          add_annotation_retired, add_rr_annotation_status, add_repex_annotation_deleted)

def void_entity_statement(reason, statement_id, status, update_date, entity_type, lei, unknown=False):
    """Create BODS statemnt to void entity/person"""
    id = generate_statement_id(f"{statement_id}", "voided")
    #print(f"Voiding entity ({statement_id}):", id)
    statement = {"statementID": id,
            "statementDate": format_date(update_date),
            "publicationDetails": {
                "publicationDate": current_date_iso(),
                "bodsVersion": "0.2",
                "publisher": {
                    "name": "GLEIF"
                }
            },
            "statementType": entity_type,
            #"entityType": "registeredEntity",
            "isComponent": False,
            "replacesStatements": [statement_id]
           }
    if entity_type == "entityStatement":
        if unknown:
            statement["entityType"] = "unknownEntity"
        else:
            statement["entityType"] = "registeredEntity"
    else:
        statement["personType"] = "unknownPerson"
    annotations = []
    if reason == "RETIRED":
        add_lei_annotation(annotations, lei, status)
    elif reason == "DELETION":
        add_repex_annotation_deleted(annotations, status, lei)
    elif reason == "CHANGED":
        add_repex_annotation_changed(annotations, status, lei)
    elif reason == "REPLACED":
        add_repex_annotation_replaced(annotations, status, lei)
    statement["annotations"] = annotations
    return statement

def void_ooc_statement(reason, statement_id, status, update_date, lei, lei2):
    """Create BODS statement to void ownership or control statement"""
    id = generate_statement_id(f"{statement_id}", "voided_ownershipOrControlStatement")
    #print(f"Voiding OOC ({statement_id}):", id)
    statement = {"statementID": id,
            "statementDate": format_date(update_date),
            "publicationDetails": {
                "publicationDate": current_date_iso(),
                "bodsVersion": "0.2",
                "publisher": {
                    "name": "GLEIF"
                }
            },
            "statementType": "ownershipOrControlStatement",
            "interestedParty":{
                "describedByEntityStatement": ""
            },
            "isComponent": False,
            "subject": {
                "describedByEntityStatement": ""
            },
            "replacesStatements": [statement_id]
           }
    annotations = []
    if reason == "RR_DELETION":
        add_rr_annotation_deleted(annotations)
        add_rr_annotation_status(annotations, lei, lei2)
    elif reason == "RETIRED":
        add_annotation_retired(annotations)
        add_rr_annotation_status(annotations, lei, lei2)
    elif reason == "REPEX_DELETION":
        add_repex_annotation_deleted(annotations, status, lei)
    statement["annotations"] = annotations
    return statement

class GleifUpdates():
    """GLEIF specific updates"""

    def __init__(self):
        self.already_voided = []
        self.already_replaced = []

    def void_entity_retired(self, latest_id, update_date, lei, status):
        """Void entity statement for retired LEI"""
        if latest_id in self.already_voided or latest_id in self.already_replaced:
            return None
        else:
            self.already_voided.append(latest_id)
            return void_entity_statement("RETIRED", latest_id, status, update_date, "entityStatement", lei)

    def void_entity_deletion(self, latest_id, update_date, lei, status):
        """Void entity statement for deleted reporting exception"""
        if latest_id in self.already_voided or latest_id in self.already_replaced:
            return None
        else:
            self.already_voided.append(latest_id)
            return void_entity_statement("DELETION", latest_id, status, update_date, "entityStatement", lei, unknown=True)

    def void_entity_changed(self, latest_id, update_date, entity_type, lei, status):
        """Void entity statement for changed reporting exception"""
        if latest_id in self.already_voided or latest_id in self.already_replaced:
            return None
        else:
            self.already_voided.append(latest_id)
            return void_entity_statement("CHANGED", latest_id, status, update_date, entity_type, lei, unknown=True)

    def void_ooc_relationship_deletion(self, latest_id, update_date, start, end):
        """Void ownership or control statement for deleted relationship record"""
        if latest_id in self.already_voided or latest_id in self.already_replaced:
            return None
        else:
            self.already_voided.append(latest_id)
            return void_ooc_statement("RR_DELETION", latest_id, None, update_date, start, end)

    def void_ooc_relationship_retired(self, latest_id, update_date, start, end):
        """Void ownership or control statement for retired relationship record """
        if latest_id in self.already_voided or latest_id in self.already_replaced:
            return None
        else:
            self.already_voided.append(latest_id)
            return void_ooc_statement("RETIRED", latest_id, None, update_date, start, end)

    def void_entity_replaced(self, latest_id, update_date, entity_type, lei, status):
        """Void entity statement for replaced (by relationship) reporting exception"""
        if latest_id in self.already_voided or latest_id in self.already_replaced:
            return None
        else:
            self.already_voided.append(latest_id)
            return void_entity_statement("REPLACED", latest_id, status, update_date, entity_type, lei, unknown=True)

    def void_ooc_exception_deletion(self, latest_id, update_date, lei, status):
        """Void ownership or control statement for deleted reporting exception"""
        if latest_id in self.already_voided or latest_id in self.already_replaced:
            return None
        else:
            self.already_voided.append(latest_id)
            return void_ooc_statement("REPEX_DELETION", latest_id, status, update_date, lei, None)

    def add_replaces(self, statement, old_statement_id):
        """Add replacesStatements to statement object"""
        if old_statement_id in self.already_replaced:
            return None
        else:
            self.already_replaced.append(old_statement_id)
            statement["replacesStatements"] = [old_statement_id]
            return statement
