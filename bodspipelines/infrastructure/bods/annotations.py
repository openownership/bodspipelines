from bodspipelines.infrastructure.utils import current_date_iso

def add_annotation(annotations, description, pointer):
    """Add commenting annotation to statement"""
    annotation = {'motivation': 'commenting',
                  'description': description,
                  'statementPointerTarget': pointer,
                  'creationDate': current_date_iso(),
                  'createdBy': {'name': 'Open Ownership',
                                'uri': "https://www.openownership.org"}}
    annotations.append(annotation)

def add_entity_annotation(annotations, name, registration_status):
    """Annotation of status for all entity statements"""
    add_annotation(annotations,
                   f"{name} Registration Status: {registration_status}",
                   "")

def add_deletion_annotation(annotations, name, record_type):
    """Annotation of deletion of statement"""
    add_annotation(annotations,
                   f"{name} {record_type} deleted",
                   "")
