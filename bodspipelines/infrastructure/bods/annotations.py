from bodspipelines.infrastructure.utils import current_date_iso

def add_annotation(annotations, description, pointer, link):
    """Add commenting annotation to statement"""
    annotation = {'motivation': 'commenting',
                  'description': description,
                  'statementPointerTarget': pointer,
                  'creationDate': current_date_iso(),
                  'createdBy': {'name': 'Open Ownership',
                                'uri': "https://www.openownership.org"}}
    if link: annotation["url"] = link
    annotations.append(annotation)

def add_entity_annotation(annotations, name, registration_status, link):
    """Annotation of status for all entity statements"""
    add_annotation(annotations,
                   f"{name} Registration Status: {registration_status}",
                   "",
                   link)

def add_deletion_annotation(annotations, name, record_type):
    """Annotation of deletion of statement"""
    add_annotation(annotations,
                   f"{name} {record_type} deleted",
                   "",
                   None)
