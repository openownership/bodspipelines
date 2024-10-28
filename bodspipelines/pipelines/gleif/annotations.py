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

def add_repex_ooc_annotation(annotations):
    """Annotation for all reporting exception ownership or control statements"""
    add_annotation(annotations,
                   "The nature of this interest is unknown",
                   "/interests/0/type")

def add_lei_annotation(annotations, lei, registration_status):
    """Annotation of status for all entity statements (not generated as a result
       of a reporting exception)"""
    add_annotation(annotations,
                   f"GLEIF data for this entity - LEI: {lei}; Registration Status: {registration_status}",
                   "/")

def add_rr_annotation_status(annotations, subject, interested):
    """Annotation for all ownership or control statements"""
    add_annotation(annotations,
                   f"Describes GLEIF relationship: {subject} is subject, {interested} is interested party",
                   "/")

def add_annotation_retired(annotations):
    """Annotation for retired statements"""
    add_annotation(annotations,
                   "GLEIF RegistrationStatus set to RETIRED on this statementDate.",
                   "/")

def add_rr_annotation_deleted(annotations):
    """Annotation for deleted ownership or control statements"""
    add_annotation(annotations,
                   "GLEIF relationship deleted on this statementDate.",
                   "/")

def add_repex_annotation_reason(annotations, reason, lei):
    """Annotation for all statements created as a reasult of a reporting exception"""
    add_annotation(annotations,
                   f"This statement was created due to a {reason} GLEIF Reporting Exception for {lei}",
                   "/")

def add_repex_annotation_changed(annotations, reason, lei):
    """Annotation for all statements when reporting exception changed"""
    add_annotation(annotations,
                   f"Statement retired due to change in a {reason} GLEIF Reporting Exception for {lei}",
                   "/")

def add_repex_annotation_replaced(annotations, reason, lei):
    """Annotation for all statements when reporting exception replaced with relationship"""
    add_annotation(annotations,
                   f"Statement series retired due to replacement of a {reason} GLEIF Reporting Exception for {lei}",
                   "/")

def add_repex_annotation_deleted(annotations, reason, lei):
    """Annotation for ownership or control statement when reporting exception deleted"""
    add_annotation(annotations,
                   f"Statement series retired due to deletion of a {reason} GLEIF Reporting Exception for {lei}",
                   "/")
