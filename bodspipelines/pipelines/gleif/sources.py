from bodspipelines.infrastructure.schemes.data import load_data, get_scheme

def relationship_type(item):
    rtype = item["Relationship"]["RelationshipType"]
    if rtype == "IS_ULTIMATELY_CONSOLIDATED_BY":
        return "U"
    elif rtype == "IS_DIRECTLY_CONSOLIDATED_BY":
        return "D"
    else:
        return "O"

def exception_type(item):
    etype = item["ExceptionCategory"]
    if etype == "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT":
        return "U"
    elif etype == "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT":
        return "D"

def exception_unspecified(item):
    if item['ExceptionReason'] == 'NO_LEI':
        return {"reason":"interestedPartyExemptFromDisclosure","description":"Exception Reason: NO_LEI. This parent legal entity does not consent to obtain an LEI or to authorize its child entity to obtain an LEI on its behalf."}
    elif item['ExceptionReason'] == 'NATURAL_PERSONS':
        return {"reason":"interestedPartyExemptFromDisclosure","description":"Exception Reason: NATURAL_PERSONS. The entity is controlled by a natural person(s) without any intermediate legal entity."}
    elif item['ExceptionReason'] == 'NON_CONSOLIDATING':
        return {"reason":"interestedPartyExemptFromDisclosure","description":"Exception Reason: NON_CONSOLIDATING. The legal entity or entities are not obliged to provide consolidated accounts in relation to the entity they control."}
    elif item['ExceptionReason'] == 'NO_KNOWN_PERSON':
        return {"reason":"informationUnknownToPublisher","description":"Exception Reason: NO KNOWN_PERSON. There is no known person(s) controlling the entity."}
    elif item['ExceptionReason'] == 'NON_PUBLIC':
        return {"reason":"interestedPartyExemptFromDisclosure","description":"Exception Reason: NON_PUBLIC. Information about the relationship with the controlling entity is not public."}

#def get_scheme(scheme_id, scheme_data):
#    match = [scheme for scheme in scheme_data if scheme[0] == scheme_id]
#    if match:
#        country = match[0][2]
#        return lookup_scheme(country, "company")
#    return None, None

class GLEIFSource():
    """GLEIF specific methods"""
    def __init__(self):
        self.scheme_data = load_data()

    def identify_item(self, item):
        """Identify type of GLEIF data"""
        print("Item:", item)
        if 'Entity' in item:
            return 'entity'
        elif 'Relationship' in item:
            return 'relationship'
        elif 'ExceptionCategory' in item:
            return 'exception'

    def record_id(self, item):
        """recordId for GLEIF item"""
        item_type = self.identify_item(item)
        if item_type == 'entity':
            return f"XI-LEI-{item['LEI']}"
        elif item_type == 'relationship':
            start = item["Relationship"]["StartNode"]['NodeID']
            end = item["Relationship"]["EndNode"]['NodeID']
            rtype = relationship_type(item)
            return f"XI-LEI-RR-{rtype}-{start}-{end}"
        elif item_type == 'exception':
            start = item["LEI"]
            etype = exception_type(item)
            return f"XI-LEI-RE-{etype}-{start}"

    def exception_id(self, record_id):
        """Relationship coresponding recordId for exception"""
        #return record_id.replace('-RR-', '-RE-')
        return record_id.rsplit("-", 1)[0].replace("-RR-", "-RE-")

    def declaration_subject(self, item):
        """declarationSubject for GLEIF item"""
        item_type = self.identify_item(item)
        if item_type == 'entity':
            return f"XI-LEI-{item['LEI']}"
        elif item_type == 'relationship':
            start = item["Relationship"]["StartNode"]['NodeID']
            return f"XI-LEI-{start}"
        elif item_type == 'exception':
            return f"XI-LEI-{item['LEI']}"

    def item_updated(self, item):
        """statementDate for GLEIF item"""
        item_type = self.identify_item(item)
        if item_type == 'entity':
            return item["Registration"]["LastUpdateDate"]
        elif item_type == 'relationship':
            return item["Registration"]["LastUpdateDate"]
        elif item_type == 'exception':
            return item["ContentDate"]

    def item_closed(self, item):
        """Is GLEIF item closed?"""
        item_type = self.identify_item(item)
        #print(item)
        if item_type == 'entity':
            return item["Registration"]["RegistrationStatus"] in ('RETIRED', 'DUPLICATE', 'ANNULLED')
        elif item_type == 'relationship':
            print("item_closed:", item)
            if "Extension" in item and "Deletion" in item["Extension"]:
                return True
            return item["Registration"]["RegistrationStatus"] in ('RETIRED', 'DUPLICATE', 'ANNULLED')
        elif item_type == 'exception':
            return True if "Extension" in item and "Deletion" in item["Extension"] else False

    def name(self, item):
        """Name for GLEIF item"""
        return item['Entity']['LegalName']

    def jurisdiction(self, item):
        return item['Entity']['LegalJurisdiction']

    @property
    def scheme(self) -> str:
        """Get scheme"""
        return 'XI-LEI'

    @property
    def scheme_name(self) -> str:
        """Get scheme name"""
        return 'Global Legal Entity Identifier Index'

    def indentifier(self, item) -> str:
        """Get entity identifier"""
        return item['LEI']

    def additional_identifiers(self, item) -> list:
        """Get list of additional identifiers"""
        if ("RegistrationAuthority" in item['Entity']
            and "RegistrationAuthorityID" in item['Entity']["RegistrationAuthority"]
            and "RegistrationAuthorityEntityID" in item['Entity']["RegistrationAuthority"]):
            authority = item['Entity']["RegistrationAuthority"]
            scheme_code, scheme_name = get_scheme(authority["RegistrationAuthorityID"],
                                                  self.scheme_data,
                                                  country_code=item['Entity']['LegalJurisdiction'])
            #print(authority, scheme_code, scheme_name)
            return [{'id': authority["RegistrationAuthorityEntityID"],
                    'scheme': scheme_code,
                    'schemeName': scheme_name}]
        else:
            return []

    def creation_date(self, item):
        """Creation date for GLEIF item"""
        if "EntityCreationDate" in item['Entity']:
            return item['Entity']["EntityCreationDate"]
        else:
            return None

    def _extract_address(self, address, data):
        print("Data:", data)
        if 'FirstAddressLine' in data:
            address['address1'] = data['FirstAddressLine']
        if 'AdditionalAddressLine' in data:
            address['address2'] = data['AdditionalAddressLine']
        if 'City' in data:
            address['city'] = data['City']
        if 'PostalCode' in data:
            address['postcode'] = data['PostalCode']
        if 'Region' in data:
            address['region'] = data['Region']
        if 'Country' in data:
            address['country'] = data['Country']

    def registered_address(self, item) -> dict:
        """Get registered address"""
        address = {}
        #print("Data:", item)
        if 'LegalAddress' in item['Entity']:
            self._extract_address(address, item['Entity']['LegalAddress'])
        return address

    def business_address(self, item) -> dict:
        """Get registered address"""
        address = {}
        #print("Data:", item)
        if 'HeadquartersAddress' in item['Entity']:
            self._extract_address(address, item['Entity']['HeadquartersAddress'])
        return address

    def relationship_subject(self, item) -> dict:
        """Get relationship subject"""
        item_type = self.identify_item(item)
        if item_type == "relationship":
            return f"XI-LEI-{item['Relationship']['StartNode']['NodeID']}"
        else:
            return f"XI-LEI-{item['LEI']}"

    def relationship_interested_party(self, item) -> dict:
        """Get relationship subject"""
        item_type = self.identify_item(item)
        if item_type == "relationship":
            return f"XI-LEI-{item['Relationship']['EndNode']['NodeID']}"
        else:
            return exception_unspecified(item)

    def interest_start_date(self, item) -> dict:
        """Get interest start date"""
        start_date = False
        if 'RelationshipPeriods' in item['Relationship']:
            periods = item['Relationship']['RelationshipPeriods']
            for period in periods:
                if 'StartDate' in period and 'PeriodType' in period:
                    if period['PeriodType'] == "RELATIONSHIP_PERIOD":
                        interestStartDate = period['StartDate']
                    else:
                        start_date = period['StartDate']
        if not start_date:
            if not interestStartDate: interestStartDate = ""
        else:
            if not interestStartDate: interestStartDate = start_date
        return interestStartDate.split("T")[0]

    def _interest_level(self, item, default):
        """Calculate interest level"""
        relationship_type = item['Relationship']['RelationshipType']
        if relationship_type == "IS_ULTIMATELY_CONSOLIDATED_BY":
            return "indirect"
        elif relationship_type in ("IS_DIRECTLY_CONSOLIDATED_BY", "IS_INTERNATIONAL_BRANCH_OF",
                                   "IS_FUND-MANAGED_BY", "IS_SUBFUND_OF", "IS_FEEDER_TO"):
            return "direct"
        else:
            return default # Other options in data

    def interest_level(self, item):
        """Get interest level"""
        #interestLevel = self._interest_level(item, 'unknown')
        interestLevel = "unknown"
        return interestLevel

    def interest_details(self, item):
        """Get interest details"""
        item_type = self.identify_item(item)
        if item_type == "relationship":
            return f"Relationship Type: {item['Relationship']['RelationshipType']}"
        else:
            return f"Exception Category: {item['ExceptionCategory']}"

    def source_type(self, item) -> str:
        """Get source type"""
        item_type = self.identify_item(item)
        if item_type == "entity":
            return (['officialRegister'] if not item['Registration']['ValidationSources'] ==
                 'FULLY_CORROBORATED' else ['officialRegister', 'verified'])
        else:
            return ['officialRegister']

    @property
    def source_description(self) -> str:
        """Get source description"""
        return 'GLEIF'

    @property
    def source_url(self) -> str:
        """Get source url"""
        return 'https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy'

    @property
    def entity_name(self) -> str:
        """Get GLEIF entity name"""
        return 'LEI'

    def status(self, item) -> str:
        """Get GLEIF entity status"""
        return item['Entity']['EntityStatus']
