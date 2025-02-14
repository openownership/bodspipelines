import re
import time

from bodspipelines.infrastructure.caching import Caching
from bodspipelines.infrastructure.bods.transforms import transform_item
from bodspipelines.infrastructure.bods.annotations import add_deletion_annotation
from bodspipelines.infrastructure.utils import current_date_iso, generate_statement_id #, unmap_unspecified

def new_record_version(record_id):
    """Create new (incremented) version of recordId"""
    match = re.search('\_(\d)$', record_id)
    if match:
        num = int(match.group(1)) + 1
        return f"{record_id.split('_')[0]}_{num}"
    else:
        return f"{record_id}_2"

async def retrieve_statement(storage, statement_type, statement_id):
    """Retrive statement using statement_id"""
    #print("Retriving:", statement_id)
    data = await storage.get_item(statement_id, statement_type)
    return data

def build_latest(latest_id, statement_id, record_id):
    """Build latest object"""
    return {'latest_id': latest_id, # Source id (e.g. LEI)
            'statement_id': statement_id, # Latest statement id
            'record_id': record_id} # Record id

def build_record(record_id, statement_id, status):
    """Build record object"""
    return {'record_id': record_id,
            'latest_record_id': statement_id, # Unused
            'record_status': status}

def build_closed(statement_id, record_id, statement_date):
    """Build closed object"""
    return {'statement_id': statement_id,
            'record_id': record_id, # Unused
            'statement_date': statement_date
           }

async def latest_save(cache, latest_id, statement_id, record_id, updates=False):
    """Save latest statement id for LEI/RR/Repex"""
    await cache.add(build_latest(latest_id, statement_id, record_id),
                "latest", overwrite=True)

async def latest_lookup(cache, latest_id, updates=False):
    """Lookup latest statement id for LEI/RR/Repex"""
    data = await cache.get(latest_id, "latest")
    if data:
         return data['statement_id'], data['record_id']
    else:
         return None, None

async def record_lookup(cache, record_id, updates=False):
    """Lookup latest statement id for record_id"""
    data = await cache.get(record_id, "records")
    if data:
         #print(data)
         return data['latest_record_id'], data['record_status']
    else:
         return None, None

async def record_save(cache, record_id, latest_record_id, status, updates=False):
    """Save latest record statementId and recordStatus"""
    await cache.add(build_record(record_id, latest_record_id, status),
                "records", overwrite=True)

async def closed_save(cache, statement_id, record_id, statement_date):
    """Save statement to close"""
    await cache.add(build_closed(statement_id, record_id, statement_date), "closed", overwrite=True)

async def closed_delete(cache, statement_id, if_exists=False):
    """Delete statement to updates"""
    await cache.delete(statement_id, "closed", if_exists=if_exists)

async def find_closed(cache, record_id):
    """Stream updates from index"""
    async for closed in cache.stream("closed"):
        #print("Closed:", closed)
        if closed['record_id'] == record_id:
            return closed['statement_id'], closed['statement_date']
    return None

async def check_for_exception(transform, cache, storage, item, record_id, statement_date, updates=False):
    if transform.identify_item(item) == "relationship":
        exception_record_id = transform.exception_id(record_id)
        latest_statement_id, latest_record_status = await record_lookup(cache,
                                                                        exception_record_id,
                                                                        updates=updates)
        if latest_statement_id and latest_record_status != "closed":
            #print("Exception needs closing")
            await closed_save(cache, latest_statement_id, exception_record_id, statement_date)
            #statement = await retrieve_statement(storage, "relationship", latest_statement_id)
            #print("Exception:", statement)

async def record_status(transform, cache, storage, item, statement, updates=False):
    """Calculate recordStatus id for record_id"""
    record_id = statement["recordId"]
    record_type = statement["recordType"]
    statement_date = statement["statementDate"]
    latest_record_id, latest_record_status = await record_lookup(cache, record_id, updates=updates)
    #print("record_status:", record_id, latest_statement_id, latest_record_status, cache.cache)
    if not latest_record_id and '-RR-' in record_id:
        #print("New relationship")
        #latest_statement_id, latest_record_status = await record_lookup(cache,
        #                                                                record_id.replace('-RR-', '-RE-'),
        #                                                                updates=updates)
        await check_for_exception(transform, cache, storage, item, record_id, statement_date, updates=updates)
    #if not latest_statement_id and '-RR-' in record_id:
    #    await closed_delete(cache, latest_id, if_exists=True)
    if not latest_record_id:
        #print(record_type, transform.item_closed(item, record_type))
        #if transform.identify_item(item) in ("relationship", "exception"):
        if record_type == "relationship":
            relationship_id = transform.relationship_id(item)
            latest_id, latest_record_id = await latest_lookup(cache, relationship_id, updates=updates)
            if latest_id:
                await closed_save(cache, latest_id, latest_record_id, statement_date)
        if transform.item_closed(item, record_type):
            return 'closed', None
        else:
            return 'new', None
    if latest_record_status == 'closed':
        #if transform.identify_item(item) in ("relationship", "exception") and not transform.item_closed(item):
        if record_type == "relationship" and not transform.item_closed(item, record_type):
            new_record_id = new_record_version(record_id)
            return 'new', new_record_id
        elif not transform.item_closed(item, record_type):
            new_record_id = new_record_version(record_id)
            return 'new', new_record_id
    if transform.item_closed(item, record_type):
        if '-RE-' in record_id:
            latest_id = await find_closed(cache, record_id)
            #print("Record id:", record_id, "Latest id:", latest_id)
            if latest_id:
                await closed_delete(cache, latest_id, if_exists=True)
        return 'closed', None
    return 'updated', None

def relationship_type(statement):
    if "interestedParty" in statement["recordDetails"]:
        if isinstance(statement["recordDetails"]["interestedParty"], dict):
            return "exception"
        else:
            return "relationship"

def record_annotations(statement, status, transform):
    """Add annotation for closed records"""
    annotations = []
    if status == 'closed':
        record_type = statement["recordType"]
        record_id = statement["recordId"]
        if record_type == "relationship":
            source_type = relationship_type(statement)
        else:
            source_type = record_type
        add_deletion_annotation(annotations, record_id, source_type)
    return annotations

async def process_closed(cache):
    """Stream updates from index"""
    async for closed in cache.stream("closed"):
        yield closed['statement_id'], closed['record_id'], closed['statement_date']

class ProcessUpdates:
    """Data processor definition class"""
    def __init__(self, id_name=None, transform=None, 
                 #updates=None,
                 storage=None):
        """Initial setup"""
        self.transform = transform
        #self.updates = updates
        self.id_name = id_name
        self.storage = storage
        self.cache = Caching(self.storage, batching=-1)

    async def setup(self):
        """Load data into cache"""
        await self.storage.setup()
        await self.cache.load()

    async def process(self, item, item_type, header, updates=False):
        """Process updates if applicable"""
        #print(f"Processing - updates: {item_type} {updates}")
        for statement in transform_item(self.transform, item, 'new'):
            status = False
            if updates:
                record_id = statement["recordId"]
                statement_id = statement["statementId"]
                status, new_record_id = await record_status(self.transform,
                                         self.cache,
                                         self.storage,
                                         item,
                                         statement,
                                         updates=updates)
                if status:
                    if new_record_id:
                        statement["recordId"] = new_record_id
                    statement["recordStatus"] = status
                    extra_annotations = record_annotations(statement, status, self.transform)
                    statement["annotations"].extend(extra_annotations)
            if status:
                await record_save(self.cache, statement["recordId"], statement_id, status, updates=updates)
                if statement["recordType"] == "relationship":
                    relationship_id = self.transform.relationship_id(item)
                    await latest_save(self.cache, relationship_id, statement_id, statement["recordId"], updates=updates)
                yield statement

    async def finish_updates(self, updates=False):
        """Process updates to referencing statements"""
        print("In finish_updates")
        if updates:
            done_updates = []
            #print("Got here")
            async for statement_id, record_id, statement_date in process_closed(self.cache):
                statement = await retrieve_statement(self.storage, "relationship", statement_id)
                #unmap_unspecified(statement)
                #print("Exception:", statement)
                statement["recordStatus"] = 'closed'
                #statement["statementDate"] = current_date_iso()
                statement["statementDate"] = statement_date
                statementID = generate_statement_id(f"{statement['recordId']}-{statement['statementId']}",
                                                    'relationshipStatement')
                #print("Closed:", statement["statementId"], statementID)
                statement["statementId"] = statementID
                yield statement
        await self.cache.flush()
