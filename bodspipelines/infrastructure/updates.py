#from bodspipelines.pipelines.gleif.transforms import rr_id
from bodspipelines.pipelines.gleif.indexes import id_rr as rr_id
from bodspipelines.infrastructure.utils import (current_date_iso, generate_statement_id, 
                                                random_string, format_date)
from bodspipelines.infrastructure.caching import Caching #cached, load_cache, flush_cache

def convert_rel_type(rel_type):
    """Convert Relationship Type To Exception Type"""
    conv = {"IS_DIRECTLY_CONSOLIDATED_BY": "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT",
            "IS_ULTIMATELY_CONSOLIDATED_BY": "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT"}
    return conv[rel_type]


def convert_except_type(except_type):
    """Convert Exception Type To Relationship Type"""
    conv = {"DIRECT_ACCOUNTING_CONSOLIDATION_PARENT": "IS_DIRECTLY_CONSOLIDATED_BY",
            "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT": "IS_ULTIMATELY_CONSOLIDATED_BY"}
    return conv[except_type]


def build_latest(lei, bods_id, reason=False):
    """Build latest object"""
    return {'latest_id': lei, 'statement_id': bods_id, 'reason': reason}


def build_references(statement_id, referencing_ids):
    """Build references object"""
    return {'statement_id': statement_id, 'references_id': referencing_ids}


def build_update(referencing_id, latest_id, updates): #old_statement_id, new_statement_id):
    """Build updates object"""
    #print(referencing_id, latest_id, updates)
    return {'referencing_id': referencing_id,
            'latest_id': latest_id,
            'updates': [{'old_statement_id': old, 'new_statement_id': updates[old]} for old in updates]}


def build_exception(latest, occ_id, other_id, reason, reference, entity_type):
    """Build exception object"""
    return {'latest_id': latest, 'statement_id': occ_id, 'other_id': other_id, 'reason': reason,
            'reference': reference, 'entity_type': entity_type}


async def latest_save(cache, lei, bods_id, reason=False, updates=False):
    """Save latest statement id for LEI/RR/Repex"""
    #print(f"Saving latest - {lei}: {bods_id}")
    #await storage.add_item(build_latest(lei, bods_id, reason=reason), "latest", overwrite=True)
    await cache.add(build_latest(lei, bods_id, reason=reason),
                "latest", overwrite=True)

async def latest_lookup(cache, lei, updates=False):
    """Lookup latest statement id for LEI/RR/Repex"""
    #data = await storage.get_item(lei, "latest")
    data = await cache.get(lei, "latest")
    if data:
         #print(data)
         return data['statement_id'], data['reason']
    else:
         return None, None


async def latest_delete(cache, old_statement_id, updates=False):
    """Delete latest statement id for LEI/RR/Repex"""
    await cache.delete(old_statement_id, "updates")


async def exception_save(cache, latest, ooc_id, other_id, reason, reference, entity_type, updates=False):
    """Save latest exception"""
    #await storage.add_item(build_exception(latest, ooc_id, other_id, reason, reference, entity_type), "exceptions")
    await cache.add(build_exception(latest, ooc_id, other_id, reason, reference, entity_type),
                    "exceptions",
                    overwrite=True)

async def exception_lookup(cache, latest, updates=False):
    """Lookup latest exception"""
    #data = await storage.get_item(latest, "exceptions")
    data = await cache.get(latest, "exceptions")
    if data:
         return data['statement_id'], data['other_id'], data['reason'], data['reference'], data['entity_type']
    else:
         return None, None, None, None, None

async def exception_delete(cache, old_statement_id, updates=False):
    """Delete exception id"""
    #await storage.delete_item(old_statement_id, "exceptions")
    await cache.delete(old_statement_id, "exceptions")

def build_referencing(referencing_ids):
    out = []
    for statement_id in referencing_ids:
        out.append({'statement_id': statement_id, 'latest_id': referencing_ids[statement_id]})
    return out

async def references_save(cache, statement_id, referencing_ids, updates=False, overwrite=False):
    """Save list of statement ids referencing statement"""
    #await storage.add_item(build_references(statement_id, referencing_ids), "references")
    await cache.add(build_references(statement_id, build_referencing(referencing_ids)),
                    "references",
                    overwrite=overwrite)

def translate_references(references):
    out = {}
    if isinstance(references, list):
        for ref in references:
            out[ref['statement_id']] = ref['latest_id']
    else:
        out[references['statement_id']] = references['latest_id']
    return out

async def lookup_references(cache, statement_id, updates=False):
    """Lookup list of statement ids referencing statement"""
    #data = await storage.get_item(statement_id, "references")
    data = await cache.get(statement_id, "references")
    if data:
        #print(data)
        return translate_references(data['references_id'])
    else:
        return {}

async def references_update(cache, referenced_id, statement_id, latest_id, updates=False):
    """Update list of statement ids referencing statement"""
    referencing_ids = await lookup_references(cache, referenced_id, updates=updates)
    referencing_ids[statement_id] = latest_id
    await references_save(cache, referenced_id, referencing_ids, updates=updates, overwrite=True)

def add_replaces(statement, old_statement_id):
    """Add replacesStatements to statement object"""
    statement["replacesStatements"] = [old_statement_id]

async def updates_save(cache, referencing_id, latest_id, updates):
    """Save statement to updates"""
    #print("Saving update")
    await cache.add(build_update(referencing_id, latest_id, updates), "updates", overwrite=True)

async def updates_delete(cache, old_statement_id):
    """Delete statement to updates"""
    await cache.delete(old_statement_id, "updates")

async def lookup_updates(cache, old_statement_id):
    """Lookup statement to updates"""
    data = await cache.get(old_statement_id, "updates")
    if data:
        #print("lookup_updates:", data)
        #return data['updates']
        return {update['old_statement_id']: update['new_statement_id'] for update in data['updates']}
    else:
        return {}

async def updates_update(cache, referencing_id, latest_id, old_statement_id, new_statement_id):
    """Save statement to update"""
    #print("Updating update:", referencing_id, latest_id, old_statement_id, new_statement_id)
    updates = await lookup_updates(cache, referencing_id)
    updates[old_statement_id] = new_statement_id
    await updates_save(cache, referencing_id, latest_id, updates)
    #await storage.add_item(build_update(referencing_id, latest_id, old_statement_id, new_statement_id), "updates")

async def process_updates(cache):
    """Stream updates from index"""
    async for update in cache.stream("updates"):
        updates = {data['old_statement_id']: data['new_statement_id'] for data in update['updates']}
        yield update['referencing_id'], update['latest_id'], updates #update['old_statement_id'], update['new_statement_id']

async def retrieve_statement(storage, statement_type, statement_id):
    """Retrive statement using statement_id"""
    data = await storage.get_item(statement_id, statement_type)
    return data

def fix_statement_reference(statement, updates, latest_id):
    """Update ownershipOrControlStatement with new_id"""
    for old_id in updates:
        new_id = updates[old_id]
        if "subject" in statement and "describedByEntityStatement" in statement["subject"]:
            if statement["subject"]["describedByEntityStatement"] == old_id:
                statement["subject"]["describedByEntityStatement"] = new_id
                #return True
        if "interestedParty" in statement and "describedByEntityStatement" in statement["interestedParty"]:
            if statement["interestedParty"]["describedByEntityStatement"] == old_id:
                statement["interestedParty"]["describedByEntityStatement"] = new_id
                #return True
    old_statement_id = statement['statementID']
    subject = statement["subject"]["describedByEntityStatement"]
    interested = statement["interestedParty"]["describedByEntityStatement"]
    statement_id = generate_statement_id(f"{latest_id}_{subject}_{interested}", 'ownershipOrControlStatement')
    statement['statementID'] = statement_id
    return old_statement_id

def source_id(data, id_name):
    """Extract identifier for specified type"""
    for id in data['identifiers']:
        if id['scheme'] == id_name:
            return id['id']
    return None

def referenced_ids(statement, item):
    """Collect statement ids referenced by statement"""
    out = []
    if "subject" in statement and "describedByEntityStatement" in statement["subject"]:
        out.append(statement["subject"]["describedByEntityStatement"])
    if ("interestedParty" in statement and "describedByEntityStatement" in statement["interestedParty"] and
        item["Registration"]["RegistrationStatus"] == "PUBLISHED"):
        out.append(statement["interestedParty"]["describedByEntityStatement"])
    return out

async def calculate_mapping(cache, item, updates=False):
    """Calculate mapping lei and latest statementID"""
    out = {}
    if "Relationship" in item:
        for lei in (item["Relationship"]["StartNode"]["NodeID"], item["Relationship"]["EndNode"]["NodeID"]):
            latest_id, _ = await latest_lookup(cache, lei, updates=updates)
            if latest_id: out[lei] = latest_id
    else:
        latest_id, _ = await latest_lookup(cache, item["LEI"], updates=updates)
        if latest_id: out[item["LEI"]] = latest_id
    #print("calculate_mapping:", out)
    return out

async def item_setup(cache, item, updates=False):
    """Setup exception and relationship mapping"""
    #print("item_setup:", item)
    if "ExceptionCategory" in item:
        except_lei = item["LEI"]
        except_type = item["ExceptionCategory"]
        except_reason = item["ExceptionReason"]
        except_reference = item["ExceptionReference"] if "ExceptionReference" in item else None
        old_ooc_id, old_other_id, old_reason, old_reference, old_entity_type = \
            await exception_lookup(cache, f"{except_lei}_{except_type}", updates=updates)
        #print(f"{except_lei}_{except_type}:", old_other_id)
    else:
        old_ooc_id, old_other_id, old_reason, old_reference, old_entity_type, except_lei, except_type, \
            except_reason, except_reference = None, None, None, None, None, None, None, None, None
    if "Relationship" in item or "ExceptionCategory" in item:
        mapping = await calculate_mapping(cache, item, updates=updates)
    else:
        mapping = {}
    return mapping, old_ooc_id, old_other_id, old_reason, old_reference, old_entity_type, except_lei, \
           except_type, except_reason, except_reference

async def process_entity_lei(statement_id, statement, item, lei, updates, mapping, data_type, cache):
    """Process entity statement from LEI"""
    #print(f"Processing {statement_id}: {lei} {updates} {mapping}")
    if updates:
        latest_id, _ = await latest_lookup(cache, lei, updates=updates) # Latest statement
        #print("Debug:", lei, latest_id)
        if latest_id:
            if item["Registration"]["RegistrationStatus"] == 'RETIRED':
                update_date = item['Registration']['LastUpdateDate']
                statement = data_type.void_entity_retired(latest_id,
                                                          update_date,
                                                          item["LEI"],
                                                          item["Registration"]["RegistrationStatus"])
                statement_id = statement['statementID']
            else:
                add_replaces(statement, latest_id) # Add replaces statement
            referencing_ids = await lookup_references(cache, latest_id, updates=updates)
        else:
            referencing_ids = []
        #print("referencing_ids:", referencing_ids)
        for ref_id in referencing_ids:
            await updates_update(cache,
                                ref_id,
                                referencing_ids[ref_id],
                                latest_id,
                                statement_id) # Save statements to update
    await latest_save(cache, lei, statement_id, updates=updates) # Save new latest
    return statement_id, statement

async def process_entity_repex(statement_id, statement, item, except_lei, except_type, except_reason,
                                    old_reason, old_other_id, old_entity_type, data_type, mapping,
                                    cache, updates=False):
    """Process entity statement from reporting exception"""
    void_statement = None
    if "Extension" in item and "Deletion" in item["Extension"]:
        latest_id, _ = await latest_lookup(cache, f"{except_lei}_{except_type}_{except_reason}_entity", updates=updates)
        statement = data_type.void_entity_deletion(latest_id,
                                                 item["Extension"]["Deletion"]["DeletedAt"],
                                                 item["LEI"],
                                                 item["ExceptionReason"])
        statement_id = statement['statementID']
    elif (old_reason and except_reason != old_reason):
        update_date = current_date_iso()
        void_statement = data_type.void_entity_changed(old_other_id,
                                                       update_date,
                                                       old_entity_type,
                                                       except_lei,
                                                       old_reason)
    await latest_save(cache, f"{except_lei}_{except_type}_{except_reason}_entity", statement_id, updates=updates)
    return statement_id, statement, void_statement

async def process_ooc_rr(statement_id, statement, item, start, end, rel_type, entity_voided,
                         updates, data_type, cache):
    """Process ownership or control statement for relationship"""
    void_statement = None
    ref_statement_ids = referenced_ids(statement, item) # Statements Referenced By OOC
    for ref_id in ref_statement_ids:
        await references_update(cache, ref_id, statement_id, f"{start}_{end}_{rel_type}", updates=updates)
    # If updating, add replacesStatement
    if updates:
        latest_id, _ = await latest_lookup(cache, f"{start}_{end}_{rel_type}", updates=updates)
        if latest_id:
            #print("OOC update:", f"{start}_{end}_{rel_type}", latest_id)
            # Check if deleted
            if "Extension" in item and "Deletion" in item["Extension"]:
                statement = data_type.void_ooc_relationship_deletion(latest_id,
                                                                   item["Extension"]["Deletion"]["DeletedAt"],
                                                                   start,
                                                                   end)
                statement_id = statement['statementID']
            elif item["Registration"]["RegistrationStatus"] == 'RETIRED':
                update_date = item['Registration']['LastUpdateDate']
                statement = data_type.void_ooc_relationship_retired(latest_id,
                                                                  update_date,
                                                                  start,
                                                                  end)
                statement_id = statement['statementID']
            else:
                add_replaces(statement, latest_id) # Add replaces statement
            await updates_delete(cache, latest_id)
    # Check if replacing exception
    if rel_type in ("IS_DIRECTLY_CONSOLIDATED_BY", "IS_ULTIMATELY_CONSOLIDATED_BY"):
        except_type = convert_rel_type(rel_type)
        except_ooc_id, except_other_id, except_reason, except_ref, except_entity_type \
                        = await exception_lookup(cache, f"{start}_{except_type}", updates=updates)
        if except_ooc_id and not entity_voided:
            update_date = current_date_iso()
            void_statement = data_type.void_entity_replaced(except_ooc_id,
                                                            update_date,
                                                            except_entity_type,
                                                            except_ref,
                                                            except_reason)
            await exception_delete(cache, f"{start}_{except_type}", updates=updates)
    # Save statementID in latest
    await latest_save(cache, f"{start}_{end}_{rel_type}", statement_id, updates=updates)
    return statement_id, statement, void_statement

async def process_ooc_repex(statement_id, statement, item, except_lei, except_type, except_reason,
                            except_reference, old_reason, old_reference, old_ooc_id, old_other_id,
                            data_type, cache, updates=False):
    """Process ownership or control statement for reporting exception"""
    if "Extension" in item and "Deletion" in item["Extension"]:
        latest_id, _ = await latest_lookup(cache, f"{except_lei}_{except_type}_{except_reason}_ownership", updates=updates)
        statement = data_type.void_ooc_exception_deletion(latest_id,
                                                          item["Extension"]["Deletion"]["DeletedAt"],
                                                          item["LEI"],
                                                          item["ExceptionReason"])
        statement_id = statement['statementID']
    elif (old_reason and except_reason != old_reason):
        add_replaces(statement, old_ooc_id) # Add replaces statement
    elif (old_reference and except_reference != old_reference):
        add_replaces(statement, old_ooc_id) # Add replaces statement
        if statement['statementID'] == old_other_id:
            statement['statementID'] = generate_statement_id(statement['statementID'], "ownership")
            statement_id = statement['statementID']
    await latest_save(cache, f"{except_lei}_{except_type}_{except_reason}_ownership", statement_id, updates=updates)
    return statement_id, statement

class ProcessUpdates:
    """Data processor definition class"""
    def __init__(self, id_name=None, transform=None, updates=None, storage=None):
        """Initial setup"""
        self.transform = transform
        self.updates = updates
        self.id_name = id_name
        self.storage = storage
        self.cache = Caching(self.storage, batching=-1)

    async def setup(self):
        """Load data into cache"""
        await self.storage.setup()
        #await load_cache(self.storage)
        await self.cache.load()

    async def process(self, item, item_type, header, updates=False):
        """Process updates if applicable"""
        #print(f"Processing - updates: {updates}")
        entity_voided = False
        entity_type = None
        mapping, old_ooc_id, old_other_id, old_reason, old_reference, old_entity_type, except_lei, \
            except_type, except_reason, except_reference = await item_setup(self.cache, item, updates=updates)
        #print("mapping:", mapping)
        async for statement in self.transform.process(item, item_type, header, mapping=mapping):
            #print(f"Statement: {statement['statementID']} ({statement['statementType']})")
            statement_id = statement['statementID']
            if statement['statementType'] in ('entityStatement', 'personStatement'):
                entity_type = statement['statementType']
                if not "ExceptionCategory" in item:
                    lei = source_id(statement, self.id_name) # LEI for statement
                    statement_id, statement = await process_entity_lei(statement_id,
                                                                       statement,
                                                                       item,
                                                                       lei,
                                                                       updates,
                                                                       mapping,
                                                                       self.updates,
                                                                       self.cache)
                else:
                    statement_id, statement, void_statement = await process_entity_repex(statement_id,
                                            statement, item, except_lei, except_type, except_reason,
                                            old_reason, old_other_id, old_entity_type, self.updates, 
                                            mapping, self.cache, updates=updates)
                    if void_statement:
                        yield void_statement
                        entity_voided = True
                other_id = statement_id
            elif statement['statementType'] == 'ownershipOrControlStatement':
                # Get identifying features
                if "ExceptionCategory" in item:
                    start = except_lei
                    end = "None"
                    rel_type = convert_except_type(except_type)
                else:
                    start = item["Relationship"]["StartNode"]["NodeID"]
                    end = item["Relationship"]["EndNode"]["NodeID"]
                    rel_type = item["Relationship"]["RelationshipType"]
                # Save references
                if not "ExceptionCategory" in item:
                    statement_id, statement, void_statement = await process_ooc_rr(statement_id, statement, item,
                                                                   start, end, rel_type, entity_voided,
                                                                   updates, self.updates, self.cache)
                    if void_statement: yield void_statement
                else:
                    statement_id, statement = await process_ooc_repex(statement_id, statement, item,
                                                                      except_lei, except_type, except_reason,
                                                                      except_reference, old_reason, old_reference,
                                                                      old_ooc_id, old_other_id, self.updates, 
                                                                      self.cache, updates=updates)
                ooc_id = statement_id
            yield statement
        if "ExceptionCategory" in item:
            except_lei = item["LEI"]
            except_type = item["ExceptionCategory"]
            except_reason = item["ExceptionReason"]
            except_reference = item["ExceptionReference"] if "ExceptionReference" in item else None
            await exception_save(self.cache, f"{except_lei}_{except_type}", ooc_id, 
                                 other_id, except_reason, except_reference, entity_type, updates=updates)

    async def finish_updates(self, updates=False):
        """Process updates to referencing statements"""
        print("In finish_updates")
        if updates:
            done_updates = []
            async for ref_id, latest_id, todo_updates in process_updates(self.cache):
                print("Update:", ref_id, latest_id, updates)
                statement = await retrieve_statement(self.storage, "ownership", ref_id)
                old_statement_id = fix_statement_reference(statement, todo_updates, latest_id)
                statement_id = statement["statementID"]
                add_replaces(statement, old_statement_id)
                await latest_save(self.cache, latest_id, statement_id, updates=updates)
                #await updates_delete(self.storage, old_statement_id)
                done_updates.append(old_statement_id)
                #print("Updated statement:", statement)
                yield statement
            for statement_id in done_updates:
                await updates_delete(self.cache, statement_id)
        await self.cache.flush()
