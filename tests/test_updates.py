import os
import sys
import time
import datetime
from pathlib import Path
import json
import itertools
from unittest.mock import patch, Mock
import asyncio
import pytest

from bodspipelines.infrastructure.pipeline import Source, Stage, Pipeline
from bodspipelines.infrastructure.inputs import KinesisInput
from bodspipelines.infrastructure.storage import Storage
from bodspipelines.infrastructure.clients.elasticsearch_client import ElasticsearchClient
from bodspipelines.infrastructure.outputs import Output, OutputConsole, NewOutput, KinesisOutput
from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData
from bodspipelines.infrastructure.processing.json_data import JSONData
from bodspipelines.pipelines.gleif.utils import gleif_download_link, GLEIFData
from bodspipelines.pipelines.gleif.transforms import Gleif2Bods
from bodspipelines.pipelines.gleif.transforms import generate_statement_id, entity_id, rr_id, repex_id
from bodspipelines.pipelines.gleif.updates import GleifUpdates
from bodspipelines.pipelines.gleif.indexes import (lei_properties, rr_properties, repex_properties,
                                          match_lei, match_rr, match_repex,
                                          id_lei, id_rr, id_repex)
from bodspipelines.infrastructure.updates import ProcessUpdates, referenced_ids
from bodspipelines.infrastructure.indexes import (entity_statement_properties, person_statement_properties,
                                          ownership_statement_properties,
                                          match_entity, match_person, match_ownership,
                                          id_entity, id_person, id_ownership,
                                          latest_properties, match_latest, id_latest,
                                          references_properties, match_references, id_references,
                                          updates_properties, match_updates, id_updates,
                                          exceptions_properties, match_exceptions, id_exceptions)

def validate_datetime(d):
    """Test is valid datetime"""
    try:
        datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S%z')
        return True
    except ValueError:
        return False


def validate_date_now(d):
    """Test is today's date"""
    return d == datetime.date.today().strftime('%Y-%m-%d')

# Elasticsearch indexes for GLEIF data
index_properties = {"lei": {"properties": lei_properties, "match": match_lei, "id": id_lei},
                    "rr": {"properties": rr_properties, "match": match_rr, "id": id_rr},
                    "repex": {"properties": repex_properties, "match": match_repex, "id": id_repex}}

# Elasticsearch indexes for BODS data
bods_index_properties = {"entity": {"properties": entity_statement_properties, "match": match_entity, "id": id_entity},
                         "person": {"properties": person_statement_properties, "match": match_person, "id": id_person},
                         "ownership": {"properties": ownership_statement_properties, "match": match_ownership, "id": id_ownership},
                         "latest": {"properties": latest_properties, "match": match_latest, "id": id_latest},
                         "references": {"properties": references_properties, "match": match_references, "id": id_references},
                         "updates": {"properties": updates_properties, "match": match_updates, "id": id_updates},
                         "exceptions": {"properties": exceptions_properties, "match": match_exceptions, "id": id_exceptions}}

# Identify type of GLEIF data
def identify_gleif(item):
    if 'Entity' in item:
        return 'lei'
    elif 'Relationship' in item:
        return 'rr'
    elif 'ExceptionCategory' in item:
        return 'repex'

# Identify type of BODS data
def identify_bods(item):
    if item['statementType'] == 'entityStatement':
        return 'entity'
    elif item['statementType'] == 'personStatement':
        return 'person'
    elif item['statementType'] == 'ownershipOrControlStatement':
        return 'ownership'

def set_environment_variables():
    """Set environment variables"""
    os.environ['ELASTICSEARCH_PROTOCOL'] = 'http'
    os.environ['ELASTICSEARCH_HOST'] = 'localhost'
    os.environ['ELASTICSEARCH_PORT'] = '9876'
    os.environ['ELASTICSEARCH_PASSWORD'] = '********'
    os.environ['BODS_AWS_REGION'] = "eu-west-1"
    os.environ['BODS_AWS_ACCESS_KEY_ID'] ="********************"
    os.environ['BODS_AWS_SECRET_ACCESS_KEY'] = "****************************************"


@pytest.fixture
def updates_json_data_file():
    """GLEIF Updates Records"""
    out = []
    with (open("tests/fixtures/lei-updates-data2-new.json", "r") as lei_file,
         open("tests/fixtures/rr-updates-data2-new.json", "r") as rr_file,
         open("tests/fixtures/repex-updates-data2-new.json", "r") as repex_file):
        out.extend(json.load(lei_file))
        out.extend(json.load(rr_file))
        out.extend(json.load(repex_file))
    return out

@pytest.fixture
def rr_json_data_file_gleif():
    """GLEIF Relationship Records"""
    with open("tests/fixtures/rr-updates-data2.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_file_stored():
    """Stored Relationship Record Statements"""
    with open("tests/fixtures/rr-updates-data2-out.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_json_data_file_gleif():
    """GLEIF LEI Records"""
    with open("tests/fixtures/lei-updates-data2.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_json_data_file_stored():
    """LEI Stored Statements"""
    with open("tests/fixtures/lei-updates-data2-out.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_file_gleif():
    """GLEIF Reporting Exception Records"""
    with open("tests/fixtures/repex-updates-data2.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_file_gleif_new():
    """GLEIF Reporting Exception Records"""
    with open("tests/fixtures/repex-updates-data2-new.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_file_stored():
    """Reporting Exception Stored Statements"""
    with open("tests/fixtures/repex-updates-data2-out.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def lei_json_data_file_output():
    """LEI Output Statements"""
    with open("tests/fixtures/lei-updates-data2-new-out.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_file_output():
    """Relationship Record Output Statements"""
    with open("tests/fixtures/rr-updates-data2-new-out.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_file_output():
    """Reporting Exception Output Statements"""
    with open("tests/fixtures/repex-updates-data2-new-out.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def stored_references(lei_json_data_file_stored, rr_json_data_file_stored, rr_json_data_file_gleif):
    """Stored References"""
    references = {}
    for lei_statement in lei_json_data_file_stored:
        statement_id = lei_statement['statementID']
        ref_statement_ids = {}
        for rr_statement, rr_record in zip(rr_json_data_file_stored, rr_json_data_file_gleif):
            start = rr_record["Relationship"]["StartNode"]["NodeID"]
            end = rr_record["Relationship"]["EndNode"]["NodeID"]
            rel_type = rr_record["Relationship"]["RelationshipType"]
            if statement_id == rr_statement["subject"]["describedByEntityStatement"]:
                ref_statement_ids[rr_statement['statementID']] = f"{start}_{end}_{rel_type}"
            if (("describedByEntityStatement" in rr_statement["interestedParty"] and
                statement_id == rr_statement["interestedParty"]["describedByEntityStatement"]) or
                ("describedByPersonStatement" in rr_statement["interestedParty"] and
                statement_id == rr_statement["interestedParty"]["describedByPersonStatement"])):
                ref_statement_ids[rr_statement['statementID']] = f"{start}_{end}_{rel_type}"
        if ref_statement_ids:
           references[statement_id] = {'statement_id': statement_id, 'references_id': ref_statement_ids}
    return references

@pytest.fixture
def latest_ids(lei_json_data_file_stored, lei_json_data_file_gleif,
               rr_json_data_file_stored, rr_json_data_file_gleif,
               repex_json_data_file_stored, repex_json_data_file_gleif):
    """Latest statementIDs"""
    latest_index = {}
    for item, statement in zip(lei_json_data_file_gleif, lei_json_data_file_stored):
        latest = {'latest_id': item["LEI"],
                  'statement_id': statement["statementID"],
                  'reason': None}
        latest_index[item["LEI"]] = latest
    for item, statement in zip(rr_json_data_file_gleif, rr_json_data_file_stored):
        start = item["Relationship"]["StartNode"]["NodeID"]
        end = item["Relationship"]["EndNode"]["NodeID"]
        rel_type = item["Relationship"]["RelationshipType"]
        latest_id = f"{start}_{end}_{rel_type}"
        latest = {'latest_id': latest_id,
                  'statement_id': statement["statementID"],
                  'reason': None}
        latest_index[latest_id] = latest
    entity = True
    repex_json_data_double_gleif = list(itertools.chain.from_iterable(itertools.repeat(x, 2) 
                                        for x in repex_json_data_file_gleif))
    for item, statement in zip(repex_json_data_double_gleif, repex_json_data_file_stored):
        lei = item['LEI']
        category = item['ExceptionCategory']
        reason = item['ExceptionReason']
        if entity:
            latest_id = f"{lei}_{category}_{reason}_entity"
        else:
            latest_id = f"{lei}_{category}_{reason}_ownership"
        latest = {'latest_id': latest_id,
                  'statement_id': statement["statementID"],
                  'reason': reason}
        latest_index[latest_id] = latest
        entity = not entity
    return latest_index


@pytest.fixture
def exception_ids(repex_json_data_file_gleif, repex_json_data_file_stored):
    """Latest data for GLEIF Reporting Exceptions"""
    exception_data = {}
    repex_json_data_double_gleif = list(itertools.chain.from_iterable(itertools.repeat(x, 2)
                                        for x in repex_json_data_file_gleif))
    for item, statement in zip(repex_json_data_double_gleif, repex_json_data_file_stored):
        lei = item['LEI']
        category = item['ExceptionCategory']
        reason = item['ExceptionReason']
        reference = item['ExceptionReference'] if 'ExceptionReference' in item else None
        latest_id = f"{lei}_{category}"
        if statement["statementType"] in ("entityStatement", "personStatement"):
            exception_data[latest_id] = [None, statement["statementID"], reason, reference, statement["statementType"]]
        else:
            exception_data[latest_id][0] = statement["statementID"]
    return exception_data


@pytest.fixture
def voided_ids():
    """Statement ids of voiding statements"""
    return ["8f08b038-e0c2-0748-2a45-acb62b17f25b",
            "1408ba29-4792-1d02-472a-661e3962778d",
            "bcae847f-988a-2f75-ac09-6138864fd5a0",
            "2d6ce004-492d-34ff-652a-b525b5fe4ca8",
            "4243104a-bfef-7d92-a151-0e412a7a5269",
            "c67bb160-25b6-c5b9-5767-b636b9846c9f"]


@pytest.fixture
def updated_ids():
    """Statement ids for final update statements"""
    return ["89788883-ff51-a2c6-1be3-bcb7797abbae",
            "996c02d8-cbbd-8b95-7702-34c295fa183f"]


def assert_annotations(statement, annotation_list):
    """Asset statement contains listed annotations"""
    for annotation_desc in annotation_list:
        count = 0
        for annotation in statement["annotations"]:
            if annotation["description"] == annotation_desc: count += 1
        assert count == 1, f"Annotation '{annotation_desc}' not found in {statement['statementID']}"

def assert_lei_data(statement, lei_out, update=False, deleted=False):
    """Assert statement matches expected output for LEI"""
    print("Checking:", statement['statementID'])
    assert statement['statementID'] == lei_out['statementID']
    assert statement['statementType'] == lei_out['statementType']
    assert statement['statementDate'] == lei_out['statementDate']
    assert validate_date_now(statement['publicationDetails']['publicationDate'])
    if update:
        assert statement["replacesStatements"] == update
    else:
        assert not "replacesStatements" in statement # New LEI
    if deleted:
        assert_annotations(statement, ["GLEIF data for this entity - LEI: 00TR8NKAEL48RGTZEW89; Registration Status: RETIRED"])
    else:
        assert statement['name'] == lei_out['name']

def assert_rr_data(statement, rr_out, update=False, deleted=False):
    """Assert statement matches expected output for RR"""
    print("Checking:", statement['statementID'])
    assert statement['statementID'] == rr_out['statementID']
    assert statement['statementType'] == rr_out['statementType']
    assert statement['statementDate'] == rr_out['statementDate']
    if update:
        assert statement["replacesStatements"] == update
    else:
        assert not "replacesStatements" in statement # New LEI
    if deleted:
        assert statement["subject"]["describedByEntityStatement"] == ""
        assert statement["interestedParty"]["describedByEntityStatement"] == ""
        assert_annotations(statement, ["GLEIF relationship deleted on this statementDate."])
    else:
        assert (statement["subject"]["describedByEntityStatement"]
               == rr_out["subject"]["describedByEntityStatement"])
        assert (statement["interestedParty"]["describedByEntityStatement"]
               == rr_out["interestedParty"]["describedByEntityStatement"])

def assert_repex_data(statement, repex_out, stype, update=False, deleted=False):
    """Assert statement matches expected ouput for Repex"""
    print("Checking:", statement['statementID'], statement)
    assert statement['statementID'] == repex_out['statementID']
    if update:
        assert statement["replacesStatements"] == update
    else:
        assert not "replacesStatements" in statement
    if stype == 0:
        assert statement['statementType'] == repex_out['statementType']
        assert statement["annotations"] == repex_out["annotations"]
    elif stype == 1:
        assert statement['statementType'] == repex_out['statementType']
        if repex_out['statementType'] == "entityStatement":
            assert statement["entityType"] == repex_out["entityType"]
            assert (statement["unspecifiedEntityDetails"]["reason"] ==
                         repex_out["unspecifiedEntityDetails"]["reason"])
            assert (statement["unspecifiedEntityDetails"]["description"] ==
                       repex_out["unspecifiedEntityDetails"]["description"])
        elif repex_out['statementType'] == "personStatement":
            assert statement["personType"] == repex_out["personType"]
            assert (statement["unspecifiedPersonDetails"]["reason"] ==
                        repex_out["unspecifiedPersonDetails"]["reason"])
            assert (statement["unspecifiedPersonDetails"]["description"] ==
                       repex_out["unspecifiedPersonDetails"]["description"])
    else:
        assert statement["statementType"] == "ownershipOrControlStatement"
        assert (statement["subject"]["describedByEntityStatement"]
               == repex_out["subject"]["describedByEntityStatement"])
        if "describedByPersonStatement" in repex_out["interestedParty"]:
            assert (statement["interestedParty"]["describedByPersonStatement"]
               == repex_out["interestedParty"]["describedByPersonStatement"])
        else:
            assert (statement["interestedParty"]["describedByEntityStatement"]
               == repex_out["interestedParty"]["describedByEntityStatement"])
    if deleted:
        assert {"reason": "interested-party-exempt-from-disclosure",
                "description": "From LEI ExemptionReason `NON_CONSOLIDATING`. The legal entity or entities are not obliged to provide consolidated accounts in relation to the entity they control."
               } in statement["annotations"]

def test_transform_stage_updates(updates_json_data_file, rr_json_data_file_stored, lei_json_data_file_stored,
                                 lei_json_data_file_gleif, stored_references, latest_ids, exception_ids, 
                                 voided_ids, updated_ids, lei_json_data_file_output, rr_json_data_file_output,
                                 repex_json_data_file_output, repex_json_data_file_gleif, 
                                 repex_json_data_file_stored, repex_json_data_file_gleif_new):
    """Test transform pipeline stage on LEI-CDF v3.1 records"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_scan') as mock_as,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.AsyncElasticsearch') as mock_es,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):

        # Mock Kinesis input
        class AsyncIterator:
            def __init__(self, seq):
                self.iter = iter(seq)
            def __aiter__(self):
                return self
            async def __anext__(self):
                try:
                    return next(self.iter)
                except StopIteration:
                    raise StopAsyncIteration
            async def close(self):
                return None
        mock_kni.return_value = AsyncIterator(updates_json_data_file)

        # Mock directory methods
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        # Mock ES async_scan
        updates_stream = {}
        async def as_result():
            print("Updates stream:", json.dumps(updates_stream))
            for item in updates_stream:
                yield updates_stream[item]
        mock_as.return_value = as_result()

        # Mock ES async_streaming_bulk
        async def sb_result():
            voided = iter(voided_ids)
            first = True
            for item in updates_json_data_file:
                if "Entity" in item:
                    id = generate_statement_id(entity_id(item), 'entityStatement')
                    if item["LEI"] == "00TR8NKAEL48RGTZEW89": id = "c9c4f816-ec2e-5292-4a0c-5b1680ff46a0"
                    yield (True, {'create': {'_id': id}})
                elif "Relationship" in item:
                    print(item)
                    id = generate_statement_id(rr_id(item), 'ownershipOrControlStatement')
                    if (item['Relationship']['StartNode']['NodeID'] == '98450051BS9C610A8T78' and 
                        item['Relationship']['EndNode']['NodeID'] == '984500501A1B1045PB30'):
                        id = "f16a374a-09f1-a640-8303-a52e12957c30"
                    yield (True, {'create': {'_id': id}})
                elif "ExceptionCategory" in item:
                    print(item)
                    if "Extension" in item:
                        yield (True, {'create': {'_id': next(voided)}})
                        yield (True, {'create': {'_id': next(voided)}})
                    else:
                        if not item["LEI"] in ("159516LKTEGWITQUIE78", "159515PKYKQYJLT0KF16"):
                            print("Voiding:")
                            yield (True, {'create': {'_id': next(voided)}})
                        if item['ExceptionReason'] in ("NATURAL_PERSONS", "NO_KNOWN_PERSON"):
                            print("Person:")
                            yield (True, {'create': {'_id': generate_statement_id(repex_id(item), 'personStatement')}})
                        else:
                            print("Entity:")
                            yield (True, {'create': {'_id': generate_statement_id(repex_id(item), 'entityStatement')}})
                    if item["LEI"] != "2549007VY3IWUVGW7A82":
                        print("Ownership:")
                        yield (True, {'create': {'_id': generate_statement_id(repex_id(item), 'ownershipOrControlStatement')}})
            for id in updated_ids:
                yield (True, {'create': {'_id': id}})
        mock_sb.return_value = sb_result()

        # Mock Kinesis output stream (save results)
        kinesis_output_stream = []
        async def async_put(record):
            kinesis_output_stream.append(record)
            return None
        mock_kno.return_value.put.side_effect = async_put

        # Mock Kinesis output
        flush_future = asyncio.Future()
        flush_future.set_result(None)
        mock_kno.return_value.flush.return_value = flush_future
        producer_close_future = asyncio.Future()
        producer_close_future.set_result(None)
        mock_kno.return_value.close.return_value = producer_close_future

        # Mock Latest Index
        latest_index_data = latest_ids

        print("Initial latest IDs:", json.dumps(latest_index_data))

        print("Initial exception IDs:", json.dumps(exception_ids))

        # Mock ES index method
        async def async_index(index, document):
            print("async_index:", index, document)
            #if index == "references":
            #    stored_references.append(document['_source'])
            if index == "updates":
                 updates_stream[document['_source']['referencing_id']] = document['_source']
            elif index == "latest":
                 latest_index_data[document['_source']['latest_id']] = document['_source']
            return None
        mock_es.return_value.index.side_effect = async_index

        # Mock ES update method
        async def async_update(index, id, document):
            print("async_update:", index, id, document)
            if index == "latest":
                 latest_index_data[document['latest_id']] = document
            elif index == "updates":
                 updates_stream[document['referencing_id']] = document
            return None
        mock_es.return_value.update.side_effect = async_update

        # Mock ES delete method
        async def async_delete(index, id):
            print("async_delete:", index, id)
            if index == "latest":
                 del latest_index_data[id]
            elif index == "updates":
                 del updates_stream[id]
            return None
        mock_es.return_value.delete.side_effect = async_delete

        print("stored_references:", json.dumps(stored_references))

        # Mock ES search method
        async def async_search(index=None, query=None):
            id = query["query"]["match"]["_id"]
            print(f"Searching for {id} in {index}")
            if index == "latest":
                if id in latest_index_data:
                    results = [latest_index_data[id]]

                else:
                    results = []
            elif index == 'references':
                if id in stored_references:
                    results = [stored_references[id]]
                else:
                    results = []
            elif index == 'exceptions':
                if id in exception_ids:
                    results = [{'latest_id': id,
                               'statement_id': exception_ids[id][0],
                               'other_id': exception_ids[id][1],
                               'reason': exception_ids[id][2],
                               'reference': exception_ids[id][3],
                               'entity_type': exception_ids[id][4]}]
                    print("Stored exception:", results)
                else:
                    results = []
            elif index == "ownership":
                match = [item for item in rr_json_data_file_stored if item ['statementID'] == id]
                if match:
                    results = [match[0]]
                else:
                    results = []
            elif index == "updates":
                if id in updates_stream:
                    results = [updates_stream[id]]
                else:
                    results = []
            out = {'hits': {'hits': results}}
            return out
        mock_es.return_value.search.side_effect = async_search

        # Setup environment variables
        set_environment_variables()

        # Kinesis stream of GLEIF data from ingest stage
        gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-test"),
                      datatype=JSONData())

        # Storage
        bods_storage = ElasticsearchClient(indexes=bods_index_properties)

        # BODS data: Store in Easticsearch and output new to Kinesis stream
        bods_output_new = NewOutput(storage=Storage(storage=bods_storage),
                            output=KinesisOutput(stream_name="bods-gleif-test"),
                            identify=identify_bods)

        # Definition of GLEIF data pipeline transform stage
        transform_stage = Stage(name="transform-test-updates",
              sources=[gleif_source],
              processors=[ProcessUpdates(id_name='XI-LEI',
                                         transform=Gleif2Bods(identify=identify_gleif), 
                                         storage=Storage(storage=bods_storage),
                                         updates=GleifUpdates())],
              outputs=[bods_output_new]
        )

        assert hasattr(transform_stage.processors[0], "finish_updates")

        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif", stages=[transform_stage])

        # Run pipelne
        pipeline.process("transform-test-updates", updates=True)

        print(json.dumps(kinesis_output_stream))

        print("Final latest IDs:", json.dumps(latest_index_data))

        # Check updates empty
        assert len(updates_stream) == 0

        # Check LEI updates
        stored_offset = -1
        for i, item in enumerate(lei_json_data_file_output):
            print(i)
            if i == 0:
                assert_lei_data(kinesis_output_stream[i], item)
            elif i == 1:
                result = {"statementID": "c9c4f816-ec2e-5292-4a0c-5b1680ff46a0",
                          "statementDate": "2023-09-03",
                          "statementType": "entityStatement"}
                assert_lei_data(kinesis_output_stream[i], result, deleted=True, 
                      update=[lei_json_data_file_stored[i+stored_offset]["statementID"]])
            else:
                assert_lei_data(kinesis_output_stream[i], item,
                      update=[lei_json_data_file_stored[i+stored_offset]["statementID"]])
            if i == 7: stored_offset = 0

        # Offset for start of RR statements
        stream_offset = i + 1

        # Check RR updates
        stored_offset = -1
        for i, item in enumerate(rr_json_data_file_output):
            print(i+stream_offset)
            if i == 0:
                assert_rr_data(kinesis_output_stream[i+stream_offset], item)
            elif i == 9:
                result = {"statementID": "f16a374a-09f1-a640-8303-a52e12957c30",
                          "statementDate": "2023-11-01",
                          "statementType": "ownershipOrControlStatement"}
                assert_rr_data(kinesis_output_stream[i+stream_offset], result,
                    update=[rr_json_data_file_stored[i+stored_offset]["statementID"]], deleted=True)
            else:
                assert_rr_data(kinesis_output_stream[i+stream_offset], item,
                    update=[rr_json_data_file_stored[i+stored_offset]["statementID"]])
            if i == 5: stored_offset = 1

        # Offset for start of Repex statements
        stream_offset = stream_offset + i + 1

        # Check Repex updates
        stored_offset = -2
        cycle = 0
        count = 0
        ent_offset = 0
        for i, item in enumerate(repex_json_data_file_output):
            print(i)
            if i == 11:
                stream_offset -= 1
                continue
            if i < 2 or i > 10:
                if i == 0 or i == 12:
                    entity = 1
                else:
                    entity = 2
                if i == 13:
                    repex_update = [repex_json_data_file_stored[-1]["statementID"]]
                    assert_repex_data(kinesis_output_stream[i+stream_offset], item, entity, update=repex_update)
                else:
                    assert_repex_data(kinesis_output_stream[i+stream_offset], item, entity)
            else:
                if cycle == 0:
                    print("Current item:", item)
                    item_lei = item['annotations'][0]['description'].split()[-1]
                    repex_item = [ritem for ritem in repex_json_data_file_gleif 
                                           if ritem["LEI"] == item_lei][0]
                    repex_item_new = [ritem for ritem in repex_json_data_file_gleif_new
                                           if ritem["LEI"] == item_lei][0]
                    item_reason = repex_item["ExceptionReason"]
                    annotation_desc_del = f'Statement series retired due to deletion of a {item_reason} GLEIF Reporting Exception for {item_lei}'
                    annotation_desc_chg = f'Statement retired due to change in a {item_reason} GLEIF Reporting Exception for {item_lei}'
                    result = {"statementID": voided_ids[count],
                              "statementDate": datetime.date.today().strftime('%Y-%m-%d'),
                              "statementType": repex_json_data_file_stored[i + stored_offset + ent_offset]["statementType"],
                              }
                    if "Extension" in repex_item_new:
                        result["annotations"] = [{'motivation': 'commenting',
                                                  'description': annotation_desc_del,
                                                  'statementPointerTarget': '/',
                                                  'creationDate': datetime.date.today().strftime('%Y-%m-%d'),
                                                  'createdBy': {'name': 'Open Ownership',
                                                                'uri': 'https://www.openownership.org'}}]
                    else:
                        result["annotations"] = [{'motivation': 'commenting',
                                                  'description': annotation_desc_chg,
                                                  'statementPointerTarget': '/',
                                                  'creationDate': datetime.date.today().strftime('%Y-%m-%d'),
                                                  'createdBy': {'name': 'Open Ownership',
                                                                'uri': 'https://www.openownership.org'}}]
                    repex_update = [repex_json_data_file_stored[i + stored_offset + ent_offset]["statementID"]]
                    assert_repex_data(kinesis_output_stream[i+stream_offset], result, cycle, update=repex_update)
                    cycle = 1
                    if i == 10:
                        count += 1
                        stream_offset += 1
                        ent_offset += 1
                        result = {"statementID": voided_ids[count],
                                  "statementDate": datetime.date.today().strftime('%Y-%m-%d'),
                                  "statementType": "ownershipOrControlStatement",
                                  "interestedParty": {
                                      "describedByEntityStatement": ""
                                  },
                                  "subject": {
                                      "describedByEntityStatement": ""
                                  },
                                  "annotations": [{"statementPointerTarget": "/",
                                               "motivation": "commenting",
                                               "description": "GLEIF RegistrationStatus set to RETIRED on this statementDate."}],
                                  }
                        repex_update = [repex_json_data_file_stored[i + stored_offset + ent_offset]["statementID"]]
                        assert_repex_data(kinesis_output_stream[i+stream_offset], result, cycle, update=repex_update)
                    else:
                        stream_offset += 1
                        assert_repex_data(kinesis_output_stream[i+stream_offset], item, cycle)
                        cycle = 2
                elif cycle == 2:
                    repex_update = [repex_json_data_file_stored[i + stored_offset + ent_offset]["statementID"]]
                    assert_repex_data(kinesis_output_stream[i+stream_offset], item, cycle, update=repex_update)
                    cycle = 0
                    count += 1
            if i == 9: ent_offset += 2
