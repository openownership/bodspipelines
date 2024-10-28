import os
import sys
import time
import datetime
from pathlib import Path
import json
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
from bodspipelines.pipelines.gleif.transforms import generate_statement_id, entity_id, rr_id
from bodspipelines.pipelines.gleif.indexes import (lei_properties, rr_properties, repex_properties,
                                          match_lei, match_rr, match_repex,
                                          id_lei, id_rr, id_repex)
from bodspipelines.infrastructure.updates import ProcessUpdates
from bodspipelines.infrastructure.indexes import (entity_statement_properties, person_statement_properties,
                                                  ownership_statement_properties,
                                                  match_entity, match_person, match_ownership,
                                                  id_entity, id_person, id_ownership,
                                                  latest_properties, match_latest, id_latest,
                                                  references_properties, match_references, id_references,
                                                  updates_properties, match_updates, id_updates)


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
                         "updates": {"properties": updates_properties, "match": match_updates, "id": id_updates}}

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
    """Set environment variable"""
    os.environ['ELASTICSEARCH_PROTOCOL'] = 'http'
    os.environ['ELASTICSEARCH_HOST'] = 'localhost'
    os.environ['ELASTICSEARCH_PORT'] = '9876'
    os.environ['ELASTICSEARCH_PASSWORD'] = '********'
    os.environ['BODS_AWS_REGION'] = "eu-west-1"
    os.environ['BODS_AWS_ACCESS_KEY_ID'] ="********************"
    os.environ['BODS_AWS_SECRET_ACCESS_KEY'] = "****************************************"


@pytest.fixture
def lei_list():
    """GLEIF LEI list"""
    return ['001GPB6A9XPE8XJICC14', '004L5FPTUREIWK9T2N63', '00EHHQ2ZHDCFXJCPCL46', '00GBW0Z2GYIER7DHDS71', '00KLB2PFTM3060S2N216', 
            '00QDBXDXLLF3W3JJJO36', '00TR8NKAEL48RGTZEW89', '00TV1D5YIV5IDUGWBW29', '00W0SLGGVF0QQ5Q36N03', '00X5RQKJQQJFFX0WPA53', 
            '1595D0QCK7Y15293JK84', '213800FERQ5LE3H7WJ58', '213800BJPX8V9HVY1Y11']

@pytest.fixture
def lei_list_updates():
    """GLEIF LEI updates list"""
    return ["001GPB6A9XPE8XJICC14", "00EHHQ2ZHDCFXJCPCL46", "00QDBXDXLLF3W3JJJO36", "00W0SLGGVF0QQ5Q36N03", "213800FERQ5LE3H7WJ58"]

@pytest.fixture
def latest_lei_ids():
    """Latest GLEIF LEI statement ids"""
    return {
            '001GPB6A9XPE8XJICC14': ('6057835c-9fbe-44ce-bdb0-0a8d82f2259d', None),
            '004L5FPTUREIWK9T2N63': ('6e58bac7-a2c8-d233-fa74-e9738bc056d6', None),
            '00EHHQ2ZHDCFXJCPCL46': ('9d22180c-dff9-2e93-b7a7-f6aacdbf4d02', None),
            '00GBW0Z2GYIER7DHDS71': ('5f6448a2-b1fe-5e4f-8b1c-db0b1c627d98', None),
            '00KLB2PFTM3060S2N216': ('c786d0f1-87fc-d6cf-5149-75b76e853289', None),
            '00QDBXDXLLF3W3JJJO36': ('04357a16-7425-ad4e-8ed0-2e96a03dcdd9', None),
            '00TR8NKAEL48RGTZEW89': ('359d7ade-acf0-aa10-3422-55e6ae1ca187', None),
            '00TV1D5YIV5IDUGWBW29': ('1553f654-adde-1be3-9d6d-72c53b5af260', None),
            '00W0SLGGVF0QQ5Q36N03': ('90eebf21-d507-a242-cd90-98410834aaf0', None),
            '00X5RQKJQQJFFX0WPA53': ('6a3d293d-cc39-5f6c-850b-7855862c360c', None),
            '1595D0QCK7Y15293JK84': ('c1dec12f-c6b2-1f69-d2fd-75166e7df72f', None),
            '213800FERQ5LE3H7WJ58': ('6c7e8e94-e375-8d6e-408f-31bba4f5969a', None),
            '213800BJPX8V9HVY1Y11': ('e2d096a9-23d5-ab26-0943-44c62c6a6a98', None),
           }

@pytest.fixture
def last_update_list():
    """GLEIF LEI last update datetimes list"""
    return ['2023-05-18T15:41:20.212Z', '2020-07-17T12:40:00.000Z', '2022-07-22T09:32:00.000Z', '2022-10-24T21:31:00.000Z',
              '2023-05-18T17:24:00.540Z', '2023-05-03T07:03:05.620Z', '2019-04-22T21:31:00.000Z', '2023-05-10T04:42:18.790Z', 
              '2020-07-17T12:40:00.000Z', '2020-07-24T19:29:00.000Z', '2023-03-10T13:08:56+01:00', '2023-02-02T09:07:52.390Z', 
              '2023-04-25T13:18:00Z']

@pytest.fixture
def last_update_list_updates():
    """GLEIF LEI updates last update datetimes list"""
    return ["2023-06-18T15:41:20.212Z", "2022-08-22T09:32:00.000Z", "2023-06-03T07:03:05.620Z", "2020-08-17T12:40:00.000Z",
              "2023-03-02T09:07:52.390Z"]

@pytest.fixture
def xml_data_file():
    """GLEIF LEI XML data"""
    return Path("tests/fixtures/lei-data.xml")

@pytest.fixture
def json_data_file():
    """GLEIF LEI updates JSON data"""
    with open("tests/fixtures/lei-updates-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def json_data_out():
    """BODS statements built from GLEIF LEI updates data"""
    with open("tests/fixtures/lei-updates-data-out.json", "r") as read_file:
        return json.load(read_file)

def test_lei_transform_stage_updates(lei_list_updates, last_update_list_updates, latest_lei_ids, json_data_file, json_data_out):
    """Test transform pipeline stage on LEI-CDF v3.1 records"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_scan') as mock_as,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.AsyncElasticsearch') as mock_es,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):

        # Mock Kinesis input
        async def result():
            for item in json_data_file:
                yield item
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
        mock_kni.return_value = AsyncIterator(json_data_file)

        # Mock directory methods
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        # Mock ES async_scan
        updates_stream = {}
        async def as_result():
            for item in updates_stream:
                yield updates_stream[item]
        mock_as.return_value = as_result()

        # Mock ES async_streaming_bulk
        async def sb_result():
            for lei, last in zip(lei_list_updates, last_update_list_updates):
                data = {"LEI": lei, 'Registration': {'LastUpdateDate': last}}
                yield (True, {'create': {'_id': generate_statement_id(entity_id(data), 'entityStatement')}})
        mock_sb.return_value = sb_result()

        # Mock Kinesis output
        put_future = asyncio.Future()
        put_future.set_result(None)
        mock_kno.return_value.put.return_value = put_future
        flush_future = asyncio.Future()
        flush_future.set_result(None)
        mock_kno.return_value.flush.return_value = flush_future
        producer_close_future = asyncio.Future()
        producer_close_future.set_result(None)
        mock_kno.return_value.close.return_value = producer_close_future

        # Mock ES index method
        index_future = asyncio.Future()
        index_future.set_result(None)
        mock_es.return_value.index.return_value = index_future

        # Mock ES search method
        async def async_search(index=None, query=None):
            id = query["query"]["match"]["_id"]
            if index == "latest":
                result = [{'latest_id': id,
                           'statement_id': latest_lei_ids[id][0],
                           'reason': latest_lei_ids[id][1]}]
            elif index == 'references':
                result = []
            elif index == 'exceptions':
                result = []
            out = {'hits': {'hits': result}}
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
                                         storage=Storage(storage=bods_storage))],
              outputs=[bods_output_new]
        )

        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif", stages=[transform_stage])

        # Run pipelne
        pipeline.process("transform-test-updates", updates=True)
        assert mock_kno.return_value.put.call_count == 5
        #assert len(mock_kn.return_value.put_records.call_args.kwargs['Records']) == 10
        item = mock_kno.return_value.put.call_args.args[0]
        print(item)


        assert item['statementID'] == '11739b48-9df9-2d05-34aa-79c04d5cca06'
        assert item['statementType'] == 'entityStatement'
        assert item['statementDate'] == '2023-03-02'
        assert item['entityType'] == 'registeredEntity'
        assert item['name'] == 'DENTSU INTERNATIONAL LIMITED'
        assert item['incorporatedInJurisdiction'] == {'name': 'United Kingdom', 'code': 'GB'}
        assert {'id': '213800FERQ5LE3H7WJ58', 'scheme': 'XI-LEI', 'schemeName': 'Global Legal Entity Identifier Index'} in item['identifiers']
        assert {'id': '01403668', 'schemeName': 'RA000585'} in item['identifiers']
        assert {'type': 'registered', 'address': '10 TRITON STREET, LONDON', 'country': 'GB', 'postCode': 'NW1 3BF'} in item['addresses']
        assert {'type': 'business', 'address': '10 TRITON STREET, LONDON', 'country': 'GB', 'postCode': 'NW1 3BF'}  in item['addresses']
        assert validate_date_now(item['publicationDetails']['publicationDate'])
        assert item['publicationDetails']['bodsVersion'] == '0.2'
        assert item['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
        assert item['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register', 'url': 'https://register.openownership.org'}
        assert item['source'] == {'type': ['officialRegister', 'verified'], 'description': 'GLEIF'}
        assert item['foundingDate'] == '1978-12-05T00:00:00Z'
        assert item['replacesStatements'] == ['6c7e8e94-e375-8d6e-408f-31bba4f5969a']


@pytest.fixture
def rr_json_data_file():
    """GLEIF RR updates JSON data"""
    with open("tests/fixtures/rr-updates-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def ooc_json_data_file():
    """BODS statement produced from GLEIF RR updates data"""
    with open("tests/fixtures/rr-updates-data-out.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_file_gleif():
    """GLEIF RR JSON data"""
    with open("tests/fixtures/rr-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_file_stored():
    """BODS statements produced from GLEIF RR data"""
    with open("tests/fixtures/rr-data-out.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def latest_ids(rr_json_data_file_stored, rr_json_data_file_gleif):
    """Latest statementIDs"""
    latest_index = {}
    for item, statement in zip(rr_json_data_file_gleif, rr_json_data_file_stored):
        start = item["Relationship"]["StartNode"]["NodeID"]
        end = item["Relationship"]["EndNode"]["NodeID"]
        rel_type = item["Relationship"]["RelationshipType"]
        latest_id = f"{start}_{end}_{rel_type}"
        latest = {'latest_id': latest_id,
                  'statement_id': statement["statementID"],
                  'reason': None}
        latest_index[latest_id] = latest
    return latest_index


def test_rr_transform_stage_updates(rr_json_data_file, ooc_json_data_file, latest_ids):
    """Test transform pipeline stage on RR-CDF v2.1 records"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_scan') as mock_as,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.AsyncElasticsearch') as mock_es,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):

        # Mock Kinesis input
        async def result():
            for item in rr_json_data_file:
                yield item
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
        mock_kni.return_value = AsyncIterator(rr_json_data_file)

        # Mock directories
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        # Mock ES async_scan
        updates_stream = {}
        async def as_result():
            for item in updates_stream:
                yield updates_stream[item]
        mock_as.return_value = as_result()

        # Mock ES async_streaming_bulk
        async def sb_result():
            for item in rr_json_data_file:
                yield (True, {'create': {'_id': generate_statement_id(rr_id(item), 'ownershipOrControlStatement')}})
        mock_sb.return_value = sb_result()

        # Mock Kinesis output
        put_future = asyncio.Future()
        put_future.set_result(None)
        mock_kno.return_value.put.return_value = put_future
        flush_future = asyncio.Future()
        flush_future.set_result(None)
        mock_kno.return_value.flush.return_value = flush_future
        producer_close_future = asyncio.Future()
        producer_close_future.set_result(None)
        mock_kno.return_value.close.return_value = producer_close_future

        # Mock Latest Index
        latest_index_data = latest_ids

        # Mock ES index method
        async def async_index(index, document):
            #print("async_index:", index, document)
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
                 del latest_rr_ids[id]
            elif index == "updates":
                 print(updates_stream)
                 if id in updates_stream: del updates_stream[id] # IS THIS RIGHT?
            return None
        mock_es.return_value.delete.side_effect = async_delete

        # Mock ES search method
        async def async_search(index=None, query=None):
            id = query["query"]["match"]["_id"]
            if index == "latest":
                if id in latest_index_data:
                    results = [latest_index_data[id]]
                else:
                    results = []
            elif index == 'references':
                results = []
            elif index == 'exceptions':
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
                                         storage=Storage(storage=bods_storage))],
              outputs=[bods_output_new]
        )

        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif", stages=[transform_stage])

        # Run pipelne
        pipeline.process("transform-test-updates", updates=True)
        assert mock_kno.return_value.put.call_count == 3

        item = mock_kno.return_value.put.call_args.args[0]
        print(item)

        assert item['statementID'] == '8fec0d1c-b31b-7632-7c47-901e10bcd367'
        assert item['statementType'] == 'ownershipOrControlStatement'
        assert item['statementDate'] == '2023-03-24'
        assert item['subject'] == {'describedByEntityStatement': '9f94abe2-349c-8e96-edaf-cf832eab1ac8'}
        assert item['interestedParty'] == {'describedByEntityStatement': '20815b26-efdc-a516-a905-7abdfa63d128'}
        assert {'type': 'other-influence-or-control',
                'details': 'LEI RelationshipType: IS_ULTIMATELY_CONSOLIDATED_BY',
                'interestLevel': 'unknown',
                'beneficialOwnershipOrControl': False,
                'startDate': '2018-02-06T00:00:00.000Z'} in item['interests']
        assert validate_date_now(item['publicationDetails']['publicationDate'])
        assert item['publicationDetails']['bodsVersion'] == '0.2'
        assert item['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
        assert item['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register', 'url': 'https://register.openownership.org'}
        assert item['source'] == {'type': ['officialRegister', 'verified'], 'description': 'GLEIF'}
        assert item['replacesStatements'] == ['3cd06b78-3c1c-0cab-881b-cbbc36af1741']
