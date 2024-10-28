import pytest
import os
import json
import requests
from unittest.mock import patch, Mock, AsyncMock

from .utils import AsyncIterator, list_index_contents
from .config import set_environment_variables

pytest_plugins = ["docker_compose"]

from bodspipelines.pipelines.gleif import config
from bodspipelines.infrastructure.pipeline import Source, Stage, Pipeline
from bodspipelines.infrastructure.inputs import KinesisInput
from bodspipelines.infrastructure.storage import Storage
from bodspipelines.infrastructure.clients.elasticsearch_client import ElasticsearchClient
from bodspipelines.infrastructure.outputs import Output, OutputConsole, NewOutput, KinesisOutput
from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData
from bodspipelines.infrastructure.processing.json_data import JSONData
from bodspipelines.pipelines.gleif.utils import gleif_download_link, GLEIFData, identify_gleif
from bodspipelines.pipelines.gleif.transforms import Gleif2Bods
from bodspipelines.pipelines.gleif.transforms import generate_statement_id, entity_id, rr_id, repex_id
from bodspipelines.pipelines.gleif.updates import GleifUpdates
from bodspipelines.pipelines.gleif.indexes import gleif_index_properties
from bodspipelines.infrastructure.updates import ProcessUpdates, referenced_ids
from bodspipelines.infrastructure.indexes import bods_index_properties
from bodspipelines.infrastructure.utils import identify_bods

# Setup environment variables
set_environment_variables()

#@pytest.fixture(scope="function")
#def wait_for_es(function_scoped_container_getter):
@pytest.fixture(scope="module")
def wait_for_es(module_scoped_container_getter):
    service = module_scoped_container_getter.get("bods_ingester_gleif_es").network_info[0]
    os.environ['ELASTICSEARCH_HOST'] = service.hostname
    os.environ['ELASTICSEARCH_PORT'] = service.host_port
    #config.setup()
    return service

@pytest.fixture
def lei_json_data_file_gleif():
    """GLEIF LEI Record update"""
    with open("tests/fixtures/lei-updates-replaces-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def relationship_update_data_file_gleif():
    """GLEIF LEI and RR Records update"""
    with open("tests/fixtures/relationship-update-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def reporting_exceptions_data_file_gleif():
    """GLEIF LEI and RR Records update"""
    with open("tests/fixtures/reporting-exceptions-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.mark.asyncio
async def test_lei_replaces(wait_for_es, lei_json_data_file_gleif):
    #print(requests.get('http://localhost:9200/_cat/indices?v').text)
    #assert False
    """Test transform pipeline stage on LEI-CDF v3.1 records"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.inputs.KinesisStream') as mock_kni,
          patch('bodspipelines.infrastructure.outputs.KinesisStream') as mock_kno,
          #patch('bodspipelines.infrastructure.outputs.KinesisOutput') as mock_kno,
          #patch('bodspipelines.infrastructure.inputs.KinesisInput') as mock_kni
          ):

        # Mock setup/finish_write functions
        async def async_func():
            return None
        mock_kni.return_value.setup.return_value = async_func()
        mock_kno.return_value.setup.return_value = async_func()
        mock_kno.return_value.finish_write.return_value = async_func()
        mock_kni.return_value.close.return_value = async_func()
        mock_kno.return_value.close.return_value = async_func()

        # Mock Kinesis stream input
        mock_kni.return_value.read_stream.return_value = AsyncIterator(lei_json_data_file_gleif)

        # Mock Kinesis output stream (save results)
        kinesis_output_stream = []
        async def async_put(record):
            kinesis_output_stream.append(record)
            return None
        mock_kno.return_value.add_record.side_effect = async_put

        # Mock directory methods
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        # Kinesis stream of GLEIF data from ingest stage
        gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-test"),
                      datatype=JSONData())

        # Storage
        bods_storage = ElasticsearchClient(indexes=bods_index_properties)

        # Setup indexes
        await bods_storage.setup_indexes()

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
        #pipeline.process("transform-test-updates", updates=True)
        await pipeline.process_stage("transform-test-updates", updates=True)

        print(kinesis_output_stream)

        assert kinesis_output_stream[0]['statementDate'] == '2023-12-29'
        assert kinesis_output_stream[-1]['statementDate'] == '2024-01-03'
        assert kinesis_output_stream[-1]['replacesStatements'][0] == kinesis_output_stream[0]['statementID']

@pytest.mark.asyncio
async def test_relationship_replaces(wait_for_es, relationship_update_data_file_gleif):
    """Test transform pipeline stage on relationship update"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.inputs.KinesisStream') as mock_kni,
          patch('bodspipelines.infrastructure.outputs.KinesisStream') as mock_kno,
          ):

        # Mock setup/finish_write functions
        async def async_func():
            return None
        mock_kni.return_value.setup.return_value = async_func()
        mock_kno.return_value.setup.return_value = async_func()
        mock_kno.return_value.finish_write.return_value = async_func()
        mock_kni.return_value.close.return_value = async_func()
        mock_kno.return_value.close.return_value = async_func()

        # Mock Kinesis stream input
        mock_kni.return_value.read_stream.return_value = AsyncIterator(relationship_update_data_file_gleif)

        # Mock Kinesis output stream (save results)
        kinesis_output_stream = []
        async def async_put(record):
            kinesis_output_stream.append(record)
            return None
        mock_kno.return_value.add_record.side_effect = async_put

        # Mock directory methods
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        # Kinesis stream of GLEIF data from ingest stage
        gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-test"),
                      datatype=JSONData())

        # Storage
        bods_storage = ElasticsearchClient(indexes=bods_index_properties)

        # Setup indexes
        await bods_storage.setup_indexes()

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
        #pipeline.process("transform-test-updates", updates=True)
        await pipeline.process_stage("transform-test-updates", updates=True)

        print(kinesis_output_stream)

        #list_index_contents("updates")

        assert kinesis_output_stream[0]['statementDate'] == '2023-02-22'
        assert kinesis_output_stream[1]['statementDate'] == '2023-02-22'
        assert kinesis_output_stream[2]['statementDate'] == '2023-02-22'
        assert kinesis_output_stream[3]['statementDate'] == '2024-01-30'
        assert kinesis_output_stream[4]['statementDate'] == '2024-01-30'
        assert kinesis_output_stream[5]['statementDate'] == '2024-01-30'
        assert kinesis_output_stream[-2]['replacesStatements'][0] == kinesis_output_stream[1]['statementID']
        assert kinesis_output_stream[-1]['replacesStatements'][0] == kinesis_output_stream[2]['statementID']

@pytest.mark.asyncio
async def test_reporting_exceptions(wait_for_es, reporting_exceptions_data_file_gleif):
    """Test transform pipeline stage on relationship update"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.inputs.KinesisStream') as mock_kni,
          patch('bodspipelines.infrastructure.outputs.KinesisStream') as mock_kno,
          ):

        # Mock setup/finish_write functions
        async def async_func():
            return None
        mock_kni.return_value.setup.return_value = async_func()
        mock_kno.return_value.setup.return_value = async_func()
        mock_kno.return_value.finish_write.return_value = async_func()
        mock_kni.return_value.close.return_value = async_func()
        mock_kno.return_value.close.return_value = async_func()

        # Mock Kinesis stream input
        mock_kni.return_value.read_stream.return_value = AsyncIterator(reporting_exceptions_data_file_gleif)

        # Mock Kinesis output stream (save results)
        kinesis_output_stream = []
        async def async_put(record):
            kinesis_output_stream.append(record)
            return None
        mock_kno.return_value.add_record.side_effect = async_put

        # Mock directory methods
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        # Kinesis stream of GLEIF data from ingest stage
        gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-test"),
                      datatype=JSONData())

        # Storage
        bods_storage = ElasticsearchClient(indexes=bods_index_properties)

        # Setup indexes
        await bods_storage.setup_indexes()

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
        #pipeline.process("transform-test-updates", updates=True)
        await pipeline.process_stage("transform-test-updates", updates=True)

        print(kinesis_output_stream)

        assert kinesis_output_stream[0]['statementDate'] == '2022-01-25'
        assert kinesis_output_stream[1]['statementDate'] == '2024-01-01'
        assert kinesis_output_stream[2]['statementDate'] == '2024-01-01'
        assert kinesis_output_stream[2]['subject']['describedByEntityStatement'] == kinesis_output_stream[0]['statementID']
        assert kinesis_output_stream[2]['interestedParty']['describedByEntityStatement'] == kinesis_output_stream[1]['statementID']
        assert kinesis_output_stream[3]['statementDate'] == '2024-01-01'
        assert kinesis_output_stream[4]['statementDate'] == '2024-01-01'
        assert kinesis_output_stream[4]['subject']['describedByEntityStatement'] == kinesis_output_stream[0]['statementID']
        assert kinesis_output_stream[4]['interestedParty']['describedByEntityStatement'] == kinesis_output_stream[3]['statementID']

