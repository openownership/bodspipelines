from unittest.mock import patch, Mock, AsyncMock

from bodspipelines.pipelines.gleif import config
from bodspipelines.infrastructure.pipeline import Source, Stage, Pipeline
from bodspipelines.infrastructure.inputs import KinesisInput
from bodspipelines.infrastructure.storage import Storage
from bodspipelines.infrastructure.clients.elasticsearch_client import ElasticsearchClient
from bodspipelines.infrastructure.outputs import Output, OutputConsole, NewOutput, KinesisOutput
from bodspipelines.infrastructure.processing.json_data import JSONData
from bodspipelines.pipelines.gleif.utils import gleif_download_link, GLEIFData, identify_gleif
from bodspipelines.pipelines.gleif.transforms import Gleif2Bods
from bodspipelines.pipelines.gleif.updates import GleifUpdates
from bodspipelines.infrastructure.updates import ProcessUpdates
from bodspipelines.infrastructure.indexes import bods_index_properties
from bodspipelines.infrastructure.utils import identify_bods
from bodspipelines.pipelines.gleif.sources import GLEIFSource

from .utils import AsyncIterator, list_index_contents

async def run_transform_pipeline(data_file_gleif):
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
        mock_kni.return_value.read_stream.return_value = AsyncIterator(data_file_gleif)

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
                                         transform=GLEIFSource(), #Gleif2Bods(identify=identify_gleif),
                                         storage=Storage(storage=bods_storage),
                                         updates=GleifUpdates())],
              outputs=[bods_output_new]
        )

        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif", stages=[transform_stage])

        # Run pipelne
        await pipeline.process_stage("transform-test-updates", updates=True)

    return kinesis_output_stream
