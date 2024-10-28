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
from bodspipelines.pipelines.gleif.transforms import Gleif2Bods, AddContentDate

from bodspipelines.pipelines.gleif.indexes import (lei_properties, rr_properties, repex_properties,
                                          match_lei, match_rr, match_repex,
                                          id_lei, id_rr, id_repex)

def set_environment_variables():
    """Set environment variables"""
    os.environ['ELASTICSEARCH_PROTOCOL'] = 'http'
    os.environ['ELASTICSEARCH_HOST'] = 'localhost'
    os.environ['ELASTICSEARCH_PORT'] = '9876'
    os.environ['ELASTICSEARCH_PASSWORD'] = '********'
    os.environ['BODS_AWS_REGION'] = "eu-west-1"
    os.environ['BODS_AWS_ACCESS_KEY_ID'] ="********************"
    os.environ['BODS_AWS_SECRET_ACCESS_KEY'] = "****************************************"

index_properties = {"lei": {"properties": lei_properties, "match": match_lei, "id": id_lei},
                    "rr": {"properties": rr_properties, "match": match_rr, "id": id_rr},
                    "repex": {"properties": repex_properties, "match": match_repex, "id": id_repex}}

# Identify type of GLEIF data
def identify_gleif(item):
    if 'Entity' in item:
        return 'lei'
    elif 'Relationship' in item:
        return 'rr'
    elif 'ExceptionCategory' in item:
        return 'repex'

@pytest.fixture
def repex_xml_data_file():
    """GLEIF Repex XML data"""
    return Path("tests/fixtures/repex-data.xml")

@pytest.fixture
def repex_list():
    """GLEIF Repex id list"""
    return ["001GPB6A9XPE8XJICC14_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NO_KNOWN_PERSON_None",
            "004L5FPTUREIWK9T2N63_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NON_CONSOLIDATING_None",
            "00EHHQ2ZHDCFXJCPCL46_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NON_CONSOLIDATING_None",
            "00GBW0Z2GYIER7DHDS71_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NON_CONSOLIDATING_None",
            "00KLB2PFTM3060S2N216_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NO_KNOWN_PERSON_None",
            "00QDBXDXLLF3W3JJJO36_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NO_KNOWN_PERSON_None",
            "00TV1D5YIV5IDUGWBW29_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NON_CONSOLIDATING_None",
            "00X8DSV26QKJPKUT5B34_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NON_CONSOLIDATING_None",
            "010G7UHBHEI87EKP0Q97_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NON_CONSOLIDATING_None",
            "01370W6ZIY66KQ4J3570_DIRECT_ACCOUNTING_CONSOLIDATION_PARENT_NON_PUBLIC_None"]

def test_repex_ingest_stage(repex_list, repex_xml_data_file):
    """Test ingest pipeline stage on Reporting Exceptions v2.1 XML records"""
    with (patch('bodspipelines.infrastructure.processing.bulk_data.BulkData.prepare') as mock_bd,
          patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):

        mock_bd.return_value = repex_xml_data_file
        mock_pdr.return_value = None
        mock_sdr.return_value = None
        async def result():
            for repex_id in repex_list:
                yield (True, {'create': {'_id': f"{repex_id}"}})
        mock_sb.return_value = result()
        put_future = asyncio.Future()
        put_future.set_result(None)
        mock_kno.return_value.put.return_value = put_future
        flush_future = asyncio.Future()
        flush_future.set_result(None)
        mock_kno.return_value.flush.return_value = flush_future
        producer_close_future = asyncio.Future()
        producer_close_future.set_result(None)
        mock_kno.return_value.close.return_value = producer_close_future
        consumer_close_future = asyncio.Future()
        consumer_close_future.set_result(None)
        mock_kni.return_value.close.return_value = consumer_close_future

        set_environment_variables()

        # Defintion of Reporting Exceptions v2.1 XML date source
        repex_source = Source(name="repex",
                      origin=BulkData(display="Reporting Exceptions v2.1",
                                      #url=gleif_download_link("https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"),
                                      data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/repex/latest"),
                                      size=3954,
                                      directory="rep-ex"),
                      datatype=XMLData(item_tag="Exception",
                                       header_tag="Header",
                                       namespace={"repex": "http://www.gleif.org/data/schema/repex/2016"},
                                       filter=['NextVersion', 'Extension']))

        # GLEIF data: Store in Easticsearch and output new to Kinesis stream
        output_new = NewOutput(storage=Storage(storage=ElasticsearchClient(indexes=index_properties)),
                       output=KinesisOutput(stream_name="gleif-test"))

        # Definition of GLEIF data pipeline ingest stage
        ingest_stage = Stage(name="ingest-test",
              sources=[repex_source],
              processors=[AddContentDate(identify=identify_gleif)],
              outputs=[output_new]
        )
        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif-test", stages=[ingest_stage])

        # Run pipelne
        pipeline.process("ingest-test")

        # Check item count
        assert mock_kno.return_value.put.call_count == 10

        item = mock_kno.return_value.put.call_args.args[0]

        print(item)

        assert item['LEI'] == '01370W6ZIY66KQ4J3570'
        assert item['ExceptionCategory'] == 'DIRECT_ACCOUNTING_CONSOLIDATION_PARENT'
        assert item['ExceptionReason'] == 'NON_PUBLIC'
        assert item['ContentDate'] == '2023-06-09T09:03:29Z'
