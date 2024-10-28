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
from bodspipelines.pipelines.gleif.transforms import generate_statement_id, entity_id, rr_id, repex_id
from bodspipelines.pipelines.gleif.indexes import (lei_properties, rr_properties, repex_properties,
                                          match_lei, match_rr, match_repex,
                                          id_lei, id_rr, id_repex)

from bodspipelines.infrastructure.indexes import (entity_statement_properties, person_statement_properties, ownership_statement_properties,
                                          match_entity, match_person, match_ownership,
                                          id_entity, id_person, id_ownership)


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
                         "ownership": {"properties": ownership_statement_properties, "match": match_ownership, "id": id_ownership}}

# Identify type of GLEIF data
def identify_gleif(item):
    print("Identifying:", item)
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
def lei_list():
    """List entity LEIs"""
    return ['001GPB6A9XPE8XJICC14', '004L5FPTUREIWK9T2N63', '00EHHQ2ZHDCFXJCPCL46', '00GBW0Z2GYIER7DHDS71', '00KLB2PFTM3060S2N216',
            '00QDBXDXLLF3W3JJJO36', '00TR8NKAEL48RGTZEW89', '00TV1D5YIV5IDUGWBW29', '00W0SLGGVF0QQ5Q36N03', '00X5RQKJQQJFFX0WPA53', 
            '1595D0QCK7Y15293JK84', '213800FERQ5LE3H7WJ58', '213800BJPX8V9HVY1Y11']

@pytest.fixture
def last_update_list():
    """List update datetimes"""
    return ['2023-05-18T15:41:20.212Z', '2020-07-17T12:40:00.000Z', '2022-07-22T09:32:00.000Z', '2022-10-24T21:31:00.000Z',
              '2023-05-18T17:24:00.540Z', '2023-05-03T07:03:05.620Z', '2019-04-22T21:31:00.000Z', '2023-05-10T04:42:18.790Z', 
              '2020-07-17T12:40:00.000Z', '2020-07-24T19:29:00.000Z', '2023-03-10T13:08:56+01:00', '2023-02-02T09:07:52.390Z', 
              '2023-04-25T13:18:00Z']

@pytest.fixture
def xml_data_file():
    """GLEIF LEI XML data"""
    return Path("tests/fixtures/lei-data.xml")


@pytest.fixture
def rr_xml_data_file():
    """GLEIF RR XML data"""
    return Path("tests/fixtures/rr-data.xml")

@pytest.fixture
def json_data_file():
    """GLEIF LEI JSON data"""
    with open("tests/fixtures/lei-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def rr_json_data_file():
    """GLEIF RR JSON data"""
    with open("tests/fixtures/rr-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def repex_json_data_file():
    """GLEIF Repex JSON data"""
    with open("tests/fixtures/repex-data.json", "r") as read_file:
        return json.load(read_file)

@pytest.fixture
def ooc_json_data_file():
    """BODS Data craeted from GLEIF RR data"""
    with open("tests/fixtures/rr-data-out.json", "r") as read_file:
        return json.load(read_file)

def test_lei_ingest_stage(lei_list, last_update_list, xml_data_file):
    """Test ingest pipeline stage on LEI-CDF v3.1 records"""
    with (patch('bodspipelines.infrastructure.processing.bulk_data.BulkData.prepare') as mock_bd,
          patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):
        mock_bd.return_value = xml_data_file
        mock_pdr.return_value = None
        mock_sdr.return_value = None
        async def result():
            for lei, last in zip(lei_list, last_update_list):
                yield (True, {'create': {'_id': f"{lei}_{last}"}})
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

        # Defintion of LEI-CDF v3.1 XML date source
        lei_source = Source(name="lei",
                     origin=BulkData(display="LEI-CDF v3.1",
                                     #url=gleif_download_link("https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"),
                                     data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest"),
                                     size=41491,
                                     directory="lei-cdf"),
                     datatype=XMLData(item_tag="LEIRecord",
                                      namespace={"lei": "http://www.gleif.org/data/schema/leidata/2016"},
                                      filter=['NextVersion', 'Extension']))

        # GLEIF data: Store in Easticsearch and output new to Kinesis stream
        output_new = NewOutput(storage=Storage(storage=ElasticsearchClient(indexes=index_properties)),
                       output=KinesisOutput(stream_name="gleif-test"))
        # Definition of GLEIF data pipeline ingest stage
        ingest_stage = Stage(name="ingest-test",
              sources=[lei_source],
              processors=[],
              outputs=[output_new]
        )
        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif-test", stages=[ingest_stage])

        # Run pipelne
        pipeline.process("ingest-test")
        assert mock_kno.return_value.put.call_count == 13

        item = mock_kno.return_value.put.call_args.args[0]
        print(item)

        assert item['LEI'] == '213800BJPX8V9HVY1Y11'
        assert item['Entity']['LegalName'] == 'Swedeit Italian Aktiebolag'
        assert item['Entity']['LegalAddress'] == {'FirstAddressLine': 'C/O Anita Lindberg', 'MailRouting': 'C/O Anita Lindberg', 
                                        'AdditionalAddressLine': 'Fortgatan 11', 'City': 'Västra Frölunda', 'Region': 'SE-O', 
                                        'Country': 'SE', 'PostalCode': '426 76'}
        assert item['Entity']['HeadquartersAddress'] == {'FirstAddressLine': 'C/O Anita Lindberg', 'MailRouting': 'C/O Anita Lindberg', 
                                                      'AdditionalAddressLine': 'Fortgatan 11', 'City': 'Västra Frölunda', 'Region': 'SE-O', 
                                                      'Country': 'SE', 'PostalCode': '426 76'}
        assert {'FirstAddressLine': 'C/O Anita Lindberg', 'MailRouting': 'C/O Anita Lindberg', 'AdditionalAddressLine': 'Fortgatan 11', 
                'City': 'Vastra Frolunda', 'Region': 'SE-O', 'Country': 'SE', 'PostalCode': '426 76', 
                'type': 'AUTO_ASCII_TRANSLITERATED_LEGAL_ADDRESS'} in item['Entity']['TransliteratedOtherAddresses']
        assert {'FirstAddressLine': 'C/O Anita Lindberg', 'MailRouting': 'C/O Anita Lindberg', 'AdditionalAddressLine': 'Fortgatan 11', 
                'City': 'Vastra Frolunda', 'Region': 'SE-O', 'Country': 'SE', 'PostalCode': '426 76', 
                'type': 'AUTO_ASCII_TRANSLITERATED_HEADQUARTERS_ADDRESS'} in item['Entity']['TransliteratedOtherAddresses']
        assert item['Entity']['RegistrationAuthority'] == {'RegistrationAuthorityID': 'RA000544', 'RegistrationAuthorityEntityID': '556543-1193'}
        assert item['Entity']['LegalJurisdiction'] == 'SE'
        assert item['Entity']['EntityCategory'] == 'GENERAL'
        assert item['Entity']['LegalForm'] == {'EntityLegalFormCode': 'XJHM'}
        assert item['Entity']['EntityStatus'] == 'ACTIVE'
        assert item['Entity']['EntityCreationDate'] == '1997-06-05T02:00:00+02:00'
        assert item['Registration']['InitialRegistrationDate'] == '2014-04-09T00:00:00Z'
        assert item['Registration']['LastUpdateDate'] == '2023-04-25T13:18:00Z'
        assert item['Registration']['RegistrationStatus'] == 'ISSUED'
        assert item['Registration']['NextRenewalDate'] == '2024-05-12T06:59:39Z'
        assert item['Registration']['ManagingLOU'] == '549300O897ZC5H7CY412'
        assert item['Registration']['ValidationSources'] == 'FULLY_CORROBORATED'
        assert item['Registration']['ValidationAuthority'] == {'ValidationAuthorityID': 'RA000544', 'ValidationAuthorityEntityID': '556543-1193'}


def test_rr_ingest_stage(rr_xml_data_file, rr_json_data_file):
    """Test ingest pipeline stage on RR-CDF v2.1 records"""
    with (patch('bodspipelines.infrastructure.processing.bulk_data.BulkData.prepare') as mock_bd,
          patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):

        # Mock bulk data input
        mock_bd.return_value = rr_xml_data_file

        # Mock directories
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        # Mock ES stream bulk output
        async def result():
            for item in rr_json_data_file:
                yield (True, {'create': {'_id': id_rr(item)}})
        mock_sb.return_value = result()

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

        # Mock Kinesis input
        consumer_close_future = asyncio.Future()
        consumer_close_future.set_result(None)
        mock_kni.return_value.close.return_value = consumer_close_future

        # Set environment variables
        set_environment_variables()

        # Defintion of RR-CDF v2.1 XML date source
        rr_source = Source(name="rr",
                   origin=BulkData(display="RR-CDF v2.1",
                                   data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/rr/latest"),
                                   size=2823,
                                   directory="rr-cdf"),
                   datatype=XMLData(item_tag="RelationshipRecord",
                                    namespace={"rr": "http://www.gleif.org/data/schema/rr/2016"},
                                    filter=['Extension']))

        # GLEIF data: Store in Easticsearch and output new to Kinesis stream
        output_new = NewOutput(storage=Storage(storage=ElasticsearchClient(indexes=index_properties)),
                       output=KinesisOutput(stream_name="gleif-test"))

        # Definition of GLEIF data pipeline ingest stage
        ingest_stage = Stage(name="ingest-test",
              sources=[rr_source],
              processors=[],
              outputs=[output_new]
        )

        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif-test", stages=[ingest_stage])

        # Run pipelne
        pipeline.process("ingest-test")

        assert mock_kno.return_value.put.call_count == 10

        item = mock_kno.return_value.put.call_args.args[0]

        print(item)

        assert item['Relationship']['StartNode'] == {'NodeID': '010G7UHBHEI87EKP0Q97', 'NodeIDType': 'LEI'}
        assert item['Relationship']['EndNode'] == {'NodeID': '549300GC5MDF1KXYMP06', 'NodeIDType': 'LEI'}
        assert item['Relationship']['RelationshipType'] == 'IS_FUND-MANAGED_BY'
        assert item['Relationship']['RelationshipPeriods'] == [{'StartDate': '2023-05-09T06:14:06.846Z',
                                                                'PeriodType': 'RELATIONSHIP_PERIOD'}]
        assert item['Relationship']['RelationshipStatus'] == 'ACTIVE'
        assert item['Registration'] == {'InitialRegistrationDate': '2012-06-06T15:57:00.000Z', 
                                        'LastUpdateDate': '2023-05-16T14:21:08.886Z', 
                                        'RegistrationStatus': 'PUBLISHED', 
                                        'NextRenewalDate': '2024-06-27T00:00:00.000Z', 
                                        'ManagingLOU': 'EVK05KS7XY1DEII3R011', 
                                        'ValidationSources': 'FULLY_CORROBORATED', 
                                        'ValidationDocuments': 'SUPPORTING_DOCUMENTS', 
                                        'ValidationReference': 'https://www.sec.gov/Archives/edgar/data/1458460/000140508622000264/xslFormDX01/primary_doc.xml'}


def test_lei_transform_stage(lei_list, last_update_list, json_data_file):
    """Test transform pipeline stage on LEI-CDF v3.1 records"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):

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
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        async def result():
            for lei, last in zip(lei_list, last_update_list):
                data = {"LEI": lei, 'Registration': {'LastUpdateDate': last}}
                yield (True, {'create': {'_id': generate_statement_id(entity_id(data), 'entityStatement')}})
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

        set_environment_variables()

        # Kinesis stream of GLEIF data from ingest stage
        gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-test"),
                      datatype=JSONData())

        # BODS data: Store in Easticsearch and output new to Kinesis stream
        bods_output_new = NewOutput(storage=Storage(storage=ElasticsearchClient(indexes=bods_index_properties)),
                            output=KinesisOutput(stream_name="bods-gleif-test"),
                            identify=identify_bods)

        # Definition of GLEIF data pipeline transform stage
        transform_stage = Stage(name="transform-test",
              sources=[gleif_source],
              processors=[Gleif2Bods(identify=identify_gleif)],
              outputs=[bods_output_new]
        )

        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif", stages=[transform_stage])

        # Run pipelne
        pipeline.process("transform-test")
        assert mock_kno.return_value.put.call_count == 13

        item = mock_kno.return_value.put.call_args.args[0]
        print(item)

        assert item['statementID'] == 'e2d096a9-23d5-ab26-0943-44c62c6a6a98'
        assert item['statementType'] == 'entityStatement'
        assert item['statementDate'] == '2023-04-25'
        assert item['entityType'] == 'registeredEntity'
        assert item['name'] == 'Swedeit Italian Aktiebolag'
        assert item['incorporatedInJurisdiction'] == {'name': 'Sweden', 'code': 'SE'}
        assert {'id': '213800BJPX8V9HVY1Y11', 'scheme': 'XI-LEI', 'schemeName': 'Global Legal Entity Identifier Index'} in item['identifiers'] 
        assert {'id': '556543-1193', 'schemeName': 'RA000544'} in item['identifiers']
        assert {'type': 'registered', 'address': 'C/O Anita Lindberg, Västra Frölunda', 'country': 'SE', 'postCode': '426 76'} in item['addresses']
        assert {'type': 'business', 'address': 'C/O Anita Lindberg, Västra Frölunda', 'country': 'SE', 'postCode': '426 76'} in item['addresses']
        assert validate_date_now(item['publicationDetails']['publicationDate'])
        assert item['publicationDetails']['bodsVersion'] == '0.2'
        assert item['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
        assert item['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register', 'url': 'https://register.openownership.org'}
        assert item['source'] == {'type': ['officialRegister', 'verified'], 'description': 'GLEIF'}


def test_rr_transform_stage(rr_json_data_file, ooc_json_data_file):
    """Test transform pipeline stage on RR-CDF v2.1 records"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
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

        # Mock ES stream bulk output
        async def result():
            for item in rr_json_data_file:
                yield (True, {'create': {'_id': generate_statement_id(rr_id(item), 'ownershipOrControlStatement')}})
        mock_sb.return_value = result()

        # Mock Kinesis output
        put_future = asyncio.Future()
        put_future.set_result(None)
        mock_kno.return_value.put.return_value = put_future
        flush_future = asyncio.Future()
        flush_future.set_result(None)
        mock_kno.return_value.flush.return_value = flush_future

        # Mock Kinesis input
        producer_close_future = asyncio.Future()
        producer_close_future.set_result(None)
        mock_kno.return_value.close.return_value = producer_close_future

        # Set environment variables
        set_environment_variables()

        # Kinesis stream of GLEIF data from ingest stage
        gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-test"),
                      datatype=JSONData())

        # BODS data: Store in Easticsearch and output new to Kinesis stream
        bods_output_new = NewOutput(storage=Storage(storage=ElasticsearchClient(indexes=bods_index_properties)),
                            output=KinesisOutput(stream_name="bods-gleif-test"),
                            identify=identify_bods)

        # Definition of GLEIF data pipeline transform stage
        transform_stage = Stage(name="transform-test",
              sources=[gleif_source],
              processors=[Gleif2Bods(identify=identify_gleif)],
              outputs=[bods_output_new]
        )

        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif", stages=[transform_stage])

        # Run pipelne
        pipeline.process("transform-test")

        # Check item count
        assert mock_kno.return_value.put.call_count == 10

        item = mock_kno.return_value.put.call_args.args[0]

        print(item)

        assert item['statementID'] == '208d41ba-977c-6760-801e-77fdc9d47307'
        assert item['statementType'] == 'ownershipOrControlStatement'
        assert item['statementDate'] == '2023-05-16'
        assert item['subject'] == {'describedByEntityStatement': '06999bda-3717-8acb-719e-d6dd6d145c2a'}
        assert item['interestedParty'] == {'describedByEntityStatement': '5bd23434-6df3-b2cf-4f28-83f16e6fea7a'}
        assert item['interests'] == [{'type': 'other-influence-or-control',
                                      'interestLevel': 'direct', 
                                      'beneficialOwnershipOrControl': False,
                                      'startDate': '2023-05-09T06:14:06.846Z'}]
        assert validate_date_now(item['publicationDetails']['publicationDate'])
        assert item['publicationDetails']['bodsVersion'] == '0.2'
        assert item['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
        assert item['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register',
                                                           'url': 'https://register.openownership.org'}
        assert item['source'] == {'type': ['officialRegister', 'verified'], 'description': 'GLEIF'}


def test_rr_transform_stage(repex_json_data_file, ooc_json_data_file):
    """Test transform pipeline stage on RR-CDF v2.1 records"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.async_streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
          patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):

        # Mock Kinesis input
        async def result():
            for item in repex_json_data_file:
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
        mock_kni.return_value = AsyncIterator(repex_json_data_file)

        # Mock directories
        mock_pdr.return_value = None
        mock_sdr.return_value = None

        # Mock ES stream bulk output
        async def result():
            for item in repex_json_data_file:
                if item["ExceptionReason"] == "NO_KNOWN_PERSON":
                    yield (True, {'create': {'_id': generate_statement_id(repex_id(item),
                                                                      'personStatement')}})
                else:
                    yield (True, {'create': {'_id': generate_statement_id(repex_id(item),
                                                                      'entityStatement')}})
                yield (True, {'create': {'_id': generate_statement_id(repex_id(item), 
                                                                      'ownershipOrControlStatement')}})
        mock_sb.return_value = result()

        # Mock Kinesis output
        put_future = asyncio.Future()
        put_future.set_result(None)
        mock_kno.return_value.put.return_value = put_future
        flush_future = asyncio.Future()
        flush_future.set_result(None)
        mock_kno.return_value.flush.return_value = flush_future

        # Mock Kinesis input
        producer_close_future = asyncio.Future()
        producer_close_future.set_result(None)
        mock_kno.return_value.close.return_value = producer_close_future

        # Set environment variables
        set_environment_variables()

        # Kinesis stream of GLEIF data from ingest stage
        gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-test"),
                      datatype=JSONData())

        # BODS data: Store in Easticsearch and output new to Kinesis stream
        bods_output_new = NewOutput(storage=Storage(storage=ElasticsearchClient(indexes=bods_index_properties)),
                            output=KinesisOutput(stream_name="bods-gleif-test"),
                            identify=identify_bods)

        # Definition of GLEIF data pipeline transform stage
        transform_stage = Stage(name="transform-test",
              sources=[gleif_source],
              processors=[Gleif2Bods(identify=identify_gleif)],
              outputs=[bods_output_new]
        )

        # Definition of GLEIF data pipeline
        pipeline = Pipeline(name="gleif", stages=[transform_stage])

        # Run pipelne
        pipeline.process("transform-test")

        # Check item count
        assert mock_kno.return_value.put.call_count == 20

        item = mock_kno.return_value.put.call_args.args[0]

        print(item)

        assert item['statementID'] == '48c88421-b1ae-671d-82fa-00c3adc063a8'
        assert item['statementType'] == 'ownershipOrControlStatement'
        assert item['statementDate'] == '2023-06-09'
        assert item['subject'] == {'describedByEntityStatement': '34db1616-4997-534b-9b93-a9da9c8edda9'}
        assert item['interestedParty'] == {'describedByEntityStatement': '639d58eb-7bcc-de1d-da9f-74748d9c5cad'}
        assert item['interests'] == [{'type': 'other-influence-or-control',
                                   'interestLevel': 'direct',
                                   'beneficialOwnershipOrControl': False,
                                   'details': 'A controlling interest.'}]
        assert validate_date_now(item['publicationDetails']['publicationDate'])
        assert item['publicationDetails']['bodsVersion'] == '0.2'
        assert item['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
        assert item['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register',
                                                            'url': 'https://register.openownership.org'}
        assert item['source'] == {'type': ['officialRegister'], 'description': 'GLEIF'}
        assert item['annotations'][0]['motivation'] == 'commenting'
        assert item['annotations'][0]['description'] == 'The nature of this interest is unknown'
        assert item['annotations'][0]['statementPointerTarget'] == '/interests/0/type'
        assert validate_date_now(item['annotations'][0]['creationDate'])
        assert item['annotations'][0]['createdBy'] == {'name': 'Open Ownership',
                                                       'uri': 'https://www.openownership.org'}
