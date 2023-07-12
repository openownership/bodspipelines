import os
import sys
import time
import datetime
from pathlib import Path
import json
from unittest.mock import patch, Mock
import pytest

from bodspipelines.infrastructure.pipeline import Source, Stage, Pipeline
from bodspipelines.infrastructure.inputs import KinesisInput
from bodspipelines.infrastructure.storage import ElasticStorage
from bodspipelines.infrastructure.outputs import Output, OutputConsole, NewOutput, KinesisOutput
from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData
from bodspipelines.infrastructure.processing.json_data import JSONData
from bodspipelines.pipelines.gleif.utils import gleif_download_link
from bodspipelines.pipelines.gleif.transforms import Gleif2Bods
from bodspipelines.pipelines.gleif.transforms import generate_statement_id
from bodspipelines.pipelines.gleif.indexes import (lei_properties, rr_properties, repex_properties,
                                          match_lei, match_rr, match_repex,
                                          id_lei, id_rr, id_repex)

from bodspipelines.infrastructure.indexes import (entity_statement_properties, person_statement_properties, ownership_statement_properties,
                                          match_entity, match_person, match_ownership,
                                          id_entity, id_person, id_ownership)


def validate_datetime(d):
    try:
        datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S%z')
        return True
    except ValueError:
        return False


def validate_date_now(d):
    return d == datetime.date.today().strftime('%Y-%m-%d')


index_properties = {"lei": {"properties": lei_properties, "match": match_lei, "id": id_lei},
                    "rr": {"properties": rr_properties, "match": match_rr, "id": id_rr},
                    "repex": {"properties": repex_properties, "match": match_repex, "id": id_repex}}

# Elasticsearch indexes for BODS data
bods_index_properties = {"entity": {"properties": entity_statement_properties, "match": match_entity, "id": id_entity},
                         "person": {"properties": person_statement_properties, "match": match_person, "id": id_person},
                         "ownership": {"properties": ownership_statement_properties, "match": match_ownership, "id": id_ownership}}

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
    os.environ['ELASTICSEARCH_PROTOCOL'] = 'http'
    os.environ['ELASTICSEARCH_HOST'] = 'localhost'
    os.environ['ELASTICSEARCH_PORT'] = '9876'
    os.environ['ELASTICSEARCH_PASSWORD'] = '********'


@pytest.fixture
def lei_list():
    return ['001GPB6A9XPE8XJICC14', '004L5FPTUREIWK9T2N63', '00EHHQ2ZHDCFXJCPCL46', '00GBW0Z2GYIER7DHDS71', '00KLB2PFTM3060S2N216',
            '00QDBXDXLLF3W3JJJO36', '00TR8NKAEL48RGTZEW89', '00TV1D5YIV5IDUGWBW29', '00W0SLGGVF0QQ5Q36N03', '00X5RQKJQQJFFX0WPA53']


@pytest.fixture
def xml_data_file():
    return Path("tests/fixtures/lei-data.xml")


@pytest.fixture
def json_data_file():
    with open("tests/fixtures/lei-data.json", "r") as read_file:
        return json.load(read_file)


def test_lei_ingest_stage(lei_list, xml_data_file):
    """Test ingest pipeline stage on LEI-CDF v3.1 records"""
    with (patch('bodspipelines.infrastructure.processing.bulk_data.BulkData.prepare') as mock_bd,
          patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.kinesis_client.create_client') as mock_kn):
        mock_bd.return_value = xml_data_file
        mock_pdr.return_value = None
        mock_sdr.return_value = None
        mock_sb.return_value = iter([(True, {'create': {'_id': lei}}) for lei in lei_list])
        mock_kn.return_value.put_records.return_value = {'FailedRecordCount': 0, 'Records': []}
        set_environment_variables()

        # Defintion of LEI-CDF v3.1 XML date source
        lei_source = Source(name="lei",
                     origin=BulkData(display="LEI-CDF v3.1",
                                     url=gleif_download_link("https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"),
                                     size=41491,
                                     directory="lei-cdf"),
                     datatype=XMLData(item_tag="LEIRecord",
                                      namespace={"lei": "http://www.gleif.org/data/schema/leidata/2016"},
                                      filter=['NextVersion', 'Extension']))

        # GLEIF data: Store in Easticsearch and output new to Kinesis stream
        output_new = NewOutput(storage=ElasticStorage(indexes=index_properties),
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
        assert len(mock_kn.return_value.put_records.call_args.kwargs['Records']) == 10
        item = json.loads(mock_kn.return_value.put_records.call_args.kwargs['Records'][0]['Data'])
        assert item["LEI"] == "001GPB6A9XPE8XJICC14"
        assert item["Entity"]["LegalName"] == "Fidelity Advisor Leveraged Company Stock Fund"
        assert item["Entity"]["OtherEntityNames"] == [{'type': 'PREVIOUS_LEGAL_NAME', 
             'OtherEntityName': 'FIDELITY ADVISOR SERIES I - Fidelity Advisor Leveraged Company Stock Fund'}]
             #"FIDELITY ADVISOR SERIES I - Fidelity Advisor Leveraged Company Stock Fund"]
        assert item["Entity"]["LegalAddress"] == {"FirstAddressLine": "245 SUMMER STREET", "City": "BOSTON", "Region": "US-MA", "Country": "US", "PostalCode": "02210"}
        assert item["Entity"]["HeadquartersAddress"] == {"FirstAddressLine": "C/O Fidelity Management & Research Company LLC", 
                                                         "City": "Boston", "Region": "US-MA", "Country": "US", "PostalCode": "02210"}
        assert item["Entity"]["RegistrationAuthority"] == {"RegistrationAuthorityID": "RA000665", "RegistrationAuthorityEntityID": "S000005113"}
        assert item["Entity"]["LegalJurisdiction"] == "US-MA"
        assert item["Entity"]["EntityCategory"] == "FUND"
        assert item["Entity"]["LegalForm"] == {"EntityLegalFormCode": "8888", "OtherLegalForm": "FUND"}
        assert item["Entity"]["EntityStatus"] == "ACTIVE"
        assert item["Entity"]["EntityCreationDate"] == "2012-11-29T00:00:00.000Z"
        assert item["Registration"]["InitialRegistrationDate"] == "2012-11-29T16:33:00.000Z"
        assert item["Registration"]["LastUpdateDate"] == "2023-05-18T15:41:20.212Z"
        assert item["Registration"]["RegistrationStatus"] == "ISSUED"
        assert item["Registration"]["NextRenewalDate"] == "2024-05-18T15:48:53.604Z"
        assert item["Registration"]["ManagingLOU"] == "EVK05KS7XY1DEII3R011"
        assert item["Registration"]["ValidationSources"] == "FULLY_CORROBORATED"
        assert item["Registration"]["ValidationAuthority"] == {"ValidationAuthorityID": "RA000665", "ValidationAuthorityEntityID": "S000005113"}


def test_lei_transform_stage(lei_list, json_data_file):
    """Test transform pipeline stage on LEI-CDF v3.1 records"""
    with (patch('bodspipelines.infrastructure.pipeline.Pipeline.directory') as mock_pdr,
          patch('bodspipelines.infrastructure.pipeline.Stage.directory') as mock_sdr,
          patch('bodspipelines.infrastructure.clients.elasticsearch_client.streaming_bulk') as mock_sb,
          patch('bodspipelines.infrastructure.clients.kinesis_client.create_client') as mock_kn):
        mock_kn.return_value.get_records.return_value = {'MillisBehindLatest': 0, 'Records': [{'Data': json.dumps(js).encode('utf-8')} for js in json_data_file]}
        mock_pdr.return_value = None
        mock_sdr.return_value = None
        mock_sb.return_value = iter([(True, {'create': {'_id': generate_statement_id(lei, 'entityStatement')}}) for lei in lei_list])
        mock_kn.return_value.put_records.return_value = {'FailedRecordCount': 0, 'Records': []}
        set_environment_variables()

        # Kinesis stream of GLEIF data from ingest stage
        gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-test"),
                      datatype=JSONData())

        # BODS data: Store in Easticsearch and output new to Kinesis stream
        bods_output_new = NewOutput(storage=ElasticStorage(indexes=bods_index_properties),
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
        assert len(mock_kn.return_value.put_records.call_args.kwargs['Records']) == 10
        item = json.loads(mock_kn.return_value.put_records.call_args.kwargs['Records'][0]['Data'])
        assert item['statementID'] == '7239ce6e-d0c6-731a-30b5-b2157eb12419'
        assert item['statementType'] == 'entityStatement'
        assert item['statementDate'] == '2023-05-18'
        assert item['entityType'] == 'registeredEntity'
        assert item['name'] == 'Fidelity Advisor Leveraged Company Stock Fund'
        assert item['incorporatedInJurisdiction'] == {'name': 'US-MA', 'code': 'US-MA'}
        assert {'id': '001GPB6A9XPE8XJICC14', 'scheme': 'XI-LEI', 'schemeName': 'Global Legal Entity Identifier Index'} in item['identifiers']
        assert {'id': 'S000005113', 'schemeName': 'RA000665'} in item['identifiers']
        assert {'type': 'registered', 'address': '245 SUMMER STREET, BOSTON', 'country': 'US', 'postCode': '02210'} in item['addresses']
        assert {'type': 'business', 'address': 'C/O Fidelity Management & Research Company LLC, Boston', 'country': 'US', 'postCode': '02210'} in item['addresses'] 
        assert validate_date_now(item['publicationDetails']['publicationDate'])
        assert item['publicationDetails']['bodsVersion'] == '0.2'
        assert item['publicationDetails']['license'] == 'https://register.openownership.org/terms-and-conditions'
        assert item['publicationDetails']['publisher'] == {'name': 'OpenOwnership Register', 'url': 'https://register.openownership.org'}
        assert item['source'] == {'type': ['officialRegister', 'verified'], 'description': 'GLEIF'}
        assert item['foundingDate'] == '2012-11-29T00:00:00.000Z'
