import time
import elastic_transport

from bodspipelines.infrastructure.pipeline import Source, Stage, Pipeline
from bodspipelines.infrastructure.inputs import KinesisInput
from bodspipelines.infrastructure.storage import ElasticStorage
from bodspipelines.infrastructure.outputs import Output, OutputConsole, NewOutput, KinesisOutput
from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData
from bodspipelines.infrastructure.processing.json_data import JSONData

from bodspipelines.infrastructure.indexes import (entity_statement_properties, person_statement_properties, ownership_statement_properties,
                                          match_entity, match_person, match_ownership,
                                          id_entity, id_person, id_ownership)

from bodspipelines.pipelines.gleif.transforms import Gleif2Bods
from bodspipelines.pipelines.gleif.indexes import (lei_properties, rr_properties, repex_properties,
                                          match_lei, match_rr, match_repex,
                                          id_lei, id_rr, id_repex)

# Defintion of LEI-CDF v3.1 XML date source
lei_source = Source(name="lei",
                     origin=BulkData(display="LEI-CDF v3.1",
                                     url='https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/30447/zip',
                                     size=41491,
                                     directory="lei-cdf"),
                     datatype=XMLData(item_tag="LEIRecord",
                                      namespace={"lei": "http://www.gleif.org/data/schema/leidata/2016"},
                                      filter=['NextVersion', 'Extension']))

# Defintion of RR-CDF v2.1 XML date source
rr_source = Source(name="rr",
                   origin=BulkData(display="RR-CDF v2.1",
                                   url='https://leidata.gleif.org/api/v1/concatenated-files/rr/get/30450/zip',
                                   size=2823,
                                   directory="rr-cdf"),
                   datatype=XMLData(item_tag="RelationshipRecord",
                                    namespace={"rr": "http://www.gleif.org/data/schema/rr/2016"},
                                    filter=['Extension']))

# Defintion of Reporting Exceptions v2.1 XML date source
repex_source = Source(name="repex",
                      origin=BulkData(display="Reporting Exceptions v2.1",
                                      url='https://leidata.gleif.org/api/v1/concatenated-files/repex/get/30453/zip',
                                      size=3954,
                                      directory="rep-ex"),
                      datatype=XMLData(item_tag="Exception",
                                       namespace={"repex": "http://www.gleif.org/data/schema/repex/2016"},
                                       filter=['NextVersion']))

# Console Output
#output_console = Output(name="console", target=OutputConsole(name="gleif-ingest"))

# Elasticsearch indexes for GLEIF data
index_properties = {"lei": {"properties": lei_properties, "match": match_lei, "id": id_lei},
                    "rr": {"properties": rr_properties, "match": match_rr, "id": id_rr},
                    "repex": {"properties": repex_properties, "match": match_repex, "id": id_repex}}

# GLEIF data: Store in Easticsearch and output new to Kinesis stream
output_new = NewOutput(storage=ElasticStorage(indexes=index_properties),
                       output=KinesisOutput(stream_name="gleif-raw-dev")) #gleif-prod-alt4"))

# Definition of GLEIF data pipeline ingest stage
ingest_stage = Stage(name="ingest",
              sources=[lei_source, rr_source, repex_source],
              processors=[],
              outputs=[output_new]
)

# Kinesis stream of GLEIF data from ingest stage
gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name="gleif-raw-dev"), #gleif-prod-alt4"),
                      datatype=JSONData())

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

# BODS data: Store in Easticsearch and output new to Kinesis stream
bods_output_new = NewOutput(storage=ElasticStorage(indexes=bods_index_properties),
                            output=KinesisOutput(stream_name="bods-gleif-dev"), #bods-gleif-prod-alt6"),
                            identify=identify_bods)

# Definition of GLEIF data pipeline transform stage
transform_stage = Stage(name="transform",
              sources=[gleif_source],
              processors=[Gleif2Bods(identify=identify_gleif)],
              outputs=[bods_output_new]
)

# Definition of GLEIF data pipeline
pipeline = Pipeline(name="gleif", stages=[ingest_stage, transform_stage])

# Setup Elasticsearch indexes
def setup():
    done = False
    while not done:
        try:
            ElasticStorage(indexes=index_properties).setup_indexes()
            ElasticStorage(indexes=bods_index_properties).setup_indexes()
            done = True
        except elastic_transport.ConnectionError:
            print("Waiting for Elasticsearch to start ...")
            time.sleep(5)

#stats = ElasticStorage(indexes=index_properties).stats
