import os
import time
import elastic_transport
#import asyncio

from bodspipelines.infrastructure.pipeline import Source, Stage, Pipeline
from bodspipelines.infrastructure.inputs import KinesisInput
#from bodspipelines.infrastructure.storage import Storage
from bodspipelines.infrastructure.storage import ElasticStorage
#from bodspipelines.infrastructure.clients.elasticsearch_client import ElasticsearchClient
#from bodspipelines.infrastructure.clients.redis_client import RedisClient
from bodspipelines.infrastructure.outputs import Output, OutputConsole, NewOutput, KinesisOutput
from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData
from bodspipelines.infrastructure.processing.json_data import JSONData
from bodspipelines.infrastructure.updates import ProcessUpdates

#from bodspipelines.infrastructure.indexes import (entity_statement_properties,
#                                          person_statement_properties, ownership_statement_properties,
#                                          match_entity, match_person, match_ownership,
#                                          id_entity, id_person, id_ownership)
from bodspipelines.infrastructure.indexes import bods_index_properties

#from bodspipelines.infrastructure.indexes import (latest_properties, references_properties, updates_properties,
#                                          exceptions_properties, match_latest, match_references, match_updates,
#                                          match_exceptions, id_latest, id_references, id_updates, id_exceptions)
#

from bodspipelines.pipelines.gleif.transforms import Gleif2Bods, AddContentDate, RemoveEmptyExtension
#from bodspipelines.pipelines.gleif.indexes import (lei_properties, rr_properties, repex_properties,
#                                          match_lei, match_rr, match_repex,
#                                          id_lei, id_rr, id_repex)
from bodspipelines.pipelines.gleif.indexes import gleif_index_properties
from bodspipelines.pipelines.gleif.utils import gleif_download_link, GLEIFData, identify_gleif
from bodspipelines.pipelines.gleif.updates import GleifUpdates
from bodspipelines.infrastructure.utils import identify_bods

# Identify type of GLEIF data
#def identify_gleif(item):
#    if 'Entity' in item:
#        return 'lei'
#    elif 'Relationship' in item:
#        return 'rr'
#    elif 'ExceptionCategory' in item:
#        return 'repex'

# Identify type of BODS data
#def identify_bods(item):
#    if item['statementType'] == 'entityStatement':
#        return 'entity'
#    elif item['statementType'] == 'personStatement':
#        return 'person'
#    elif item['statementType'] == 'ownershipOrControlStatement':
#        return 'ownership'

# Defintion of LEI-CDF v3.1 XML date source
lei_source = Source(name="lei",
                     origin=BulkData(display="LEI-CDF v3.1",
                                     #url='https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/30447/zip',
                                     #url=gleif_download_link("https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"),
                                     data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest",
                                                    data_date="2024-01-01"),
                                     size=41491,
                                     directory="lei-cdf"),
                     datatype=XMLData(item_tag="LEIRecord",
                                      namespace={"lei": "http://www.gleif.org/data/schema/leidata/2016",
                                                 "gleif": "http://www.gleif.org/data/schema/golden-copy/extensions/1.0"},
                                      filter=['NextVersion', 'Extension']))

# Defintion of RR-CDF v2.1 XML date source
rr_source = Source(name="rr",
                   origin=BulkData(display="RR-CDF v2.1",
                                   #url='https://leidata.gleif.org/api/v1/concatenated-files/rr/get/30450/zip',
                                   #url=gleif_download_link("https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"),
                                   data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/rr/latest",
                                                  data_date="2024-01-01"),
                                   size=2823,
                                   directory="rr-cdf"),
                   datatype=XMLData(item_tag="RelationshipRecord",
                                    namespace={"rr": "http://www.gleif.org/data/schema/rr/2016",
                                               "gleif": "http://www.gleif.org/data/schema/golden-copy/extensions/1.0"},
                                    #filter=['Extension']
                                    filter=['NextVersion', ]))

# Defintion of Reporting Exceptions v2.1 XML date source
repex_source = Source(name="repex",
                      origin=BulkData(display="Reporting Exceptions v2.1",
                                      #url='https://leidata.gleif.org/api/v1/concatenated-files/repex/get/30453/zip',
                                      #url=gleif_download_link("https://goldencopy.gleif.org/api/v2/golden-copies/publishes/latest"),
                                      data=GLEIFData(url="https://goldencopy.gleif.org/api/v2/golden-copies/publishes/repex/latest",
                                                    data_date="2024-01-01"),
                                      size=3954,
                                      directory="rep-ex"),
                      datatype=XMLData(item_tag="Exception",
                                       header_tag="Header",
                                       namespace={"repex": "http://www.gleif.org/data/schema/repex/2016",
                                                  "gleif": "http://www.gleif.org/data/schema/golden-copy/extensions/1.0"},
                                       #filter=['NextVersion', 'Extension']
                                       filter=['NextVersion', ]))

# Elasticsearch indexes for GLEIF data
#index_properties = {"lei": {"properties": lei_properties, "match": match_lei, "id": id_lei},
#                    "rr": {"properties": rr_properties, "match": match_rr, "id": id_rr},
#                    "repex": {"properties": repex_properties, "match": match_repex, "id": id_repex}}

# Easticsearch storage for GLEIF data
#gleif_storage = ElasticsearchClient(indexes=index_properties)
#gleif_storage = ElasticStorage(indexes=index_properties)

# GLEIF data: Store in Easticsearch and output new to Kinesis stream
output_new = NewOutput(storage=ElasticStorage(indexes=gleif_index_properties),
                       output=KinesisOutput(stream_name=os.environ.get('GLEIF_KINESIS_STREAM')))

# Definition of GLEIF data pipeline ingest stage
ingest_stage = Stage(name="ingest",
              sources=[lei_source, rr_source, repex_source],
              #sources=[repex_source],
              processors=[AddContentDate(identify=identify_gleif),
                          RemoveEmptyExtension(identify=identify_gleif)],
              outputs=[output_new])

# Kinesis stream of GLEIF data from ingest stage
gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_name=os.environ.get('GLEIF_KINESIS_STREAM')),
                      datatype=JSONData())

# Elasticsearch indexes for BODS data
#bods_index_properties = {"entity": {"properties": entity_statement_properties, "match": match_entity, "id": id_entity},
#                         "person": {"properties": person_statement_properties, "match": match_person, "id": id_person},
#                         "ownership": {"properties": ownership_statement_properties, "match": match_ownership, "id": id_ownership},
#                         "latest": {"properties": latest_properties, "match": match_latest, "id": id_latest},
#                         "references": {"properties": references_properties, "match": match_references, "id": id_references},
#                         "updates": {"properties": updates_properties, "match": match_updates, "id": id_updates},
#                         "exceptions": {"properties": exceptions_properties, "match": match_exceptions, "id": id_exceptions}}

# Easticsearch storage for BODS data
#bods_storage = ElasticsearchClient(indexes=bods_index_properties)
#bods_storage = ElasticStorage(indexes=bods_index_properties)

# BODS data: Store in Easticsearch and output new to Kinesis stream
bods_output_new = NewOutput(storage=ElasticStorage(indexes=bods_index_properties),
                            output=KinesisOutput(stream_name=os.environ.get('BODS_KINESIS_STREAM')),
                            identify=identify_bods)

# Definition of GLEIF data pipeline transform stage
transform_stage = Stage(name="transform",
              sources=[gleif_source],
              processors=[ProcessUpdates(id_name='XI-LEI',
                                         transform=Gleif2Bods(identify=identify_gleif),
                                         storage=ElasticStorage(indexes=bods_index_properties),
                                         updates=GleifUpdates())],
              outputs=[bods_output_new])

# Definition of GLEIF data pipeline
pipeline = Pipeline(name="gleif", stages=[ingest_stage, transform_stage])

# Setup storage indexes
#async def setup_indexes():
#    await gleif_storage.setup_indexes()
#    await bods_storage.setup_indexes()

# Setup pipeline storage
#def setup():
#    loop = asyncio.new_event_loop()
#    asyncio.set_event_loop(loop)
#    loop.run_until_complete(setup_indexes())

# Setup Elasticsearch indexes
def setup():
    done = False
    while not done:
        try:
            ElasticStorage(indexes=gleif_index_properties).setup_indexes()
            ElasticStorage(indexes=bods_index_properties).setup_indexes()
            done = True
        except elastic_transport.ConnectionError:
            print("Waiting for Elasticsearch to start ...")
            time.sleep(5)
