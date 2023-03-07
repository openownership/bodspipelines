from bodspipelines.infrastructure.pipeline import Source, Stage, Pipeline
from bodspipelines.infrastructure.inputs import KinesisInput
from bodspipelines.infrastructure.storage import ElasticStorage
from bodspipelines.infrastructure.outputs import Output, OutputConsole, NewOutput, KinesisOutput
from bodspipelines.infrastructure.processing.bulk_data import BulkData
from bodspipelines.infrastructure.processing.xml_data import XMLData
from bodspipelines.infrastructure.processing.json_data import JSONData
from bodspipelines.transforms.gleif import Gleif2Bods
from bodspipelines.mappings.gleif import (lei_properties, rr_properties, repex_properties,
                                          match_lei, match_rr, match_repex,
                                          id_lei, id_rr, id_repex)
from bodspipelines.mappings.gleif import (entity_statement_properties, person_statement_properties, ownership_statement_properties,
                                          match_entity, match_person, match_ownership,
                                          id_entity, id_person, id_ownership)

# Defintion of LEI-CDF v3.1 XML date source
lei2_source = Source(name="lei2",
                     origin=BulkData(display="LEI-CDF v3.1",
                                     url='https://leidata.gleif.org/api/v1/concatenated-files/lei2/get/30447/zip',
                                     size=41491,
                                     directory="lei-cdf"),
                     datatype=XMLData(item_tag="LEIRecord",
                                      namespace={"lei": "http://www.gleif.org/data/schema/leidata/2016"}))

# Defintion of RR-CDF v2.1 XML date source
rr_source = Source(name="rr",
                   origin=BulkData(display="RR-CDF v2.1",
                                   url='https://leidata.gleif.org/api/v1/concatenated-files/rr/get/30450/zip',
                                   size=2823,
                                   directory="rr-cdf"),
                   datatype=XMLData(item_tag="RelationshipRecord",
                                    namespace={"rr": "http://www.gleif.org/data/schema/rr/2016"}))

# Defintion of Reporting Exceptions v2.1 XML date source
repex_source = Source(name="repex",
                      origin=BulkData(display="Reporting Exceptions v2.1",
                                      url='https://leidata.gleif.org/api/v1/concatenated-files/repex/get/30453/zip',
                                      size=3954,
                                      directory="rep-ex"),
                      datatype=XMLData(item_tag="Exception",
                                       namespace={"repex": "http://www.gleif.org/data/schema/repex/2016"}))

output_console = Output(name="console", target=OutputConsole(name="gleif-ingest"))

index_properties = {"lei2": {"properties": lei_properties, "match": match_lei, "id": id_lei},
                    "rr": {"properties": rr_properties, "match": match_rr, "id": id_rr},
                    "repex": {"properties": repex_properties, "match": match_repex, "id": id_repex}}

output_new = NewOutput(storage=ElasticStorage(indexes=index_properties),
                       output=KinesisOutput(stream_arn="arn:aws:kinesis:eu-west-1:696709126511:stream/new-gleif-dev"))

# Definition of GLEIF data pipeline injest stage
ingest_stage = Stage(name="ingest",
              sources=[lei2_source, rr_source, repex_source],
              processors=[],
              outputs=[output_new]
)

gleif_source = Source(name="gleif",
                      origin=KinesisInput(stream_arn="arn:aws:kinesis:eu-west-1:696709126511:stream/new-gleif-dev"),
                      datatype=JSONData())

bods_index_properties = {"entity": {"properties": entity_statement_properties, "match": match_entity, "id": id_entity},
                         "person": {"properties": person_statement_properties, "match": match_person, "id": id_person},
                         "ownership": {"properties": ownership_statement_properties, "match": match_ownership, "id": id_ownership}}

def identify_bods(item):
    if item['statementType'] == 'entityStatement':
        return 'entity'
    elif item['statementType'] == 'personStatement':
        return 'person'
    elif item['statementType'] == 'ownershipOrControlStatement':
        return 'ownership'

bods_output_new = NewOutput(storage=ElasticStorage(indexes=bods_index_properties),
                            output=KinesisOutput(stream_arn="arn:aws:kinesis:eu-west-1:696709126511:stream/bods-gleif-dev"),
                            identify=identify_bods)

# Definition of GLEIF data pipeline transform stage
transform_stage = Stage(name="transform",
              sources=[gleif_source],
              processors=[Gleif2Bods()],
              outputs=[bods_output_new]
)

# Definition of GLEIF data pipeline
pipeline = Pipeline(name="gleif", stages=[ingest_stage, transform_stage])

def setup():
    ElasticStorage(indexes=index_properties).setup_indexes()
    ElasticStorage(indexes=bods_index_properties).setup_indexes()

stats = ElasticStorage(indexes=index_properties).stats
