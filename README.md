# bodspipelines

Shared library intended to support for building pipelines to produce beneficial 
ownership statements (BODS) data. This library makes it possible to configure 
pipelines (of one or more stages) which can ingest data and transform it 
into [BODS v0.2](https://standard.openownership.org/en/0.2.0/) records. These 
records are then stored in Elasticsearch and optionally emitted into a AWS Kinesis 
stream (to transmit to a later pipeline stage or as final ouput). Often the final 
stage will have a Firehose delivery stream attached to the Kinesis stream, to
to write the BODS statements to a S3 bucket as JSON lines. 

## Usage

An example of a pipeline using the bodspipelines library is the 
[BODS GLEIF Pipeline](https://github.com/openownership/bods-gleif-pipeline). 
The basic logic for the pipeline is contained within the bodspipelines/pipelines 
directory of the library, whereas the more system specfic details (Docker 
configuration, scripts to control pipeline etc.) are in the specific pipeline's 
repository.

## Development

### Implimenting a new pipeline

The bodspipelines/pipelines directory contains a directory for each pipeline the 
library supports (e.g. GLEIF). Adding a new pipeline to the library requires adding
a new directory, with the pipeline name, in the bodspipelines/pipelines directory.
