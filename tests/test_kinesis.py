import os
import sys
import time
import datetime
from pathlib import Path
import json
from unittest.mock import patch, Mock
import pytest

from bodspipelines.infrastructure.inputs import KinesisInput
from bodspipelines.infrastructure.outputs import KinesisOutput

def validate_datetime(d):
    try:
        datetime.datetime.strptime(d, '%Y-%m-%dT%H:%M:%S%z')
        return True
    except ValueError:
        return False


def validate_date_now(d):
    return d == datetime.date.today().strftime('%Y-%m-%d')


def set_environment_variables():
    os.environ['ELASTICSEARCH_PROTOCOL'] = 'http'
    os.environ['ELASTICSEARCH_HOST'] = 'localhost'
    os.environ['ELASTICSEARCH_PORT'] = '9876'
    os.environ['ELASTICSEARCH_PASSWORD'] = '********'


@pytest.fixture
def json_data_file():
    with open("tests/fixtures/lei-data.json", "r") as read_file:
        return json.load(read_file)


def test_kinesis_output(json_data_file):
    """Test Kinesis output stream"""
    with patch('bodspipelines.infrastructure.clients.kinesis_client.create_client') as mock_kn:
        mock_kn.return_value.put_records.return_value = {'FailedRecordCount': 0, 'Records': []}
        set_environment_variables()

        kinesis_output = KinesisOutput(stream_name="gleif-test")
        for item in json_data_file:
            kinesis_output.process(item, "lei")
        kinesis_output.finish()

        #assert len(mock_kn.return_value.put_records.call_args.kwargs['Records']) == 10
        count = 0
        for item in mock_kn.return_value.put_records.call_args.kwargs['Records']:
            assert json.loads(item['Data']) == json_data_file[count]
            count += 1
        assert count == 10


def test_kinesis_input(json_data_file):
    """Test Kinesis input stream"""
    with patch('bodspipelines.infrastructure.clients.kinesis_client.create_client') as mock_kn:
        mock_kn.return_value.get_records.return_value = {'MillisBehindLatest': 0, 'Records': [{'Data': json.dumps(js).encode('utf-8')} for js in json_data_file]}
        set_environment_variables()

        kinesis_input = KinesisInput(stream_name="gleif-test")
        count = 0
        for item in kinesis_input.process():
            assert item == json_data_file[count]
            count += 1
        assert count == 10

