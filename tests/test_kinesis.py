import os
import sys
import time
import datetime
from pathlib import Path
import json
from unittest.mock import patch, Mock, MagicMock
import asyncio
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


@pytest.fixture
def json_data_file():
    with open("tests/fixtures/lei-data.json", "r") as read_file:
        return json.load(read_file)


@pytest.fixture
def lei_list():
    return ['001GPB6A9XPE8XJICC14', '004L5FPTUREIWK9T2N63', '00EHHQ2ZHDCFXJCPCL46', '00GBW0Z2GYIER7DHDS71', '00KLB2PFTM3060S2N216',
            '00QDBXDXLLF3W3JJJO36', '00TR8NKAEL48RGTZEW89', '00TV1D5YIV5IDUGWBW29', '00W0SLGGVF0QQ5Q36N03', '00X5RQKJQQJFFX0WPA53',
            '1595D0QCK7Y15293JK84', '213800FERQ5LE3H7WJ58', '213800BJPX8V9HVY1Y11']


@pytest.mark.asyncio
async def test_kinesis_output(json_data_file):
    """Test Kinesis output stream"""
    with (patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
         patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):
        put_future = asyncio.Future()
        put_future.set_result(None)
        mock_kno.return_value.put.return_value = put_future
        flush_future = asyncio.Future()
        flush_future.set_result(None)
        mock_kno.return_value.flush.return_value = flush_future
        set_environment_variables()

        kinesis_output = KinesisOutput(stream_name="gleif-test")
        await kinesis_output.setup()

        for item in json_data_file:
            await kinesis_output.process(item, "lei")
        await kinesis_output.finish()

        assert mock_kno.return_value.put.call_count == 13


@pytest.mark.asyncio
async def test_kinesis_input(json_data_file, lei_list):
    """Test Kinesis input stream"""
    with (patch('bodspipelines.infrastructure.clients.kinesis_client.Producer') as mock_kno,
         patch('bodspipelines.infrastructure.clients.kinesis_client.Consumer') as mock_kni):
        async def result():
            for item in json_data_file:
                yield item
        mock_kni.return_value = result()
        #mock_kni.return_value = AsyncIterator(lei_list)
        set_environment_variables()

        kinesis_input = KinesisInput(stream_name="gleif-test")
        await kinesis_input.setup()

        count = 0
        async for item in kinesis_input.process():
            print(item)
            assert item == json_data_file[count]
            count += 1
        assert count == 13

