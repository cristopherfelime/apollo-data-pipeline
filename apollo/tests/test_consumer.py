"""
    unit testing script for ApolloKafkaConsumer in consumer.py
    v0.1
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
import os
import orjson
from uuid import uuid4
from datetime import datetime, timezone
from asyncio import CancelledError
from unittest.mock import patch, AsyncMock
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.structs import ConsumerRecord, TopicPartition

from apollo.kafka.consumer import ApolloKafkaConsumer

load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture
def sample_review_raw_bytes():
    """synthetic single ReviewPayload event serialized as UTF-8 JSON bytes"""
    return orjson.dumps({
        "event_id": str(uuid4()),
        "app_id": "my.com.gxbank.app",
        "app_name": "GX Bank",
        "user_name": "Farhan Azmi",
        "rating": 5,
        "review_text": "Great UI and instant transfers with no hidden fees!",
        "app_version": "1.4.2",
        "submitted_at": "2026-08-10T14:30:00Z",
        "ingested_at": "2026-08-10T14:35:00Z"
    })

@pytest.fixture
def sample_news_raw_bytes():
    """synthetic single FinancialNewsPayload event serialized as UTF-8 JSON bytes"""
    return orjson.dumps({
        "event_id": str(uuid4()),
        "article_uuid": "marketaux-uuid-12345",
        "title": "Bank Negara Malaysia Issues Updated Digital Banking Framework",
        "snippet": "BNM today announced comprehensive updated guidelines for digital banks operating in Malaysia.",
        "url": "https://www.thestar.com.my/business/2026/08/digital-banks",
        "source": "thestar.com.my",
        "sentiment_score": 0.456,
        "published_at": "2026-08-01T10:30:00Z",
        "ingested_at": "2026-08-01T10:35:00Z"
    })

@pytest.fixture
def sample_consumer_records(sample_review_raw_bytes, sample_news_raw_bytes):
    """synthetic list of 4 ConsumerRecord instances across both topics and multiple partition keys"""
    return [
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=101,
            timestamp=1787119344000,
            timestamp_type=0,
            key=b"my.com.gxbank.app",
            value=sample_review_raw_bytes,
            checksum=None,
            serialized_key_size=17,
            serialized_value_size=len(sample_review_raw_bytes),
            headers=()
        ),
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=102,
            timestamp=1787119345000,
            timestamp_type=0,
            key=b"com.maybank2u.life",
            value=sample_review_raw_bytes,
            checksum=None,
            serialized_key_size=18,
            serialized_value_size=len(sample_review_raw_bytes),
            headers=()
        ),
        ConsumerRecord(
            topic="market-news-events",
            partition=0,
            offset=201,
            timestamp=1787119346000,
            timestamp_type=0,
            key=b"thestar.com.my",
            value=sample_news_raw_bytes,
            checksum=None,
            serialized_key_size=14,
            serialized_value_size=len(sample_news_raw_bytes),
            headers=()
        ),
        ConsumerRecord(
            topic="market-news-events",
            partition=0,
            offset=202,
            timestamp=1787119347000,
            timestamp_type=0,
            key=b"fintechnews.my",
            value=sample_news_raw_bytes,
            checksum=None,
            serialized_key_size=14,
            serialized_value_size=len(sample_news_raw_bytes),
            headers=()
        )
    ]

@pytest.fixture
def sample_getmany_batch_dict(sample_consumer_records):
    """synthetic dict[TopicPartition, list[ConsumerRecord]] returned by aiokafka.AIOKafkaConsumer.getmany()"""
    tp_reviews = TopicPartition("app-reviews-events", 0)
    tp_news = TopicPartition("market-news-events", 0)
    return {
        tp_reviews: [sample_consumer_records[0], sample_consumer_records[1]],
        tp_news: [sample_consumer_records[2], sample_consumer_records[3]]
    }

@pytest.fixture
def sample_empty_getmany_batch():
    """synthetic empty dictionary returned by getmany() on poll timeout when no new messages exist"""
    return {}

@pytest.fixture
def sample_custom_consumer_config():
    """synthetic custom bootstrap servers, topics tuple, and group_id configuration"""
    return {
        "bootstrap_servers": "custom-kafka-broker:9094",
        "topics": ("custom-reviews-topic", "custom-news-topic"),
        "group_id": "custom-analytics-group"
    }

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# management tests

"""
    MANAGEMENT TEST
    test ApolloKafkaConsumer initialization with default bootstrap servers and custom bootstrap servers
"""
def test_kafka_consumer_init_default_and_custom(sample_custom_consumer_config) -> None:
    # default initialization
    default_consumer = ApolloKafkaConsumer()
    assert default_consumer.bootstrap_servers == f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}"
    assert default_consumer._topics == ("app-reviews-events", "market-news-events")
    assert default_consumer._group_id == "apollo-db-persister"

    # custom initialization
    custom_consumer = ApolloKafkaConsumer(**sample_custom_consumer_config)
    assert custom_consumer.bootstrap_servers == sample_custom_consumer_config["bootstrap_servers"]
    assert custom_consumer._topics == sample_custom_consumer_config["topics"]
    assert custom_consumer._group_id == sample_custom_consumer_config["group_id"]

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# lifecycle and context manager tests

"""
    LIFECYCLE TEST
    tests ApolloKafkaConsumer start and stop methods
"""
@pytest.mark.anyio
async def test_kafka_cosumer_start_and_stop() -> None:
    default_consumer = ApolloKafkaConsumer()
    assert default_consumer._consumer is None

    mock_consumer = AsyncMock(spec=AIOKafkaConsumer)
    with patch("apollo.kafka.consumer.AIOKafkaConsumer", return_value=mock_consumer):
        # attempt to start the kafka consumer instance
        await default_consumer.start()
        assert default_consumer._consumer is not None

        # attempt to stop above
        await default_consumer.stop()
        mock_consumer.stop.assert_awaited_once()
        assert default_consumer._consumer is None

"""
    CONTEXT MANAGER TEST
    tests ApolloKafkaConsumer async context manager
"""
@pytest.mark.anyio
async def test_kafka_consumer_context_manager() -> None:
    default_consumer = ApolloKafkaConsumer()
    assert default_consumer._consumer is None

    mock_consumer = AsyncMock(spec=AIOKafkaConsumer)
    with patch("apollo.kafka.consumer.AIOKafkaConsumer", return_value=mock_consumer):
        # attempt to start with context manager
        async with default_consumer as c:
            assert c is default_consumer
            assert c._consumer is mock_consumer

        # should automatically stop after context manager
        mock_consumer.stop.assert_awaited_once()
        assert default_consumer._consumer is None

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# get_batch() method tests

"""
    PROCESSING TEST
    tests ApolloKafkaConsumer.get_batch() with valid batch sample sample_getmany_batch_dict
    verifies getmany() payload processing and list of ConsumerRecord are properly returned
"""
@pytest.mark.anyio
async def test_kafka_consumer_get_batch_valid(sample_getmany_batch_dict, sample_consumer_records) -> None: # sample_consumer_records is pretty much our expected result
    default_consumer = ApolloKafkaConsumer()
    mock_consumer = AsyncMock(spec=AIOKafkaConsumer)
    mock_consumer.getmany.return_value = sample_getmany_batch_dict # make getmany() method of AIOKafkaConsumer to return our valid sample batch fixture
    default_consumer._consumer = mock_consumer

    with patch("apollo.kafka.consumer.AIOKafkaConsumer", return_value=mock_consumer):
        # then we call get_batch() method, which in turn calls getmany() on the consumer instance
        batch = await default_consumer.get_batch()

        mock_consumer.getmany.assert_awaited_once() # confirm that getmany() has been called once
        assert isinstance(batch, list) and (len(batch) == len(sample_consumer_records)) # confirm that the result is a dictionary and has the same number of items as the sample consumer records
        
        # and then we confirm that our ConsumerRecord objects are indeed the ones we expect
        for record in batch:
            assert record in sample_consumer_records

"""
    TODO:
    - more get_batch() tests: empty getmany() return, and unexpected exception edge cases tests
    - commit() tests: uninitialized consumer unexpected commit and other exceptions edge cases tests
"""