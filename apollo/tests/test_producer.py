"""
    unit testing script for ApolloKafkaProducer in producer.py
    v1.0 - unit tests for initialization, connection lifecycle, async context manager, payload serialization with DLQ fallback, single event delivery, and concurrent multi-topic streaming pipeline
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
from unittest.mock import patch, AsyncMock # AsyncMock is to mock asynchronous (coroutines-based) objects here, will be used to mock httpx.AsyncClient later
from dotenv import load_dotenv
import os
from uuid import uuid4
from datetime import datetime, timezone
import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from aiokafka.structs import RecordMetadata, TopicPartition

from apollo.kafka.producer import ApolloKafkaProducer

load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend():
    return "asyncio" # in apollo we only use asyncio event loop for concurrency, not trio

@pytest.fixture
def sample_review_event():
    """synthetic single ReviewPayload model_dump dictionary"""
    return {
        "event_id": str(uuid4()),
        "app_id": "my.com.gxbank.app",
        "app_name": "GX Bank",
        "user_name": "Farhan Azmi",
        "rating": 5,
        "review_text": "Great UI and instant transfers with no hidden fees!",
        "app_version": "1.4.2",
        "submitted_at": "2026-08-10T14:30:00Z",
        "ingested_at": "2026-08-10T14:35:00Z"
    }

@pytest.fixture
def sample_news_event():
    """synthetic single FinancialNewsPayload model_dump dictionary"""
    return {
        "event_id": str(uuid4()),
        "article_uuid": "marketaux-uuid-12345",
        "title": "Bank Negara Malaysia Issues Updated Digital Banking Framework",
        "snippet": "BNM today announced comprehensive updated guidelines for digital banks operating in Malaysia.",
        "url": "https://www.thestar.com.my/business/2026/08/digital-banks",
        "source": "thestar.com.my",
        "sentiment_score": 0.456,
        "published_at": "2026-08-01T10:30:00Z",
        "ingested_at": "2026-08-01T10:35:00Z"
    }

@pytest.fixture
def sample_events_dict(sample_review_event, sample_news_event): # {topic1: [(pk1, pk1event1), (pk1, pk1event2), (pk2, pk2event1), (pk2, pk2event2)], topic2: [ ... ]}, refer to event appending part in main.py
    """synthetic valid multi-topic events dictionary with 4 events per topic (2 events per partition key)"""
    return {
        "app-reviews-events": [
            ("my.com.gxbank.app", sample_review_event),
            ("my.com.gxbank.app", {**sample_review_event, "event_id": str(uuid4()), "user_name": "Ahmad Dani", "rating": 4}),
            ("com.maybank2u.life", {**sample_review_event, "event_id": str(uuid4()), "app_id": "com.maybank2u.life", "app_name": "MAE"}),
            ("com.maybank2u.life", {**sample_review_event, "event_id": str(uuid4()), "app_id": "com.maybank2u.life", "app_name": "MAE", "rating": 3})
        ],
        "market-news-events": [
            ("thestar.com.my", sample_news_event),
            ("thestar.com.my", {**sample_news_event, "event_id": str(uuid4()), "article_uuid": "thestar-news-2", "title": "Second The Star Article"}),
            ("fintechnews.my", {**sample_news_event, "event_id": str(uuid4()), "source": "fintechnews.my", "title": "GXBank 500k Active Depositors"}),
            ("fintechnews.my", {**sample_news_event, "event_id": str(uuid4()), "source": "fintechnews.my", "title": "Digital Banking In Malaysia 2026"})
        ]
    }

@pytest.fixture
def sample_edge_case_events_dict(sample_review_event):
    """synthetic events dictionary testing partition key sanitation and DLQ fallback edge cases"""
    return {
        "app-reviews-events": [
            (None, sample_review_event), # None partition key -> fallback to b"dlq"
            ("", sample_review_event), # empty string -> fallback to b"dlq"
            ("   ", sample_review_event), # whitespace-only string -> fallback to b"dlq"
            (12345, sample_review_event), # non-string type -> fallback to b"dlq"
            ("  MY.COM.GXBANK.APP  ", sample_review_event) # uppercase with spaces -> b"my.com.gxbank.app"
        ]
    }

@pytest.fixture
def sample_malformed_events_dict(sample_review_event):
    """synthetic events dictionary containing a valid event and an event with a non-serializable object (to test serialization resilience)"""
    return {
        "app-reviews-events": [
            ("my.com.gxbank.app", sample_review_event), # valid serializable event
            ("my.com.gxbank.app", {"event_id": "invalid", "non_serializable": set([1, 2, 3])}) # sets cannot be serialized by orjson -> skips item
        ]
    }

@pytest.fixture
def sample_record_metadata(): # used in send_event tests
    """synthetic RecordMetadata object as returned by aiokafka send_and_wait() upon broker ACK"""
    return RecordMetadata(
        topic="app-reviews-events",
        partition=0,
        topic_partition=TopicPartition("app-reviews-events", 0),
        offset=42,
        timestamp=1786370000000,
        timestamp_type=0,
        log_start_offset=0
    )

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# some management tests (js 1 for now)

"""
    MANAGEMENT TEST
    tests ApolloKafkaProducer initialization with default bootstrap servers and custom one
"""
def test_kafka_producer_init_default_and_custom() -> None:
    # default initialization
    default_producer = ApolloKafkaProducer()
    assert default_producer.bootstrap_servers == f"{os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")}"
    assert default_producer._producer is None # check if producer is None initially

    # custom initialization
    custom_bootstrap_servers = "testhost:9092"
    custom_producer = ApolloKafkaProducer(bootstrap_servers=custom_bootstrap_servers)
    assert custom_producer.bootstrap_servers == custom_bootstrap_servers
    assert custom_producer._producer is None        

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# kafka producer and context manager lifecycle tests

"""
    LIFECYCLE TEST
    tests ApolloKafkaProducer start and stop methods
"""
@pytest.mark.anyio
async def test_kafka_producer_start_and_stop() -> None:
    default_producer = ApolloKafkaProducer()
    assert default_producer._producer is None # check if producer is None initially

    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    with patch("apollo.kafka.producer.AIOKafkaProducer", return_value=mock_producer):
        # attempt to start the kafka producer instance
        await default_producer.start()
        assert default_producer._producer is not None # check if producer is not None after start

        # attempt to stop the above
        await default_producer.stop()
        mock_producer.stop.assert_awaited_once() # check if stop() above actually awaited producer.stop() to close all kafka producer instances
        assert default_producer._producer is None # check if producer is None after stop

"""
    CONTEXT MANAGER TEST
    tests ApolloKafkaProducer async context manager
"""
@pytest.mark.anyio
async def test_kafka_producer_context_manager() -> None:
    default_producer = ApolloKafkaProducer()
    assert default_producer._producer is None # check if producer is None initially

    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    with patch("apollo.kafka.producer.AIOKafkaProducer", return_value=mock_producer):
        # attempt to start the kafka producer instance
        async with default_producer as p: # the context manager itself
            assert p is default_producer # testing the producer instance if it was set properly
            assert default_producer._producer is mock_producer # the mock producer now, should automatically be opened since context manager (__aenter__)

        mock_producer.stop.assert_awaited_once() # __aexit__ automatically calls the stop method and tests if it awaited producer.stop()
        assert default_producer._producer is None # check if producer is None after stop

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# _prepare_payload() method tests

"""
    PROCESSING TEST
    tests ApolloKafkaProducer._prepare_payload() with valid sample_events_dict
    verifies nested dictionary structure, partition key byte encoding, and orjson byte serialization
"""
def test_kafka_producer_prepare_payload_valid(sample_events_dict) -> None:
    producer = ApolloKafkaProducer()
    payload = producer._prepare_payload(sample_events_dict)
    
    assert isinstance(payload, dict) # verifies payload is a dictionary
    assert "app-reviews-events" in payload # verifies review topic in payload
    assert "market-news-events" in payload # verifies news topic in payload

    # inspect app-reviews-events topic
    review_topic_payload = payload["app-reviews-events"]
    assert b"my.com.gxbank.app" in review_topic_payload # partition key is encoded to bytes
    assert b"com.maybank2u.life" in review_topic_payload
    assert len(review_topic_payload[b"my.com.gxbank.app"]) == 2 # 2 events for gxbank
    assert len(review_topic_payload[b"com.maybank2u.life"]) == 2 # 2 events for maybank
    
    # verifies review event is serialized to bytes and can be deserialized back
    raw_event_bytes = review_topic_payload[b"my.com.gxbank.app"][0]
    assert isinstance(raw_event_bytes, bytes) # check if individual event is serialized to bytes
    deserialized = orjson.loads(raw_event_bytes) # deserialize them for test
    assert deserialized["app_id"] == "my.com.gxbank.app" # check if individual event is deserialized back to dict by checking its app_id
    assert deserialized["rating"] == 5 # then rating

    # inspect market-news-events topic
    news_topic_payload = payload["market-news-events"]
    assert b"thestar.com.my" in news_topic_payload # news source partition key is encoded to bytes
    assert b"fintechnews.my" in news_topic_payload
    assert len(news_topic_payload[b"thestar.com.my"]) == 2 # 2 events for thestar
    assert len(news_topic_payload[b"fintechnews.my"]) == 2 # 2 events for fintechnews

    # verifies news event is serialized to bytes and can be deserialized back
    raw_news_bytes = news_topic_payload[b"thestar.com.my"][0]
    assert isinstance(raw_news_bytes, bytes)
    deserialized_news = orjson.loads(raw_news_bytes)
    assert deserialized_news["source"] == "thestar.com.my"
    assert deserialized_news["article_uuid"] == "marketaux-uuid-12345"
    assert deserialized_news["sentiment_score"] == 0.456

"""
    PROCESSING TEST (PARTITION KEY & DLQ FALLBACK)
    tests ApolloKafkaProducer._prepare_payload() with edge cases in partition keys
    verifies that None, empty string, whitespace string, and non-string keys fallback to b"dlq", while valid keys are lowercased and stripped
"""
def test_kafka_producer_prepare_payload_dlq_fallback(sample_edge_case_events_dict) -> None:
    producer = ApolloKafkaProducer()
    payload = producer._prepare_payload(sample_edge_case_events_dict)

    assert isinstance(payload, dict) # verifies payload is a dictionary
    review_topic_payload = payload["app-reviews-events"] # subset to app reviews topic to check partition keys

    # DLQ partition should have collected 4 edge cases (None, "", "   ", 12345)
    assert b"dlq" in review_topic_payload # verifies dlq partition key
    assert len(review_topic_payload[b"dlq"]) == 4 # verifies 4 edge cases (None, "", "   ", 12345)

    # Valid key with leading/trailing spaces and uppercase should be normalized
    assert b"my.com.gxbank.app" in review_topic_payload # verifies valid key with leading/trailing spaces and uppercase is normalized
    assert len(review_topic_payload[b"my.com.gxbank.app"]) == 1 # verifies 1 valid event

"""
    PROCESSING TEST (MALFORMED EVENT RESILIENCE)
    tests ApolloKafkaProducer._prepare_payload() skipping un-serializable events
    verifies individual malformed events are skipped while valid events in the same topic are preserved
"""
def test_kafka_producer_prepare_payload_malformed_event_skipped(sample_malformed_events_dict) -> None:
    producer = ApolloKafkaProducer()
    payload = producer._prepare_payload(sample_malformed_events_dict)

    assert isinstance(payload, dict)
    review_topic_payload = payload["app-reviews-events"]
    # only the valid serializable event is kept, set object event skipped
    assert len(review_topic_payload[b"my.com.gxbank.app"]) == 1

"""
    PROCESSING TEST (INVALID INPUT TYPE)
    tests ApolloKafkaProducer._prepare_payload() handling invalid input types gracefully
    verifies error logging and returning empty dictionary {}
"""
def test_kafka_producer_prepare_payload_invalid_input() -> None:
    producer = ApolloKafkaProducer()
    # passing a non-dict type (e.g. list or string)
    payload = producer._prepare_payload(["not", "a", "dict"]) # type: ignore
    assert payload == {}

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# send_event() method tests

"""
    DELIVERY TEST
    tests ApolloKafkaProducer.send_event() delivering a single event and waiting for broker ACK
    verifies returned RecordMetadata and send_and_wait call arguments
"""
@pytest.mark.anyio
async def test_kafka_producer_send_event_success(sample_review_event, sample_record_metadata) -> None:
    producer = ApolloKafkaProducer()
    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    mock_producer.send_and_wait.return_value = sample_record_metadata # set send_and_wait to return sample_record_metadata instead
    producer._producer = mock_producer

    event_bytes = orjson.dumps(sample_review_event) # taking that one sample_review_event and serializing them
    metadata = await producer.send_event(topic="app-reviews-events", value=event_bytes, key=b"my.com.gxbank.app") # setting topic, value, and key, sends that singular event above

    assert metadata is sample_record_metadata # verifies if the returned metadata is the exact same as sample_record_metadata
    assert metadata.topic == "app-reviews-events" # verifies metadata topic
    assert metadata.partition == 0 # verifies metadata partition
    assert metadata.offset == 42 # verifies metadata offset
    mock_producer.send_and_wait.assert_awaited_once_with( # verifies that send_event() above actually ran send_and_wait() once with the given topic, value, and key arguments
        topic="app-reviews-events",
        value=event_bytes,
        key=b"my.com.gxbank.app"
    )

"""
    DELIVERY TEST (STANDALONE LIFECYCLE)
    tests ApolloKafkaProducer.send_event() standalone execution (opened_locally = True)
    verifies producer starts and stops cleanly via finally block
"""
@pytest.mark.anyio
async def test_kafka_producer_send_event_standalone_lifecycle(sample_review_event, sample_record_metadata) -> None:
    producer = ApolloKafkaProducer()
    assert producer._producer is None # should be initially None

    # same mock setup stuff
    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    mock_producer.send_and_wait.return_value = sample_record_metadata

    with patch("apollo.kafka.producer.AIOKafkaProducer", return_value=mock_producer): # patch AIOKafkaProducer
        # testing opened_locally so no context manager, start() and stop() calling test is ran below
        event_bytes = orjson.dumps(sample_review_event) # take that one sample_review_event and serialize them
        metadata = await producer.send_event(topic="app-reviews-events", value=event_bytes, key=b"my.com.gxbank.app") # setting topic, value, and key, sends that singular event above

        assert metadata is sample_record_metadata # verifies if the returned metadata is the exact same as sample_record_metadata
        mock_producer.start.assert_awaited_once() # verifies producer started locally
        mock_producer.stop.assert_awaited_once() # verifies producer stopped locally in finally
        assert producer._producer is None # verifies producer reset to None

"""
    DELIVERY TEST (BROKER KAFKA ERROR RESILIENCE)
    tests ApolloKafkaProducer.send_event() handling broker KafkaError delivery failures
    verifies KafkaError is caught and None is returned
"""
@pytest.mark.anyio
async def test_kafka_producer_send_event_kafka_error(sample_review_event) -> None:
    producer = ApolloKafkaProducer()
    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    mock_producer.send_and_wait.side_effect = KafkaError("Broker unavailable / delivery timeout") # instead of returning something, it wil raise KafkaError
    producer._producer = mock_producer

    event_bytes = orjson.dumps(sample_review_event)
    metadata = await producer.send_event(topic="app-reviews-events", value=event_bytes, key=b"my.com.gxbank.app")

    assert metadata is None # verifies None returned on KafkaError

"""
    DELIVERY TEST (UNEXPECTED ERROR HANDLING)
    tests ApolloKafkaProducer.send_event() handling unexpected fatal exceptions
    verifies exception is caught and None is returned
"""
@pytest.mark.anyio
async def test_kafka_producer_send_event_unexpected_error(sample_review_event) -> None:
    producer = ApolloKafkaProducer()
    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    mock_producer.send_and_wait.side_effect = Exception("Unexpected network socket error") # similar to above but this one tryna simulate an unexpected error on send
    producer._producer = mock_producer

    event_bytes = orjson.dumps(sample_review_event)
    metadata = await producer.send_event(topic="app-reviews-events", value=event_bytes, key=b"my.com.gxbank.app")

    assert metadata is None

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# run() pipeline tests

"""
    PIPELINE TEST
    tests ApolloKafkaProducer.run() concurrent streaming across multiple topics and partition keys with return_results=True
    verifies asyncio.gather execution, success count aggregation per topic, and standalone producer lifecycle
""" # if werent for this we would have never imported asyncio in producer.py and nothing will be streamed to kafka due to NameError 😭☠️🥀
@pytest.mark.anyio
async def test_kafka_producer_run_success_with_results(sample_events_dict, sample_record_metadata) -> None:
    producer = ApolloKafkaProducer()
    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    mock_producer.send_and_wait.return_value = sample_record_metadata

    with patch("apollo.kafka.producer.AIOKafkaProducer", return_value=mock_producer):
        results = await producer.run(sample_events_dict, return_results=True) # here, return_results is set to True

        assert results == { # verifies the return value is the exact same as sample_events_dict, having 4 events total from 2 partition keys in each topic
            "app-reviews-events": 4, # 4 review events streamed successfully
            "market-news-events": 4  # 4 news events streamed successfully
        }
        assert mock_producer.send_and_wait.call_count == 8 # 8 total events streamed, 4 per topic with 2 partition keys
        mock_producer.start.assert_awaited_once() # opened locally, producer.run() is the one calling start() without using context manager
        mock_producer.stop.assert_awaited_once() # closed locally, same as above as the finally block in producer.run()
        assert producer._producer is None # producer should be reset to None after producer.run() completes

"""
    PIPELINE TEST
    tests ApolloKafkaProducer.run() with return_results=False (no returning total events sent per topic)
    verifies return value is None
"""
@pytest.mark.anyio
async def test_kafka_producer_run_without_results(sample_events_dict, sample_record_metadata) -> None:
    producer = ApolloKafkaProducer()
    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    mock_producer.send_and_wait.return_value = sample_record_metadata

    with patch("apollo.kafka.producer.AIOKafkaProducer", return_value=mock_producer):
        results = await producer.run(sample_events_dict, return_results=False)

        assert results is None # in here, it verifies None returned when return_results is False
        assert mock_producer.send_and_wait.call_count == 8 # 8 total events streamed

"""
    PIPELINE TEST (EMPTY PAYLOAD)
    tests ApolloKafkaProducer.run() with empty events dictionary
    verifies early return None without attempting to stream
"""
@pytest.mark.anyio
async def test_kafka_producer_run_empty_payload() -> None:
    producer = ApolloKafkaProducer()
    mock_producer = AsyncMock(spec=AIOKafkaProducer)

    with patch("apollo.kafka.producer.AIOKafkaProducer", return_value=mock_producer):
        results = await producer.run({}, return_results=True)
        assert results is None # empty payload returns None
        mock_producer.send_and_wait.assert_not_called() # verifies that no network calls were made, meaning run() successfully stopped early

"""
    PIPELINE TEST (PARTIAL BROKER FAILURE)
    tests ApolloKafkaProducer.run() when some individual events fail due to KafkaError
    verifies that failed events do not terminate the pipeline and only successful events are counted
"""
@pytest.mark.anyio
async def test_kafka_producer_run_partial_broker_failure(sample_events_dict, sample_record_metadata) -> None:
    producer = ApolloKafkaProducer()
    mock_producer = AsyncMock(spec=AIOKafkaProducer)
    
    # side_effect can also be useful to return different values for sequential calls to make them more realistic (also allows that KafkaError to be raised), just like below. return_values will just return the entire list
    mock_producer.send_and_wait.side_effect = [
        KafkaError("Partition leader unavailable"), # for the first call, this one is the mock failed call
        sample_record_metadata, # and the rest succeeds
        sample_record_metadata,
        sample_record_metadata,
        sample_record_metadata, # market-news-events starts from here
        sample_record_metadata,
        sample_record_metadata,
        sample_record_metadata
    ]

    with patch("apollo.kafka.producer.AIOKafkaProducer", return_value=mock_producer):
        results = await producer.run(sample_events_dict, return_results=True)

        assert results is not None
        # 3 successes in app-reviews-events (1 failed out of 4), 4 successes in market-news-events
        assert results["app-reviews-events"] == 3
        assert results["market-news-events"] == 4

"""
    PIPELINE TEST (ERROR HANDLING)
    tests ApolloKafkaProducer.run() handling unexpected fatal pipeline errors
"""
@pytest.mark.anyio
async def test_kafka_producer_run_unexpected_error(sample_events_dict) -> None:
    producer = ApolloKafkaProducer()
    mock_producer = AsyncMock(spec=AIOKafkaProducer)

    with patch("apollo.kafka.producer.AIOKafkaProducer", return_value=mock_producer):
        with patch.object(producer, "_prepare_payload", side_effect=Exception("Fatal serialization crash")): # made it so that producer calling _prepare_payload() will instead raise an unexpected Exception
            results = await producer.run(sample_events_dict, return_results=True)
            assert results == {} # verifies that it return an empty dict on fatal error

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass
