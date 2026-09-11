"""
    unit testing script for PostgresPersister in service.py
    v0.1
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
import os
import logging
import orjson
from uuid import uuid4
from asyncio import CancelledError
from unittest.mock import patch, AsyncMock, MagicMock
from dotenv import load_dotenv
from psycopg_pool import AsyncConnectionPool
from aiokafka.structs import ConsumerRecord

from apollo.database.service import PostgresPersister

load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture
def sample_review_event():
    """synthetic single ReviewPayload event dictionary"""
    return {
        "event_id": "str(uuid4())-but-its-a-sample-for-review-events",
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
    """synthetic single FinancialNewsPayload event dictionary"""
    return {
        "event_id": "str(uuid4())-but-its-a-sample-for-news-events",
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
def sample_consumer_records(sample_review_event, sample_news_event):
    """synthetic list of 4 ConsumerRecord instances across both topics and multiple partition keys"""
    review_bytes_1 = orjson.dumps(sample_review_event)
    review_bytes_2 = orjson.dumps({**sample_review_event, "user_name": "Ahmad Dani", "rating": 4}) # event_id, user_name, and rating will replace the original values from unpacking sample_review_event
    news_bytes_1 = orjson.dumps(sample_news_event)
    news_bytes_2 = orjson.dumps({**sample_news_event, "article_uuid": "marketaux-uuid-67890", "title": "GXBank Expands Features"}) # similar thing as above

    return [
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=101,
            timestamp=1787119344000,
            timestamp_type=0,
            key=b"my.com.gxbank.app",
            value=review_bytes_1,
            checksum=None,
            serialized_key_size=17,
            serialized_value_size=len(review_bytes_1),
            headers=()
        ),
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=102,
            timestamp=1787119345000,
            timestamp_type=0,
            key=b"my.com.gxbank.app",
            value=review_bytes_2,
            checksum=None,
            serialized_key_size=17,
            serialized_value_size=len(review_bytes_2),
            headers=()
        ),
        ConsumerRecord(
            topic="market-news-events",
            partition=0,
            offset=201,
            timestamp=1787119346000,
            timestamp_type=0,
            key=b"thestar.com.my",
            value=news_bytes_1,
            checksum=None,
            serialized_key_size=14,
            serialized_value_size=len(news_bytes_1),
            headers=()
        ),
        ConsumerRecord(
            topic="market-news-events",
            partition=0,
            offset=202,
            timestamp=1787119347000,
            timestamp_type=0,
            key=b"thestar.com.my",
            value=news_bytes_2,
            checksum=None,
            serialized_key_size=14,
            serialized_value_size=len(news_bytes_2),
            headers=()
        )
    ]

@pytest.fixture
def sample_malformed_consumer_records(sample_review_event):
    """synthetic list of ConsumerRecords with empty values and malformed JSON bytes for edge case testing"""
    valid_bytes = orjson.dumps(sample_review_event)
    return [
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=301,
            timestamp=1787119348000,
            timestamp_type=0,
            key=b"empty-val",
            value=b"", # empty bytes
            checksum=None,
            serialized_key_size=9,
            serialized_value_size=0,
            headers=()
        ),
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=302,
            timestamp=1787119349000,
            timestamp_type=0,
            key=b"bad-json",
            value=b"invalid json {missing_brackets", # malformed JSON
            checksum=None,
            serialized_key_size=8,
            serialized_value_size=30,
            headers=()
        ),
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=303,
            timestamp=1787119350000,
            timestamp_type=0,
            key=b"valid-record",
            value=valid_bytes, # valid record
            checksum=None,
            serialized_key_size=12,
            serialized_value_size=len(valid_bytes),
            headers=()
        )
    ]

@pytest.fixture
def sample_parsed_events(sample_review_event, sample_news_event):
    """synthetic dictionary of parsed event batches grouped by topic"""
    return {
        "app-reviews-events": [
            sample_review_event,
            {**sample_review_event, "user_name": "Ahmad Dani", "rating": 4}
        ],
        "market-news-events": [
            sample_news_event,
            {**sample_news_event, "article_uuid": "marketaux-uuid-67890", "title": "GXBank Expands Features"}
        ]
    }

@pytest.fixture
def sample_unknown_topic_parsed_events(sample_review_event):
    """synthetic dictionary containing an unrecognized topic name"""
    return {
        "unknown-unsupported-topic": [sample_review_event]
    }

@pytest.fixture
def sample_custom_persister_config():
    """synthetic custom connection pool size configuration"""
    return {
        "min_size": 3,
        "max_size": 15
    }

""" testing assserting core clauses of the queries only will remove later prob
@pytest.fixture
def reviews_dml_query():
    return '''
        INSERT INTO staging_reviews(
            event_id, 
            app_id, 
            app_name, 
            user_name, 
            review_text, 
            rating, 
            app_version, 
            submitted_at, 
            ingested_at
        )
        VALUES
            (%(event_id)s, %(app_id)s, %(app_name)s, %(user_name)s, %(review_text)s, %(rating)s, %(app_version)s, %(submitted_at)s, %(ingested_at)s)
        ON CONFLICT (event_id)
        DO NOTHING;
    '''

@pytest.fixture
def news_dml_query():
    return '''
        INSERT INTO staging_marketaux(
            event_id, 
            article_uuid, 
            title, 
            snippet, 
            url, 
            source, 
            sentiment_score, 
            published_at, 
            ingested_at
        )
        VALUES
            (%(event_id)s, %(article_uuid)s, %(title)s, %(snippet)s, %(url)s, %(source)s, %(sentiment_score)s, %(published_at)s, %(ingested_at)s)
        ON CONFLICT (event_id)
        DO NOTHING;
    '''
"""

@pytest.fixture
def mock_async_pool():
    '''synthetic mock AsyncConnectionPool with nested connection and cursor async context managers, as well as monkeypatch.setenv() to mock os.getenv()'''
    mock_pool = MagicMock(spec=AsyncConnectionPool) # yea so AsyncConnectionPool itself is not asynchronous and so does its connection() method (we don't await when using them) so we use MagicMock, but that connection() method does return an object that uses async context manager protocol (aenter and aexit)
    mock_pool.closed = False
    mock_pool.open = AsyncMock() # unlike above, all below (except connection) must be awaited so use AsyncMock
    mock_pool.close = AsyncMock()

    mock_conn = MagicMock() # switched to MagicMock to stop "RuntimeWarning: coroutine 'AsyncMockMixin._execute_mock_call' was never awaited" because the connection object itself is synchronous like above
    mock_cur = AsyncMock()
    mock_cur.executemany = AsyncMock()

    # setup cursor async context manager
    mock_conn.cursor.return_value.__aenter__ = AsyncMock(return_value=mock_cur)
    mock_conn.cursor.return_value.__aexit__ = AsyncMock(return_value=None)

    # setup connection async context manager (continuation from above, here is what i meant by it returning an object that uses async context manager protocol)
    mock_pool.connection.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)

    return mock_pool, mock_conn, mock_cur

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# management tests
"""
    MANAGEMENT TEST
    tests PostgresPersister initialization with default and custom connection pool sizes (min_size and max_size)
"""
def test_postgres_persister_init_default_and_custom(sample_custom_persister_config):
    # default init
    default_persister = PostgresPersister()
    assert default_persister.min_size == 1 # test default value being 1
    assert default_persister.max_size == 10 # ts 10
    assert default_persister._pool is None # and pool should have lazy initialization

    # custom init
    custom_persister = PostgresPersister(**sample_custom_persister_config)
    assert custom_persister.min_size == sample_custom_persister_config["min_size"]
    assert custom_persister.max_size == sample_custom_persister_config["max_size"]
    assert custom_persister._pool is None

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# lifecycle and context manager tests

"""
    LIFECYCLE TEST
    tests PostgresPersister start and stop methods
"""
@pytest.mark.anyio
async def test_postgres_persister_start_and_stop(mock_async_pool) -> None:
    default_persister = PostgresPersister()
    assert default_persister._pool is None

    mock_pool, mock_conn, mock_cur = mock_async_pool # tuple unpacking baby
    with patch("apollo.database.service.AsyncConnectionPool", return_value=mock_pool):
        # attempt to start postgres persister instance
        await default_persister.start()
        mock_pool.open.assert_awaited_once() # confirm that mock_pool.open() was called once at the start
        assert default_persister._pool is not None

        # attempt to stop above
        await default_persister.stop()
        mock_pool.close.assert_awaited_once() # and confirm that mock_pool.close() was called once in the end
        assert default_persister._pool is None

"""
    CONTEXT MANAGER TEST
    test PostgresPersister async context manager
"""
@pytest.mark.anyio
async def test_postgres_persister_context_manager(mock_async_pool) -> None:
    default_persister = PostgresPersister()
    assert default_persister._pool is None

    mock_pool, mock_conn, mock_cur = mock_async_pool
    with patch("apollo.database.service.AsyncConnectionPool", return_value=mock_pool):
        async with default_persister as p:
            assert p is default_persister # test that the same object is returned
            mock_pool.open.assert_awaited_once() # when opening context manager will run open
            assert p._pool is mock_pool # check if the pool did get initialized by context manager and see if patch above works
        
        mock_pool.close.assert_awaited_once() # and context manager closing shld run close automatically
        assert default_persister._pool is None # and the pool should be cleaned up

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# core func tests (connection string, parsing, and persisting. and some error handlings too)

"""
    CONNECTION STRING TEST
    tests PostgresPersister._create_conninfo() method for correctness in conn string formation
    string: postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}
"""
def test_postgres_persister_create_conninfo(monkeypatch) -> None:
    # with monkeypatch we can essentially "mock" the values of the environment variables so that we can test the _create_conninfo() method without having to set up the actual environment variables
    monkeypatch.setattr("apollo.database.service.POSTGRES_USER", "mockuser")
    monkeypatch.setattr("apollo.database.service.POSTGRES_PASSWORD", "mockpassword")
    monkeypatch.setattr("apollo.database.service.POSTGRES_HOST", "0.0.0.0")
    monkeypatch.setattr("apollo.database.service.POSTGRES_PORT", "0000")
    monkeypatch.setattr("apollo.database.service.POSTGRES_DB", "mockdb")

    default_persister = PostgresPersister()
    assert default_persister._pool is None

    # test _create_conninfo()
    assert default_persister._create_conninfo() == "postgresql://mockuser:mockpassword@0.0.0.0:0000/mockdb"

"""
    PARSING TEST
    tests PostgresPersister.parse_events() on sample_consumer_records
"""
def test_postgres_persister_parse_events(sample_consumer_records, sample_parsed_events) -> None:
    default_persister = PostgresPersister()
    assert default_persister._pool is None

    result = default_persister.parse_events(sample_consumer_records)
    assert result == sample_parsed_events # they should be the same

"""
    PARSING TEST (ERROR HANDLING: MALFORMED DATA)
    tests PostgresPersister.parse_events() error handling on malformed ConsumerRecord
"""
def test_postgres_persister_parse_events_malformed(sample_malformed_consumer_records):
    default_persister = PostgresPersister()
    assert default_persister._pool is None

    result = default_persister.parse_events(sample_malformed_consumer_records)
    assert len(result["app-reviews-events"]) == 1 # should only have 1 valid event, the other 2 malformed ones should be skipped

"""
    PARSING TEST (ERROR HANDLING: UNEXPECTED EXCEPTION)
    tests PostgresPersister.parse_events() error handling on unexpected exception, should just print the error and return empty dict
"""
def test_postgres_persister_parse_events_exception(caplog) -> None:
    default_persister = PostgresPersister()
    assert default_persister._pool is None

    result = default_persister.parse_events(None) # 
    assert result == {} # from parse_events(), unexpected errors should return empty dict
    assert "(Apollo) Error while Postgres persister was parsing consumer records" in caplog.text

"""
    PERSISTING TEST
    tests PostgresPersister.persist_events() on sample_parsed_events (ofc on a mock connection pool)
"""
@pytest.mark.anyio
async def test_postgres_persister_persist_events(mock_async_pool, sample_parsed_events) -> None:
    default_persister = PostgresPersister()
    assert default_persister._pool is None

    mock_pool, mock_conn, mock_cur = mock_async_pool

    with patch("apollo.database.service.AsyncConnectionPool", return_value=mock_pool):
        await default_persister.start()
        mock_pool.open.assert_awaited_once()
        assert default_persister._pool is mock_pool # confirms patch did work

        success = await default_persister.persist_events(sample_parsed_events) # run it
        assert success is True # on successful insert should return True

        calls = mock_cur.executemany.await_args_list # from executemany we will get the list of all calls (awaits in this async case) and their specific arguments
        assert len(calls) == 2 # there should be 2 calls, one for reviews and one for news

        # checking the first call (which is reviews)
        review_sql, review_records = calls[0].args
        assert "INSERT INTO staging_reviews" in review_sql # check if the sql insertion query is proper
        assert "ON CONFLICT (event_id)" in review_sql
        assert "DO NOTHING" in review_sql
        assert review_records == sample_parsed_events["app-reviews-events"] # check if the records are correct

        # checking the second call (which is news)
        news_sql, news_records = calls[1].args
        assert "INSERT INTO staging_marketaux" in news_sql # check if the sql insertion query is proper
        assert "ON CONFLICT (event_id)" in news_sql
        assert "DO NOTHING" in news_sql
        assert news_records == sample_parsed_events["market-news-events"] # check if the records are correct

        # finally stop the postgres persister instance
        await default_persister.stop()
        assert default_persister._pool is None # and pool should be None after stopping

"""
    PERSISTING TEST (ERROR HANDLING: UNKNOWN TOPIC)
    tests PostgresPersister.persist_events() error handling on malformed parsing that has unknown topic (sample_unknown_topic_parsed_events)
"""
@pytest.mark.anyio
async def test_postgres_persister_persist_events_unknown_topic(mock_async_pool, sample_unknown_topic_parsed_events, caplog) -> None:
    default_persister = PostgresPersister()
    assert default_persister._pool is None

    mock_pool, mock_conn, mock_cur = mock_async_pool

    with patch("apollo.database.service.AsyncConnectionPool", return_value=mock_pool):
        await default_persister.start()
        mock_pool.open.assert_awaited_once()
        assert default_persister._pool is mock_pool # confirms patch did work

        success = await default_persister.persist_events(sample_unknown_topic_parsed_events) # run it
        assert success is True # in the actual method, unknown topics are skipped and only logs a warning, still counts as successful

        mock_cur.executemany.assert_not_called() # executemany() shouldnt be called at all here
        assert "(Apollo) Unrecognized topic 'unknown-unsupported-topic', skipping its database insertion" in caplog.text

        await default_persister.stop()
        assert default_persister._pool is None

"""
    PERSISTING TEST (ERROR HANDLING: UNEXPECTED EXCEPTION)
    tests PostgresPersister.persist_events() error handling on unexpected exceptions
    NOTE: this does not test for transactional failure itself, just unexpected exception arising during the persisting process
"""
@pytest.mark.anyio
async def test_postgres_persister_persist_events_unexpected_exception(mock_async_pool, sample_parsed_events, caplog) -> None:
    default_persister = PostgresPersister()
    assert default_persister._pool is None

    mock_pool, mock_conn, mock_cur = mock_async_pool

    with patch("apollo.database.service.AsyncConnectionPool", return_value=mock_pool):
        await default_persister.start()
        mock_pool.open.assert_awaited_once()
        assert default_persister._pool is mock_pool

        # raise exception during executemany
        mock_cur.executemany.side_effect = Exception("executemany error stuff")

        # run it
        success = await default_persister.persist_events(sample_parsed_events)
        assert success is False # on failed insert should return False

        assert "(Apollo) Error while Postgres persister was pushing to database: executemany error stuff" in caplog.text

        await default_persister.stop()
        assert default_persister._pool is None

# btw transactional failure handling and testing will be done on later integration tests so for now these are enough

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass