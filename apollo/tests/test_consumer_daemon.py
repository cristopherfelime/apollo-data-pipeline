"""
    unit testing script for background consumer daemon service in consumer_daemon.py
    v0.1 - test fixtures and synthetic data setup for consumer daemon lifecycle, batch processing, and error handling
    v1.0 - completed unit test coverage for signal handling, cooperative shutdown, batch ingestion resilience, and lifecycle management
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
import logging
import asyncio
import signal
import sys # for mocking os platform
import orjson
from uuid import uuid4
from typing import Any
from unittest.mock import patch, AsyncMock, MagicMock
from aiokafka.structs import ConsumerRecord

from apollo.consumer_daemon import main
from apollo.kafka.consumer import ApolloKafkaConsumer
from apollo.database.service import PostgresPersister

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

@pytest.fixture
def sample_raw_records() -> list[ConsumerRecord]:
    """synthetic list of 3 ConsumerRecords spanning reviews, news, and transaction topics"""
    review_bytes: bytes = orjson.dumps({
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
    news_bytes: bytes = orjson.dumps({
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
    tx_bytes: bytes = orjson.dumps({
        "transaction_id": str(uuid4()),
        "timestamp": "2026-09-24T18:30:00Z",
        "transaction_method": "DUITNOW_QR",
        "amount_myr": 28.50,
        "user_id": str(uuid4()),
        "merchant_name": "MR. D.I.Y.",
        "merchant_mcc": "5331",
        "payment_status": "SUCCESS",
        "ingested_at": "2026-09-24T18:30:05Z",
        "is_flagged_fraud": False
    })

    return [
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=101,
            timestamp=1787119344000,
            timestamp_type=0,
            key=b"my.com.gxbank.app",
            value=review_bytes,
            checksum=None,
            serialized_key_size=17,
            serialized_value_size=len(review_bytes),
            headers=()
        ),
        ConsumerRecord(
            topic="market-news-events",
            partition=0,
            offset=201,
            timestamp=1787119346000,
            timestamp_type=0,
            key=b"thestar.com.my",
            value=news_bytes,
            checksum=None,
            serialized_key_size=14,
            serialized_value_size=len(news_bytes),
            headers=()
        ),
        ConsumerRecord(
            topic="myr-transactions",
            partition=0,
            offset=301,
            timestamp=1787119348000,
            timestamp_type=0,
            key=b"sample-user-id",
            value=tx_bytes,
            checksum=None,
            serialized_key_size=14,
            serialized_value_size=len(tx_bytes),
            headers=()
        )
    ]

@pytest.fixture
def sample_parsed_records(sample_raw_records: list[ConsumerRecord]) -> dict[str, list[dict[str, Any]]]:
    """synthetic dictionary of deserialized event batches matching sample_raw_records"""
    return {
        record.topic: [orjson.loads(record.value or b"")] for record in sample_raw_records
    }

@pytest.fixture
def sample_malformed_raw_records() -> list[ConsumerRecord]:
    """synthetic list of ConsumerRecords containing empty bytes and malformed JSON payloads"""
    return [
        ConsumerRecord(
            topic="app-reviews-events",
            partition=0,
            offset=901,
            timestamp=1787119344000,
            timestamp_type=0,
            key=b"empty-key",
            value=b"",
            checksum=None,
            serialized_key_size=9,
            serialized_value_size=0,
            headers=()
        ),
        ConsumerRecord(
            topic="market-news-events",
            partition=0,
            offset=902,
            timestamp=1787119345000,
            timestamp_type=0,
            key=b"bad-json-key",
            value=b"invalid-json{not_closed",
            checksum=None,
            serialized_key_size=12,
            serialized_value_size=23,
            headers=()
        )
    ]

@pytest.fixture
def mock_kafka_consumer() -> MagicMock:
    """synthetic mock ApolloKafkaConsumer handler with async context manager and batch methods"""
    mock_consumer: MagicMock = MagicMock(spec=ApolloKafkaConsumer)
    mock_consumer._consumer = MagicMock() # alive by default
    mock_consumer.get_batch = AsyncMock(return_value=[])
    mock_consumer.commit = AsyncMock()
    mock_consumer.__aenter__ = AsyncMock(return_value=mock_consumer)
    mock_consumer.__aexit__ = AsyncMock(return_value=None)
    return mock_consumer

@pytest.fixture
def mock_postgres_persister() -> MagicMock:
    """synthetic mock PostgresPersister handler with async context manager and persistence methods"""
    mock_persister: MagicMock = MagicMock(spec=PostgresPersister)
    mock_persister.parse_events = MagicMock(return_value={})
    mock_persister.persist_events = AsyncMock(return_value=True)
    mock_persister.__aenter__ = AsyncMock(return_value=mock_persister)
    mock_persister.__aexit__ = AsyncMock(return_value=None)
    return mock_persister

@pytest.fixture
def mock_signal_loop() -> tuple[dict[signal.Signals, Any], MagicMock, MagicMock]:
    """synthetic capture hook for loop.add_signal_handler and loop.remove_signal_handler"""
    handlers: dict[signal.Signals, Any] = {} # basically signal table, mapping signal.SIGTERM or SIGINT to no argument function (callback function)
    mock_add: MagicMock = MagicMock(side_effect=lambda sig, cb, *args: handlers.update({sig: lambda: cb(*args)})) # supposed to mock loop.add_signal_handler, the side effect updates the handlers dict, mapping signal to no argument callback (args are discarded)
    mock_remove: MagicMock = MagicMock() # this one for loop.remove_signal_handler
    return handlers, mock_add, mock_remove

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# signal handling & os compability tests

"""
    SIGNAL REGISTRATION TEST (UNIX PLATFORM)
    tests that consumer daemon registers shutdown callbacks for SIGTERM and SIGINT on Unix
"""
@pytest.mark.anyio
async def test_consumer_daemon_unix_signal_registration(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux") # this is to mock that we are on linux

    # cancel on first get_batch to exit loop after signal setup
    mock_kafka_consumer.get_batch = AsyncMock(side_effect=asyncio.CancelledError)

    # from the current running loop it will patch loop.add_signal_handler with mock_add, it's side effect will update handlers dict, mapping signal to no argument callback (args are discarded like said earlier), similar thing to loop.remove_signal_handler with mock_remove
    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister):

        await main()

        assert mock_add.call_count == 2 # this asserts that the add_signal_handler was called twice, once for SIGTERM and once for SIGINT
        registered_signals = [call.args[0] for call in mock_add.call_args_list] # this looks inside mock_add's call history and extracts the first argument of each call to it, which is the signal
        assert signal.SIGTERM in registered_signals # and checks if SIGTERM is in the extracted signals
        assert signal.SIGINT in registered_signals # same for SIGINT

"""
    SIGNAL REGISTRATION TEST (WINDOWS PLATFORM)
    tests that consumer daemon skips signal handler registration on Windows and logs appropriate warnings
"""
@pytest.mark.anyio
async def test_consumer_daemon_windows_platform_guard(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "win32") # windows now

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=asyncio.CancelledError)

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         caplog.at_level(logging.WARNING):

        await main()

        # none of these should be called, 
        mock_add.assert_not_called()
        mock_remove.assert_not_called()
        assert "(Apollo) add_signal_handler() not supported in Windows" in caplog.text
        assert "(Apollo) remove_signal_handler not supported in Windows" in caplog.text

"""
    SIGNAL REGISTRATION TEST (REGISTRATION FAILURE EXCEPTION)
    tests that RuntimeError or OS error during add_signal_handler is caught and logged without crashing initialization
"""
@pytest.mark.anyio
async def test_consumer_daemon_signal_handler_registration_failure(
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")
    mock_fail_add: MagicMock = MagicMock(side_effect=RuntimeError("signal handler cannot be added outside main thread")) # this will make the add_signal_handler raise RuntimeError

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=asyncio.CancelledError)

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_fail_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", MagicMock()), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         caplog.at_level(logging.ERROR):

        await main()

        assert "(Apollo) Error while setting signal handler for signal" in caplog.text

"""
    SIGNAL CLEANUP TEST (FINALLY BLOCK REMOVAL)
    tests that remove_signal_handler is executed for all registered signals upon daemon shutdown
"""
@pytest.mark.anyio
async def test_consumer_daemon_signal_handler_removal_finally(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    # simulate removal error on one of the signals to test exception handling in finally block
    mock_remove.side_effect = [None, RuntimeError("Failed to remove signal")] # so this makes it remove once successfully, then raise RuntimeError the second time
    mock_kafka_consumer.get_batch = AsyncMock(side_effect=asyncio.CancelledError) # canceling consumer to exit loop after setup

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         caplog.at_level(logging.ERROR):

        await main()

        assert mock_remove.call_count == 2 # this asserts that the remove_signal_handler was called twice, once for SIGTERM and once for SIGINT
        removed_signals = [call.args[0] for call in mock_remove.call_args_list] # this looks inside mock_remove's call history and extracts the first argument of each call to it, which is the signal
        assert signal.SIGTERM in removed_signals # and checks if SIGTERM is in the extracted signals
        assert signal.SIGINT in removed_signals # same for SIGINT
        assert "(Apollo) Error while removing signal handler for signal" in caplog.text # and checks if the error message is in the logs

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# cooperative shutdown tests

"""
    SHUTDOWN SIGNAL TEST (CALLBACK EXECUTION)
    tests that invoking shutdown_callback logs the shutdown intention and triggers shutdown_event
"""
@pytest.mark.anyio
async def test_consumer_daemon_shutdown_callback_trigger(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    async def get_batch_and_trigger_signal() -> list[ConsumerRecord]: # we cant trigger a real SIGTERM or else pytest dies, and shutdown_callback is a nested func that can't be called outside main(), so we do this instead
        if signal.SIGTERM in handlers: # checks if the SIGTERM signal was captured
            handlers[signal.SIGTERM]() # execute captured shutdown_callback(signal.SIGTERM)
        return []

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=get_batch_and_trigger_signal) # here, when get_batch() is called by main(), it will trigger the shutdown_callback, mimicking SIGTERM being sent to the process

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         caplog.at_level(logging.INFO):

        await main()

        # yea see if shutdown callback was executed
        assert "(Apollo) received signal" in caplog.text
        assert "gracefully stopping the consumer daemon" in caplog.text
        assert "(Apollo) consumer daemon was shutdown gracefully" in caplog.text

"""
    COOPERATIVE SHUTDOWN TEST (COMPLETION OF IN-FLIGHT BATCH)
    tests that when a shutdown signal arrives during persistence, the current batch finishes
    persisting and committing before the daemon loop terminates without data loss
"""
@pytest.mark.anyio
async def test_consumer_daemon_cooperative_completion_current_batch(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    sample_raw_records: list[ConsumerRecord],
    sample_parsed_records: dict[str, list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    mock_kafka_consumer.get_batch = AsyncMock(return_value=sample_raw_records)
    mock_postgres_persister.parse_events = MagicMock(return_value=sample_parsed_records)

    async def persist_and_signal(parsed_events: dict[str, list[dict[str, Any]]]) -> bool:
        # simulate arrival of SIGINT in the middle of database persistence
        if signal.SIGINT in handlers:
            handlers[signal.SIGINT]()
        return True # returns True, simulating the success of the previous persistence

    mock_postgres_persister.persist_events = AsyncMock(side_effect=persist_and_signal)

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister):

        await main()

        # assert the in-flight batch successfully persisted and committed offsets
        mock_postgres_persister.persist_events.assert_awaited_once_with(sample_parsed_records)
        mock_kafka_consumer.commit.assert_awaited_once()

        # assert the loop terminated without initiating a second get_batch poll
        assert mock_kafka_consumer.get_batch.await_count == 1

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# batch processing loop & resilience tests

"""
    BATCH PROCESSING TEST (HAPPY PATH)
    tests complete end-to-end batch ingestion: get_batch -> parse_events -> persist_events -> commit
"""
@pytest.mark.anyio
async def test_consumer_daemon_happy_path_batch_ingestion(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    sample_raw_records: list[ConsumerRecord],
    sample_parsed_records: dict[str, list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    async def get_batch_and_stop() -> list[ConsumerRecord]: # another helper function, this time returning the sample records, but also triggering the shutdown callback
        if signal.SIGTERM in handlers:
            handlers[signal.SIGTERM]() # same as before, triggers shutdown_callback(signal.SIGTERM)
        return sample_raw_records

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=get_batch_and_stop)
    mock_postgres_persister.parse_events = MagicMock(return_value=sample_parsed_records)
    mock_postgres_persister.persist_events = AsyncMock(return_value=True)

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister):

        await main()

        mock_kafka_consumer.get_batch.assert_awaited_once() # asserts that get_batch() was called exactly once
        mock_postgres_persister.parse_events.assert_called_once_with(sample_raw_records) # asserts that parse_events() was called exactly once with the sample records
        mock_postgres_persister.persist_events.assert_awaited_once_with(sample_parsed_records) # asserts that persist_events() was called exactly once with the parsed records
        mock_kafka_consumer.commit.assert_awaited_once() # asserts that commit() was called exactly once

"""
    BATCH PROCESSING TEST (EMPTY BATCH WITH HEALTHY BROKER)
    tests that an empty poll result with an active broker skips parsing, persisting, and committing without sleeping
"""
@pytest.mark.anyio
async def test_consumer_daemon_empty_batch_healthy_broker(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    async def get_empty_batch_and_stop() -> list[ConsumerRecord]:
        if signal.SIGTERM in handlers:
            handlers[signal.SIGTERM]()
        return [] # similar to above but ye the sample raw record batch is empty

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=get_empty_batch_and_stop)
    mock_kafka_consumer._consumer = MagicMock() # consumer is alive

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         patch("apollo.consumer_daemon.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:

        await main()

        # however, these shouldnt be called, as the batch was empty, no parsing or persisting or committing should occur, and the consumer is healthy, so no sleep should occur
        mock_postgres_persister.parse_events.assert_not_called()
        mock_postgres_persister.persist_events.assert_not_called()
        mock_kafka_consumer.commit.assert_not_called()
        mock_sleep.assert_not_called()

"""
    RESILIENCE TEST (BROKER DOWNTIME BACKOFF)
    tests that an empty poll result when consumer instance is dead (_consumer is None)
    logs broker downtime warning and awaits 3s sleep backoff
"""
@pytest.mark.anyio
async def test_consumer_daemon_broker_downtime_backoff(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    async def get_empty_batch_and_stop() -> list[ConsumerRecord]:
        if signal.SIGTERM in handlers:
            handlers[signal.SIGTERM]()
        return []

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=get_empty_batch_and_stop)
    mock_kafka_consumer._consumer = None # broker is unavailable now

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         patch("apollo.consumer_daemon.asyncio.sleep", new_callable=AsyncMock) as mock_sleep, \
         caplog.at_level(logging.WARNING):

        await main()

        # hence, the warning should be logged, and sleep should be awaited once, other methods shouldnt be called
        assert "(Apollo) Kafka consumer instance is not alive, the broker is most likely unavailable" in caplog.text
        mock_sleep.assert_awaited_once_with(3)
        mock_postgres_persister.parse_events.assert_not_called()
        mock_kafka_consumer.commit.assert_not_called()

"""
    RESILIENCE TEST (POISON PILL / UNPARSEABLE BATCH GUARD)
    tests that when all records in a batch are malformed and parse_events returns {},
    the daemon logs a warning, skips database persistence, and commits offsets to avoid infinite looping
"""
@pytest.mark.anyio
async def test_consumer_daemon_poison_pill_unparseable_batch(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    sample_malformed_raw_records: list[ConsumerRecord],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    async def get_malformed_batch_and_stop() -> list[ConsumerRecord]:
        if signal.SIGTERM in handlers:
            handlers[signal.SIGTERM]()
        return sample_malformed_raw_records # UNHEALTHY batch

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=get_malformed_batch_and_stop)
    mock_postgres_persister.parse_events = MagicMock(return_value={}) # suppose all records failed parsing

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         caplog.at_level(logging.WARNING):

        await main()

        # so yea the log should be logged, persist_events shouldnt be called, but kafka consumer should still be committed in order to not repeat attempting to persist the malformed batch
        assert "(Apollo) All records in batch were malformed or unparseable, committing offsets to avoid infinite looping.." in caplog.text
        mock_postgres_persister.persist_events.assert_not_called()
        mock_kafka_consumer.commit.assert_awaited_once()

"""
    RESILIENCE TEST (PERSISTENCE FAILURE & RETRY BACKOFF)
    tests that database persistence failure skips offset commitment and triggers 2s rate-limiting backoff
"""
@pytest.mark.anyio
async def test_consumer_daemon_persistence_failure_retry_backoff(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    sample_raw_records: list[ConsumerRecord],
    sample_parsed_records: dict[str, list[dict[str, Any]]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    async def get_batch_and_stop() -> list[ConsumerRecord]:
        if signal.SIGTERM in handlers:
            handlers[signal.SIGTERM]()
        return sample_raw_records

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=get_batch_and_stop)
    mock_postgres_persister.parse_events = MagicMock(return_value=sample_parsed_records)
    mock_postgres_persister.persist_events = AsyncMock(return_value=False) # database write failed

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         patch("apollo.consumer_daemon.asyncio.sleep", new_callable=AsyncMock) as mock_sleep, \
         caplog.at_level(logging.ERROR):

        await main()

        # as the database failed persisting, the log should be logged, commit must be strictly skipped, rewind_batch called, and the daemon should sleep for 2 seconds (the simple rate limiting)
        assert "(Apollo) Failed to persist events, skipping Kafka consumer offset commit for retry" in caplog.text
        mock_kafka_consumer.rewind_batch.assert_called_once_with(sample_raw_records)
        mock_kafka_consumer.commit.assert_not_called() # commit must be strictly skipped
        mock_sleep.assert_awaited_once_with(2)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# context managers & exception handling tests

"""
    EXCEPTION HANDLING TEST (GRACEFUL CANCELLATION)
    tests that CancelledError raised inside the daemon loop is handled gracefully without re-raising
"""
@pytest.mark.anyio
async def test_consumer_daemon_graceful_cancellation(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=asyncio.CancelledError)

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         caplog.at_level(logging.INFO):

        await main()

        assert "(Apollo) consumer daemon was running, then was stopped by the user (KeyboardInterrupt)" in caplog.text # inner fallback
        assert "(Apollo) consumer daemon was shutdown gracefully" in caplog.text # top level fallback within finally block

"""
    EXCEPTION HANDLING TEST (UNEXPECTED EXCEPTION RECOVERY)
    tests that fatal unexpected exceptions during daemon processing are caught, logged, and shutdown gracefully
"""
@pytest.mark.anyio
async def test_consumer_daemon_unexpected_exception_recovery(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=RuntimeError("critical network failure or something"))

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister), \
         caplog.at_level(logging.INFO):

        await main()

        # similar to above but this time the error is fatal and comes from somewhere else (not cancellation ctrl+c)
        assert "(Apollo) consumer daemon unexpected error: critical network failure or something" in caplog.text
        assert "(Apollo) consumer daemon was shutdown gracefully" in caplog.text

"""
    CONTEXT MANAGER PROTOCOL TEST
    tests that both ApolloKafkaConsumer and PostgresPersister enter and exit properly in all daemon lifecycles
"""
@pytest.mark.anyio
async def test_consumer_daemon_context_manager_protocol(
    mock_signal_loop: tuple[dict[signal.Signals, Any], MagicMock, MagicMock],
    mock_kafka_consumer: MagicMock,
    mock_postgres_persister: MagicMock,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    handlers, mock_add, mock_remove = mock_signal_loop
    monkeypatch.setattr("apollo.consumer_daemon.sys.platform", "linux")

    mock_kafka_consumer.get_batch = AsyncMock(side_effect=asyncio.CancelledError)

    with patch.object(asyncio.get_running_loop(), "add_signal_handler", mock_add), \
         patch.object(asyncio.get_running_loop(), "remove_signal_handler", mock_remove), \
         patch("apollo.consumer_daemon.ApolloKafkaConsumer", return_value=mock_kafka_consumer), \
         patch("apollo.consumer_daemon.PostgresPersister", return_value=mock_postgres_persister):

        await main()

        mock_kafka_consumer.__aenter__.assert_awaited_once()
        mock_kafka_consumer.__aexit__.assert_awaited_once()
        mock_postgres_persister.__aenter__.assert_awaited_once()
        mock_postgres_persister.__aexit__.assert_awaited_once()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass