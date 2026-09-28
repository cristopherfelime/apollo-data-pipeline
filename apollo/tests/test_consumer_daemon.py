"""
    unit testing script for background consumer daemon service in consumer_daemon.py
    v0.1 - test fixtures and synthetic data setup for consumer daemon lifecycle, batch processing, and error handling
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
import logging
import asyncio
import signal
import sys
import orjson
from uuid import uuid4
from unittest.mock import patch, AsyncMock, MagicMock
from aiokafka.structs import ConsumerRecord

from apollo.consumer_daemon import main
from apollo.kafka.consumer import ApolloKafkaConsumer
from apollo.database.service import PostgresPersister

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture
def sample_raw_records() -> list[ConsumerRecord]:
    """synthetic list of 3 ConsumerRecords spanning reviews, news, and transaction topics"""
    review_bytes = orjson.dumps({
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
    news_bytes = orjson.dumps({
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
    tx_bytes = orjson.dumps({
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
def sample_parsed_records(sample_raw_records) -> dict[str, list[dict]]:
    """synthetic dictionary of deserialized event batches matching sample_raw_records"""
    return {
        record.topic: [orjson.loads(record.value)] for record in sample_raw_records
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
def mock_kafka_consumer():
    """synthetic mock ApolloKafkaConsumer handler with async context manager and batch methods"""
    mock_consumer = MagicMock(spec=ApolloKafkaConsumer)
    mock_consumer._consumer = MagicMock() # alive by default
    mock_consumer.get_batch = AsyncMock(return_value=[])
    mock_consumer.commit = AsyncMock()
    mock_consumer.__aenter__ = AsyncMock(return_value=mock_consumer)
    mock_consumer.__aexit__ = AsyncMock(return_value=None)
    return mock_consumer

@pytest.fixture
def mock_postgres_persister():
    """synthetic mock PostgresPersister handler with async context manager and persistence methods"""
    mock_persister = MagicMock(spec=PostgresPersister)
    mock_persister.parse_events = MagicMock(return_value={})
    mock_persister.persist_events = AsyncMock(return_value=True)
    mock_persister.__aenter__ = AsyncMock(return_value=mock_persister)
    mock_persister.__aexit__ = AsyncMock(return_value=None)
    return mock_persister