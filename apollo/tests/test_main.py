"""
    unit testing script for apollo main entry point and orchestrator in main.py
    v1.0 - unit tests for scraper concurrency, payload type classification, OCP event tuple collection, producer lifecycle, partial scraper failures, unexpected type filtering, and cancellation handling
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
from unittest.mock import patch, AsyncMock
from uuid import uuid4
from datetime import datetime, timezone
from asyncio import CancelledError

from apollo.main import main
from apollo.schemas import ReviewPayload, FinancialNewsPayload
from apollo.kafka.producer import ApolloKafkaProducer
from apollo.scrapers.play_store import PlayStoreScraper
from apollo.scrapers.marketaux import MarketauxScraper

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture
def sample_review_payload():
    """synthetic single valid ReviewPayload Pydantic model instance"""
    return ReviewPayload(
        event_id=uuid4(),
        app_id="my.com.gxbank.app",
        app_name="GX Bank",
        user_name="Farhan Azmi",
        rating=5,
        review_text="Great UI and instant transfers with no hidden fees!",
        app_version="1.4.2",
        submitted_at=datetime(2026, 8, 10, 14, 30, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 8, 10, 14, 35, tzinfo=timezone.utc)
    )

@pytest.fixture
def sample_news_payload():
    """synthetic single valid FinancialNewsPayload Pydantic model instance"""
    return FinancialNewsPayload(
        event_id=uuid4(),
        article_uuid="marketaux-uuid-12345",
        title="Bank Negara Malaysia Issues Updated Digital Banking Framework",
        snippet="BNM today announced comprehensive updated guidelines for digital banks operating in Malaysia.",
        url="https://www.thestar.com.my/business/2026/08/digital-banks",
        source="thestar.com.my",
        sentiment_score=0.456,
        published_at=datetime(2026, 8, 1, 10, 30, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 8, 1, 10, 35, tzinfo=timezone.utc)
    )

@pytest.fixture
def sample_producer_results():
    """synthetic producer return count dictionary"""
    return {
        "app-reviews-events": 1,
        "market-news-events": 1
    }

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# orchestrator pipeline tests

"""
    PIPELINE TEST
    tests main() executing scrapers polymorphically, classifying outputs into ReviewPayload and FinancialNewsPayload,
    collecting (partition_key, event_dict) tuples, and streaming to ApolloKafkaProducer inside async context manager
"""
@pytest.mark.anyio
async def test_main_success(sample_review_payload, sample_news_payload, sample_producer_results) -> None:
    mock_playstore_run = AsyncMock(return_value=[sample_review_payload]) # makes a mock scraper basically, doesn't actually create a PlayStoreScraper instance
    mock_marketaux_run = AsyncMock(return_value=[sample_news_payload]) # same as above but for MarketauxScraper
    
    mock_producer = AsyncMock(spec=ApolloKafkaProducer) # mocks ApolloKafkaProducer class for async context manager 
    mock_producer.run.return_value = sample_producer_results # and when the run() method of the mock kafka producer above is ran, it returns a mock sample_producer_results (return count)

    with patch.object(PlayStoreScraper, "run", mock_playstore_run), \
         patch.object(MarketauxScraper, "run", mock_marketaux_run), \
         patch("apollo.main.ApolloKafkaProducer") as mock_producer_class: # python's multi-line, patch both scapers' run() methods to return a mock version, and mock the ApolloKafkaProducer class that was initialized with with-as statement to return a mock instance
        
        # setup async context manager mock for ApolloKafkaProducer, context manager runs __aenter__ where we change its return value to mock_producer
        mock_producer_class.return_value.__aenter__.return_value = mock_producer
        await main() # then we run

        mock_playstore_run.assert_awaited_once_with(count=1) # verify the mock play store scraper were invoked with count=1
        mock_marketaux_run.assert_awaited_once_with(count=1) # same as above but with the mock marketaux scarper

        mock_producer.run.assert_awaited_once_with( # verify producer.run() method was awaited once with the expected events dict: {topic: [(partition_key, event_dict), ...]}
            {
                "app-reviews-events": [
                    ("my.com.gxbank.app", sample_review_payload.model_dump())
                ],
                "market-news-events": [
                    ("thestar.com.my", sample_news_payload.model_dump())
                ]
            },
            return_results=True
        )

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# resilience and scraper error handling tests

"""
    RESILIENCE TEST (PARTIAL SCRAPER FAILURE)
    tests main() when one scraper raises an exception during asyncio.gather()
    verifies that the failed scraper is logged and skipped, while surviving scraper events are still collected and streamed
"""
@pytest.mark.anyio
async def test_main_partial_scraper_failure(sample_news_payload) -> None:
    mock_playstore_run = AsyncMock(side_effect=Exception("Play Store rate limit reached")) # mock play store scraper will throw exception when run()
    mock_marketaux_run = AsyncMock(return_value=[sample_news_payload])
    
    mock_producer = AsyncMock(spec=ApolloKafkaProducer)
    mock_producer.run.return_value = {"app-reviews-events": 0, "market-news-events": 1} # mock producer result now shows 0 events from play store scraper, 1 from marketaux scraper

    with patch.object(PlayStoreScraper, "run", mock_playstore_run), \
         patch.object(MarketauxScraper, "run", mock_marketaux_run), \
         patch("apollo.main.ApolloKafkaProducer") as mock_producer_class:
        
        mock_producer_class.return_value.__aenter__.return_value = mock_producer

        await main() # should not raise exception despite play store failure, the if guard in action in main.py

        mock_producer.run.assert_awaited_once_with( # verify producer was still called with the surviving marketaux news events
            {
                "app-reviews-events": [],
                "market-news-events": [
                    ("thestar.com.my", sample_news_payload.model_dump())
                ]
            },
            return_results=True
        )

"""
    RESILIENCE TEST (ALL SCRAPERS FAIL)
    tests main() when all scrapers encounter exceptions during asyncio.gather()
    verifies empty events dictionary is passed to producer and main() completes gracefully
"""
@pytest.mark.anyio
async def test_main_all_scrapers_fail() -> None:
    mock_playstore_run = AsyncMock(side_effect=Exception("Play Store network timeout")) # both dead
    mock_marketaux_run = AsyncMock(side_effect=Exception("Marketaux API 500 error"))
    
    mock_producer = AsyncMock(spec=ApolloKafkaProducer)
    mock_producer.run.return_value = None # and now they're gone

    with patch.object(PlayStoreScraper, "run", mock_playstore_run), \
         patch.object(MarketauxScraper, "run", mock_marketaux_run), \
         patch("apollo.main.ApolloKafkaProducer") as mock_producer_class:
        
        mock_producer_class.return_value.__aenter__.return_value = mock_producer

        await main() # verifies clean completion with 0 events

        mock_producer.run.assert_awaited_once_with(
            {
                "app-reviews-events": [],
                "market-news-events": []
            },
            return_results=True
        )

"""
    VALIDATION TEST (UNEXPECTED EVENT TYPE FILTERING)
    tests main() receiving unrecognized object types from scrapers
    verifies invalid items are safely skipped and logged without crashing the iteration loop
"""
@pytest.mark.anyio
async def test_main_skips_unexpected_event_type(sample_review_payload) -> None:
    # returns 1 valid payload and 2 invalid types (a string and a raw dict)
    mock_playstore_run = AsyncMock(return_value=[
        sample_review_payload, # this one is valid
        "unexpected_string_item", # but rest are some bs to simulate unrecognized objects returned by PlayStoreScraper
        {"invalid": "dictionary_without_schema"}
    ])
    mock_marketaux_run = AsyncMock(return_value=[]) # empty to keep it simple for this test
    
    mock_producer = AsyncMock(spec=ApolloKafkaProducer)
    mock_producer.run.return_value = {"app-reviews-events": 1, "market-news-events": 0}

    with patch.object(PlayStoreScraper, "run", mock_playstore_run), \
         patch.object(MarketauxScraper, "run", mock_marketaux_run), \
         patch("apollo.main.ApolloKafkaProducer") as mock_producer_class:
        
        mock_producer_class.return_value.__aenter__.return_value = mock_producer

        await main()

        mock_producer.run.assert_awaited_once_with( # only the 1 valid ReviewPayload is added to app-reviews-events, invalid objects skipped
            {
                "app-reviews-events": [
                    ("my.com.gxbank.app", sample_review_payload.model_dump())
                ],
                "market-news-events": []
            },
            return_results=True
        )

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# lifecycle & exception handling tests

"""
    LIFECYCLE TEST (TASK CANCELLATION)
    we test for asyncio.CancelledError in test_main.py only as it is the main orchestrator
    tests main() handling asyncio.CancelledError (SIGINT / KeyboardInterrupt)
    verifies CancelledError is caught and logged, returning cleanly without re-raising like inner tasks
"""
@pytest.mark.anyio
async def test_main_handles_cancellation() -> None:
    async def mock_cancelled_gather(*coros, **kwargs): # thx gemini
        for coro in coros: # close passed scraper coroutines cleanly to prevent un-awaited coroutine runtime warnings
            coro.close()
        raise CancelledError()

    with patch("apollo.main.asyncio.gather", side_effect=mock_cancelled_gather):
        await main() # main() should trap CancelledError and return cleanly

"""
    RESILIENCE TEST (UNEXPECTED FATAL ERROR)
    tests main() handling unexpected top-level exceptions (like fatal producer crash)
    verifies top-level try-except logs the error and exits without crashing unhandled
"""
@pytest.mark.anyio
async def test_main_unexpected_exception_handling(sample_review_payload) -> None:
    mock_playstore_run = AsyncMock(return_value=[sample_review_payload])
    mock_marketaux_run = AsyncMock(return_value=[])

    with patch.object(PlayStoreScraper, "run", mock_playstore_run), \
         patch.object(MarketauxScraper, "run", mock_marketaux_run), \
         patch("apollo.main.ApolloKafkaProducer", side_effect=Exception("Fatal Kafka broker connection failure")):
        
        await main() # should catch the top-level exception and return cleanly

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass