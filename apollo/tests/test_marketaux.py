"""
    unit testing script for apollo MarketauxScraper in marketaux.py
    v1.0 - unit tests for parameter management, client lifecycle, context manager, mocked HTTP responses, multi-level error handling, and run pipeline
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
import httpx
from unittest.mock import patch, AsyncMock # AsyncMock is to mock asynchronous (coroutines-based) objects here, will be used to mock httpx.AsyncClient later
from datetime import datetime, timezone
from uuid import UUID

from apollo.scrapers.marketaux import MarketauxScraper
from apollo.schemas import FinancialNewsPayload

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture
def sample_marketaux_api_response():
    """synthetic raw marketaux REST API response payload matching production API structure with extra fields"""
    return {
        "meta": {
            "found": 1420,
            "returned": 1,
            "limit": 3,
            "page": 1
        },
        "data": [
            {
                "uuid": "marketaux-article-uuid-12345",
                "title": "Bank Negara Malaysia Issues Updated Digital Banking Framework",
                "description": "BNM announced comprehensive operational frameworks for all licensed digital banks...",
                "keywords": "banking, malaysia, bnm, digital banking, finance",
                "snippet": "BNM today announced comprehensive updated guidelines for digital banks operating in Malaysia.",
                "url": "https://www.thestar.com.my/business/2026/08/digital-banks",
                "image_url": "https://images.thestar.com.my/news/digital-banks.jpg",
                "language": "en",
                "published_at": "2026-08-01T10:30:00.000000Z",
                "source": "thestar.com.my",
                "relevance_score": None,
                "entities": [
                    {
                        "symbol": "MAYBANK.KL",
                        "name": "Malayan Banking Berhad",
                        "exchange": "KLSE",
                        "exchange_long": "Bursa Malaysia",
                        "country": "my",
                        "type": "equity",
                        "industry": "Financial Services",
                        "match_score": 14.89,
                        "sentiment_score": 0.456,
                        "highlights": [
                            {
                                "highlight": "BNM announced <em>updated</em> frameworks",
                                "sentiment": 0.456,
                                "highlighted_in": "title"
                            }
                        ]
                    }
                ],
                "similar": []
            }
        ]
    }

@pytest.fixture
def sample_marketaux_no_sentiment_response():
    """synthetic raw marketaux response without entities / sentiment score matching production API"""
    return {
        "meta": {"found": 1, "returned": 1, "limit": 3, "page": 1},
        "data": [
            {
                "uuid": "marketaux-article-uuid-67890",
                "title": "GXBank Reaches 500k Active Depositors in First Year",
                "description": "GXBank Malaysia reports strong adoption among retail consumers.",
                "keywords": "gxbank, grab, digital bank, depositors",
                "snippet": "GXBank Malaysia reports strong adoption among retail consumers.",
                "url": "https://fintechnews.my/gxbank-growth",
                "image_url": "https://fintechnews.my/assets/gxbank.png",
                "language": "en",
                "published_at": "2026-08-05T08:15:00.000000Z",
                "source": "fintechnews.my",
                "relevance_score": None,
                "entities": [], # empty entities list
                "similar": []
            }
        ]
    }

@pytest.fixture
def sample_marketaux_malformed_response():
    """synthetic marketaux response containing one valid item and one malformed item matching production API"""
    return {
        "meta": {"found": 2, "returned": 2, "limit": 3, "page": 1},
        "data": [
            {
                "uuid": "valid-news-1",
                "title": "Valid Headline for Digital Banking Growth",
                "description": "Valid summary description.",
                "keywords": "banking, growth",
                "snippet": "Valid summary snippet.",
                "url": "https://thestar.com.my/news/1",
                "image_url": "https://thestar.com.my/img1.jpg",
                "language": "en",
                "published_at": "2026-08-01T10:00:00.000000Z",
                "source": "thestar.com.my",
                "relevance_score": None,
                "entities": [{"symbol": "MAYBANK", "name": "Maybank", "sentiment_score": 0.25}],
                "similar": []
            },
            {
                "uuid": "invalid-news-2",
                "title": "Hi", # invalid: min_length=5
                "description": "Short",
                "keywords": "invalid",
                "snippet": "Snippet",
                "url": "ftp://invalid-protocol.com", # invalid url pattern
                "image_url": "https://thestar.com.my/img2.jpg",
                "language": "en",
                "published_at": "2026-08-01T10:00:00.000000Z",
                "source": "invalid",
                "relevance_score": None,
                "entities": [],
                "similar": []
            }
        ]
    }

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# params and search targets management tests

"""
    MANAGEMENT TEST
    tests MarketauxScraper initialization with default and custom params and search targets
"""
def test_marketaux_init_default_and_custom() -> None:
    # default initialization
    default_scraper = MarketauxScraper()
    assert default_scraper.params.get("country") == "my" # check if default country is Malaysia
    assert "Maybank" in default_scraper.search_targets # check if Maybank is in default search targets
    assert len(default_scraper.search_targets) == 4 # check if default search targets length is 4
    assert default_scraper.client is None # check if client is None

    # custom initialization
    custom_params = {"api_token": "test_token", "limit": 5} # custom params
    custom_targets = ["CIMB", "Public Bank"] # custom search targets
    custom_scraper = MarketauxScraper(params=custom_params, search_targets=custom_targets)
    assert custom_scraper.params == custom_params # check if custom params are set
    assert custom_scraper.search_targets == custom_targets # check if custom search targets are set

"""
    MANAGEMENT TEST
    tests MarketauxScraper add_params and remove_params methods
"""
def test_marketaux_params_management() -> None:
    scraper = MarketauxScraper(params={"api_token": "token123"}) # initialize scraper with custom params
    assert scraper.params == {"api_token": "token123"} # check if custom params are set

    # add params
    scraper.add_params({"limit": 10, "language": "en"}) # add params
    assert scraper.params.get("limit") == 10 # check if limit is set, added properly to params
    assert scraper.params.get("language") == "en" # same for language

    # remove params
    scraper.remove_params(["limit", "non_existent_key"]) # remove params, non_existent_key tests error handling when popping from params
    assert "limit" not in scraper.params # check if limit is not in params, because it was removed properly
    assert "language" in scraper.params # same for language

"""
    MANAGEMENT TEST
    tests MarketauxScraper add_search_targets, remove_search_targets, and set_endpoint methods
"""
def test_marketaux_search_targets_and_endpoint_management() -> None:
    scraper = MarketauxScraper(search_targets=["Maybank"]) # initialize scraper with custom search targets
    
    # add search targets
    scraper.add_search_targets(["Boost Bank", "GX Bank"]) # add search targets
    assert scraper.search_targets == ["Maybank", "Boost Bank", "GX Bank"] # check if search targets are set, added properly to search_targets

    # remove search targets
    scraper.remove_search_targets(["Maybank", "NonExistent"]) # remove search targets, NonExistent isnt in search_targets so should properly be ignored by the if guard
    assert scraper.search_targets == ["Boost Bank", "GX Bank"] # check if search targets are set, removed properly from search_targets

    # set endpoint
    custom_url = "https://api.marketaux.com/v1/news/custom" # custom url for endpoint
    scraper.set_endpoint(custom_url) # set endpoint
    assert scraper.endpoint == custom_url # check if endpoint is set, added properly to endpoint

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# async client lifecycle and context manager tests

"""
    LIFECYCLE TEST
    tests MarketauxScraper start_client and close_client methods
"""
@pytest.mark.anyio
async def test_marketaux_client_lifecycle() -> None:
    scraper = MarketauxScraper()
    assert scraper.client is None # check if client is None, which it should be by default

    mock_client = AsyncMock(spec=httpx.AsyncClient) # create mock async httpx.AsyncClient instance
    with patch("apollo.scrapers.marketaux.httpx.AsyncClient", return_value=mock_client): # basically patches the imported httpx module (the AsyncClient class specifically) to start a mock httpx.AsyncClient instead of the actual httpx.AsyncClient that opens up network connections for real, happens in start_client()
        # start client
        await scraper.start_client()
        assert scraper.client is mock_client # check if client is set to mock client

        # close client
        await scraper.close_client()
        mock_client.aclose.assert_awaited_once() # check if close_client() above actually awaited client.aclose() to close all HTTP connection pools
        assert scraper.client is None # check if client is None, which it should be after being closed

"""
    CONTEXT MANAGER TEST
    tests MarketauxScraper async context manager (__aenter__ and __aexit__)
"""
@pytest.mark.anyio
async def test_marketaux_context_manager() -> None:
    scraper = MarketauxScraper()
    assert scraper.client is None

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    with patch("apollo.scrapers.marketaux.httpx.AsyncClient", return_value=mock_client):
        async with scraper as s: # the context manager itself
            assert s is scraper # testing the scraper instance if it was set properly
            assert scraper.client is mock_client # the mock client now, should automatically be opened since context manager (__aenter__)

        mock_client.aclose.assert_awaited_once() # __aexit__ automatically calls the close_client method and tests if it awaited client.aclose()
        assert scraper.client is None

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fetch method tests (mocked httpx.AsyncClient)

"""
    FETCH TEST
    tests MarketauxScraper.fetch() with a single target endpoint using mocked httpx.AsyncClient
"""
@pytest.mark.anyio
async def test_marketaux_fetch_single_target(sample_marketaux_api_response) -> None:
    scraper = MarketauxScraper()
    mock_request = httpx.Request("GET", "https://api.marketaux.com/v1/news/all") # mock get (read) request for single target to marketaux api
    mock_response = httpx.Response(200, json=sample_marketaux_api_response, request=mock_request) # mock status code 200 response with json body (from sample above) when the request above is made

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.get.return_value = mock_response # ensures that client.get() returns mock_response, as the fetch() method uses this
    scraper.client = mock_client # the client attribute is set to mock_client 

    result = await scraper.fetch(target="https://api.marketaux.com/v1/news/all", count=1) # target is the endpoint, count=1 means only 1 request is made
    
    mock_client.get.assert_called_once() # ensures that client.get() was called only once from the fetch method calling above
    assert len(result) == 1 # ensures that only 1 result was returned
    assert result[0].status_code == 200 # ensures that the result has status code 200
    assert result[0].json() == sample_marketaux_api_response # ensures that the result has the same json body as the mock response

"""
    FETCH TEST
    tests MarketauxScraper.fetch() with multiple target endpoints running concurrently
"""
@pytest.mark.anyio
async def test_marketaux_fetch_multiple_targets(sample_marketaux_api_response) -> None:
    scraper = MarketauxScraper()
    mock_request = httpx.Request("GET", scraper.endpoint) # scraper.endpoint is the exact same url as above anyways
    mock_response = httpx.Response(200, json=sample_marketaux_api_response, request=mock_request)
    targets = ["https://api.marketaux.com/v1/news/1", "https://api.marketaux.com/v1/news/2"] # list of endpoints to be fetched concurrently

    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.get.return_value = mock_response
    scraper.client = mock_client

    result = await scraper.fetch(target=targets, count=1)
    
    assert mock_client.get.call_count == 2 # ensures that client.get() was called twice, once for each target
    assert len(result) == 2 # ensures that only 2 results were returned
    assert result[0].status_code == 200 # ensures that the first result has status code 200
    assert result[1].status_code == 200 # ensures that the second result has status code 200

"""
    FETCH TEST (ERROR HANDLING)
    tests MarketauxScraper.fetch() handling top-level unexpected exceptions gracefully
"""
@pytest.mark.anyio
async def test_marketaux_fetch_exception_handling() -> None:
    scraper = MarketauxScraper()

    with patch.object(scraper, "start_client", new_callable=AsyncMock, side_effect=Exception("Socket creation failure")): # start_client() is an asynchronous method, so instead of using MagicMock we gotta use AsyncMock
        result = await scraper.fetch()
        assert result == [] # testing if the fetch() method returns an empty list when encountering unexpected exceptions

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# process method tests

"""
    VALIDATION TEST
    tests MarketauxScraper.process() converting valid raw response into FinancialNewsPayload
    verifies entities sentiment score extraction and Pydantic validation
"""
@pytest.mark.anyio
async def test_marketaux_process_valid_response(sample_marketaux_api_response) -> None:
    scraper = MarketauxScraper()
    mock_request = httpx.Request("GET", scraper.endpoint)
    mock_response = httpx.Response(200, json=sample_marketaux_api_response, request=mock_request)

    processed = await scraper.process([mock_response]) # pass the mock response to process() method inside a list
    assert len(processed) == 1 # ensures that only 1 processed article is returned (as only 1 mock response was passed)
    article = processed[0] # take the first (and only) processed article

    assert isinstance(article, FinancialNewsPayload) # verifies that the processed article is an instance of FinancialNewsPayload
    assert article.article_uuid == "marketaux-article-uuid-12345" # verifies the article_uuid
    assert article.title == "Bank Negara Malaysia Issues Updated Digital Banking Framework" # verifies the title
    assert article.source == "thestar.com.my" # verifies the source
    assert article.sentiment_score == 0.456 # extracted from entities[0]["sentiment_score"], verifies the extraction logic
    assert isinstance(article.event_id, UUID) # verifies the event_id is a UUID object (converted from string)
    assert article.published_at.tzinfo == timezone.utc # verifies the published_at has UTC timezone (converted from string)

"""
    VALIDATION TEST
    tests MarketauxScraper.process() handling news item without entities / sentiment score
    verifies sentiment_score defaults to None
"""
@pytest.mark.anyio
async def test_marketaux_process_missing_sentiment_fallback(sample_marketaux_no_sentiment_response) -> None:
    scraper = MarketauxScraper()
    mock_request = httpx.Request("GET", scraper.endpoint)
    mock_response = httpx.Response(200, json=sample_marketaux_no_sentiment_response, request=mock_request) # this time the mock response object carries the no sentiment sample response version

    processed = await scraper.process([mock_response])
    assert len(processed) == 1
    article = processed[0]

    assert article.article_uuid == "marketaux-article-uuid-67890" # uuid check why not
    assert article.sentiment_score is None # should fallback to None here

"""
    RESILIENCE TEST (BATCH LEVEL)
    tests MarketauxScraper.process() skipping network exceptions in payload (from asyncio.gather())
"""
@pytest.mark.anyio
async def test_marketaux_process_skips_network_exceptions(sample_marketaux_api_response) -> None:
    scraper = MarketauxScraper()
    mock_request = httpx.Request("GET", scraper.endpoint)
    valid_response = httpx.Response(200, json=sample_marketaux_api_response, request=mock_request)
    timeout_exception = httpx.TimeoutException("Read timed out") # as the other fetching result

    # batch containing 1 timeout exception and 1 valid response
    processed = await scraper.process([timeout_exception, valid_response])
    assert len(processed) == 1 # only 1 valid response (2 total item) so 1 processed article returned
    assert processed[0].article_uuid == "marketaux-article-uuid-12345" # as the timeout exception is skipped

"""
    RESILIENCE TEST (BATCH LEVEL)
    tests MarketauxScraper.process() skipping HTTP status code errors (like HTTP 429 Too Many Requests and HTTP 500 Internal Server Error)
"""
@pytest.mark.anyio
async def test_marketaux_process_skips_http_status_errors(sample_marketaux_api_response) -> None:
    scraper = MarketauxScraper()
    mock_request = httpx.Request("GET", scraper.endpoint)
    valid_response = httpx.Response(200, json=sample_marketaux_api_response, request=mock_request)
    error_response_429 = httpx.Response(429, json={"error": "Rate limit reached"}, request=mock_request)
    error_response_500 = httpx.Response(500, json={"error": "Internal server error"}, request=mock_request)

    # batch containing error status responses and 1 valid response
    processed = await scraper.process([error_response_429, error_response_500, valid_response])
    assert len(processed) == 1 # only 1 valid response (3 total item) so 1 processed article returned
    assert processed[0].article_uuid == "marketaux-article-uuid-12345" # as the error responses are skipped

"""
    RESILIENCE TEST (INDIVIDUAL ITEM LEVEL)
    tests MarketauxScraper.process() skipping invalid individual news items without rejecting the whole batch
"""
@pytest.mark.anyio
async def test_marketaux_process_skips_invalid_news_items(sample_marketaux_malformed_response) -> None:
    scraper = MarketauxScraper()
    mock_request = httpx.Request("GET", scraper.endpoint)
    response = httpx.Response(200, json=sample_marketaux_malformed_response, request=mock_request) # uses malformed sample response here (different from no sentiment response version)

    processed = await scraper.process([response])
    # only the valid item should be processed, malformed item skipped
    assert len(processed) == 1 # only 1 valid response (1 total item) so 1 processed article returned
    assert processed[0].article_uuid == "valid-news-1" # as the malformed response is skipped

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# run pipeline tests

"""
    PIPELINE TEST
    tests MarketauxScraper.run() end-to-end standalone (opened_locally = True) with mocked fetch responses
    verifies keyword batching, client lifecycle management, and FinancialNewsPayload return list
"""
@pytest.mark.anyio
async def test_marketaux_run_standalone_success(sample_marketaux_api_response) -> None:
    scraper = MarketauxScraper(search_targets=["Maybank", "GX Bank"])
    mock_request = httpx.Request("GET", scraper.endpoint)
    mock_response = httpx.Response(200, json=sample_marketaux_api_response, request=mock_request)
    mock_client = AsyncMock(spec=httpx.AsyncClient)

    with patch("apollo.scrapers.marketaux.httpx.AsyncClient", return_value=mock_client): # required since open_locally = True
        with patch.object(scraper, "fetch", new_callable=AsyncMock, return_value=[mock_response]) as mock_fetch:
            results = await scraper.run(count=1)
            assert mock_fetch.call_count == 2 # called for Maybank and GX Bank
            assert len(results) == 2 # both Maybank and GX Bank should be processed
            assert all(isinstance(r, FinancialNewsPayload) for r in results) # all results should be FinancialNewsPayload objects, all() returns True if all elements inside of it are True

"""
    PIPELINE TEST (ERROR HANDLING)
    tests MarketauxScraper.run() handling unexpected fatal pipeline errors
"""
@pytest.mark.anyio
async def test_marketaux_run_unexpected_error() -> None:
    scraper = MarketauxScraper()
    mock_client = AsyncMock(spec=httpx.AsyncClient)

    with patch("apollo.scrapers.marketaux.httpx.AsyncClient", return_value=mock_client):
        with patch.object(scraper, "fetch", new_callable=AsyncMock, side_effect=Exception("Fatal network crash")):
            results = await scraper.run(count=1)
            assert results == [] # Ok empty list should be returned by run() if entire batch failed

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass
