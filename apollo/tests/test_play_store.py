"""
    unit testing script for apollo PlayStoreScraper in play_store.py
    v1.0 - unit tests for dictionary management, raw review processing, mocked network fetching, and end-to-end run aggregation
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
from unittest.mock import patch
from datetime import datetime, timezone
from uuid import UUID

from apollo.scrapers.play_store import PlayStoreScraper
from apollo.schemas import ReviewPayload

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data
# reusable synthetic review dictionaries that can be passed onto test functions as arguments

@pytest.fixture
def anyio_backend():
    return "asyncio" # in apollo we only use asyncio event loop for concurrency, not trio

@pytest.fixture
def sample_raw_review():
    """synthetic raw review dictionary as returned by google_play_scraper library with realistic extra fields"""
    return {
        "reviewId": "gp:AOqpTOE_example_review_id_12345",
        "userName": "Farhan Azmi",
        "userImage": "https://play-lh.googleusercontent.com/a-/ALV-UjV_example_avatar",
        "content": "Great UI and instant transfers with no hidden fees!",
        "score": 5,
        "thumbsUpCount": 14,
        "reviewCreatedVersion": "1.4.2",
        "at": datetime(2026, 8, 10, 14, 30, 0),
        "replyContent": None,
        "repliedAt": None,
        "appVersion": "1.4.2"
    }

@pytest.fixture
def sample_invalid_raw_review():
    """synthetic invalid review with out-of-bound score and empty content matching google_play_scraper structure"""
    return {
        "reviewId": "gp:AOqpTOE_invalid_review_id",
        "userName": "Bad Reviewer",
        "userImage": "https://play-lh.googleusercontent.com/a-/avatar_bad",
        "content": "", # invalid: min_length=2
        "score": 10, # invalid: ge=1, le=5
        "thumbsUpCount": 0,
        "reviewCreatedVersion": "1.0",
        "at": datetime(2026, 8, 10, 14, 30, 0),
        "replyContent": None,
        "repliedAt": None,
        "appVersion": "1.0"
    }

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# app dictionary management tests
# mostly testing default initialization attributes and crud methods (such as getters and setters) of PlayStoreScraper

"""
    MANAGEMENT TEST
    tests PlayStoreScraper initialization with default and custom app_dict
"""
def test_play_store_init_default_and_custom() -> None:
    # default initialization
    default_scraper = PlayStoreScraper()
    assert "my.com.gxbank.app" in default_scraper.get_app_dict() # to see if the default app_id is in the app_dict
    assert default_scraper.get_app_dict()["my.com.gxbank.app"] == "GX Bank" # to see if the default app_name is in the app_dict
    assert len(default_scraper.get_app_dict()) == 4 # to see if default 4 apps are initialized and can be accessed

    # custom initialization
    custom_dict = {"com.custom.app": "Custom App"} # custom apps dictionary containing target apps and their corresponding app names
    custom_scraper = PlayStoreScraper(app_dict=custom_dict) # initializing scraper with custom apps dictionary
    assert custom_scraper.get_app_dict() == custom_dict # to see if the custom apps dictionary is correctly initialized and can be accessed

"""
    MANAGEMENT TEST
    tests PlayStoreScraper add_app_dict, get_app_dict, and remove_app methods
"""
def test_play_store_app_dict_crud() -> None:
    scraper = PlayStoreScraper(app_dict={}) # initializing scraper with empty apps dictionary
    assert scraper.get_app_dict() == {} # to see if the apps dictionary is empty

    # add app
    scraper.add_app_dict("com.test.app", "Test App") # adding a new app to the dictionary
    assert scraper.get_app_dict() == {"com.test.app": "Test App"} # to see if the new app is added to the dictionary

    # remove app
    scraper.remove_app("com.test.app") # removing an app from the dictionary
    assert scraper.get_app_dict() == {} # to see if the app is removed from the dictionary

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# process method tests

"""
    VALIDATION TEST
    tests PlayStoreScraper.process() on converting raw review dicts to ReviewPayload
    verifies target app_name and app_id mapping and Pydantic validation
"""
@pytest.mark.anyio # IMPORTANT: pytest needs this to run async tests on async methods
async def test_play_store_process_valid_reviews(sample_raw_review) -> None:
    scraper = PlayStoreScraper()
    raw_payload = [sample_raw_review]
    target_app = "my.com.gxbank.app"

    processed = await scraper.process(raw_payload, target=target_app)
    assert len(processed) == 1 # to see if only 1 review is processed and returned (as expected from input)
    review = processed[0] # process() intended to return list of ReviewPayload objects, so this is needed

    assert isinstance(review, ReviewPayload) # to see if the returned object is an instance of ReviewPayload class
    assert review.app_id == "my.com.gxbank.app" # to see if the app_id is correctly mapped from the raw review dict
    assert review.app_name == "GX Bank" # to see if the app_name is correctly mapped from the raw review dict
    assert review.user_name == "Farhan Azmi" # to see if the user_name is correctly mapped from the raw review dict
    assert review.rating == 5 # to see if the rating is correctly mapped from the raw review dict
    assert review.review_text == "Great UI and instant transfers with no hidden fees!" # to see if the review_text is correctly mapped from the raw review dict
    assert isinstance(review.event_id, UUID) # to see if the event_id is an instance of UUID
    assert review.submitted_at.tzinfo == timezone.utc # to see if the submitted_at datetime is in UTC timezone

"""
    VALIDATION TEST
    tests PlayStoreScraper.process() fallback when target is unknown or unprovided
    verifies default to Unknown App and com.unknown
"""
@pytest.mark.anyio
async def test_play_store_process_unknown_target_fallback(sample_raw_review) -> None:
    scraper = PlayStoreScraper()
    raw_payload = [sample_raw_review]

    processed = await scraper.process(raw_payload, target="com.unregistered.app") # test on unregistered app
    assert len(processed) == 1 # to see if only 1 review is processed and returned (as expected from input)
    review = processed[0]

    assert review.app_name == "Unknown App" # to see if the app_name is fallback to "Unknown App"
    assert review.app_id == "com.unknown" # to see if the app_id is fallback to "com.unknown"

"""
    VALIDATION TEST
    tests PlayStoreScraper.process() on filtering extra fields
    verifies that only fields required by ReviewPayload are extracted, extra fields are filtered out
"""
@pytest.mark.anyio
async def test_play_store_process_filters_extra_fields(sample_raw_review) -> None:
    scraper = PlayStoreScraper()
    raw_payload = [sample_raw_review]
    target_app = "my.com.gxbank.app"

    processed = await scraper.process(raw_payload, target=target_app)
    review = processed[0]

    # check if extra fields are filtered out
    assert "reviewId" not in review.model_dump()
    assert "userImage" not in review.model_dump()
    assert "thumbsUpCount" not in review.model_dump()
    assert "reviewCreatedVersion" not in review.model_dump()
    assert "appVersion" not in review.model_dump()

    # verify the remaining fields are correct
    assert review.app_id == "my.com.gxbank.app"
    assert review.app_name == "GX Bank"
    assert review.user_name == "Farhan Azmi"
    assert review.rating == 5
    assert review.review_text == "Great UI and instant transfers with no hidden fees!"
    assert review.app_version == "1.4.2"
    assert review.submitted_at.tzinfo == timezone.utc

"""
    VALIDATION TEST
    tests PlayStoreScraper.process() HTML sanitization on review text
    verifies that reviews containing HTML tags like <b>, <br>, and entities are cleanly sanitized
"""
@pytest.mark.anyio
async def test_play_store_process_html_sanitization() -> None:
    scraper = PlayStoreScraper()
    raw_review_with_html = {
        "reviewId": "gp:AOqpTOE_html_test",
        "userName": "Hafizah",
        "userImage": "https://play-lh.googleusercontent.com/avatar",
        "content": "<b>Fast application approval!</b><br><br>Super easy &amp; reliable.",
        "score": 5,
        "thumbsUpCount": 3,
        "reviewCreatedVersion": "2.0.1",
        "at": datetime(2026, 8, 12, 9, 15, 0),
        "appVersion": "2.0.1"
    }
    target_app = "my.com.gxbank.app"

    processed = await scraper.process([raw_review_with_html], target=target_app)
    assert len(processed) == 1 # to see if only 1 review is processed and returned (as expected from input)
    review = processed[0]

    assert review.review_text == "Fast application approval! Super easy reliable." # verifies that <b>, <br>, and &amp; were sanitized and extra whitespaces removed

"""
    INVALIDATION TEST
    tests PlayStoreScraper.process() resilience against invalid reviews
    verifies that malformed reviews are skipped without terminating the batch
"""
@pytest.mark.anyio
async def test_play_store_process_skips_invalid_reviews(sample_raw_review, sample_invalid_raw_review) -> None:
    scraper = PlayStoreScraper()
    raw_payload = [sample_raw_review, sample_invalid_raw_review]
    target_app = "com.maybank2u.life"

    processed = await scraper.process(raw_payload, target=target_app) # test on invalid input data (out-of-bound rating and empty content)
    # only the valid review should be processed, invalid one skipped
    assert len(processed) == 1 # to see if only 1 review is processed and returned (invalid one skipped)
    assert processed[0].user_name == "Farhan Azmi" # to see if the user_name is correctly mapped from the raw review dict

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fetch method tests (mocked google_play_scraper)

"""
    FETCH TEST
    tests PlayStoreScraper.fetch() with a single target app using mocked reviews function, default values insertions, and tuple unpacking in the end (google_play_scraper returns 2 items in a tuple, the latter being a continuation token for fetching more reviews next time)
    and asynchronous execution ig
"""
@pytest.mark.anyio
async def test_play_store_fetch_single_target(sample_raw_review) -> None:
    scraper = PlayStoreScraper()
    mock_reviews_return = ([sample_raw_review], "continuation_token_123") # what google_play_scraper.reviews() is expected to return (list of results, continuation_token)

    # unittest.mock.patch() is used to mock the google_play_scraper.reviews() function to prevent actual api calls and speed up tests
    with patch("apollo.scrapers.play_store.reviews", return_value=mock_reviews_return) as mock_reviews: # in this with statement, we are mocking google_play_scraper.reviews() to return mock_reviews_return above
        result = await scraper.fetch(target="my.com.gxbank.app", count=1) # fetch reviews for a single target app
        mock_reviews.assert_called_once_with( # internal inspection where we check if the mocked function was called with the correct arguments
            "my.com.gxbank.app", # to see if the app_id is correctly passed to the mocked reviews function
            count=1, # to see if the count is correctly passed to the mocked reviews function
            lang="ms", # to see if the lang is correctly passed to the mocked reviews function
            country="my" # to see if the country is correctly passed to the mocked reviews function
        )
        assert result == mock_reviews_return[0] # check if the result is what we expected (the mock_reviews_return)

"""
    FETCH TEST
    tests PlayStoreScraper.fetch() with multiple target apps running concurrently, basically same as above but for multiple target apps
    and
"""
@pytest.mark.anyio
async def test_play_store_fetch_multiple_targets(sample_raw_review) -> None:
    scraper = PlayStoreScraper()
    mock_reviews_return = ([sample_raw_review], "continuation_token_123")
    target_apps = ["my.com.gxbank.app", "com.maybank2u.life"] # 2 apps, for 2 tasks in asyncio.gather(), both are expected to return in format of ([sample_raw_review], "continuation_token_123") from above down below

    with patch("apollo.scrapers.play_store.reviews", return_value=mock_reviews_return) as mock_reviews:
        result = await scraper.fetch(target=target_apps, count=1)
        assert mock_reviews.call_count == 2 # call_count is used to see if the mocked function (that reviews() method) was called twice
        assert len(result) == 2 # to see if the result is a list of two items (one for each target app), asyncio.gather() should be used and it returns the results of all the tasks as a list in the order they were given
        assert result[0] == [sample_raw_review] # to see if the first item is the expected result (which is gxbank's raw review data)
        assert result[1] == [sample_raw_review] # to see if the second item is the expected result (which is maybank's raw review data)

"""
    FETCH TEST (ERROR HANDLING)
    tests PlayStoreScraper.fetch() handling unexpected network exceptions gracefully
"""
@pytest.mark.anyio
async def test_play_store_fetch_exception_handling() -> None:
    scraper = PlayStoreScraper()

    with patch("apollo.scrapers.play_store.reviews", side_effect=Exception("Connection timed out")): # side_effect is used to mock the function to raise an exception instead of returning a value like above
        result = await scraper.fetch(target="my.com.gxbank.app", count=1)
        assert result == [] # to see if the result is an empty list (as fetch() is expected to catch the exception and return an empty list)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# run pipeline tests

"""
    PIPELINE TEST
    tests PlayStoreScraper.run() end-to-end with mocked fetch results
    verifies multi-app concurrent execution, result flattening, and ReviewPayload return list
"""
@pytest.mark.anyio
async def test_play_store_run_success(sample_raw_review) -> None:
    custom_apps = {
        "my.com.gxbank.app": "GX Bank",
        "com.maybank2u.life": "MAE"
    } # custom apps dictionary containing target apps and their corresponding app names (well theyre 2 apps from out default dict but whatever)
    scraper = PlayStoreScraper(app_dict=custom_apps)
    mock_fetch_output = [[sample_raw_review], [sample_raw_review]] # what we expect fetch() to return (list of lists of raw reviews)

    with patch.object(scraper, "fetch", return_value=mock_fetch_output): # patch.object is used to mock the fetch() method of the scraper object
        results = await scraper.run(count=1)
        assert len(results) == 2 # to see if the result is a list of two items (one for each target app)
        assert all(isinstance(r, ReviewPayload) for r in results) # to see if all items in the result are ReviewPayload instances
        assert results[0].app_name == "GX Bank" # to see if my.com.gxbank.app's app_name is GX Bank
        assert results[1].app_name == "MAE" # to see if com.maybank2u.life's app_name is MAE

"""
    PIPELINE TEST (ERROR HANDLING)
    tests PlayStoreScraper.run() handling unexpected errors during pipeline execution
"""
@pytest.mark.anyio
async def test_play_store_run_unexpected_error() -> None:
    scraper = PlayStoreScraper()

    with patch.object(scraper, "fetch", side_effect=Exception("Fatal pipeline crash")): # to mock the fetch() method to raise an exception instead of returning a value like above
        results = await scraper.run(count=1)
        assert results == [] # to see if the result is an empty list (as run() is expected to catch the exception and return an empty list)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass