"""
    unit testing script for apollo schemas.py
    v1.0 - comprehensive validation, html cleaning, timezone standardization, immutability, and boundary tests for ReviewPayload and FinancialNewsPayload
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
from datetime import datetime, timezone
from uuid import UUID
from pydantic import ValidationError

from apollo.schemas import ReviewPayload, FinancialNewsPayload

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# ReviewPayload test cases

""" 
    VALIDATION TEST
    tests ReviewPayload initialization of a valid scraper payload
    primarily tests field aliases, field validators (on submitted_at datetime modification and review_text cleaning), and auto-generation of event_id and ingested_at timestamps
"""
def test_review_payload_valid_from_scraper_dict() -> None:
    raw_data = { # synthetic review data
        "appId": "my.com.gxbank.app",
        "title": "GX Bank",
        "userName": "Alice Tan",
        "content": "Super smooth digital banking experience!",
        "score": 5,
        "appVersion": "1.2.0",
        "at": datetime(2026, 8, 1, 12, 0, 0)
    }
    payload = ReviewPayload(**raw_data) # initialize ReviewPayload model with unpacked raw_data, testing field aliases and validators
    assert isinstance(payload.event_id, UUID) # testing auto-generation of event_id UUID
    assert payload.app_id == "my.com.gxbank.app" # testing field alias
    assert payload.app_name == "GX Bank" # testing field alias
    assert payload.user_name == "Alice Tan" # testing field alias
    assert payload.rating == 5 # testing field alias
    assert payload.submitted_at.tzinfo == timezone.utc # testing field validator on submitted_at datetime
    assert payload.ingested_at.tzinfo == timezone.utc # testing auto-generation of ingested_at timestamp

"""
    VALIDATION TEST
    tests ReviewPayload HTML cleaning on review_text field
    primarily tests field validator for review_text
"""
def test_review_payload_html_cleaning() -> None:
    payload = ReviewPayload( # using fixed values since this only tests review_text cleaning
        app_id="com.maybank2u.life",
        app_name="MAE",
        user_name="Bob",
        review_text="<b>Great app!</b> &amp; fast transfer.<br><br>Recommended!",
        rating=5,
        submitted_at=datetime.now(timezone.utc)
    )
    assert payload.review_text == "Great app! fast transfer. Recommended!" # expected output with HTML tags and entities stripped

"""
    INVALIDATION TEST
    tests ReviewPayload field value validation
    primarily tests field validators for rating
"""
def test_review_payload_invalid_rating() -> None:
    """tests that ratings outside 1-5 raise ValidationError"""
    with pytest.raises(ValidationError):
        ReviewPayload(
            app_id="com.maybank2u.life",
            app_name="MAE",
            user_name="Bob",
            review_text="Bad app",
            rating=6, # ts invalid, must be between 1-5
            submitted_at=datetime.now(timezone.utc)
        )

"""
    INVALIDATION TEST
    tests ReviewPayload field value validation
    primarily tests field validators for app_id
"""
def test_review_payload_invalid_app_id() -> None:
    """tests that invalid package names raise ValidationError"""
    with pytest.raises(ValidationError):
        ReviewPayload(
            app_id="invalid_app_name", # invalid, must start with com. or my.com.
            app_name="Test",
            user_name="User",
            review_text="Test review text",
            rating=4,
            submitted_at=datetime.now(timezone.utc)
        )

"""
    INVALIDATION TEST
    tests ReviewPayload field value validation
    primarily tests field validators for review_text
"""
def test_review_payload_empty_review_text() -> None:
    """tests that empty review text raises ValidationError"""
    with pytest.raises(ValidationError):
        ReviewPayload(
            app_id="com.maybank2u.life",
            app_name="MAE",
            user_name="Bob",
            review_text="", # ts invalid, must be at least 2 characters
            rating=4,
            submitted_at=datetime.now(timezone.utc)
        )

"""
    INVALIDATION TEST
    tests ReviewPayload extra fields forbidden configuration
    primarily tests extra='forbid' in model_config
"""
def test_review_payload_extra_fields_forbidden() -> None:
    """tests that passing extra unexpected fields raises ValidationError"""
    with pytest.raises(ValidationError):
        ReviewPayload(
            app_id="com.maybank2u.life",
            app_name="MAE",
            user_name="Bob",
            review_text="Great app!",
            rating=5,
            submitted_at=datetime.now(timezone.utc),
            unexpected_extra_field="malicious_or_unknown_data" # invalid, extra fields are forbidden
        )

"""
    IMMUTABILITY TEST
    tests ReviewPayload immutability
    primarily tests field immutability
"""
def test_review_payload_frozen_immutability() -> None:
    """tests that ReviewPayload instances cannot be mutated"""
    payload = ReviewPayload(
        app_id="com.maybank2u.life",
        app_name="MAE",
        user_name="Bob",
        review_text="Original text",
        rating=5,
        submitted_at=datetime.now(timezone.utc)
    )
    with pytest.raises(ValidationError):
        payload.rating = 4

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# FinancialNewsPayload test cases

"""
    VALIDATION TEST
    tests FinancialNewsPayload initialization of a valid marketaux scraper payload
    primarily tests field aliases (uuid), field validators (title/snippet cleaning), and auto-generation of event_id and ingested_at timestamps
"""
def test_financial_news_valid_from_scraper_dict() -> None:
    raw_data = { # synthetic marketaux news data
        "uuid": "marketaux-article-uuid-12345",
        "title": "Bank Negara Malaysia Issues New Digital Banking Framework",
        "snippet": "BNM today announced updated operational frameworks for all licensed digital banks...",
        "url": "https://www.thestar.com.my/business/2026/08/digital-banks",
        "source": "thestar.com.my",
        "sentiment_score": 0.45,
        "published_at": datetime(2026, 8, 1, 10, 30, 0, tzinfo=timezone.utc)
    }
    payload = FinancialNewsPayload(**raw_data) # initialize FinancialNewsPayload model with unpacked raw_data
    assert isinstance(payload.event_id, UUID) # testing auto-generation of event_id UUID
    assert payload.article_uuid == "marketaux-article-uuid-12345" # testing field alias
    assert payload.title == "Bank Negara Malaysia Issues New Digital Banking Framework"
    assert payload.source == "thestar.com.my"
    assert payload.sentiment_score == 0.45
    assert payload.published_at.tzinfo == timezone.utc
    assert payload.ingested_at.tzinfo == timezone.utc # testing auto-generation of ingested_at timestamp

"""
    VALIDATION TEST
    tests FinancialNewsPayload with optional sentiment_score as None
    primarily tests sentiment_score nullable field validation
"""
def test_financial_news_valid_none_sentiment() -> None:
    payload = FinancialNewsPayload(
        article_uuid="news-uuid-none-sentiment",
        title="Maybank Expands Cross-Border QR Payment Network",
        snippet="Maybank announced new bilateral QR payment integrations...",
        url="https://fintechnews.my/maybank-qr",
        source="fintechnews.my",
        sentiment_score=None, # None is explicitly allowed when sentiment analysis is unavailable, marketaux tend to do that
        published_at=datetime.now(timezone.utc)
    )
    assert payload.sentiment_score is None

"""
    VALIDATION TEST
    tests FinancialNewsPayload HTML cleaning on title and snippet fields
    primarily tests field validator for clean_news_text
"""
def test_financial_news_html_cleaning() -> None:
    payload = FinancialNewsPayload(
        article_uuid="news-html-clean-123",
        title="<h1>Digital Banks Surpass &lt;500k&gt; Users</h1>",
        snippet="<p>GX Bank and Boost Bank report <b>strong</b> customer growth in Q2.</p>",
        url="https://fintechnews.my/article",
        source="fintechnews.my",
        sentiment_score=0.8,
        published_at=datetime.now(timezone.utc)
    )
    assert payload.title == "Digital Banks Surpass 500k Users" # expected output with stripped HTML tags and entities
    assert payload.snippet == "GX Bank and Boost Bank report strong customer growth in Q2."

"""
    INVALIDATION TEST
    tests FinancialNewsPayload URL regex validation
    primarily tests url field pattern matching for standard http/https links
"""
def test_financial_news_invalid_url() -> None:
    """tests that non-http/https URLs raise ValidationError"""
    with pytest.raises(ValidationError):
        FinancialNewsPayload(
            article_uuid="news-123",
            title="Valid Title Here",
            snippet="Valid Snippet Here",
            url="ftp://invalid-protocol.com/file", # invalid, only http and https protocols are allowed
            source="thestar.com.my",
            sentiment_score=0.1,
            published_at=datetime.now(timezone.utc)
        )

"""
    INVALIDATION TEST
    tests FinancialNewsPayload sentiment_score boundary constraints
    primarily tests sentiment_score ge=-1.0 and le=1.0 validation
"""
def test_financial_news_invalid_sentiment_score() -> None:
    """tests that sentiment scores outside [-1.0, 1.0] raise ValidationError"""
    with pytest.raises(ValidationError):
        FinancialNewsPayload(
            article_uuid="news-123",
            title="Valid Title Here",
            snippet="Valid Snippet Here",
            url="https://thestar.com.my/news",
            source="thestar.com.my",
            sentiment_score=1.5, # invalid, sentiment score must be between -1.0 and 1.0
            published_at=datetime.now(timezone.utc)
        )

"""
    INVALIDATION TEST
    tests FinancialNewsPayload extra fields forbidden configuration
    primarily tests extra='forbid' in model_config
"""
def test_financial_news_extra_fields_forbidden() -> None:
    """tests that passing extra unexpected fields raises ValidationError"""
    with pytest.raises(ValidationError):
        FinancialNewsPayload(
            article_uuid="news-123",
            title="Valid Title Here",
            snippet="Valid Snippet Here",
            url="https://thestar.com.my/news",
            source="thestar.com.my",
            sentiment_score=0.2,
            published_at=datetime.now(timezone.utc),
            unexpected_extra_field="some_unknown_property" # invalid, extra fields are forbidden
        )

"""
    IMMUTABILITY TEST
    tests FinancialNewsPayload immutability
    primarily tests frozen=True in model_config
"""
def test_financial_news_frozen_immutability() -> None:
    """tests that FinancialNewsPayload instances cannot be mutated"""
    payload = FinancialNewsPayload(
        article_uuid="news-123",
        title="Valid Title Here",
        snippet="Valid Snippet Here",
        url="https://thestar.com.my/news",
        source="thestar.com.my",
        sentiment_score=0.2,
        published_at=datetime.now(timezone.utc)
    )
    with pytest.raises(ValidationError):
        payload.title = "Modified Title" # invalid, model is frozen

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass