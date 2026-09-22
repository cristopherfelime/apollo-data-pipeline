"""
    unit testing script for apollo schemas.py
    v1.0 - comprehensive validation, html cleaning, timezone standardization, immutability, and boundary tests for ReviewPayload and FinancialNewsPayload
    v1.1 - added comprehensive test suite for TransactionPayload covering Decimal serialization, ISO timestamp parsing, MCC code validation, and immutability
    v1.1.1 - turned mechant_mcc regex pattern validation unit test block docstring to raw string to avoid dumbahh terminal warning
    v1.2 - added boundary and invalidation tests for review_text length, news title and snippet limits, invalid timestamp string, non-UUID user_id, merchant_name max length, minimum valid amount_myr boundary, and is_flagged_fraud
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4
from decimal import Decimal
from pydantic import ValidationError

from apollo.schemas import ReviewPayload, FinancialNewsPayload, TransactionPayload

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
    BOUNDARY TEST
    tests ReviewPayload review_text max_length constraint
    primarily tests max_length=2000 validation
"""
def test_review_payload_review_text_max_length() -> None:
    """tests that review text exceeding 2000 characters raises ValidationError"""
    with pytest.raises(ValidationError):
        ReviewPayload(
            app_id="com.maybank2u.life",
            app_name="MAE",
            user_name="Bob",
            review_text="A" * 2001, # invalid, exceeds max 2000 characters
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
    BOUNDARY TEST
    tests FinancialNewsPayload title min_length and max_length constraints
    primarily tests min_length=5 and max_length=500 validations
"""
def test_financial_news_invalid_title_length() -> None:
    """tests that title under 5 characters or over 500 characters raises ValidationError"""
    # title under 5 characters
    with pytest.raises(ValidationError):
        FinancialNewsPayload(
            article_uuid="news-123",
            title="News", # min_length is 5
            snippet="Valid snippet here",
            url="https://thestar.com.my/news",
            source="thestar.com.my",
            sentiment_score=0.2,
            published_at=datetime.now(timezone.utc)
        )

    # title over 500 characters
    with pytest.raises(ValidationError):
        FinancialNewsPayload(
            article_uuid="news-123",
            title="A" * 501, # and max_length is 500
            snippet="Valid snippet here",
            url="https://thestar.com.my/news",
            source="thestar.com.my",
            sentiment_score=0.2,
            published_at=datetime.now(timezone.utc)
        )

"""
    BOUNDARY TEST
    tests FinancialNewsPayload snippet max_length constraint
    primarily tests max_length=2000 validation
"""
def test_financial_news_invalid_snippet_length() -> None:
    """tests that snippet over 2000 characters raises ValidationError"""
    with pytest.raises(ValidationError):
        FinancialNewsPayload(
            article_uuid="news-123",
            title="Valid Title Here",
            snippet="A" * 2001, # invalid, max_length is 2000
            url="https://thestar.com.my/news",
            source="thestar.com.my",
            sentiment_score=0.2,
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

# TransactionPayload test cases

""" 
    VALIDATION TEST
    tests TransactionPayload initialization with valid synthetic transaction data
    primarily tests default uuid generation, string ISO 8601 parsing, and auto-generation of transaction_id and ingested_at timestamps
"""
def test_transaction_payload_valid_from_dict() -> None:
    test_user_id = uuid4()
    raw_data = { # synthetic transaction data
        "timestamp": "2026-09-20T14:30:00Z",
        "transaction_method": "DUITNOW_QR",
        "amount_myr": Decimal("25.50"),
        "user_id": test_user_id,
        "merchant_name": "Village Park Restaurant",
        "merchant_mcc": "5812",
        "payment_status": "SUCCESS",
        "is_flagged_fraud": False
    }
    payload = TransactionPayload(**raw_data) # initialize TransactionPayload model with unpacked raw_data
    assert isinstance(payload.transaction_id, UUID) # testing auto-generation of transaction_id UUID
    assert payload.timestamp.tzinfo == timezone.utc # testing automatic ISO string conversion to UTC datetime
    assert payload.transaction_method == "DUITNOW_QR"
    assert payload.amount_myr == Decimal("25.50")
    assert payload.user_id == test_user_id
    assert payload.merchant_name == "Village Park Restaurant"
    assert payload.merchant_mcc == "5812"
    assert payload.payment_status == "SUCCESS"
    assert payload.is_flagged_fraud is False
    assert payload.ingested_at.tzinfo == timezone.utc # testing auto-generation of ingested_at timestamp

""" 
    SERIALIZATION TEST
    tests TransactionPayload PlainSerializer behavior on amount_myr during model_dump(mode='json')
    primarily tests that Decimal amounts are serialized to float instead of string for orjson compatibility downstream
"""
def test_transaction_payload_plain_serializer_json_dump() -> None:
    payload = TransactionPayload(
        timestamp=datetime.now(timezone.utc),
        transaction_method="FPX",
        amount_myr=Decimal("1250.75"),
        user_id=uuid4(),
        merchant_name="Shopee Malaysia",
        merchant_mcc="5311",
        payment_status="SUCCESS",
        is_flagged_fraud=False
    )
    dumped_json = payload.model_dump(mode="json")
    assert isinstance(dumped_json["amount_myr"], float) # PlainSerializer should serialize Decimal to float in JSON mode
    assert dumped_json["amount_myr"] == 1250.75

    # standard python mode should still retain Decimal
    dumped_python = payload.model_dump(mode="python")
    assert isinstance(dumped_python["amount_myr"], Decimal)

""" 
    VALIDATION TEST
    tests TransactionPayload timestamp conversion across various formats (naive datetime, timezone-aware datetime, and ISO strings)
    primarily tests field validator verify_and_convert_timestamp ensuring UTC standardization
"""
def test_transaction_payload_timestamp_conversions() -> None:
    # naive datetime gets converted to UTC
    naive_dt = datetime(2026, 9, 20, 10, 0, 0)
    payload_naive = TransactionPayload(
        timestamp=naive_dt,
        transaction_method="DEBIT_CARD",
        amount_myr=Decimal("15.00"),
        user_id=uuid4(),
        merchant_name="FamilyMart",
        merchant_mcc="5411",
        payment_status="SUCCESS",
        is_flagged_fraud=False
    )
    assert payload_naive.timestamp.tzinfo == timezone.utc
    assert payload_naive.timestamp.hour == 10

    # non-UTC timezone-aware datetime (+08:00 Malaysia time) gets converted to UTC
    kl_tz = timezone(timedelta(hours=8))
    aware_dt = datetime(2026, 9, 20, 16, 0, 0, tzinfo=kl_tz)
    payload_aware = TransactionPayload(
        timestamp=aware_dt,
        transaction_method="CREDIT_CARD",
        amount_myr=Decimal("88.00"),
        user_id=uuid4(),
        merchant_name="Uniqlo Mid Valley",
        merchant_mcc="5651",
        payment_status="SUCCESS",
        is_flagged_fraud=False
    )
    assert payload_aware.timestamp.tzinfo == timezone.utc
    assert payload_aware.timestamp.hour == 8 # 16:00 +08:00 is 08:00 UTC

    # ISO 8601 string with +00:00
    payload_iso = TransactionPayload(
        timestamp="2026-09-20T12:00:00+00:00",
        transaction_method="E_WALLET",
        amount_myr=Decimal("5.50"),
        user_id=uuid4(),
        merchant_name="Tealive",
        merchant_mcc="5814",
        payment_status="SUCCESS",
        is_flagged_fraud=False
    )
    assert payload_iso.timestamp.tzinfo == timezone.utc

""" 
    INVALIDATION TEST
    tests TransactionPayload amount_myr constraints
    primarily tests gt=Decimal('0.00') and decimal_places=2 validation
"""
def test_transaction_payload_invalid_amount() -> None:
    # invalid: zero amount
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("0.00"), # must be greater than 0.00
            user_id=uuid4(),
            merchant_name="Warung Kopi",
            merchant_mcc="5814",
            payment_status="SUCCESS",
            is_flagged_fraud=False
        )

    # invalid: negative amount
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("-10.50"), # must be greater than 0.00
            user_id=uuid4(),
            merchant_name="Warung Kopi",
            merchant_mcc="5814",
            payment_status="SUCCESS",
            is_flagged_fraud=False
        )

    # invalid: more than 2 decimal places
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("10.999"), # max 2 decimal places allowed
            user_id=uuid4(),
            merchant_name="Warung Kopi",
            merchant_mcc="5814",
            payment_status="SUCCESS",
            is_flagged_fraud=False
        )

"""
    VALIDATION TEST
    tests TransactionPayload amount_myr minimum valid boundary
    primarily tests gt=Decimal('0.00') acceptance of RM 0.01
"""
def test_transaction_payload_amount_minimum_boundary() -> None:
    payload = TransactionPayload(
        timestamp=datetime.now(timezone.utc),
        transaction_method="DUITNOW_QR",
        amount_myr=Decimal("0.01"), # minimum valid amount
        user_id=uuid4(),
        merchant_name="99 Speedmart",
        merchant_mcc="5411",
        payment_status="SUCCESS",
        is_flagged_fraud=False
    )
    assert payload.amount_myr == Decimal("0.01")

r""" (this needs to be a raw string or else there will be SyntaxWarning: invalid escape sequence '\d' warning in terminal)
    INVALIDATION TEST
    tests TransactionPayload merchant_mcc regex pattern validation
    primarily tests 4-digit code constraint (pattern=r'^\d{4}$')
"""
def test_transaction_payload_invalid_mcc() -> None:
    for bad_mcc in ["123", "12345", "abcd", "", "12a4"]:
        with pytest.raises(ValidationError):
            TransactionPayload(
                timestamp=datetime.now(timezone.utc),
                transaction_method="FPX",
                amount_myr=Decimal("50.00"),
                user_id=uuid4(),
                merchant_name="Test Merchant",
                merchant_mcc=bad_mcc, #  must be exactly 4 digits
                payment_status="SUCCESS",
                is_flagged_fraud=False
            )

""" 
    INVALIDATION TEST
    tests TransactionPayload Literal fields for transaction_method and payment_status
    primarily tests Literal string enum constraints
"""
def test_transaction_payload_invalid_literal_fields() -> None:
    # invalid transaction_method
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="BITCOIN", # not in Literal enum allowed payment rails
            amount_myr=Decimal("50.00"),
            user_id=uuid4(),
            merchant_name="Test Merchant",
            merchant_mcc="5411",
            payment_status="SUCCESS",
            is_flagged_fraud=False
        )

    # invalid payment_status
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("50.00"),
            user_id=uuid4(),
            merchant_name="Test Merchant",
            merchant_mcc="5411",
            payment_status="CANCELLED", # not in Literal enum allowed statuses
            is_flagged_fraud=False
        )

""" 
    INVALIDATION TEST
    tests TransactionPayload merchant_name min_length constraint
    primarily tests min_length=1 validation
"""
def test_transaction_payload_empty_merchant_name() -> None:
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("50.00"),
            user_id=uuid4(),
            merchant_name="", # min length is 1
            merchant_mcc="5411",
            payment_status="SUCCESS",
            is_flagged_fraud=False
        )

""" 
    INVALIDATION TEST
    tests TransactionPayload extra fields forbidden configuration
    primarily tests extra='forbid' in model_config
"""
def test_transaction_payload_extra_fields_forbidden() -> None:
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("50.00"),
            user_id=uuid4(),
            merchant_name="Speedmart 99",
            merchant_mcc="5411",
            payment_status="SUCCESS",
            is_flagged_fraud=False,
            unexpected_field="should_fail_immediately" # invalid, extra fields are forbidden
        )

""" 
    IMMUTABILITY TEST
    tests TransactionPayload immutability
    primarily tests frozen=True in model_config
"""
def test_transaction_payload_frozen_immutability() -> None:
    payload = TransactionPayload(
        timestamp=datetime.now(timezone.utc),
        transaction_method="DUITNOW_QR",
        amount_myr=Decimal("20.00"),
        user_id=uuid4(),
        merchant_name="KFC Malaysia",
        merchant_mcc="5814",
        payment_status="SUCCESS",
        is_flagged_fraud=False
    )
    with pytest.raises(ValidationError):
        payload.amount_myr = Decimal("30.00") # invalid, model is frozen

"""
    INVALIDATION TEST
    tests TransactionPayload timestamp validator on malformed date string
    primarily tests verify_and_convert_timestamp raising ValidationError
"""
def test_transaction_payload_invalid_timestamp() -> None:
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp="not-a-valid-timestamp", # invalid datetime string
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("50.00"),
            user_id=uuid4(),
            merchant_name="99 Speedmart",
            merchant_mcc="5411",
            payment_status="SUCCESS",
            is_flagged_fraud=False
        )

"""
    INVALIDATION TEST
    tests TransactionPayload user_id validation on invalid UUID string
    primarily tests UUID type validation
"""
def test_transaction_payload_invalid_user_id() -> None:
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("50.00"),
            user_id="invalid-uuid-string", # invalid UUID
            merchant_name="99 Speedmart",
            merchant_mcc="5411",
            payment_status="SUCCESS",
            is_flagged_fraud=False
        )

"""
    BOUNDARY TEST
    tests TransactionPayload merchant_name max_length boundary
    primarily tests max_length=500 constraint
"""
def test_transaction_payload_merchant_name_max_length() -> None:
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("50.00"),
            user_id=uuid4(),
            merchant_name="A" * 501, # exceeds max_length of 500
            merchant_mcc="5411",
            payment_status="SUCCESS",
            is_flagged_fraud=False
        )

"""
    INVALIDATION TEST
    tests TransactionPayload is_flagged_fraud validation on non-boolean values
    primarily tests bool type validation
"""
def test_transaction_payload_invalid_fraud_flag() -> None:
    with pytest.raises(ValidationError):
        TransactionPayload(
            timestamp=datetime.now(timezone.utc),
            transaction_method="DUITNOW_QR",
            amount_myr=Decimal("50.00"),
            user_id=uuid4(),
            merchant_name="99 Speedmart",
            merchant_mcc="5411",
            payment_status="SUCCESS",
            is_flagged_fraud="not_a_boolean" # invalid boolean
        )

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass