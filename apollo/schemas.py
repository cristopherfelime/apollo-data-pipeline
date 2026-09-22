"""
        pydantic base model schemas for google play reviews, marketaux rest api, and synthetic transaction logs
        v1.2.1 - changed ConfigDict() model config for both BaseModel parameter from 'extras' to 'extra' ☠️☠️ (thx pytest)
        v1.3 - added TransactionPayload model with UTC timestamp standardization, MCC pattern checking, and Decimal amount validation for synthetic transaction logs
        v1.3.1 - fixed wrong kafka topic label in TransactionPayload docstring, modified some field names in TransactionPayload, and found out about pydantic's automatic ISO 8601 string conversion so cool
        v1.3.2 - used PlainSerializer to change model_dump(mode="json") behavior from parsing Decimal to string to immediately cast it to float, check amount_myr again
        v1.3.3 - cleaned up TransactionPayload model_config (no aliases needed), updated outdated comments regarding native Pydantic v2 ISO 8601 parsing
"""

import re # re is used for regular expressions, which is used for cleaning review text down below (re.sub())
from uuid import UUID, uuid4 # uuid4 is used for auto-generating unique identifiers
from datetime import datetime, timezone # datetime is used for handling date and time, timezone is used for handling timezones
from pydantic import BaseModel, Field, ConfigDict, field_validator, PlainSerializer # pydantic base model and field for defining data models and validations, configdict for configuring the model, field_validator for validating fields. PlainSerializer overrides .model_dump() behavior basically, for example: model_dump(mode="json") behavior is changed by using PlainSerializer(when_used="json") instead of PlainSerializer() alone which changes .model_dump(mode="before") behavior
from typing import Annotated, Literal # annotated is used for adding metadata to types, in this case for adding constraints to the types (min_length, max_length, ge, le, etc), literal is basically for string enums
from decimal import Decimal # way more preferred than standard computer float when storing financial data

# some regex caching for maximum speed (no evaluation every re.sub)
# took the regex pattern straight up from Genesis' pipeline.py lol (which i took previously from internet)
# removes html tags like <br />, </br />, <a> </a>, etc
# also removes html entities like &lt;, &gt;, &amp;, etc
HTML_REGEX_CLEANER = re.compile(r"<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});")

# -------------------------------------------------------------------------------------------------------

"""
    pydantic validation model for google play store app reviews (target topic: app-reviews-events)
    attributes:
        event_id (UUID): unique identifier for the review event (auto-generated)
        app_id (str): application package identifier on google play store (alias: appId)
        app_name (str): human-readable application name (alias: title)
        user_name (str): reviewer user name on google play store (alias: userName)
        review_text (str): cleaned text content of the review (alias: content)
        rating (int): star rating between 1 and 5 (alias: score)
        app_version (str | None): application version at the time of review (alias: appVersion)
        submitted_at (datetime): timestamp of when review was posted in UTC (alias: at)
        ingested_at (datetime): UTC timestamp of when review was ingested into pipeline
    methods:
        clean_review_text -> field validator that cleans html tags and whitespace from review text before validation
        convert_datetime_submitted_at -> field validator that standardizes submitted_at datetime to UTC timezone
"""
# google play reviews validation model, target topic on kafka: app-reviews-events
class ReviewPayload(BaseModel): # a class inherits Pydantic's BaseModel to automatically get type checking, data validation, and other useful methods will be used downstream
    model_config = ConfigDict(
        populate_by_name=True, # allows the model to be populated by field names (alias)
        extra="forbid", # fails or raises ValidationError if there are extra unexpected fields in the data (like if the scraper unexpectedly scrapes a new data)
        frozen=True # makes the instance immutable for data safety
    )

    # review_id might be added soon if planning to run multiple scraper instances on same app to avoid duplicate reviews
    event_id: Annotated[UUID, Field(default_factory=uuid4)] # unique identifier for the review event (auto-generated)
    app_id: Annotated[str, Field(alias="appId", pattern=r"^(com|my\.com)\.[a-z0-9_]+(\.[a-z0-9_]+)*$")] # application identifier on google play store, example: com.nianticlabs.pokemongo from package doc
    
    # disclaimer: this one can be taken from app detail only, not reviews. but to minimize scraping time, we will not be taking the massive app detail payload just to get the app name and js use a fixed string later
    app_name: Annotated[str, Field(alias="title")] # human readable application name, example: Pokemon Go from above

    user_name: Annotated[str, Field(alias="userName")] # human readable username of the reviewer from google play store
    review_text: Annotated[str, Field(alias="content", min_length=2, max_length=2000)] # actual review text from reviewers, min 2 chars to max 2000 chars
    rating: Annotated[int, Field(alias="score", ge=1, le=5)] # google play review is rated between 1 to 5 stars
    app_version: Annotated[str | None, Field(alias="appVersion", default=None)] # app version at time of review, can be empty
    submitted_at: Annotated[datetime, Field(alias="at")] # when the review was submitted
    ingested_at: Annotated[datetime, Field(default_factory=lambda: datetime.now(timezone.utc))] # when the review was ingested into the data pipeline, leave it empty and default_factory will automatically produce the current datetime in utc from the time the model was initialized

    """
        field validator for review_text, mainly to clean them from unnecessary HTML tags before type validation
        arguments: cls (class itself), review_text (the review text)
        returns: cleaned review text
    """
    @field_validator("review_text", mode="before")
    @classmethod # classmethod is a method that belongs to the class and not to an instance of the class, like static method but can access class attributes
    def clean_review_text(cls, review_text: str) -> str: # type hinting the parameter as str
        if isinstance(review_text, str): # checking if the review text is a string
            stripped_text = HTML_REGEX_CLEANER.sub(" ", review_text) # using the regex compile from above, it will substitute any html tags and entities with a single space
            cleaned_text = " ".join(stripped_text.split()) # split the string by whitespace and join it back with single space in between each word to remove extra spaces
            return cleaned_text # finally return the cleaned review text
        return review_text # return the review text as is if it's not a string
    # note: i heard beautifulsoup can actually do this typa cleaning better and more robust (some malformed html tags like missing tags can break this one currently)
    #       use regex for now and if needed, will change to beautifulsoup in future

    """
        field validator for submitted_at, automatic conversion of datetime.datetime object in the JSON payload from scraper to utc
        arguments: cls (class itself), submitted_at (the datetime object)
        returns: converted datetime object
    """
    @field_validator("submitted_at", mode="before") # before cuz the payload already returns datetime.datetime object
    @classmethod
    def convert_datetime_submitted_at(cls, submitted_at: datetime) -> datetime: # type hinting the parameter as datetime
        if submitted_at.tzinfo is None: # if the datetime object is naive (no timezone info, most likely since the docs only say it returns something like datetime.datetime(2020, 12, 2, 16, 36, 39))
            return submitted_at.replace(tzinfo=timezone.utc) # replace with utc timezone
        return submitted_at.astimezone(timezone.utc) # convert to utc timezone if timezone info is already present
    
# -------------------------------------------------------------------------------------------------------

"""
    pydantic validation model for marketaux financial news articles (target topic: market-news-events)
    attributes:
        event_id (UUID): unique identifier for the news event (auto-generated)
        article_uuid (str): unique identifier for the news article from marketaux (alias: uuid)
        title (str): cleaned headline title of the financial news article
        snippet (str): cleaned summary snippet of the article
        url (str): valid http or https web url link to the original article source
        source (str): domain or publisher source name of the financial news article
        sentiment_score (float | None): sentiment polarity score between -1.0 and 1.0
        published_at (datetime): original article published timestamp in UTC
        ingested_at (datetime): UTC timestamp of when article was ingested into pipeline
    methods:
        clean_news_text -> field validator that cleans html tags and whitespace from article title and snippet before validation
"""
# marketaux api validation model, target topic on kafka: market-news-events
class FinancialNewsPayload(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True
    )

    event_id: Annotated[UUID, Field(default_factory=uuid4)]
    article_uuid: Annotated[str, Field(alias="uuid")] # specific article UUID
    title: Annotated[str, Field(min_length=5, max_length=500)] # title of article
    snippet: Annotated[str, Field(max_length=2000)] # summary of article, description field is also a good alternative for snippet, as we've implemented this in MarketauxScraper
    url: Annotated[str, Field(pattern=r"^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$")]
    source: Annotated[str, Field(description="financial news website source")]
    sentiment_score: Annotated[float | None, Field(ge=-1.0, le=1.0)] # news sentiment score is between -1 and 1, heard that sometimes its not provided so None is allowed
    published_at: Annotated[datetime, Field(description="exact UTC timestamp of when article was published")] # original article published timestamp, pydantic v2 natively parses ISO 8601 strings into datetime objects
    ingested_at: Annotated[datetime, Field(default_factory=lambda: datetime.now(timezone.utc))] # when the article news payload was ingested into data pipeline
    
    # might as well use the previos html tags cleaner regex again to clean up title and snippet as well
    """
        field validator for title and snippet, mainly to clean them from unnecessary HTML tags before type validation
        arguments: cls (class itself), news_text (the news text)
        returns: cleaned news text
    """
    @field_validator("title", "snippet", mode="before") # cleans both title and snippet
    @classmethod
    def clean_news_text(cls, news_text: str) -> str:
        if isinstance(news_text, str):
            stripped_text = HTML_REGEX_CLEANER.sub(" ", news_text)
            cleaned_text = " ".join(stripped_text.split())
            return cleaned_text
        return news_text

# -------------------------------------------------------------------------------------------------------

"""
    pydantic validation model for synthetic financial transaction logs (target topic: myr-transactions)
    attributes:
        transaction_id (UUID): unique identifier for the transaction event (auto-generated)
        timestamp (datetime): timestamp of when transaction was conducted in UTC
        transaction_method (str): payment method used (DUITNOW_QR, CREDIT_CARD, DEBIT_CARD, FPX, E_WALLET)
        amount_myr (Decimal): monetary transaction amount in MYR (minimum RM 0.01, serialized to float in json mode)
        user_id (UUID): unique identifier of the user who conducted the transaction
        merchant_name (str): name of the merchant or business entity
        merchant_mcc (str): 4-digit ISO 18245 merchant category code
        payment_status (str): transaction payment status as of logging (SUCCESS, FAILED, PENDING, REVERSED)
        ingested_at (datetime): UTC timestamp of when transaction was ingested into pipeline
        is_flagged_fraud (bool): preliminary boolean flag indicating if the transaction is flagged as fraud, important for Artemis
    methods:
        verify_and_convert_timestamp -> field validator that standardizes timestamp datetime to UTC timezone
"""
# Faker fake transaction payload validation model, look i dont have any expandable transaction log api source
class TransactionPayload(BaseModel):
    model_config = ConfigDict(
#       populate_by_name=True, (not needed, we're generating our own data for this one, not pulling from an API)
        extra="forbid",
        frozen=True
    )

    transaction_id: Annotated[UUID, Field(default_factory=uuid4)]
    timestamp: Annotated[datetime, Field(description="exact UTC timestamp of when transaction was conducted")] # yyeeee
    transaction_method: Annotated[Literal["DUITNOW_QR", "CREDIT_CARD", "DEBIT_CARD", "FPX", "E_WALLET"], Field(max_length=100, description="transaction method used")]
    amount_myr: Annotated[Decimal, Field(gt=Decimal("0.00"), decimal_places=2), PlainSerializer(lambda x: float(x), when_used="json")] # transaction amount in rm, plainserializer to ensure that when dumped using mode="json" it would be float and not str, to also avoid JSONEncodeError by orjson downstream
    user_id: Annotated[UUID, Field(description="user ID of the user who conducted the transaction type shi")]
    merchant_name: Annotated[str, Field(min_length=1, max_length=500, description="name of the merchant or business")]
    merchant_mcc: Annotated[str, Field(max_length=4, pattern=r"^\d{4}$", description="merchant category code")] # merchant category code has 4 digits 
    payment_status: Annotated[Literal["SUCCESS", "FAILED", "PENDING", "REVERSED"], Field(max_length=100)] # transaction payment status as of logging
    ingested_at: Annotated[datetime, Field(default_factory=lambda: datetime.now(timezone.utc))]
    is_flagged_fraud: Annotated[bool, Field(description="boolean flag indicating if the transaction is flagged as fraud (ts primarily for artemis later)")]

    """
        field validator for timestamp, standardizing datetime objects to UTC timezone
        arguments: cls (class itself), timestamp (datetime)
        returns: standardized UTC datetime object
    """
    @field_validator("timestamp", mode="after") # pydantic v2 automatically converts ISO 8601 formatted string into timezone-aware datetime object natively, so yeah this validator will just ensure that those datetime objects be timezone-aware after pydantic's validation
    @classmethod
    def verify_and_convert_timestamp(cls, timestamp: datetime) -> datetime:
        if isinstance(timestamp, datetime): # in case if its a datetime already
            if timestamp.tzinfo is None: # but no time zone info at all
                return timestamp.replace(tzinfo=timezone.utc) # replace with utc timezone
            return timestamp.astimezone(timezone.utc) # convert to utc timezone if timezone info is already present
        raise ValueError("timestamp must be a string formatted in datetime or a straight up datetime object")

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass