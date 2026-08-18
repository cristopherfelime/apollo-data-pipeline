"""
        pydantic base model schemas for google play reviews and marketaux rest api
        v1.2.1 - changed ConfigDict() model config for both BaseModel parameter from 'extras' to 'extra' ☠️☠️ (thx pytest)
"""

from uuid import UUID, uuid4 # uuid4 is used for auto-generating unique identifiers
from datetime import datetime, timezone # datetime is used for handling date and time, timezone is used for handling timezones
import re # re is used for regular expressions, which is used for cleaning review text down below (re.sub())
from pydantic import BaseModel, Field, ConfigDict, field_validator # pydantic base model and field for defining data models and validations, configdict for configuring the model, field_validator for validating fields
from typing import Annotated # annotated is used for adding metadata to types, in this case for adding constraints to the types (min_length, max_length, ge, le, etc)

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
        clean_review_text -> cleans html tags and whitespace from review text before validation
        convert_datetime_submitted_at -> standardizes submitted_at datetime to UTC timezone
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
        clean_news_text -> cleans html tags and whitespace from article title and snippet before validation
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
    published_at: Annotated[datetime, Field(description="exact UTC timestamp of when article was published")] # original article published timestamp. the payload returns a string, so there's automatic coversion field validation below
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