/*
 postgres database initialization script
 v1.0
 */

-- table schema for staging queue of play store reviews
/*
    event_id: Annotated[UUID, Field(default_factory=uuid4)]
    app_id: Annotated[str, Field(alias="appId", pattern=r"^(com|my\.com)\.[a-z0-9_]+(\.[a-z0-9_]+)*$")]
    app_name: Annotated[str, Field(alias="title")]
    user_name: Annotated[str, Field(alias="userName")]
    review_text: Annotated[str, Field(alias="content", min_length=2, max_length=2000)]
    rating: Annotated[int, Field(alias="score", ge=1, le=5)]
    app_version: Annotated[str | None, Field(alias="appVersion", default=None)]
    submitted_at: Annotated[datetime, Field(alias="at")]
    ingested_at: Annotated[datetime, Field(default_factory=lambda: datetime.now(timezone.utc))]
*/
CREATE TABLE IF NOT EXISTS staging_reviews (
    event_id UUID PRIMARY KEY,
    app_id VARCHAR(255) NOT NULL DEFAULT 'com.unknown',
    app_name VARCHAR(255) NOT NULL DEFAULT 'Unknown App',
    user_name VARCHAR(255) NOT NULL,
    review_text TEXT NOT NULL,
    rating INT NOT NULL,
    app_version VARCHAR(50), -- pydantic basemodel also allows it to be None
    submitted_at TIMESTAMPTZ NOT NULL, -- TIMESTAMPTZ standardizes to UTC i heard, so better than TIMESTAMP here
    ingested_at TIMESTAMPTZ NOT NULL, -- time when data was ingested (scraped to be exact)
    is_cleaned BOOLEAN NOT NULL DEFAULT FALSE, -- this one is particularly for artemis later
    is_embedded BOOLEAN NOT NULL DEFAULT FALSE, -- same as above
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- time when this message was pushed to the db
    
    -- constraints
    CONSTRAINT chk_app_id_regex CHECK ( -- following Field(alias="appId", pattern=r"^(com|my\.com)\.[a-z0-9_]+(\.[a-z0-9_]+)*$")
        app_id ~ '^(com|my\.com)\.[a-z0-9_]+(\.[a-z0-9_]+)*$'
    ),
    CONSTRAINT chk_review_text_length CHECK ( -- following Field(alias="content", min_length=2, max_length=2000)
        (LENGTH(review_text) >= 2) AND (LENGTH(review_text) <= 2000)
    ),
    CONSTRAINT chk_rating_range CHECK (
        (rating >= 1) AND (rating <= 5) -- following Field(alias="score", ge=1, le=5)
    )
);

-- table schema for staging queue of marketaux reviews
/*
    event_id: Annotated[UUID, Field(default_factory=uuid4)]
    article_uuid: Annotated[str, Field(alias="uuid")]
    title: Annotated[str, Field(min_length=5, max_length=500)]
    snippet: Annotated[str, Field(max_length=2000)]
    url: Annotated[str, Field(pattern=r"^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$")]
    source: Annotated[str, Field(description="financial news website source")]
    sentiment_score: Annotated[float | None, Field(ge=-1.0, le=1.0)]
    published_at: Annotated[datetime, Field(description="exact UTC timestamp of when article was published")]
    ingested_at: Annotated[datetime, Field(default_factory=lambda: datetime.now(timezone.utc))]
*/
CREATE TABLE IF NOT EXISTS staging_marketaux (
    event_id UUID PRIMARY KEY,
    article_uuid VARCHAR(255) NOT NULL, -- in schemas.py pydantic base model type hint expected str not UUID, so it's also safer to use VARCHAR(255) here
    title TEXT NOT NULL,
    snippet TEXT NOT NULL,
    url VARCHAR(2048) NOT NULL,
    source VARCHAR(100) NOT NULL,
    sentiment_score REAL, -- REAL is 4-byte precision floating number unlike FLOAT that is 8-byte, so ts more efficient, and also sentiment_score can be None in schemas.py
    -- the rest of these are pretty much similar toabove
    published_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    is_cleaned BOOLEAN NOT NULL DEFAULT FALSE,
    is_embedded BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- constraints
    CONSTRAINT chk_title_length CHECK ( -- following Field(min_length=5, max_length=500)
        (LENGTH(title) >= 5) AND (LENGTH(title) <= 500)
    ),
    CONSTRAINT chk_url_regex CHECK ( -- following Field(pattern=r"^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$")
        url ~ '^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$'
    ),
    CONSTRAINT chk_snippet_length CHECK ( -- following Field(max_length=2000)
        LENGTH(snippet) <= 2000
    ),
    CONSTRAINT chk_sentiment_score_range CHECK ( -- following Field(ge=-1.0, le=1.0)
        (sentiment_score >= -1.0) AND (sentiment_score <= 1.0)
    )
);

-- table schema for staging queue of transactions
/*
    transaction_id: Annotated[UUID, Field(default_factory=uuid4)]
    timestamp: Annotated[datetime, Field(description="exact UTC timestamp of when transaction was conducted")]
    transaction_method: Annotated[Literal["DUITNOW_QR", "CREDIT_CARD", "DEBIT_CARD", "FPX", "E_WALLET"], Field(max_length=100, description="transaction method used")]
    amount_myr: Annotated[Decimal, Field(gt=Decimal("0.00"), decimal_places=2), PlainSerializer(lambda x: float(x), when_used="json")]
    user_id: Annotated[UUID, Field(description="user ID of the user who conducted the transaction type shi")]
    merchant_name: Annotated[str, Field(min_length=1, max_length=500, description="name of the merchant or business")]
    merchant_mcc: Annotated[str, Field(max_length=4, pattern=r"^\d{4}$", description="merchant category code")]
    payment_status: Annotated[Literal["SUCCESS", "FAILED", "PENDING", "REVERSED"], Field(max_length=100)]
    ingested_at: Annotated[datetime, Field(default_factory=lambda: datetime.now(timezone.utc))]
    is_flagged_fraud: Annotated[bool, Field(description="boolean flag indicating if the transaction is flagged as fraud (ts primarily for artemis later)")]
*/
CREATE TABLE IF NOT EXISTS staging_transactions (
    transaction_id UUID PRIMARY KEY,
    "timestamp" TIMESTAMPTZ NOT NULL, -- the word timestamp is a reserved keyword in postgres, so yeah
    transaction_method VARCHAR(100) NOT NULL,
    amount_myr DECIMAL(20, 2) NOT NULL, -- up to 20 precision, 2 decimal places
    "user_id" UUID NOT NULL, -- similar thing to timestamp above
    merchant_name VARCHAR(500) NOT NULL, -- max length of merchant name is 500
    merchant_mcc VARCHAR(4) NOT NULL, -- mcc is 4 digits
    payment_status VARCHAR(100) NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    is_flagged_fraud BOOLEAN NOT NULL,
    /* these two artemis flag might not actually be used for transactions as itll not be used for training later on, keeping in jic
    is_cleaned BOOLEAN NOT NULL DEFAULT FALSE,
    is_embedded BOOLEAN NOT NULL DEFAULT FALSE,
    */
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- constraints
    CONSTRAINT chk_transaction_method_enum CHECK ( -- following Literal["DUITNOW_QR", "CREDIT_CARD", "DEBIT_CARD", "FPX", "E_WALLET"]
        transaction_method IN ('DUITNOW_QR', 'CREDIT_CARD', 'DEBIT_CARD', 'FPX', 'E_WALLET')
    ),
    CONSTRAINT chk_transaction_amount_range CHECK ( -- following Field(gt=Decimal("0.00")
        amount_myr > 0.00
    ),
    CONSTRAINT chk_merchant_name_length CHECK ( -- following Field(min_length=1, max_length=500)
        (LENGTH(merchant_name) >= 1) AND (LENGTH(merchant_name) <= 500)
    ),
    CONSTRAINT chk_mcc_pattern CHECK ( -- following Field(pattern=r"^\d{4}$")
        merchant_mcc ~ '^\d{4}$'
    ),
    CONSTRAINT chk_payment_status_enum CHECK ( -- following Literal["SUCCESS", "FAILED", "PENDING", "REVERSED"]
        payment_status IN ('SUCCESS', 'FAILED', 'PENDING', 'REVERSED')
    )
);

-- indexes for fast lookup

-- for data cleaning workers in artemis
-- partial index for finding uncleaned play store reviews 
CREATE INDEX IF NOT EXISTS idx_staging_reviews_uncleaned -- partial indexing on created_at for only uncleaned records, so that artemis only scans the needed record for cleaning (tbd as well)
ON staging_reviews(created_at)
WHERE is_cleaned = FALSE;

-- partial index for finding uncleaned marketaux news
CREATE INDEX IF NOT EXISTS idx_staging_marketaux_uncleaned -- same purpose as above
ON staging_marketaux(created_at)
WHERE is_cleaned = FALSE;

-- for text embedding/vectorizer workers in artemis
-- partial index for finding cleaned but not yet embedded play store reviews 
CREATE INDEX IF NOT EXISTS idx_staging_reviews_unembedded -- another partial indexing
ON staging_reviews(created_at)
WHERE (is_cleaned = TRUE) AND (is_embedded = FALSE);

-- partial index for finding cleaned but not yet embedded marketaux news
CREATE INDEX IF NOT EXISTS idx_staging_marketaux_unembedded
ON staging_marketaux(created_at)
WHERE (is_cleaned = TRUE) AND (is_embedded = FALSE);

-- for fraud detection and alert monitoring in artemis
-- partial index for instantly querying flagged fraudulent transactions without indexing 99.5% clean records and in artemis querying the transaction logs will always be sorted descending by timestamp as well
CREATE INDEX IF NOT EXISTS idx_staging_transactions_fraud
ON staging_transactions("timestamp" DESC)
WHERE is_flagged_fraud = TRUE;

-- for user behavioral profiling and transaction history lookups
-- indexes for fast lookups of user's transaction history
CREATE INDEX IF NOT EXISTS idx_staging_transactions_user_history
ON staging_transactions("user_id", "timestamp" DESC)
