/*
 postgres database initialization script
 v0.1
 */

-- table schema for staging queue of play store reviews
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
    CONSTRAINT chk_review_text_length CHECK ( -- following Field(alias="content", min_length=2, max_length=2000)
        (LENGTH(review_text) >= 2) AND (LENGTH(review_text) <= 2000)
    ),
    CONSTRAINT chk_rating_range CHECK (
        (rating >= 1) AND (rating <= 5) -- following Field(alias="score", ge=1, le=5)
    )
);

-- table schema for staging queue of marketaux reviews
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
    CONSTRAINT chk_snippet_length CHECK ( -- following Field(max_length=2000)
        LENGTH(snippet) <= 2000
    ),
    CONSTRAINT chk_sentiment_score_range CHECK ( -- following Field(ge=-1.0, le=1.0)
        (sentiment_score >= -1.0) AND (sentiment_score <= 1.0)
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