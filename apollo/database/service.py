"""
    postgres service engine/model whatever u wanna call it
    v0.1
"""

import os
import asyncio
import logging
import orjson
from asyncio import CancelledError
from dotenv import load_dotenv
from psycopg_pool import AsyncConnectionPool
from aiokafka.structs import ConsumerRecord
from typing import Any

logger = logging.getLogger(__name__)
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    the apollo postgres persister handler class
    attributes:
        min_size (int): minimum number of database connections retained in the connection pool
        max_size (int): maximum number of database connections allowed in the connection pool
        _pool (AsyncConnectionPool | None): async connection pool instance from psycopg_pool
    methods:
        __init__ -> initializes the persister handler with pool size boundaries
        _create_conninfo -> constructs the postgres connection URI string from environment variables
        initialize -> instantiates the AsyncConnectionPool instance without opening connections
        start -> asynchronously opens the postgres connection pool
        stop -> asynchronously closes the postgres connection pool
        __aenter__ -> enters the async context manager and starts the connection pool
        __aexit__ -> exits the async context manager and closes the connection pool
        parse_events -> deserializes raw Kafka ConsumerRecords into event dictionaries grouped by topic
        persist_events -> executes idempotent bulk SQL inserts into postgres staging tables
"""
class PostgresPersister:
    _pool: AsyncConnectionPool | None # async postgres connection pool
    min_size: int # minimum number of connections in the pool (default: 1)
    max_size: int # maximum number of connections in the pool (default: 10)

    """
        initializes apollo postgres persister handler class
        arguments: self, min_size (int): minimum pool connections (default: 1), max_size (int): maximum pool connections (default: 10)
        EXPECTED TO return: None
    """
    def __init__(self, min_size: int=1, max_size: int=10) -> None:
        self.min_size = min_size
        self.max_size = max_size
        self._pool = None
    
    """
        constructs postgres connection URI string from environment variables
        arguments: self
        EXPECTED TO return: str (formatted postgresql connection URI string)
    """
    def _create_conninfo(self) -> str:
        # creates the connection string, we aint storing this cuz db password
        return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    """
        initializes async postgres connection pool instance with configured boundaries
        arguments: self
        EXPECTED TO return: None
    """
    def initialize(self) -> None:
        if self._pool is None:
            try:
                self._pool = AsyncConnectionPool(
                    conninfo=self._create_conninfo(), # use the method above to create the connection string
                    min_size=self.min_size,
                    max_size=self.max_size,
                    open=False # just to make sure it doesn't open connections auto til open() is called down below in start()
                )
                logger.info(f"(Apollo) Postgres connection pool initialized successfully with min_size: {self.min_size}, max_size: {self.max_size}")
            except Exception as e:
                logger.error(f"(Apollo) Error while initializing postgres connection pool: {e}")
                self._pool = None

    """
        starts and opens the postgres connection pool to accept database client connections
        arguments: self
        EXPECTED TO return: None
    """
    async def start(self) -> None:
        if self._pool is None:
            self.initialize()
            try:
                await self._pool.open() # opens the connection pool, which means it can start accepting client connections
                logger.info(f"(Apollo) Postgres connection pool was opened successfully")
            except Exception as e:
                logger.error(f"(Apollo) Error while starting postgres connection pool: {e}")
                self._pool = None
        else:
            logger.info(f"(Apollo) Postgres connection pool is already running!")

    """
        stops and closes the postgres connection pool gracefully
        arguments: self
        EXPECTED TO return: None
    """
    async def stop(self) -> None:
        if self._pool is None:
            logger.info(f"(Apollo) cannot stop postgres connection pool as it is not running")
        else:
            try:
                await self._pool.close() # closes connection pool gracefully, no more connections
                self._pool = None
                logger.info(f"(Apollo) Postgres connection pool stopped successfully")
            except Exception as e:
                logger.error(f"(Apollo) Error while stopping postgres connection pool: {e}")
    
    """
        enters the asynchronous context, initializes and starts the postgres connection pool
        arguments: self
        EXPECTED TO return: the context manager instance (self)
    """
    async def __aenter__(self):
        await self.start()
        return self
    
    """
        exits the asynchronous context and gracefully closes the postgres connection pool
        arguments: self, exc_type (exception type, None if no exception), exc_val (exception value, None if no exception), exc_tb (traceback object, None if no exception)
        EXPECTED TO return: None
    """
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
    
    """
        deserializes raw Kafka ConsumerRecord byte payloads into a dictionary of event mappings grouped by topic
        arguments: self, list_events (list[ConsumerRecord]): raw message records pulled from Kafka consumer
        EXPECTED TO return: dict[str, list[dict[str, Any]]] (deserialized event payloads mapped by topic name)
    """
    # btw same like _prepare_payload not async cuz no need to interact with network i/o client stuff shi
    def parse_events(self, list_events: list[ConsumerRecord]) -> dict[str, list[dict[str, Any]]]: # so like {topic1: ({event_metadata1_1: event_data1_1}, {event_metadata1_2: event_data1_2}, ...), topic2: ({event_metadata2_1: event_data2_1}, {event_metadata2_2: event_data2_2}, ...), ...}
        try:
            """
                so they say uh:
                class aiokafka.structs.ConsumerRecord(
                    topic: 'str',
                    partition: 'int',
                    offset: 'int',
                    timestamp: 'int',
                    timestamp_type: 'int',
                    key: 'KT | None',
                    value: 'VT | None',
                    checksum: 'int | None',
                    serialized_key_size: 'int',
                    serialized_value_size: 'int',
                    headers: 'Sequence[tuple[str, bytes]]'
                )
                we serialized key anv value into bytes in ApolloKafkaProducer so i assume we also receive them in bytes here
                for now ts method just looks for topic and value
            """
            parse_result: dict[str, list[dict[str, Any]]] = {} # return result
            
            for event in list_events: # iterates over each event
                try:
                    if not event.value:
                        logger.warning(f"(Apollo) Found an empty event record, skipping")
                        continue
                    event_data: dict[str, Any] = orjson.loads(event.value) # so we deserialze each event, getting their metadata and data
                    parse_result.setdefault(event.topic, []).append(event_data) # appends it to its corresponding topic, creating a new key topic and its list if not exist, similar thing in ApolloKafkaProducer as well happened in _prepare_payload() go check it out
                except CancelledError:
                    logger.info(f"(Apollo) Postgres persister parse_events() inside consumer loop was running, then was stopped by the user (KeyboardInterrupt)")
                    raise
                except Exception as e:
                    logger.error(f"(Apollo) Error while Postgres persister was parsing an event record, skipping it: {e}")
                    continue
            return parse_result # and now we return the result
        except CancelledError:
            logger.info(f"(Apollo) Postgres persister parse_events() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except Exception as e:
            logger.error(f"(Apollo) Error while Postgres persister was parsing consumer records: {e}")
            return {}
    
    """
        asynchronously persists parsed event batches into PostgreSQL staging tables with idempotent conflict handling
        arguments: self, parsed_events (dict[str, list[dict[str, Any]]]): mapping of topic names to lists of event dictionaries
        EXPECTED TO return: bool (True if persistence succeeded, False if failed or empty)
    """
    async def persist_events(self, parsed_events: dict[str, list[dict[str, Any]]]) -> bool: # returns bool success signaling downstream (like preventing kafka consumer commit if it fails)
        try:
            if not parsed_events:
                logger.debug(f"(Apollo) No events to persist, skipping db persistence execution")
                return False
            if (self._pool is None) or (self._pool.closed):
                await self.start() # ensure pool is running
            async with self._pool.connection() as conn:
                async with conn.cursor() as cur:
                    # parametrized DML insertion statements stuff
                    sql_reviews: str = """
                        INSERT INTO staging_reviews(
                            event_id, 
                            app_id, 
                            app_name, 
                            user_name, 
                            review_text, 
                            rating, 
                            app_version, 
                            submitted_at, 
                            ingested_at
                        )
                        VALUES
                            (%(event_id)s, %(app_id)s, %(app_name)s, %(user_name)s, %(review_text)s, %(rating)s, %(app_version)s, %(submitted_at)s, %(ingested_at)s)
                        ON CONFLICT (event_id)
                        DO NOTHING;
                    """ # one cool thing, these parametrized insert on the values u can pass in the key names to get their values when you passed in a mapping (dict) in executemany, if not (order based on the positional values) you get index based position in tuple instead. look down below
                    sql_marketaux: str = """
                        INSERT INTO staging_marketaux(
                            event_id, 
                            article_uuid, 
                            title, 
                            snippet, 
                            url, 
                            source, 
                            sentiment_score, 
                            published_at, 
                            ingested_at
                        )
                        VALUES
                            (%(event_id)s, %(article_uuid)s, %(title)s, %(snippet)s, %(url)s, %(source)s, %(sentiment_score)s, %(published_at)s, %(ingested_at)s)
                        ON CONFLICT (event_id)
                        DO NOTHING;
                    """ # ON CONFLICT (event_id) DO NOTHING guarantees kafka's at-least-once delivery behavior, implementing idempotency so if a kafka commit fails, it may retry reinserting the same events, but since the on conflict statement it won't do anything (no creating duplicate entry nor throwing any error on the database side)
                    for topic, events in parsed_events.items():
                        if topic == "app-reviews-events": # atomic batching: postgres has this stuff where in an executemany if a single constraint or other error happens, that entire transaction will be aborted, if we put exception handling here and let the rest of the insertion to run (like for market-news-events), even though they're correct, they will not the committed to the database since they belong to the same aborted transaction, so it is better to not implement try-except at this level to prevent data loss
                            await cur.executemany(sql_reviews, events) # so, if i pass in the list of dicts (events) directly here, it will automatically map the values based on the keys i passed in the insertion statement
                            logger.info(f"(Apollo) Successfully persisted {len(events)} events from topic: {topic} into staging tables")
                        elif topic == "market-news-events":
                            await cur.executemany(sql_marketaux, events) # SSDD
                            logger.info(f"(Apollo) Successfully persisted {len(events)} events from topic: {topic} into staging tables")
                        else:
                            logger.warning(f"(Apollo) Unrecognized topic '{topic}', skipping its database insertion")
                    return True # ts success
        except CancelledError:
            logger.info(f"(Apollo) Postgres persister persist_events() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except Exception as e:
            logger.error(f"(Apollo) Error while Postgres persister was pushing to database: {e}")
            return False


if __name__ == "__main__":
    pass