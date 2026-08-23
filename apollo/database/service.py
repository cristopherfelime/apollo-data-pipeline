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
    class docstring placeholder
"""
class PostgresPersister:
    _pool: AsyncConnectionPool | None # async postgres connection pool
    min_size: int # minimum number of connections in the pool (default: 1)
    max_size: int # maximum number of connections in the pool (default: 10)

    """
        method docstring placeholder
    """
    def __init__(self, min_size: int=1, max_size: int=10) -> None:
        self.min_size = min_size
        self.max_size = max_size
        self._pool = None
    
    """
        method docstring placeholder
    """
    def _create_conninfo(self) -> str:
        # creates the connection string, we aint storing this cuz db password
        return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    """
        method docstring placeholder
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
        method docstring placeholder
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
        method docstring placeholder
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
        method docstring placeholder
    """
    async def __aenter__(self):
        await self.start()
        return self
    
    """
        method docstring placeholder
    """
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
    
    """
        method docstring placeholder
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
                    logger.info(f"(Apollo) Postgres persister parse_consumer() inside consumer loop was running, then was stopped by the user (KeyboardInterrupt)")
                    raise
                except Exception as e:
                    logger.error(f"(Apollo) Error while Postgres persister was parsing an event record, skipping it: {e}")
                    continue
            return parse_result # and now we return the result
        except CancelledError:
            logger.info(f"(Apollo) Postgres persister parse_consumer() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except Exception as e:
            logger.error(f"(Apollo) Error while Postgres persister was parsing consumer records: {e}")
            return {}
    
    """
        method docstring placeholder
    """
    async def persist_events(self, parsed_events: dict[str, list[dict[str, Any]]]) -> None:
        try:
            pass
        except CancelledError:
            logger.info(f"(Apollo) Postgres persister persist_events() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except Exception as e:
            logger.error(f"(Apollo) Error while Postgres persister was pushing to database: {e}")


if __name__ == "__main__":
    pass