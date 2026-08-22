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


if __name__ == "__main__":
    pass