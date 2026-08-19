"""
    kafka consumer model
    v0.1
"""

import os
import logging
import asyncio
from asyncio import CancelledError
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)
load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

""" test
consumer = AIOKafkaConsumer(
    "app-reviews-events",
    "market-news-events",
    bootstrap_servers=f"{os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")}",
    group_id="apollo-db-persister",
    enable_auto_commit=False, # we control the offset manually (await consumer.commit() later after successful db push, adhering at-least once delivery)
    auto_offset_reset="earliest" # start reading from the beginning of the topic if no offset is found
)
"""


"""
    class docstring placeholder
"""
class ApolloKafkaConsumer:
    bootstrap_servers: str # target network address for consumer to read
    _consumer: AIOKafkaConsumer | None # the consumer instance
    _topics: tuple[str] # topic(s) to be read by the consumer, tuple for immutability
    _group_id: str # which consumer group id is 

    """
        method docstring placeholder
    """
    def __init__(self, bootstrap_servers: str=f"{os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")}", topics: tuple[str]=("app-reviews-events", "market-news-events"), group_id: str="apollo-db-persister") -> None:
        self.bootstrap_servers: str = bootstrap_servers
        self._topics: tuple[str] = topics
        self._group_id: str = group_id
        self._consumer: AIOKafkaConsumer | None = None
    
    """
        method docstring placeholder
    """
    def initialize(self) -> None:
        if (self._consumer is None) or (not isinstance(self._consumer, AIOKafkaConsumer)):
            self._consumer = AIOKafkaConsumer(
                *self._topics, # unpacking the topics list
                bootstrap_servers=self.bootstrap_servers,
                group_id=self._group_id,
                enable_auto_commit=False, # we control the offset manually (await consumer.commit() later after successful db push, adhering at-least once delivery)
                auto_offset_reset="earliest" # start reading from the beginning of the topic if no offset is found
            )
            logger.info(f"(Apollo) Kafka Consumer initialized successfully with bootstrap servers: {self.bootstrap_servers}")
    
    """
        method docstring placeholder
    """
    async def start(self) -> None:
        if (self._consumer is None) or (not isinstance(self._consumer, AIOKafkaConsumer)):
            try:
                self.initialize()
                await self._consumer.start()
                logger.info(f"(Apollo) Kafka Consumer started successfully with bootstrap servers: {self.bootstrap_servers}")
            except Exception as e:
                logger.error(f"(Apollo) Error while starting Kafka Consumer: {e}")
                self._consumer = None
        else:
            logger.info(f"(Apollo) Kafka Consumer already running!")

    """
        method docstring placeholder
    """
    async def stop(self) -> None:
        if (self._consumer is None) or (not isinstance(self._consumer, AIOKafkaConsumer)):
            logger.info(f"(Apollo) cannot stop Kafka Consumer as it is not running")
        else:
            try:
                await self._consumer.stop()
                self._consumer = None
                logger.info("(Apollo) Kafka Consumer stopped successfully")
            except Exception as e:
                logger.error(f"(Apollo) Error while stopping Kafka Consumer: {e}")
    
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