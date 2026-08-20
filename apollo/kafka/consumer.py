"""
    kafka consumer model
    v1.0
"""

import os
import logging
import asyncio
from asyncio import CancelledError
from dotenv import load_dotenv
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord

logger = logging.getLogger(__name__)
load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    the apollo kafka consumer handler class
    attributes:
        bootstrap_servers (str): target network address for kafka bootstrap servers
        _consumer (AIOKafkaConsumer | None): async kafka consumer client instance from aiokafka
        _topics (tuple[str, ...]): tuple of kafka topics subscribed by the consumer
        _group_id (str): consumer group identifier for offset tracking and partition coordination
    methods:
        __init__ -> initializes the consumer handler with bootstrap servers, subscribed topics, and group id
        initialize -> instantiates the AIOKafkaConsumer instance
        start -> asynchronously starts the kafka consumer connection
        stop -> asynchronously stops the kafka consumer connection
        __aenter__ -> enters the async context manager and starts the consumer
        __aexit__ -> exits the async context manager and stops the consumer
        get_batch -> pulls a batch of messages from subscribed topics up to max_records or until timeout_ms
        commit -> manually commits offsets for processed messages adhering to at-least-once delivery
"""
class ApolloKafkaConsumer:
    bootstrap_servers: str # target network address for consumer to read
    _consumer: AIOKafkaConsumer | None # the consumer instance
    _topics: tuple[str, ...] # topic(s) to be read by the consumer, tuple for immutability
    _group_id: str # which consumer group id is 

    """
        initializes apollo kafka consumer handler class
        arguments: self, bootstrap_servers (str): network address for kafka broker (default: {os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")}), topics (tuple[str, ...]): tuple of topic names to subscribe to (default: ("app-reviews-events", "market-news-events")), group_id (str): consumer group identifier (default: "apollo-db-persister")
        EXPECTED TO return: None
    """
    def __init__(self, bootstrap_servers: str=f"{os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")}", topics: tuple[str, ...]=("app-reviews-events", "market-news-events"), group_id: str="apollo-db-persister") -> None:
        self.bootstrap_servers: str = bootstrap_servers
        self._topics: tuple[str, ...] = topics
        self._group_id: str = group_id
        self._consumer: AIOKafkaConsumer | None = None
    
    """
        initializes kafka consumer instance of the handler class
        arguments: self
        EXPECTED TO return: None
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
        starts kafka consumer instance and joins the consumer group
        arguments: self
        EXPECTED TO return: None
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
        stops kafka consumer instance and gracefully leaves the consumer group
        arguments: self
        EXPECTED TO return: None
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
        enters the asynchronous context, initializes and starts the kafka consumer instance
        arguments: self
        EXPECTED TO return: the context manager instance (self)
    """
    async def __aenter__(self):
        await self.start()
        return self
    
    """
        exits the asynchronous context and gracefully stops the kafka consumer connection
        arguments: self, exc_type (exception type, None if no exception), exc_val (exception value, None if no exception), exc_tb (traceback object, None if no exception)
        EXPECTED TO return: None
    """
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    """
        asynchronously fetches a batch of messages from subscribed topics up to max_records or until timeout_ms expires
        arguments: self, max_records (int): maximum records to pull (default: 100), timeout_ms (int): maximum wait time in milliseconds (default: 1000)
        EXPECTED TO return: list of ConsumerRecord (flattened list of pulled Kafka messages)
    """
    async def get_batch(self, max_records: int=100, timeout_ms: int=1000) -> list[ConsumerRecord]: # timeout is 1 second with max records of 100, our scrapers scrapes little-by-little (around 16 events per run approx), so ts may cause slight delay for smaller scrapes like ts but not a problem when we increase the size (if my marketaux api token is enough that is)
        try:
            if (self._consumer is None) or (not isinstance(self._consumer, AIOKafkaConsumer)): # unlike kafka producer nature that accepts one-shot sends, consumers needs to be running to be able to consume as a group, we're also manually committing and kafka will not need to rebalance consumer groups
                await self.start()

            return_batch: list[ConsumerRecord] = []
            batch = await self._consumer.getmany(max_records=max_records, timeout_ms=timeout_ms) # get many returns dict[TopicPartition, list[ConsumerRecord]]
            for topic_partition, messages in batch.items(): # TopicPartition, list[ConsumerRecord]
                try:
                    return_batch.extend(messages) # extend adds all messages (ConsumerRecord) in the list to the return batch
                except CancelledError:
                    logger.info(f"(Apollo) Kafka Consumer get_batch() message loop was running, then was stopped by the user (KeyboardInterrupt)")
                    raise
                except Exception as e:
                    logger.error(f"(Apollo) Error while processing a partition key group messages in get_batch(), skipping key group: {e}")
                    continue
            logger.info(f"(Apollo) Kafka Consumer successfully batched {len(return_batch)} messages")
            return return_batch # return the list of ConsumerRecord objects
        except CancelledError:
            logger.info(f"(Apollo) Kafka Consumer get_batch() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except Exception as e:
            logger.error(f"(Apollo) Error while getting batch from Kafka Consumer: {e}")
            return []

    """
        asynchronously commits offsets for all messages pulled so far to ensure at-least-once delivery
        arguments: self
        EXPECTED TO return: None
    """
    async def commit(self) -> None:
        try:
            if (self._consumer is None) or (not isinstance(self._consumer, AIOKafkaConsumer)):
                raise Exception("Kafka Consumer is not even initialized")
            await self._consumer.commit() # committing the offset manually (as we disabled auto commit in constructor above)
            logger.info("(Apollo) Kafka Consumer offset committed successfully")
        except CancelledError:
            logger.info("(Apollo) Kafka Consumer commit() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except Exception as e:
            logger.error(f"(Apollo) Error while committing offset from Kafka Consumer: {e}")
            return


if __name__ == "__main__":
    pass