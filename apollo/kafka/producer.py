"""
        kafka producer model
        v0.7 - finished core stuff, still needs to implement a method following OCP for streaming to kafka and fix the sequential network bottleneck
"""

import logging
import os
import orjson # super fast rust written replacement for json
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer # already imports asyncio in under the hood
from asyncio import CancelledError

logger = logging.getLogger(__name__)
load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    the apollo kafka producer handler class
    attributes:
        bootstrap_servers (str): target network address for kafka bootstrap servers
        _producer (AIOKafkaProducer | None): async kafka producer client instance from aiokafka
    methods:
        __init__ -> initializes the handler with target bootstrap servers
        initialize -> instantiates the AIOKafkaProducer instance
        start -> asynchronously starts the kafka producer connection
        stop -> asynchronously stops the kafka producer connection
        __aenter__ -> enters the async context manager and starts the producer
        __aexit__ -> exits the async context manager and stops the producer
        _prepare_payload -> batches and groups events by partition key and encodes them with orjson
        send_events -> streams review and news event payloads into their respective kafka topics
"""
class ApolloKafkaProducer:
    bootstrap_servers: str # target netword address for producer
    _producer: AIOKafkaProducer | None # the producer instance from aiokafka itself

    """
        initializes apollo kafka producer handler class
        arguments: self, bootstrap_servers (str): network address for producer (default: {os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")})
        EXPECTED TO return: None
    """
    def __init__(self, bootstrap_servers: str=f"{os.getenv("KAFKA_HOST")}:{os.getenv("KAFKA_PORT")}") -> None: # bootstrap_servers defaults to network host and port within .env file
        self.bootstrap_servers: str = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None
    
    """
        initializes kafka producer instance of the handler class
        arguments: self
        EXPECTED TO return: None
    """
    def initialize(self) -> None:
        if (self._producer is None) or (not isinstance(self._producer, AIOKafkaProducer)): # checks if producer is not initialized or not an AIOKafkaProducer instance
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers
            )
            logger.info(f"(Apollo) Kafka Producer initialized successfully with bootstrap servers: {self.bootstrap_servers}")

    """
        starts kafka producer instance
        arguments: self
        EXPECTED TO return: None
    """
    async def start(self) -> None:
        if self._producer is None: # checks if the producer is not initialized
            try: # attempt to initialize the producer instance and start them
                self.initialize()
                await self._producer.start()
                logger.info(f"(Apollo) Kafka Producer started successfully with bootstrap servers: {self.bootstrap_servers}")
            except Exception as e:
                logger.error(f"(Apollo) Error while starting Kafka Producer: {e}")
                self._producer = None # if error, set producer back to None to allow retries
        else:
            logger.info(f"(Apollo) Kafka Producer already running!")
    
    """
        stops kafka producer instance
        arguments: self
        EXPECTED TO return: None
    """
    async def stop(self) -> None:
        if self._producer is None: # checks if producer is not initialized
            logger.info("(Apollo) cannot stop Kafka Producer as it is not running")
        else:
            try:
                await self._producer.stop() # stops the producer gracefully
                self._producer = None # set producer back to None
                logger.info("(Apollo) Kafka Producer stopped successfully")
            except Exception as e:
                logger.error(f"(Apollo) Error while stopping Kafka Producer: {e}")

    """
        enters the asynchronous context, initializes and starts the kafka producer instance
        arguments: self
        EXPECTED TO return: the context manager instance (self)
    """
    async def __aenter__(self):
        await self.start()
        return self

    """
        exits the asynchronous context and gracefully stops the kafka producer connection
        arguments: self, exc_type (exception type, None if no exception), exc_val (exception value, None if no exception), exc_tb (traceback object, None if no exception)
        EXPECTED TO return: None
    """
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    """
        prepares and batches events grouped by partition key into serialized byte arrays using orjson
        arguments: self, events (list of python dict representing individual events)
        EXPECTED TO return: dict of partition key bytes mapped to list of event byte strings (or None on failure)
    """
    def _prepare_payload(self, events: list[dict]) -> dict[bytes, list[bytes]] | None: # protected only intended to be used in send_events() in the future as well
        try:
            if (not isinstance(events, list)):
                raise ValueError(f"(Apollo) expected events to be a list, but got {type(events).__name__}")
            payload: dict[bytes, list[bytes]] = {} # {partition_key1: [event1_in_bytes, event2_in_bytes], partition_key2: [event1_in_bytes, ...], ...}
            for event in events:
                try:
                    key: str | None = event.get("app_id") or event.get("source") # so basically checks if it is a play store review or marketaux news, and if it fails to get the partition key of either topics, it will return None (use get() instead of direct key access to avoid KeyError)
                    if (key is None) or (not isinstance(key, str)) or (key.strip() == ""): # NGAHHHHH
                        key = "DLQ" # dead letter queue, place for events with a malformed partition key (the PIT)
                    key = key.lower().strip().encode("utf-8") # cleaning the key then encode it to bytes
                    byte_event: bytes = orjson.dumps(event) # orjson dumps the dict to byte directly unlike json which dumps to (how convenient)
                    payload.setdefault(key, []).append(byte_event) # setdefault() will return the value for key if key is in the dictionary, if not, it will insert key with a value of default (in this case it is []) and return that
                except CancelledError:
                    logger.info("(Apollo) Kafka Producer _prepare_payload() was running, then was stopped by the user (KeyboardInterrupt)")
                    raise
                except Exception as e:
                    logger.error(f"(Apollo) Error while preparing an event for payload for Kafka, resulting in skipping the event: {e}")
                    continue # continue to next event
            return payload
        except CancelledError:
            logger.info("(Apollo) Kafka Producer _prepare_payload() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except Exception as e:
            logger.error(f"(Apollo) Error while preparing payload for Kafka: {e}")
            return {} # if error, then return empty dict

    """
        streams review and news event payloads into their respective kafka topics with delivery acknowledgement
        arguments: self, reviews_events (list of dict for review events), news_events (list of dict for news events)
        EXPECTED TO return: list of int containing counts of successfully delivered events [reviews_count, news_count]
    """
    async def send_events(self, reviews_events: list[dict], news_events: list[dict]) -> list[int]:
        try:
            opened_locally: bool = self._producer is None
            try:
                if opened_locally:
                    await self.start()

                reviews_payload: dict[bytes, list[bytes]] | None = self._prepare_payload(reviews_events) # prepare review events payload
                news_payload: dict[bytes, list[bytes]] | None = self._prepare_payload(news_events) # prepare news events payload
                successes_list: list[int] = [0, 0] # success counts: [reviews_events_count, news_events_count]
                if reviews_payload: # process reviews first
                    for partition_key, event_list in reviews_payload.items(): # loop through reviews_payload
                        try:
                            for event in event_list: # and loop through individual events in each partition key group
                                await self._producer.send_and_wait("app-reviews-events", value=event, key=partition_key) # send_and_wait() sends the event and waits for it to be sent successfully, if any broker error happens it will trigger an Exception that will be caught
                                successes_list[0] += 1 # increments success list for reviews
                        except CancelledError:
                            logger.info("(Apollo) Kafka Producer send_events() (while sending reviews_events) was running, then was stopped by the user (KeyboardInterrupt)")
                            raise
                        except Exception as e:
                            logger.error(f"(Apollo) Error while streaming reviews_events to Kafka, skipping event: {e}")
                            continue # continue to next event
                if news_payload: # then process news
                    for partition_key, event_list in news_payload.items(): # basically similar to above
                        try:
                            for event in event_list:
                                await self._producer.send_and_wait("market-news-events", value=event, key=partition_key)
                                successes_list[1] += 1
                        except CancelledError:
                            logger.info("(Apollo) Kafka Producer send_events() (while sending news_events) was running, then was stopped by the user (KeyboardInterrupt)")
                            raise
                        except Exception as e:
                            logger.error(f"(Apollo) Error while streaming news_events to Kafka, skipping event: {e}")
                            continue # continue to next event
                return successes_list
            except Exception as e:
                logger.error(f"(Apollo) ApolloKafkaProducer error while running send_events(): {e}")
                return [0, 0]
            finally:
                if opened_locally:
                    await self.stop()
        except CancelledError:
            logger.info("(Apollo) Kafka Producer send_events() was running, then was stopped by the user (KeyboardInterrupt)")
            await self.stop()
            raise # raising cancelled error again, propagating it to main()
        except Exception as e:
            logger.error(f"(Apollo) Unexpected error in send_events(): {e}")
            await self.stop()
            return [0, 0] # if error, then return 0 events sent


if __name__ == "__main__":
    pass
            