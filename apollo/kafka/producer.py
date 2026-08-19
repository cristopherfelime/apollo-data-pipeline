"""
		kafka producer model
		v1.2.1 - added import asyncio to fix missing import in run()...
        v1.2.1.1 - start() now checks if _producer is an instance of AIOKafkaProducer or not
"""

import logging
import os
import orjson # super fast rust written replacement for json
import asyncio # like how did i miss this
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer # already imports asyncio in under the hood
from aiokafka.errors import KafkaError
from aiokafka.structs import RecordMetadata
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
        _prepare_payload -> batches and groups events per topic and partition key, encoding with orjson
        send_event -> sends an individual event to a kafka topic with broker ACK (send_and_wait) and returns RecordMetadata
        run -> concurrently streams all topic event batches to kafka using asyncio.gather
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
        if (self._producer is None) or (not isinstance(self._producer, AIOKafkaProducer)): # checks if the producer is not initialized
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
        prepares and batches events grouped by topic and partition key into serialized byte arrays using orjson
        arguments: self, events (dict mapping topic names to lists of (partition_key, event_dict) tuples)
        EXPECTED TO return: nested dict of topic -> {partition_key_bytes: list[event_bytes]} (or empty dict on failure)
    """
    def _prepare_payload(self, events: dict[str, list[tuple[str | None, dict]]]) -> dict[str, dict[bytes, list[bytes]]] | None: # protected only intended to be used in send_events() in the future as well
        try:
            if (not isinstance(events, dict)):
                raise ValueError(f"(Apollo) expected events to be a dict, but got {type(events).__name__}")
            payload: dict[str, dict[bytes, list[bytes]]] = {} # {topic1: {partition_key1: [event1_in_bytes, event2_in_bytes], partition_key2: [event1_in_bytes, ...], ...}, topic2: {...}, ...}
            for topic, events_list in events.items():
                try:
                    per_topic: dict[bytes, list[bytes]] = {} # {partition_key1: [event1_in_bytes, event2_in_bytes], partition_key2: [event1_in_bytes, ...], ...} per specific topic 
                    for key, event in events_list: # unpacking the partition key and event from the tuple we appened in main.py
                        try:
                            if (key is None) or (not isinstance(key, str)) or (key.strip() == ""): # NGAHHHHH
                                key = "DLQ" # dead letter queue, place for events with a malformed partition key (the PIT)
                            key = key.lower().strip().encode("utf-8") # cleaning the key then encode it to bytes
                            byte_event: bytes = orjson.dumps(event) # orjson dumps the dict to byte directly unlike json which dumps to (how convenient)
                            per_topic.setdefault(key, []).append(byte_event) # setdefault() will return the value for key if key is in the dictionary, if not, it will insert key with a value of default (in this case it is []) and return that
                        except CancelledError:
                            logger.info("(Apollo) Kafka Producer _prepare_payload() was running, then was stopped by the user (KeyboardInterrupt)")
                            raise
                        except Exception as e:
                            logger.error(f"(Apollo) Error while preparing an event for payload for Kafka, resulting in skipping the event: {e}")
                            continue # continue to next event
                    payload.update({topic: per_topic}) # add the processed topic to the payload
                except CancelledError:
                    logger.info("(Apollo) Kafka Producer _prepare_payload() was running, then was stopped by the user (KeyboardInterrupt)")
                    raise
                except Exception as e:
                    logger.error(f"(Apollo) Error while preparing a topic in payload for Kafka, resulting in skipping topic {topic}: {e}")
                    continue
            logger.debug(f"(Apollo) Payload prepared for Kafka: {payload}") # debug log showing the per-partition-key payload
            return payload
        except CancelledError:
            logger.info("(Apollo) Kafka Producer _prepare_payload() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except Exception as e:
            logger.error(f"(Apollo) Error while preparing payload for Kafka: {e}")
            return {} # if error, then return empty dict

    """
        asynchronously sends a single event to a target kafka topic and waits for broker delivery acknowledgement
        arguments: self, topic (str), value (bytes | dict), key (str | bytes | None, default None)
        EXPECTED TO return: RecordMetadata representing delivery confirmation (or None on failure)
    """
    async def send_event(self, topic: str, value: bytes | dict, key: str | bytes | None=None) -> RecordMetadata | None: # returns RecordMetadata on success
        try:
            opened_locally: bool = self._producer is None
            if opened_locally:
                await self.start()

            record_metadata: RecordMetadata = await self._producer.send_and_wait(topic=topic, value=value, key=key) # sends event and waits for broker acknowledgement (ACK)
            logger.debug(f"(Apollo) Event delivered to Kafka topic '{topic}' [partition {record_metadata.partition}, offset {record_metadata.offset}]") # debug log with partition/offset metadata
            return record_metadata # return delivery metadata on success
        except CancelledError:
            logger.info("(Apollo) Kafka Producer send_event() was running, then was stopped by the user (KeyboardInterrupt)")
            raise
        except KafkaError as e: # catch aiokafka broker network and delivery exceptions
            logger.error(f"(Apollo) Kafka broker delivery error while streaming to topic '{topic}': {e}")
            return None # return None so run() knows this event failed
        except Exception as e:
            logger.error(f"(Apollo) Error while sending event to Kafka: {e}")
            return None # if error, then return None
        finally:
            if opened_locally:
                await self.stop()

    """
        concurrently streams all topic event batches to kafka using asyncio.gather
        arguments: self, events (dict mapping topic names to lists of (partition_key, event_dict) tuples), return_results (bool, default False)
        EXPECTED TO return: dict mapping topic names to counts of successfully sent events (or None if return_results is False)
    """
    async def run(self, events: dict[str, list[tuple[str | None, dict]]], return_results: bool=False) -> dict[str, int] | None:
        try:
            opened_locally: bool = self._producer is None
            try:
                if opened_locally:
                    await self.start()

                payload: dict[str, dict[bytes, list[bytes]]] | None = self._prepare_payload(events) # prepare the kafka payload
                
                if not payload:
                    logger.warning("(Apollo) Payload is empty, nothing to stream to Kafka")
                    return None

                if return_results: # if user wants to know how many events were sent per topic
                    successes: dict[str, int] = dict.fromkeys(payload.keys(), 0) if payload else {} # success counts per topic: {"topic": 0} if payload exists, else empty dict
                
                for topic, partition_key_dict in payload.items(): # separate gather tasks by topics
                    try:
                        tasks = [
                            self.send_event(topic, event, partition_key)
                            for partition_key, event_list in partition_key_dict.items()
                            for event in event_list
                        ]
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                    except CancelledError:
                        logger.info("(Apollo) Kafka Producer run() was running, then was stopped by the user (KeyboardInterrupt)")
                        raise
                    except Exception as e:
                        logger.error(f"(Apollo) Error while streaming topic {topic}: {e}")
                        continue # continue to next topic
                    
                    if return_results:
                        successes[topic] = sum(1 for result in results if result and not isinstance(result, Exception)) # basically same as increment by 1 for all non-None and non-Exception results

                return successes if return_results else None

                """ what a monolithic nightmare i keep forgetting gather() exists, the inner try-except also redundant
                if return_results: # if user wants to know how many events were sent per topic
                    successes: dict[str: int] = dict.fromkeys(payload.keys(), 0) if payload else {} # success counts per topic: {"topic": 0} if payload exists, else empty dict
                    if payload: # if payload is not None
                        for topic, partition_key_dict in payload.items(): # loop through payload's topics
                            try:
                                for partition_key, event_list in partition_key_dict.items(): # and loop through events per partition key
                                    try:
                                        for event in event_list: # and loop through individual events in that batch I LOVE O(I * J * K) COMPLETXITY !!
                                            try:
                                                result = await self.send_event(topic, event, partition_key) # send individual event to kafka
                                                if return_results: # if user wants to know how many events were sent per topic
                                                    if result: # if send_event() return non-None value, it means send() was successful
                                                        successes[topic] += 1 # add the number of events sent in that batch to the success count
                                                    except CancelledError:
                                                        logger.info("(Apollo) Kafka Producer run() was running, then was stopped by the user (KeyboardInterrupt)")
                                                        raise
                                                    except Exception as e:
                                                        logger.error(f"(Apollo) Error while streaming an event from {topic} with key = ('{partition_key.decode('utf-8')}') to Kafka, skipping that event: {e}")
                                                        continue # continue to next event
                                            except CancelledError:
                                                logger.info("(Apollo) Kafka Producer run() was running, then was stopped by the user (KeyboardInterrupt)")
                                                raise
                                            except Exception as e:
                                                logger.error(f"(Apollo) Error while streaming events with key = ('{partition_key.decode('utf-8')}') from {topic} to Kafka, skipping that key: {e}")
                                                continue
                                    except CancelledError:
                                        logger.info("(Apollo) Kafka Producer run() was running, then was stopped by the user (KeyboardInterrupt)")
                                        raise
                                    except Exception as e:
                                        logger.error(f"(Apollo) Error while streaming entire topic '{topic}' to Kafka, skipping topic: {e}")
                                        continue
                    else:
                        logger.warning("(Apollo) Payload is empty, nothing to stream to Kafka")
                        return successes
                """
                
            except Exception as e:
                logger.error(f"(Apollo) ApolloKafkaProducer error while running run(): {e}")
                return dict.fromkeys(payload.keys(), 0) if payload else {}
            finally:
                if opened_locally:
                    await self.stop()
        except CancelledError:
            logger.info("(Apollo) Kafka Producer run() was running, then was stopped by the user (KeyboardInterrupt)")
            await self.stop()
            raise # raising cancelled error again, propagating it to main()
        except Exception as e:
            logger.error(f"(Apollo) Unexpected error in run(): {e}")
            await self.stop()
            return {} # empty dict since unexpected error


if __name__ == "__main__":
    pass
            