"""
        kafka producer model
        v0.1
"""

import logging
import os
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer # already imports asyncio in under the hood

logger = logging.getLogger(__name__)
load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    class docstring placeholder
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


if __name__ == "__main__":
    pass
            