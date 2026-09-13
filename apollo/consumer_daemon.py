"""
    kafka consumer daemon process for apollo (orchestrator for consumer)
    v1.0 - completed consumer orchestrator integrating ApolloKafkaConsumer with PostgresPersister, cooperative graceful shutdown (SIGINT/SIGTERM), rate-limiting backoff, and at-least-once offset commitment
    v1.1 - added unparseable batch or offset acknowledgement guard to avoid infinite looping and broker downtime backoff
"""

import logging
import asyncio
import os
import signal # for handling SIGTERM (docker stop), SIGINT (ctrl + C) or other stuff
import sys # to check the os running ts
from asyncio import CancelledError
from typing import Any, Callable
from aiokafka.structs import ConsumerRecord

from apollo.kafka.consumer import ApolloKafkaConsumer
from apollo.database.service import PostgresPersister

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    main orchestrator function (consumer and persisting side)
    listens to Kafka topics, persists event batches idempotently to PostgreSQL staging tables, and acknowledges consumer offsets
"""
async def main() -> None:
    loop = asyncio.get_running_loop() # get the current running asyncio event loop
    shutdown_event = asyncio.Event() # creates an event to be set when the consumer daemon needs to shut down

    """
        handles shutdown signals to gracefully stop the consumer daemon (look at that, a nested function)
        arguments: sig (signal type)
        EXPECTED TO return: None
    """
    def shutdown_callback(sig: signal.Signals) -> None:
        logger.info(f"(Apollo) received signal {sig}, gracefully stopping the consumer daemon after finishing ts batch..")
        shutdown_event.set()

    # setup signal handlers for graceful shutdown, we attach the signal handles to the active event loop, and it will run when other tasks are completed. this way python doesnt instantly kill the daemon in the middle of a persisting process or such
    for sig in (signal.SIGTERM, signal.SIGINT): # common meaning of these signals are above
        try:
            if sys.platform != 'win32': # yeah, add_signal_handler() is not supported in Windows
                loop.add_signal_handler(sig, shutdown_callback, sig)
            else:
                logger.warning("(Apollo) add_signal_handler() not supported in Windows")
        except Exception as e:
            logger.error(f"(Apollo) Error while setting signal handler for signal {sig}: {e}")

    try:
        async with ApolloKafkaConsumer() as consumer, PostgresPersister() as persister:
            while not shutdown_event.is_set(): # will exit when shutdown_event is set
                raw_records: list[ConsumerRecord] = await consumer.get_batch()
                if not raw_records:
                    if consumer._consumer is None: # checks if kafka consumer instance is dead
                        logger.warning("(Apollo) Kafka consumer instance is not alive, the broker is most likely unavailable. Retrying in a few secs...")
                        await asyncio.sleep(3)
                    continue # early exit if get_batch() is empty, to not waste resources (and also avoid unnecessary commits)

                parsed_records: dict[str, list[dict[str, Any]]] = persister.parse_events(raw_records)
                if not parsed_records: # avoiding indefinite loop if parse_events() caught an unexpected exception or all records in batch were malformed, returning {}
                    logger.warning("(Apollo) All records in batch were malformed or unparseable, committing offsets to avoid infinite looping..")
                    await consumer.commit()
                    continue

                if await persister.persist_events(parsed_records): # at-least-once delivery, where only commit kafka consuemr offset after database persisting succeed
                    await consumer.commit()
                else:
                    logger.error("(Apollo) Failed to persist events, skipping Kafka consumer offset commit for retry")
                    await asyncio.sleep(2) # wait 2 secs before retrying to not overload the db, a simple rate limiting could change things!
                        
    except CancelledError:
        logger.info("(Apollo) consumer daemon was running, then was stopped by the user (KeyboardInterrupt)")
        return
    except Exception as e:
        logger.error(f"(Apollo) consumer daemon unexpected error: {e}")
        return
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                if sys.platform != 'win32':
                    loop.remove_signal_handler(sig)
                else:
                    logger.warning("(Apollo) remove_signal_handler not supported in Windows")
            except Exception as e:
                logger.error(f"(Apollo) Error while removing signal handler for signal {sig}: {e}")
        logger.info("(Apollo) consumer daemon was shutdown gracefully")

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Apollo (consumer daemon) was stopped by the user (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Apollo (consumer daemon) unexpectedly failed to run: {e}")