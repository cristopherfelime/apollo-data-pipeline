"""
    kafka consumer daemon process for apollo (orchestrator for consumer)
    v0.1
"""

import logging
import asyncio
import os

from apollo.kafka.consumer import ApolloKafkaConsumer
from apollo.database.service import PostgresPersister

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    main orchestrator function (consumer and persisting side)
    (tba)
"""
async def main() -> None:
    pass

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