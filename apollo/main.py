"""
		main entry point and orchestrator for apollo
		v1.0 - completed end-to-end async orchestration, polymorphic scraper execution, OCP (partition_key, event_dict) tuple streaming to ApolloKafkaProducer achieving full SoC, and centralized logging configuration
"""

import logging
import asyncio
from asyncio import CancelledError
import os
import itertools # just for flattening the results list
from dotenv import load_dotenv
from typing import Sequence

from apollo.scrapers.play_store import PlayStoreScraper
from apollo.scrapers.marketaux import MarketauxScraper
from apollo.scrapers.base import BaseScraper
from apollo.schemas import ReviewPayload, FinancialNewsPayload
from apollo.kafka.producer import ApolloKafkaProducer

logger = logging.getLogger(__name__)
load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    main orchestrator function
    executes all registered scrapers polymorphically, classifies scraped payloads into Kafka topic batches, and streams events to ApolloKafkaProducer
"""
async def main() -> None:
    try:
        # some arguments for the individual scrapers
        playstore_app_dict: dict[str, str] = {
                        "my.com.gxbank.app": "GX Bank",
                        "my.com.tngdigital.ewallet": "TNG eWallet",
                        "com.maybank2u.life": "MAE by Maybank2u",
                        "my.com.myboost": "Boost"
                        }
        marketaux_params: dict[str, str] = {
            "api_token": os.getenv("MARKETAUX_TOKEN"),
            "limit": 3,
            "language": "en",
            "country": "my"
        }
        marketaux_targets: list[str] = ["Maybank", "Boost Bank", "GXBank Malaysia", "TNG eWallet"]
        count: int = 1
        events: dict[str, list[tuple[str | None, dict]]] = {
            "app-reviews-events": [],
            "market-news-events": [],
        } # a dict of lists to store kafka topics and their respective (partition_key, event_dict) tuple list

        scrapers: Sequence[BaseScraper] = [ # future scrapers planning to be added can be put into this list, make sure it inherits BaseScraper tho
            PlayStoreScraper(app_dict=playstore_app_dict),
            MarketauxScraper(params=marketaux_params, search_targets=marketaux_targets)
        ]
        results = await asyncio.gather(*[scraper.run(count=count) for scraper in scrapers], return_exceptions=True) # polymorphically run the run() method for each scraper in the sequence, allowing exceptions will ensure one exception will not cause every other scrape to fail and stop, maximizes throughput and exceptions will be handled later

        for scraper_output in results: # check if any scraper fails, continuation of asyncio.gather()'s return_exceptions=True explanation above
            if isinstance(scraper_output, Exception): # if that scraper failed, log it as a warning and skip processing it
                logger.warning(f"(Apollo) main(), a scraper failed to scrape: {scraper_output}")
                continue
            # otherwise process the scraped data
            for event in scraper_output: # iterate over the scraped data from the current scraper
                if isinstance(event, ReviewPayload): # reviews goes to reviews_events
                    events["app-reviews-events"].append((event.app_id, event.model_dump())) # tuples of (partition_key, event_dict) are collected for generic open-closed principle (OCP) kafka payload batching
                elif isinstance(event, FinancialNewsPayload): # news goes to news_events
                    events["market-news-events"].append((event.source, event.model_dump())) # for future reference: {topic1: [(pk1, pk1event1), (pk1, pk1event2), (pk2, pk2event1), (pk2, pk2event2)], topic2: [ ... ]}
                else: # unexpected type handling
                    logger.warning(f"(Apollo) main() unexpectedly received '({type(event)})' from 'results' resulting in skipping the following: {event}")

        async with ApolloKafkaProducer() as producer:
            producer_results = await producer.run(events, return_results=True)
        
        logger.info(f"(Apollo) Successfully processed play store reviews and marketaux news, and sent all data to Kafka, entire operation was successful:\n {producer_results}")

        # print("Result of results: ") # test
        # print(results)
    except CancelledError:
        logger.info("(Apollo) main() was running, then was stopped by the user (KeyboardInterrupt)")
        # no raise, this main() is the upper most method that catches the CancelledError
        return
    except Exception as e:
        logger.error(f"(Apollo) main() unexpected error while running Apollo: {e}")
        return

if __name__ == "__main__":

    # setting the logger config for all instances within apollo will be done here, since main.py is the main entry point
    logging.basicConfig(
        level=logging.INFO, # set log severity threshold, the lowest to higher is: debug (10) -> info (20) -> warning (30) -> error (40) -> critical (50), any message with severity lower than this threshold will be ignored
        format="%(asctime)s [%(levelname)s] %(message)s" # logging format, will include timestamp, log severity level, and the message
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Apollo was stopped by the user (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Apollo failed to run: {e}")