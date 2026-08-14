"""
		main entry point and orchestrator for apollo
		v0.2 - gonna pass list[dict] instead of list[byte] directly to ApolloKafkaProducer, needed for taking partitioning keys
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

logger = logging.getLogger(__name__)
load_dotenv()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    placeholder
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
        reviews_events: list[dict] = [] # a list of dict to store the reviews events
        news_events: list[dict] = [] # a list of dict to store the news events

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
                    reviews_events.append(event.model_dump()) # model_dump() will dump the BaseModel to python dictionary, but kafka only thinks in bytestreams, so we will encode to utf-8 in ApolloKafkaProducer.send_events()
                elif isinstance(event, FinancialNewsPayload): # news goes to news_events
                    news_events.append(event.model_dump())
                else: # unexpected type handling
                    logger.warning(f"(Apollo) main() unexpectedly received '({type(event)})' from 'results' resulting in skipping the following: {event}")

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Apollo was stopped by the user (KeyboardInterrupt)")
    except Exception as e:
        logger.error(f"Apollo failed to run: {e}")