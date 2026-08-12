"""
		main entry point and orchestrator for apollo
		v0.1 - unfinished, testing core orchestration first
"""

import logging
import asyncio
from asyncio import CancelledError
import os
from dotenv import load_dotenv
from typing import Sequence

from apollo.scrapers.play_store import PlayStoreScraper
from apollo.scrapers.marketaux import MarketauxScraper
from apollo.scrapers.base import BaseScraper

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

        scrapers: Sequence[BaseScraper] = [ # future scrapers planning to be added can be put into this list, make sure it inherits BaseScraper tho
            PlayStoreScraper(app_dict=playstore_app_dict),
            MarketauxScraper(params=marketaux_params, search_targets=marketaux_targets)
        ]
        results = await asyncio.gather(*[scraper.run(count=count) for scraper in scrapers], return_exceptions=True) # polymorphically run the run() method for each scraper in the sequence

        print("Result of results: ") # test
        print(results)
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