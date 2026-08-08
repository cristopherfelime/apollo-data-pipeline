"""
        abstract controller class for both scrapers to be used (play_store.py and marketaux.py)
        v1.1 - parameters of the abstract methods are modified, to accomodate for MarketauxScraper more
"""

from abc import ABC, abstractmethod # for implementing abstract classes and methods on python
from pydantic import BaseModel # just for expected return in process()
from typing import Any # just for expected return in fetch() and payload's type hint in process()

# ------------------------------------------------------------------------------------------------------------------------------

# abstract base controller class for both scrapers to use
class BaseScraper(ABC):
    """
        asynchronously fetches data using the given scraper, the target_id will be app_id (for play_store.py) or keywords (for marketaux.py) and count is the number of data to fetch
        arguments: target (the target to scrape), count (number of data to fetch)
        EXPECTED TO return: list of Any (raw data fetched from the target, most of the time would be dict)
    """
    @abstractmethod
    async def fetch(self, target: str, count: int) -> list[Any]:
        pass

    """
        asynchronously processes the raw data fetched from the target, the target_id will be app_id (for play_store.py) or keywords (for marketaux.py) and payload is the raw data fetched from the fetch() method
        arguments: payload (raw data)
        EXPECTED TO return: list of BaseModel (validated and cleaned data based on the schemas in schemas.py)
    """
    @abstractmethod
    async def process(self, payload: list[Any]) -> list[BaseModel]:
        pass