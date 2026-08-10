"""
        abstract controller class for both scrapers to be used (play_store.py and marketaux.py)
        v1.2 - run() is now an abstract method to be overriden by the children
"""

from abc import ABC, abstractmethod # for implementing abstract classes and methods on python
from pydantic import BaseModel # just for expected return in process()
from typing import Any # just for expected return in fetch() and payload's type hint in process()

# ------------------------------------------------------------------------------------------------------------------------------

# abstract base controller class for both scrapers to use
class BaseScraper(ABC):
    """
        asynchronously fetches data using the given scraper, the target_id will be app_id (for play_store.py) or keywords (for marketaux.py) and count is the number of data to fetch
        arguments: target (the target or list of targets to scrape), count (number of data to fetch)
        EXPECTED TO return: list of Any (raw data fetched from the target, most of the time would be dict)
    """
    @abstractmethod
    async def fetch(self, target: str | list[str], count: int) -> list[Any] | list[list[Any]]:
        pass

    """
        asynchronously processes the raw data fetched from the target, the target_id will be app_id (for play_store.py) or keywords (for marketaux.py) and payload is the raw data fetched from the fetch() method
        arguments: payload (raw data)
        EXPECTED TO return: list of BaseModel (validated and cleaned data based on the schemas in schemas.py)
    """
    @abstractmethod
    async def process(self, payload: list[Any]) -> list[BaseModel]:
        pass

    """
        asynchronously run the entire fetching and processing as well as validation procedure using limited parameters and some default values
        arguments: count (number of data to fetch)
        EXPECTED TO return: list of BaseModel (ready to be streamed to kafka)
    """
    @abstractmethod
    async def run(self, count: int) -> list[BaseModel]:
        pass