"""
        google play reviews scraper
        v1.1 - switch from function based to object based to support multiple, independent scraping instances
"""

import logging # logging purposes
import asyncio # python's asynchronois programming library
from google_play_scraper import reviews # google play scraper library we'll be using
from pydantic import ValidationError # for catching validation errors
import itertools # for flattening the list of lists

from apollo.schemas import ReviewPayload # to validate the data
from apollo.scrapers.base import BaseScraper # to inherit the abstract class

# initialize logger
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------------------------------------

# inherits BaseScraper: abstract attribute required -> data (list[BaseModel])
class PlayStoreScraper(BaseScraper):
    app_dict: dict = {}

    def __init__(self, app_dict: dict):
        self.app_dict = app_dict

    def add_app_dict(self, app_id: str, app_name: str) -> None:
        """
            adds an app to the scraper
            arguments: self, app_id (the app id of the app to be scraped), app_name (the name of the app)
            EXPECTED TO return: None
        """
        self.app_dict[app_id] = app_name
    
    def get_app_dict(self) -> dict:
        """
            returns the app_dict
            arguments: self
            EXPECTED TO return: dict (the app_dict)
        """
        return self.app_dict
    
    def remove_app(self, app_id: str) -> None:
        """
            removes an app from the scraper
            arguments: self, app_id (the app id of the app to be removed)
            EXPECTED TO return: None
        """
        del self.app_dict[app_id]

    """
        OVERRIDES fetch() FROM BaseScraper
        fetch reviews from google play store for a specific app, google_play_scraper was written with synchronous blocking, so this will ofload it to an async function using asyncio.to_thread instead
        js realized this can also be used as a standalone fetcher outside of the class, make a static version later maybe when necessary?
        arguments: self, target_id (the target to scrape), count (number of data to fetch), lang (language of the reviews to fetch, default is ms or malaysian), country (country of the reviews to fetch, default is my or malaysian) idk why malaysia needs ms for lang and my for country but Ok
        EXPECTED TO return: list of dict (raw review data scraped by the package)
    """
    async def fetch(self, target_id: str, count: int, lang: str, country: str) -> list[dict]:
        try:
            result, _ = await asyncio.to_thread( # runs the sync function in a separate thread using the thread pool
                reviews,
                target_id,
                count=count,
                lang=lang,
                country=country
            )
            return result
        except Exception as e:
            logger.error(f"Error fetching reviews for app {target_id}: {e}")
            return [] # returns empty list so itertools.chain down below dont break

    """
        OVERRIDES process() FROM BaseScraper
        process and validates each reviews, correctly mapping app name and id to the reviews, using pydantic to ensure and validate the data
        same as above ngl, can make the static version, this version is mainly for batch processing in run() method
        arguments: self, target_id (app id of where the review responses come from), raw_api_responses (list of raw review data scraped by the package)
        EXPECTED TO return: list of ReviewPayload (validated and cleaned data based on the schemas in schemas.py)
    """
    async def process(self, target_id: str, raw_api_responses: list[dict]) -> list[ReviewPayload]:
        processed_reviews = [] # to store all the processed reviews from all the raw api responses for the final return
        for review in raw_api_responses: # iterate through each of the raw api responses for the current app id
            review.update({"app_name": self.app_dict[target_id], "app_id": target_id}) # update the raw review data to include the app name and app id, matching the schema in ReviewPayload
            try: # try to validate each reviews
                validated_review = ReviewPayload.model_validate(review)
                processed_reviews.append(validated_review) # if there's no ValidationError raised by pydantic, then append the validated review to processed_reviews
            except ValidationError as e: # if a review data does not match the schema, validation failed so log an error and skip the review
                logger.error(f"Model validation error, skipping following review: {e}")
        return processed_reviews

    """
        runs the scraper on all given app ids in the app_dict as well as review count on each app
        arguments: self, count (number of data to fetch per app), lang (language of the reviews to fetch, default is ms or malaysian), country (country of the reviews to fetch, default is my or malaysian)
        EXPECTED TO return: list of ReviewPayload (now with all the reviews combined from all apps!)
    """
    async def run(self, count: int = 100, lang: str="ms", country: str="my") -> list[ReviewPayload]:

        # fetch
        app_ids = list(self.app_dict.keys()) # list of app ids to be scraped
        tasks_fetch = [self.fetch(app_id, count, lang, country) for app_id in app_ids] # list of fetch_reviews() tasks to be passed into asyncio.gather()
        fetch_results = await asyncio.gather(*tasks_fetch) # execute the list of fetch tasks concurrently, wait for all the fetch tasks to complete
        raw_data_map = dict(zip(app_ids, fetch_results)) # map results back to their respective app id (asyncio.gather guarantees following the order the tasks were given so zip is viable), maintains O(1) rather than hardcoding list index for each app (gx_app = fetch_results[0] and etc)

        # process and validate
        tasks_process = [self.process(app_id, raw_data_map[app_id]) for app_id in app_ids] # similar process above but for processing the reviews instead
        processed_results = await asyncio.gather(*tasks_process)

        # asyncio.gather() returns a list of returns from each of its tasks, which means we must flatten them here
        combined_results = list(itertools.chain.from_iterable(processed_results)) # should work now, fetch failures now returns empty list and process failures didnt append anything

        return combined_results

        """ some test
        # sample = await fetch_reviews("my.com.gxbank.app", 1)
        # print(sample)

        sample = await fetch_reviews("my.com.gxbank.app", 1)
        returned_review =  await process_review("GX Bank", "my.com.gxbank.app", sample[0])
        print(returned_review.model_dump_json())
        """

async def main():
    """
        main function to run the scraper from terminal, js test purposes only
    """
    app_dict = { # list of apps to be scraped: keys are app ids and values are app names, more app targets can js be added here
        "my.com.gxbank.app": "GX Bank",
        "my.com.tngdigital.ewallet": "TNG eWallet",
        "com.maybank2u.life": "MAE by Maybank2u",
        "my.com.myboost": "Boost"
    }

    scraper = PlayStoreScraper(app_dict)
    results = await scraper.run(10, "ms", "my")
    print(results)
    print(f"succesfully scrapped {len(results)} reviews")

if __name__ == "__main__": # if u want to test on running it directly on terminal
    results = asyncio.run(main())