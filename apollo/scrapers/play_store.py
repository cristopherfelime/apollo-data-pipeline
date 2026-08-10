"""
        google play reviews scraper
        v1.1.4 - more LSP implementation updates
"""

import logging # logging purposes
import asyncio # python's asynchronous programming library
from google_play_scraper import reviews # google play scraper library we'll be using
from pydantic import ValidationError # for catching validation errors
import itertools # for flattening the list of lists

from apollo.schemas import ReviewPayload # to validate the data
from apollo.scrapers.base import BaseScraper # to inherit the abstract class

# initialize logger
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------------------------------------

"""
    the play store scraper class
    attributes:
        app_dict (dict): dictionary to store app ids and app names
    methods:
        __init__ -> initializes the scraper with the given app_dict
        add_app_dict -> adds an app to the scraper's app_dict
        get_app_dict -> returns the scraper's app_dict
        remove_app -> removes an app from the scraper's app_dict
        fetch -> fetches reviews from google play store for a specific app (overrides abstract method)
        process -> processes and validates each review (overrides abstract method)
        run -> runs the scraper on all given app ids in the app_dict as well as review count on each app
"""
# inherits BaseScraper: abstract attribute required -> data (list[BaseModel])
class PlayStoreScraper(BaseScraper):
    app_dict: dict


    """
        initializes the scraper
        arguments: self, app_dict (dict): dictionary to store app ids and app names
        EXPECTED TO return: None
    """
    def __init__(self, app_dict: dict=None):
        if app_dict:
            self.app_dict = app_dict
        else: # default instance app_dict
            self.app_dict = {
                    "my.com.gxbank.app": "GX Bank",
                    "my.com.tngdigital.ewallet": "TNG eWallet",
                    "com.maybank2u.life": "MAE by Maybank2u",
                    "my.com.myboost": "Boost"
                }



    # these getter and setter methods are mostly js for debugging and testing btw
    """
        adds an app to the scraper
        arguments: self, app_id (the app id of the app to be scraped), app_name (the name of the app)
        EXPECTED TO return: None
    """
    def add_app_dict(self, app_id: str, app_name: str) -> None:
        self.app_dict[app_id] = app_name

    """
        returns the app_dict
        arguments: self
        EXPECTED TO return: dict (the app_dict)
    """
    def get_app_dict(self) -> dict:
        return self.app_dict

    """
        removes an app from the scraper
        arguments: self, app_id (the app id of the app to be removed)
        EXPECTED TO return: None
    """   
    def remove_app(self, app_id: str) -> None:
        del self.app_dict[app_id]



    """
        OVERRIDES fetch() FROM BaseScraper
        fetch reviews from google play store for a specific app, google_play_scraper was written with synchronous blocking, so this will ofload it to an async function using asyncio.to_thread instead
        arguments: self, target (the target or list of targets to scrape), count (number of data to fetch), lang (language of the reviews to fetch, default is ms or malaysian), country (country of the reviews to fetch, default is my or malaysian) idk why malaysia needs ms for lang and my for country but Ok
        EXPECTED TO return: list of dict (raw review data scraped by the package)
    """
    async def fetch(self, target: str | list[str] | None = None, count: int = 1, lang: str = "ms", country: str = "my") -> list[dict] | list[list[dict]]: # lang and country needs default values to follow Listkov Substitution Principle, PlayStoreScraper is a child class of BaseScraper and BaseScraper doesnt have default values for lang and country, so we need to provide them here, if not it'll raise TypeError: fetch() missing 2 required positional argument: 'lang', 'country' ()
        responses = []
        target = target or list(self.app_dict.keys()) # if target is None, set it to all app ids in the app_dict
        try:
            if isinstance(target, str): # for singular target
                responses, _ = await asyncio.to_thread( # runs the sync function in a separate thread using the thread pool
                    reviews,
                    target,
                    count=count,
                    lang=lang,
                    country=country
                )
            elif isinstance(target, list): # for multiple targets
                tasks_fetch = [
                    asyncio.to_thread(
                        reviews,
                        t,
                        count=count,
                        lang=lang,
                        country=country
                    )
                    for t in target # for every target in the targets list, make a fetch task for them and append them to the list above
                ] # list containing fetch tasks to be passed into gather()
                results_list = await asyncio.gather(*tasks_fetch)
                responses = [r[0] for r in results_list] # google_play_scraper returns a list of (results_list, token), we only need the results_list so we take r[0]
        except Exception as e:
            logger.error(f"Error fetching reviews for app {target}: {e}")
        return responses

    """
        OVERRIDES process() FROM BaseScraper
        process and validates each reviews, correctly mapping app name and id to the reviews, using pydantic to ensure and validate the data
        arguments: self, payload (list of raw review data scraped by the package), target (target where the review responses come from, default is None)
        EXPECTED TO return: list of ReviewPayload (validated and cleaned data based on the schemas in schemas.py)
    """
    async def process(self, payload: list[dict], target: str | None=None) -> list[ReviewPayload]:
        processed_reviews = [] # to store all the processed reviews from all the raw api responses for the final return
        for review in payload: # iterate through each of the raw api responses for the current app id
            if target and (target in self.app_dict.keys()): # if target is provided
                review.update({"app_name": self.app_dict.get(target), "app_id": target}) # update the raw review data to include the app name and app id, matching the schema in ReviewPayload
            else: # if target is not provided or not in the app_dict
                review.update({"app_name": "Unknown App", "app_id": "com.unknown"}) # default to unknown app name and id
            try: # try to validate each reviews
                validated_review = ReviewPayload.model_validate(review)
                processed_reviews.append(validated_review) # if there's no ValidationError raised by pydantic, then append the validated review to processed_reviews
            except ValidationError as e: # if a review data does not match the schema, validation failed so log an error and skip the review
                logger.error(f"Model validation error, skipping following review: {e}")
            except Exception as e: # just in case if theres any other unexpected error
                logger.error(f"Unexpected error occured in process(): {e}\nSkipping following review: {review}")
        return processed_reviews

    """
        OVERRIDES run() FROM BaseScraper
        runs the scraper on all given app ids in the app_dict as well as review count on each app
        arguments: self, count (number of data to fetch per app), lang (language of the reviews to fetch, default is ms or malaysian), country (country of the reviews to fetch, default is my or malaysian)
        EXPECTED TO return: list of ReviewPayload (now with all the reviews combined from all apps!)
    """
    async def run(self, count: int=100, lang: str="ms", country: str="my") -> list[ReviewPayload]:

        # fetch
        app_ids = list(self.app_dict.keys()) # list of app ids to be scraped
        fetch_results = await self.fetch(app_ids, count, lang, country) #  execute the fetch method concurrently
        raw_data_map = dict(zip(app_ids, fetch_results)) # map results back to their respective app id (asyncio.gather guarantees following the order the tasks were given so zip is viable), maintains O(1) rather than hardcoding list index for each app (gx_app = fetch_results[0] and etc)

        # process and validate
        tasks_process = [self.process(raw_data_map[app_id], app_id) for app_id in app_ids] # similar process above but for processing the reviews instead
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


"""
    main function to run the scraper from terminal, js test purposes only
"""
async def main():
    scraper = PlayStoreScraper()
    results = await scraper.run(10)
    print(results)
    print(f"succesfully scrapped {len(results)} reviews")

if __name__ == "__main__": # if u want to test on running it directly on terminal
    results = asyncio.run(main())