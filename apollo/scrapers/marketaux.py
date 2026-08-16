"""
		marketaux api scraper
		v1.2.3 - refactored a bit of process(), it explicitly maps fields into FinancialNewsPayload to safely filter all raw API extra fields as 'extra' attribute is now fixed in its respective schema
"""

import logging # logging purposes
import asyncio # python's asynchronous programming library
from asyncio import CancelledError # to catch cancelled error (like KeyboardInterrupt)
import os # os file navigation purposes
from dotenv import load_dotenv # for loading environment variables from .env file
import httpx # python's improved http requests library, used to make request to the rest api
import itertools # for flattening nested list results from concurrent async requests
from pydantic import ValidationError # to handle validation error from pydantic models

from apollo.schemas import FinancialNewsPayload # to validate data
from apollo.scrapers.base import BaseScraper # to inherit the abstract class

# initialize logger
logger = logging.getLogger(__name__)

# loading .env file and assigning marketaux token as global variable to be used
load_dotenv()

# ------------------------------------------------------------------------------------------------------------------------------

"""
	the marketaux scraper class
	attributes:
		endpoint (str): url endpoint for marketaux rest api
		params (dict[str, str]): dictionary of query parameters for the http request
		search_targets (list[str]): list of search target keywords to query concurrently
		client (httpx.AsyncClient | None): async http client for making requests to the rest api
	methods:
		__init__ -> initializes the scraper with params and search_targets
		add_params -> adds parameters to request params
		remove_params -> removes parameters from request params
		add_search_targets -> adds search keywords to search_targets
		remove_search_targets -> removes search keywords from search_targets
		set_endpoint -> sets the endpoint url for the http request
		start_client -> initializes the httpx.AsyncClient
		close_client -> closes the httpx.AsyncClient
		fetch -> fetches news data from marketaux rest api (overrides BaseScraper)
		process -> processes and validates raw responses into FinancialNewsPayload (overrides BaseScraper)
		__aenter__ -> enters async context manager
		__aexit__ -> exits async context manager
		run -> runs full fetch and process pipeline for search targets concurrently (overrides BaseScraper)
"""
# inherits BaseScraper: abstract attribute required -> data (list[BaseModel])
class MarketauxScraper(BaseScraper):
	endpoint: str = "https://api.marketaux.com/v1/news/all"
	params: dict[str, str]
	search_targets: list[str]
	client: httpx.AsyncClient

	def __init__(self, params: dict[str, str] | None=None, search_targets: list[str] | None=None):
		self.client = None
		if params:
			self.params = params
		else: # default instance params
			self.params = {
				"api_token": os.getenv("MARKETAUX_TOKEN"),
				"limit": 3,
				"language": "en",
				"country": "my"
			}
		if search_targets:
			self.search_targets = search_targets
		else: # default instance search_targets
			self.search_targets = ["Maybank", "Boost Bank", "GXBank Malaysia", "TNG eWallet"]
			



	# getters and setters for testing and debugging purpose
	"""
		adds parameters to the http request params
		arguments: self, params (dictionary of string:string to be added as parameters)
		EXPECTED TO return: None
	"""
	def add_params(self, params: dict[str, str]) -> None:
		self.params.update(params)

	"""
		removes parameters from the http request params
		arguments: self, params (list of strings (parameter names or keys in this case) to be removed)
		EXPECTED TO return: None
	"""
	def remove_params(self, params: list[str]) -> None:
		for param in params:
			self.params.pop(param, None) # use None as the default value, so that if the parameter is not in the dictionary, it will not raise an error

	"""
		adds targets to the search_targets list
		arguments: self, search_targets (list of strings to be added)
		EXPECTED TO return: None
	"""
	def add_search_targets(self, search_targets: list[str]) -> None:
		self.search_targets.extend(search_targets)

	"""
		removes targets from the search_targets list
		arguments: self, search_targets (list of strings (target names or keys in this case) to be removed)
		EXPECTED TO return: None
	"""
	def remove_search_targets(self, search_targets: list[str]) -> None:
		for target in search_targets:
			if target in self.search_targets:
				self.search_targets.remove(target)

	"""
		sets the endpoint url for the http request
		arguments: self, endpoint (string url of the api endpoint)
		EXPECTED TO return: None
	"""
	def set_endpoint(self, endpoint: str) -> None:
		self.endpoint = endpoint



	"""
		starts the async http client
		arguments: self
		EXPECTED TO return: None
	"""
	async def start_client(self) -> None:
		if self.client is None:
			self.client = httpx.AsyncClient(http2=False) # http2=False bcs marketaux rest api apparently only uses HTTP/1.1, so this is just a safe measure
		else:
			logger.info("(Apollo) httpx client was already initialized for this instance")
	
	"""
		closes the async http client
		arguments: self
		EXPECTED TO return: None
	"""
	async def close_client(self) -> None:
		if self.client:
			await self.client.aclose() # yeah, aclose literally means ASYNCHRONOUSclose
			self.client = None
	
	"""
		OVERRIDES fetch() FROM BaseScraper
		fetches data from the api (asynchronously)
		static version possible
		arguments: self, target (string url of the api endpoint, default is self.endpoint (doesnt show it like that in the method parameter signature MAINLY TO AVOID ERROR BELOW)), params (dictionary of string:string to be added as parameters, default is self.params), count (integer number of requests to make, default is None)
		EXPECTED TO return: list of httpx.Response or Exception objects
	"""
	# the error i was talking about:
	# TypeError: fetch() missing 1 required positional argument: 'target'
	# son
	async def fetch(self, target: str | list[str] | None=None, params: dict[str, str] | None=None, count: int=1) -> list[httpx.Response | Exception] | list[list[httpx.Response | Exception]]:
		responses = []
		# cant use self for default argument values because they're evaluated during module import, can raise AttributeError
		# should work now
		target = target or self.endpoint # or keyword: if target is None then use self.endpoint, useful when trying to use instance attributes as defaults
		params = params or self.params
		try:
			if self.client is None: # ensure httpx client is initialized before making requests
				await self.start_client()
			if isinstance(target, str): # for singular target
				tasks_fetch = [
					self.client.get(
						url=target,
						params=params
					)
					for _ in range(count) # using * count only duplicates the memory pointer, not the actual task objects so we should use ts comprehension instead
				]
				responses = await asyncio.gather(*tasks_fetch, return_exceptions=True) # unlike fetch() in PlayStoreScraper that processes reviews one-by-one, this fetch() uses gather to fetch multiple requests at once so to avoid one bad request invalidates the entire batch, use return_exception=True
			elif isinstance(target, list): # for multiple targets
				tasks_fetch = [
					self.client.get(
						url=t,
						params=params
					)
					for t in target # for each target in the targets list
					for _ in range(count) # and for each target, make 'count' amount of requests
				]
				responses = await asyncio.gather(*tasks_fetch, return_exceptions=True) # unlike fetch() in PlayStoreScraper that processes reviews one-by-one, this fetch() uses gather to fetch multiple requests at once so to avoid one bad request invalidates the entire batch, use return_exception=True

			# rather than raising each status one by one iterating through responses, we will handle them later in process(), mostly same reason as above:
			# if one response fail, the entire batch is not rejected immediately, we can process good responses and drop bad ones instead, thus improving throughput
		
		except CancelledError: # handle CancelledError that may arise from the KeyboardInterrupt
			logger.info(f"(Apollo) MarketauxScraper.fetch() was running, then was stopped by the user (KeyboardInterrupt)")
			raise # also raise CancelledError to let higher level async methods clean/close any resources as well
		except Exception as e:
			logger.error(f"(Apollo) MarketauxScraper.fetch() error fetching from API: {e}")
			return []  # return empty list when exception occurs
		return responses
	
	""" 
		OVERRIDES process() FROM BaseScraper
		processes the fetched payload and returns a list of validated FinancialNewsPayload objects
		arguments: self, payload (list of httpx.Response or Exception from api call)
		EXPECTED TO return: list of FinancialNewsPayload objects
	"""
	# TODO: the triple nested try-except lowk getting ridiculous i might need to separate each processing level into their own method
	async def process(self, payload: list[httpx.Response | Exception]) -> list[FinancialNewsPayload]:
		processed_news = []
		try:
			for news_batch in payload:
				if isinstance(news_batch, Exception): # if guard to catch any asyncio.gather() related exceptions like httpx timeouts
					logger.error(f"(Apollo) MarketauxScraper.process(): Network exception occurred from asyncio.gather(), most likely due to timeouts")
					continue
				try:
					news_batch.raise_for_status() # this is a continuation from fetch() explanation above, if the response has an error status code, raise httpx.HTTPStatusError so that it is caught below
					news_dict = news_batch.json() # essentially parses the json httpx.Response objects to python dict
					for news_item in news_dict.get("data", []): # iterating through the list of news items (need to subset to data field since there are other field in the json response (meta))
						try: # prev ver accidentally put try-except outside of the for loop, this should properly handle individual news item level exception now
							entities = news_item.get("entities") # to check if entities key exists
							sentiment_score = None
							if entities and isinstance(entities, list) and (len(entities) > 0): # if guard for entities, checks if its not None, is a list, and has content
								sentiment_score = entities[0].get("sentiment_score") # sentiment_score is located in the entities key of each data
							
							extracted_news = { # explicitly extract only the fields required by FinancialNewsPayload to avoid extra_forbidden ValidationError from raw API fields (like keywords, image_url, description) the one i fixed earlier
								"uuid": news_item.get("uuid"),
								"title": news_item.get("title"),
								"snippet": news_item.get("snippet") or news_item.get("description", ""), # description works as a snippet too lowk, good as a backup before resorting to empty string
								"url": news_item.get("url"),
								"source": news_item.get("source"),
								"sentiment_score": sentiment_score,
								"published_at": news_item.get("published_at")
							}
							validated_news = FinancialNewsPayload.model_validate(extracted_news) # any ValidationError will be caught below
							processed_news.append(validated_news)
						# this is individual news item level
						except ValidationError as e:
							logger.error(f"(Apollo) MarketauxScraper.process() model validation error at [INDIVIDUAL NEWS ITEM LEVEL], skipping following news item: {e}")
						except Exception as e:
							logger.error(f"(Apollo) MarketauxScraper.process() unexpected error occurred at [INDIVIDUAL NEWS ITEM LEVEL], skipping following news item: {e}")
				# this is news_batch level
				except httpx.HTTPStatusError as e: # if a status code error was in fact encountered, only skip the current news batch instead of terminating the entire process()
					logger.error(f"(Apollo) MarketauxScraper.process() httpx.HTTPStatusError {e.response.status_code} occurred from httpx.get() at [BATCH LEVEL], skipping current news batch: {e}")
				except Exception as e:
					logger.error(f"(Apollo) MarketauxScraper.process() unexpected error occurred at [BATCH LEVEL], skipping following news batch: {e}")
		except CancelledError: # handle CancelledError that may arise from the KeyboardInterrupt
			logger.info(f"(Apollo) MarketauxScraper.process() was running, then was stopped by the user (KeyboardInterrupt)")
			raise # also raise CancelledError to let higher level async methods clean/close any resources as well
		except Exception as e:
			logger.error(f"(Apollo) MarketauxScraper.process() error processing data: {e}")
			return []
		return processed_news

	# these two helper methods are similar to AutoCloseable in java in a sense that it allows for automatic resource cleaning upon done using them
	# in java, utilizing AutoCLoseable requires try-with-resource
	# in python, utilizing __enter__ & __exit__ (synchronouse) or __aenter__ & __aexit__ (asynchronous) requires using with or async with statement
	# these are formally called the context manager protocol
	"""
		enters the asynchronous context, this will be called when using 'async with' statement
		arguments: self
		EXPECTED TO return: the context manager instance (self)
	"""
	async def __aenter__(self):
		await self.start_client()
		return self
	
	"""
		exits the asynchronous context, this will be called when exiting the 'async with' statement block
		arguments: self, exc_type (exception type, None if no exception), exc_val (exception value, None if no exception), exc_tb (traceback object, None if no exception)
		EXPECTED TO return: None
	"""
	async def __aexit__(self, exc_type, exc_val, exc_tb):
		await self.close_client()

	"""
		OVERRIDES run() FROM BaseScraper
		runs the api fetch and process pipeline on the marketaux rest api for multiple search target keywords concurrently
		arguments: self, count (integer number of requests per keyword, default is 1), search_targets (list of search target strings like ['Maybank', 'Boost']), params (optional query parameters dictionary)
		EXPECTED TO return: list of FinancialNewsPayload
	"""
	async def run(self, count: int=3, search_targets: list[str] | None=None, params: dict[str, str] | None=None) -> list[FinancialNewsPayload]:
		try:
			opened_locally = self.client is None # using async with statement, the context manager client will be initialized (from __aenter__()), so this flag will only evaluate to true if run() is executed standalone without with statement
			try:
				if opened_locally: # since standalone (local) run() don't automatically call __aenter__() unlike using with statement, this if guard ensures that the client is initialized
					await self.start_client()

				search_targets = [*(self.search_targets if search_targets is None else search_targets)] # unpacks instance search_targets if the provided argument is None, else unpacks that argument instead
				request_params = {**(self.params if params is None else params)} # same as above but dictionary comprehension for params

				tasks_fetch = [ # create a fetch task now for each search target keyword (one for Maybank, one for GX Bank, etc)
					self.fetch(target=self.endpoint, params={**request_params, "search": target}, count=count)
					for target in search_targets
				]
				fetch_results_nested = await asyncio.gather(*tasks_fetch) # gather all fetch tasks to run concurrently
				fetch_results = list(itertools.chain.from_iterable(fetch_results_nested)) # and flatten the nested list of responses from each search target
				processed_results = await self.process(fetch_results) # process and validate all the results

				return processed_results

			except Exception as e: # basically the very upper level of exception catching when using run()
				logger.error(f"(Apollo) error running MarketauxScraper: {e}")
				return [] # returns empty list as a measure to avoid any downstream issue
			finally:
				if opened_locally: # now with the same flag above, we can know whether the method was ran individually (locally) or not, if so then close the client with this finally block. context manager client will always be closed in the end through __aexit__() instead of the finally block check
					await self.close_client()
		except CancelledError:
			logger.info(f"(Apollo) MarketauxScraper.run() was running, then was stopped by the user (KeyboardInterrupt)")
			await self.close_client() # client was open when exception was caught, so we need to close it
			raise
		except Exception as e:
			logger.error(f"(Apollo) MarketauxScraper.run() unexpected error while running scraper: {e}")
			await self.close_client()
			return []



"""
	the main function to run the scraper from terminal, js test purposes only
"""
async def main():
	params = { # parameters for the api call
		"api_token": os.getenv("MARKETAUX_TOKEN"),
		"limit": 3, # we using free plan so article limit per call is 3 maximum,
		"language": "en", # not a lot of malaysian lang news so had to use english. might need to use a different BERT model as well later in artemis
		"country": "my"
	}
	search_targets = ["Maybank", "Boost", "GX Bank", "TNG eWallet"] # kinda suck now that itll take like 4 api usage now since search parameter in the api call turns out to be searching for news with all those words instead of having at least one of those words
	async with MarketauxScraper(params, search_targets) as scraper: # example of using the scraper with context manager
		results = await scraper.run(count=1)
	print(results)
	print(f"succesfully scrapped {len(results)} news")

if __name__ == "__main__":
	try:
		asyncio.run(main())
	except KeyboardInterrupt:
		logger.info("marketaux.py keyboard interrupt test")