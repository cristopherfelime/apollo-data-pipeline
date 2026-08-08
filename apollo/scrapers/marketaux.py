"""
		marketaux api scraper
		v1.0
"""

import logging # logging purposes
import asyncio # python's asynchronous programming library
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
		marketaux_token (str): api token for marketaux rest api
		client (httpx.AsyncClient): async http client for making requests to the rest api
	methods:
		__init__ -> initializes the scraper
		
"""
# inherits BaseScraper: abstract attribute required -> data (list[BaseModel])
class MarketauxScraper(BaseScraper):
	endpoint: str = "https://api.marketaux.com/v1/news/all"
	params: dict[str, str]
	client: httpx.AsyncClient

	def __init__(self, params: dict[str, str] | None=None):
		self.client = None
		self.params = {}
		if params:
			self.params.update(params)



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
			logger.info("httpx client was already initialized for this instance")
	
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
		EXPECTED TO return: list of dictionaries containing the api response
	"""
	# the error i was talking about:
	# TypeError: fetch() missing 1 required positional argument: 'target'
	# son
	async def fetch(self, target: str | None = None, params: dict[str, str] | None = None, count: int=1) -> list[dict]:
		responses = []
		# cant use self for default argument values because they're evaluated during module import, can raise AttributeError
		# should work now
		target = target or self.endpoint # or keyword: if target is None then use self.endpoint, useful when trying to use instance attributes as defaults
		params = params or self.params
		try:
			if self.client is None: # ensure httpx client is initialized before making requests
				await self.start_client()
			tasks_fetch = [
				self.client.get(
					url=target,
					params=params
				) for _ in range(count) # using * count only duplicates the memory pointer, not the actual task objects so we should use ts comprehension instead
			]
			responses = await asyncio.gather(*tasks_fetch, return_exceptions=True) # unlike fetch() in PlayStoreScraper that processes reviews one-by-one, this fetch() uses gather to fetch multiple requests at once so to avoid one bad request invalidates the entire batch, use return_exception=True

			# rather than raising each status one by one iterating through responses, we will handle them later in process(), mostly same reason as above:
			# if one response fail, the entire batch is not rejected immediately, we can process good responses and drop bad ones instead, thus improving throughput
			
		except Exception as e:
			logger.error(f"Error fetching from API: {e}")
		return responses
	
	"""
		processes the fetched payload and returns a list of validated FinancialNewsPayload objects
		STATIC POTENTIAL GUY ahh
		arguments: self, payload (list of dictionaries (response) from api call)
		EXPECTED TO return: list of FinancialNewsPayload objects
	"""
	async def process(self, payload: list[dict]) -> list[FinancialNewsPayload]:
		processed_news = []
		for news_batch in payload:
			if isinstance(news_batch, Exception): # if guard to catch any asyncio.gather() related exceptions like httpx timeouts
				logger.error(f"Network exception occurred from asyncio.gather(): {news_batch}")
				continue
			try:
				news_batch.raise_for_status() # this is a continuation from fetch() explanation above, if the response has an error status code, raise httpx.HTTPStatusError so that it is caught below
				news_dict = news_batch.json() # essentially parses the json response to python dict
				try:
					for news_item in news_dict.get("data", []): # iterating through the list of news items (need to subset to data field since there are other field in the json response (meta))
						entities = news_item.get("entities") # to check if entities key exists
						if entities and isinstance(entities, list) and (len(entities) > 0): # if guard for entities, checks if its not None, is a list, and has content
							news_item["sentiment_score"] = news_item["entities"][0].get("sentiment_score") # and yea so sentiment_score is located in the entities key of each data, where entities store their dictionary data inside a list
						else:
							news_item["sentiment_score"] = None # if condition fails, sentiment_score can be set to None, still acceptable in FinancialNewsPayload schema
						validated_news = FinancialNewsPayload.model_validate(news_item) # any ValidationError will be caught below
						processed_news.append(validated_news)
				# this is individual news item level
				except ValidationError as e:
					logger.error(f"Model validation error, skipping following news item: {e}")
				except Exception as e:
					logger.error(f"Unexpected error occurred in process() [INDIVIDUAL NEWS ITEM LEVEL], skipping following news item: {e}")
			# this is news_batch level
			except httpx.HTTPStatusError as e: # if a status code error was in fact encountered, only skip the current news batch instead of terminating the entire process()
				logger.error(f"httpx.HTTPStatusError {e.response.status_code} occurred from httpx.get(), skipping current news")
			except Exception as e:
				logger.error(f"Unexpected error occurred in process() [BATCH LEVEL], skipping following news batch: {e}")
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
		runs the api fetch and process pipeline on the marketaux rest api for multiple search target keywords concurrently
		arguments: self, search_targets (list of search target strings like ['Maybank', 'Boost']), count (integer number of requests per keyword, default is 1), params (optional query parameters dictionary)
		EXPECTED TO return: list of FinancialNewsPayload
	"""
	async def run(self, search_targets: list[str], count: int = 1, params: dict[str, str] | None = None) -> list[FinancialNewsPayload]:
		request_params = {**self.params, **(params or {})} # unpack instance params for the fetch call batch for each target search keyword, defaults to class-level params or empty dict
		tasks_fetch = [ # create a fetch task now for each search target keyword (one for Maybank, one for GX Bank, etc)
			self.fetch(target=self.endpoint, params={**request_params, "search": target}, count=count)
			for target in search_targets
		]
		fetch_results_nested = await asyncio.gather(*tasks_fetch) # gather all fetch tasks to run concurrently
		fetch_results = list(itertools.chain.from_iterable(fetch_results_nested)) # and flatten the nested list of responses from each search target
		processed_results = await self.process(fetch_results) # process and validate all the results
		return processed_results
		

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
	search_targets = ["Maybank", "Boost", "GX Bank", "TNG eWallet"] # kidna suck now that itll take like 4 api usage now since search parameter in the api call turns out to be searching for news with all those words instead of having at least one of those words
	async with MarketauxScraper(params) as scraper: # example of using the scraper with context manager
		results = await scraper.run(search_targets=search_targets, count=1)
	print(results)
	print(f"succesfully scrapped {len(results)} news")

if __name__ == "__main__":
	results = asyncio.run(main())