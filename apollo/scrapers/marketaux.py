"""
        marketaux api scraper
        v0.1 - not finished as of current commit
"""

import os # os file navigation purposes
from dotenv import load_dotenv # for loading environment variables from .env file

# loading .env file and assigning marketaux token as global variable to be used
load_dotenv()
MARKETAUX_TOKEN=os.getenv("MARKETAUX_TOKEN")
