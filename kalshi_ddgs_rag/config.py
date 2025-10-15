import os
from openai import OpenAI
from pymongo import MongoClient

# -----------------------------------------------------------------------------
# Global Configuration
# -----------------------------------------------------------------------------

MODEL_NAME = "gpt-5-mini-2025-08-07"
NUM_QUERIES = 6
NUM_URLS = 5
MAX_QUERY_WORDS = 7

# Environment variables
MONGO_URI = os.getenv("MONGO_URI")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_ORG_ID = os.getenv("OPENAI_ORG_ID")

# Clients
client = OpenAI(organization=OPENAI_ORG_ID, api_key=OPENAI_API_KEY)
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["forecasting"]
