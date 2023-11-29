import os
from pymongo import MongoClient
from dotenv import load_dotenv
import settings

class MongoDBConnection:
    def __init__(self):
        self.host = os.getenv("MONGO_HOST")
        self.db_name = os.getenv("MONGO_DBNAME")
        self.client = None
        self.db = None

    def __enter__(self):
        self.client = MongoClient(host=self.host, port=27017)
        self.db = self.client[self.db_name]
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


def get_topics(db):
    all_topics = []
    collection = db["topics"]
    for doc in collection.find():
        all_topics.append(doc["topicName"])
    return all_topics