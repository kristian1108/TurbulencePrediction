from settings_secret import *
from pymongo import MongoClient
import numpy as np
from bson.objectid import ObjectId

class MongoUtility:

    def __init__(self):

        client = MongoClient(MONGO_END_POINT, username=MONGO_USER, password=MONGO_PWD)
        self.db = client.turbulence
        self.pilotreports = self.db.pilotreports

    @staticmethod
    def remove_all_documents(collection):
        for document in collection.find():
            doc_id = document['_id']
            collection.remove({'_id': ObjectId(doc_id)})

    @staticmethod
    def get_doc_count(collection):
        return collection.estimated_document_count()

    def send_sample(self, json):
        self.pilotreports.insert_one(json)

    def get_sample(self, id):
        return self.pilotreports.find_one({'key': id})
