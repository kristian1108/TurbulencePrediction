#!../venv/bin/python3

from mongo_utils import MongoUtility

if __name__ == '__main__':
    mongo = MongoUtility()
    mongo.remove_all_documents(mongo.pilotreports)