from pymongo import MongoClient
import csv

client = MongoClient('mongodb://root:password@localhost:27017/')
db = client['DB_NoSQL_1']
collection = db['my_collection']

with open('data/.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        collection.insert_one(row)