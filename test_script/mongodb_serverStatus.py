from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
import json

# Read DB configuration from json_file
with open('../mongodb_configuration.json') as json_file:
    dbConfig = json.load(json_file)

#Step 1: Connect to MongoDB - Note: Change connection string as needed
url = dbConfig['url']
mongodb_url = url.format(dbConfig['username'], dbConfig['password'], dbConfig['dbname'])
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient(mongodb_url)
db=client.admin
# Issue the serverStatus command and print the results
serverStatusResult=db.command("serverStatus")
pprint(serverStatusResult)