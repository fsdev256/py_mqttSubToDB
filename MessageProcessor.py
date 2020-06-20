from pymongo import MongoClient
import json

class MessageStruct:
    def __init__(self):
        self.data = None
        self.timestamp = None

class MessageProcessor:
    def __init__(self, db_config):
        self.msg_queue = []
        self.dbConfig = db_config
        self.dbClient = self.connect_db()
        self.db = self.dbClient.myworld

    def process_msg(self):
        msg = self.msg_queue.pop()
        json_data = json.loads(msg.data.payload.decode('utf-8'))
        print('-----------------------------------------------------------------')
        print("msg.timestamp = {0}".format(msg.timestamp))
        print("msg.data.topic = {0}".format(msg.data.topic))
        print("msg.data.payload = {0}".format(msg.data.payload))
        print('-----------------------------------------------------------------')
        topic_split = msg.data.topic.split('/')
        db = topic_split[-2]
        collection = topic_split[-1]
        self.insert_data(db, collection, msg.timestamp, json_data)
    
    def insert_data(self, db, collection, timestamp, json_data):
        filter_data = {
            'device_id' : json_data['device_id'],
            'sensor' : json_data['sensor'],
            'timestamp' : timestamp.replace(microsecond=0, second=0)
        }
        update_data = {
            '$inc':{'transaction_count':1},
            '$addToSet': { 
                'measurements': {
                    'timestamp': timestamp,
                    'temperature' : json_data['temperature']
                } 
            },
            '$setOnInsert' : filter_data
        }
        try:
            result=self.dbClient[db][collection].update(
                filter_data, 
                update_data, 
                upsert = True
            )
            print(result)
        except Exception as err:
            print("Database Exception error: {0}".format(err))
    
    def get_msg_queue_size(self):
        return len(self.msg_queue)
    
    def close(self):
        disconnect_db()

    def connect_db(self):
        print("Connecting to DB")
        # connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
        url = self.dbConfig['url']
        mongodb_url = url.format(self.dbConfig['username'], self.dbConfig['password'], self.dbConfig['dbname'])
        client = MongoClient(mongodb_url)
        return client

    def disconnect_db(self):
        self.dbClient.close()
        print("DB disconnected")
        pass