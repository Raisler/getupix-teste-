from db import MongoAtlas
from dotenv import load_dotenv
import os

load_dotenv()
uri = os.getenv('URI')
db_name = os.getenv('DB_NAME')
collection = os.getenv('DB_COLLECTION')

client = MongoAtlas(dBName='<dbname>', collectionName='covid', uri=uri)
print(client.client)

client.InsertDataFromCSV(path="data.csv")