from pymongo import MongoClient
import pandas as pd


class MongoAtlas(object):

    def __init__(self, dBName=None, collectionName=None, uri=None):

        self.dBName = dBName
        self.collectionName = collectionName
        self.uri = uri

        self.client = MongoClient(self.uri)

        self.DB = self.client[self.dBName]
        self.collection = self.DB[self.collectionName]



    def InsertDataFromCSV(self, path=None):

        df = pd.read_csv(path)
        df['date'] = pd.to_datetime(df['date'])
        data_dict = df.to_dict('records')
        start = 0
        end = 500
        if path == None:
            pass
        else:
            while True:
                try:
                    to_db = data_dict[start:end]
                    start += 500
                    self.collection.insert_many(to_db, ordered=False)
                    end += 500
                    print('success')
                except:
                    final = data_dict[start:]
                    self.collection.insert_many(final, ordered=False)
                    break

# if __name__ == "__main__":
#     mongodb = MongoDB(dBName = 'admin', collectionName='collection_1', uri=uri)
#     mongodb.InsertData(path="data.csv")

