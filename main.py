from flask import Flask, request, jsonify
from db import MongoAtlas
from dotenv import load_dotenv
import os
from bson.objectid import ObjectId
from flask_caching import Cache
import datetime
load_dotenv()

uri = os.getenv('URI')
db_name = os.getenv('DB_NAME')
collection = os.getenv('DB_COLLECTION')
server_port = os.getenv('SERVER_PORT')

db = MongoAtlas(dBName=db_name,collectionName=collection, uri=uri)
app = Flask(__name__)

# Check Configuring Flask-Cache section for more details
cache = Cache(app,config={'CACHE_TYPE': 'memcached'})


@app.route('/db_status', methods=['GET'])
@cache.cached(timeout=50)
def hello_world():
    if db.client:
        return 'OK'
    else:
        return "It's not ok"


@app.route('/get_all_documents', methods=['GET'])
@cache.cached(timeout=50)
def get_all_documents():
    data = db.collection.find({})
    output = []
    for i in data:
        attributes = []
        for q in i:
            if q == '_id':
                pass
            else:
                attributes.append({q:i[q]})
        output.append({str(i['_id']): attributes})
    return jsonify({'result': output})


@app.route('/get_documents/<limit>', methods=['GET'])
@cache.cached(timeout=50)
def get_documents(limit):
    data = db.collection.find({}).limit(int(limit))
    output = []
    for i in data:
        attributes = []
        for q in i:
            if q == '_id':
                pass
            else:
                attributes.append({q:i[q]})
        output.append({str(i['_id']): attributes})
    return jsonify({'result': output})


@app.route('/post_document', methods=['POST'])
@cache.cached(timeout=50)
def post_one_document():
    body = request.get_json()
    if body:
        db.collection.insert_one(body)
    else:
        pass
    find = db.collection.find_one(body)
    find_id = str(find.get('_id'))
    find.pop('_id')
    return jsonify({find_id:find})


@app.route('/get_document/<population>', methods=['GET'])
@cache.cached(timeout=50)
def get_document_population(population):
    output = []
    try:
        data = db.collection.find({'population' : float(population)})
        for i in data:
            attributes = []
            for q in i:
                if q == '_id':
                    pass
                else:
                    attributes.append({q:i[q]})
            output.append({str(i['_id']): attributes})
        return jsonify({'result': output})

    except Exception as e:
        return str(e)


@app.route('/get_documents_parameters/<date>/<location>/<iso_code>', methods=['GET'])
@cache.cached(timeout=50)
def get_documents_parameters(date, location, iso_code):
    year = date[0:4]
    month = date[5:7]
    day = date[8:10]
    date_ = datetime.datetime(int(year), int(month), int(day))
    output = []
    data = db.collection.find({
                                'date':{"$gt": date_}, 
                                'iso_code': str(iso_code),
                                'location':str(location)
                                
                                })
    
    for i in data:
        attributes = []
        for q in i:
            if q == '_id':
                pass
            else:
                attributes.append({q:i[q]})
        output.append({str(i['_id']): attributes})

    return jsonify({'result': output})


@app.route('/delete_document', methods=['DELETE'])
@cache.cached(timeout=50)
def delete_document():
    body = request.get_json()
    id_ =  ObjectId(body.get('_id'))
    query = {'_id':id_}
    try:
        db.collection.delete_one(query)
        return 'success'
    except:
        return 'fail'
        

if __name__ == '__main__':
    app.run(debug=True, port = server_port)