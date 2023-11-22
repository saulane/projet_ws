from flask import Flask
from flask.json import jsonify
from database import MongoDBConnection
import json


app = Flask(__name__)

@app.route('/')
def home():
    res = []
    with MongoDBConnection() as db:
        collection = db['films']
        # Perform database operations
        for doc in collection.find():
            res.append(doc['title'])
    return jsonify(res)


@app.get('/ws/topics')
def topics():
    all_topics = []
    with MongoDBConnection() as db:
        collection = db['topics']
        for doc in collection.find():
            all_topics.append(doc['topicName'])
    return jsonify(all_topics)

@app.get('/ws/topic/<topic>')
def topic(topic):
    with MongoDBConnection() as db:
        collection = db['topics']
        res = collection.find({"topicName": topic}, {"_id": 0})[0]
        return jsonify(res)

@app.get('/ws/annuaire')
def annuaire():
    with MongoDBConnection() as db:
        collection = db['annuaire']
        res = collection.find({}, {"_id": 0})
        return jsonify(list(res))

@app.get('/db/reset')
def reset_db():
    with MongoDBConnection() as db:
        collection = db['topics']
        collection.delete_many({})

        data = json.load(open('temp.json'))

        collection.insert_many(data['topics'])

    return "ok"

if __name__ == '__main__':
    app.run(debug=True)
