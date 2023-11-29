from flask import Flask, request, Response, abort
from flask.json import jsonify
from database import MongoDBConnection, get_topics
import json
import requests
from collections import defaultdict
import logging

app = Flask(__name__)


logging.basicConfig(
    filename='example.log',  # Log file name
    filemode='a',            # Append to the log file if it exists, otherwise create a new file
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format of log messages
    level=logging.DEBUG      # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
)

def merge_remote(host, port):
    print("Getting data from:",host,port)
    topics = requests.get(f"http://{host}:{port}/ws/topics").json()
    print(topics)
    if len(topics) == 0:
        return jsonify({"status": 404, "message": "No topics found"})
    for topic in topics:
        with MongoDBConnection() as db:
            if topic not in get_topics(db):
                logging.info(f"Inserting topic: {topic}")
                collection = db["topics"]
                collection.insert_one({"topicName": topic})


        res = requests.get(f"http://{host}:{port}/ws/topic/{topic}").json()
        resources = [{"category": el, "urls": res[el]} for el in res]
        for category in resources:
            cat = category["category"]
            urls = category["urls"]
            with MongoDBConnection() as db:
                collection = db["topics"]
                document = collection.find_one({"topicName": topic})
                print(urls, cat)

                if 'resources' in document and any(resource['category'] == cat for resource in document['resources']):
                    logging.info(f"Category {cat} already in database")
                    collection.find_one_and_update(
                            {"topicName": topic, "resources.category": cat},
                            {"$addToSet": {"resources.$.urls": {"$each": urls}}}
                        )
                else:
                    collection.find_one_and_update(
                        {"topicName": topic},
                        {"$push": {"resources": {"category": cat, "urls": urls}}}
                    )
                logging.info(f"Added {urls} to category {cat} for topic {topic}")


@app.before_request
def auto_populate_annuaire():
    if request.remote_addr not in ["localhost", "127.0.0.1"]:
        p = 5000
        if (
            requests.get(f"http://{request.remote_addr}:{p}/ws/annuaire").status_code
            == 200
        ):
            with MongoDBConnection() as db:
                collection = db["annuaire"]

                if collection.find_one({"host": request.remote_addr}) is not None:
                    logging.info(f"{request.remote_addr}:{p} already in database")
                    try:
                        merge_remote(request.remote_addr, p)
                    except Exception as e:
                        logging.error(f"Error from {request.remote_addr}:{p}: {e}")
                else:
                    try:
                        collection.insert_one({"host": request.remote_addr, "port": p})
                        print(f"{request.remote_addr}:{p} added to database")
                        merge_remote(request.remote_addr, p)
                    except Exception as e:
                        logging.error(f"Error from {request.remote_addr}:{p}: {e}")


@app.route("/ws")
def home():
    return jsonify("Hello World!")


@app.get("/db/update")
def update_remote():
    with MongoDBConnection() as db:
        collection = db["annuaire"]
        for doc in collection.find():
            print(doc)
            merge_remote(doc["host"], doc["port"])
    return jsonify({"status": 200, "message": "Database updated successfully"})


@app.get("/ws/topics")
def topics():
    all_topics = []
    with MongoDBConnection() as db:
        collection = db["topics"]
        for doc in collection.find():
            all_topics.append(doc["topicName"])
    return jsonify(all_topics)


@app.get("/ws/topic/<topic>")
def topic(topic):
    res = {}
    with MongoDBConnection() as db:
        collection = db["topics"]
        res_mongo = collection.find_one({"topicName": topic}, {"_id": 0})
        for doc in res_mongo["resources"]:
            res[doc["category"]] = doc["urls"]

    return jsonify(res)


@app.get("/ws/annuaire")
def annuaire():
    res = []
    with MongoDBConnection() as db:
        collection = db["annuaire"]
        query = collection.find({}, {"_id": 0})
        for doc in query:
            res.append(f"http://{doc['host']}:{doc['port']}")
    return jsonify(list(res))


@app.get("/ws/annuaire/reset")
def reset_annuaire():
    with MongoDBConnection() as db:
        collection = db["annuaire"]
        collection.delete_many({})
        collection.drop_indexes()
        collection.create_index([("host", 1)], unique=True)
        return jsonify({"status": 200, "message": "Annuaire reset"})


@app.route("/ws/annuaire/add", methods=["GET", "POST"])
def post_annuaire():
    if request.method == "POST":
        host = request.form.get("host")
        port = request.form.get("port")
        with MongoDBConnection() as db:
            collection = db["annuaire"]
            try:
                collection.insert_one({"host": host, "port": port})
            except:
                return jsonify({"status": 404, "message": "Host already in database"})

            try:
                merge_remote(host, port)
                return jsonify({"host": host, "port": port})
            except:
                return jsonify({"status": 404, "message": "Host not found"})
    return """
        <form method="POST">
            <div><label>Host: <input type="text" name="host"></label></div>
            <div><label>Port: <input type="number" name="port"></label></div>
            <input type="submit" value="Submit">
        </form>"""


@app.get("/db/reset")
def reset_db():
    with MongoDBConnection() as db:
        collection = db["topics"]
        collection.delete_many({})
        collection.drop_indexes()
        collection.create_index([("topicName", 1)], unique=True)

        data = json.load(open("temp.json"))

        collection.insert_many(data["topics"])

    return jsonify({"status": 200, "message": "Database reset successfully"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
