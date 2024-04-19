from pymongo import MongoClient
import json

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['movierec']
collection = db['movies']

# Aggregation pipeline
pipeline = [
    {
        "$match": {
            "release_date": {"$exists": True, "$ne": []},
            "genres": {"$exists": True, "$ne": []}
        }
    },
    {
        "$addFields": {
            "decade": {
                "$concat": [
                    {"$substr": [{"$year": "$release_date"}, 0, 3]},
                    "0"
                ]
            }
        }
    },
    {
        "$unwind": "$genres"
    },
    {
        "$group": {
            "_id": {
                "decade": "$decade",
                "genre_id": "$genres.id",
                "genre_name": "$genres.name"
            },
            "avg_popularity": {"$avg": "$popularity"},
            "avg_vote_average": {"$avg": "$vote_average"}
        }
    },
    {
        "$addFields": {
            "popularity_score": {
                "$add": [
                    {"$multiply": [0.7, "$avg_popularity"]},
                    {"$multiply": [0.3, "$avg_vote_average"]}
                ]
            }
        }
    },
    {
        "$group": {
            "_id": {
                "decade": "$_id.decade",
                "genre_id": "$_id.genre_id",
                "genre_name": "$_id.genre_name"
            },
            "popularity_score": {"$avg": "$popularity_score"}
        }
    },
    {
        "$sort": {
            "_id.decade": 1,
            "_id.genre_id": 1
        }
    }
]

# Execute aggregation pipeline
result = list(collection.aggregate(pipeline))

# Write output to JSON file with readable formatting
with open('output3.json', 'w') as f:
    json.dump(result, f, indent=4)

client.close()
