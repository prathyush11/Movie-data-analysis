from pymongo import MongoClient
import json

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
# db = client['DB_project']
# collection = db['IMDB Movie set']

db = client['movierec']
collection = db['movies']


# Aggregation pipeline
# Aggregation pipeline
pipeline = [
    {
        "$addFields": {
            "keywords": [
                "action",
                "romance",
                "comedy",
                "drama",
                "thriller",
                "fantasy",
                "horror",
                "science fiction",
                "adventure",
                "crime",
                "mystery",
                "animation",
                "family",
                "biography",
                "history",
                "war",
                "musical",
                "western",
                "documentary",
                "sport",
                "music",
                "superhero",
                "spy",
                "revenge",
                "love",
                "friendship",
                "survival",
                "tragedy",
                "magic",
                "aliens",
                "zombies",
                "vampires",
                "robots",
                "time travel",
                "post-apocalyptic",
                "coming of age",
                "space",
                "heist",
                "environmental",
                "political",
                "religious",
                "courtroom",
                "romantic comedy",
                "buddy",
                "historical drama",
                "found footage",
                "noir"
            ]
        }
    },
    {
        "$project": {
            "overview": 1,
            "popularity": 1,
            "vote_average": 1,
            "keywords": {
                "$filter": {
                    "input": "$keywords",
                    "as": "keyword",
                    "cond": {
                        "$regexMatch": {
                            "input": "$overview",
                            "regex": {
                                "$concat": [".\\b", "$$keyword", "\\b."]
                            },
                            "options": "i"
                        }
                    }
                }
            }
        }
    },
    {
        "$unwind": "$keywords"
    },
    {
        "$group": {
            "_id": "$keywords",
            "average_popularity": {
                "$avg": "$popularity"
            },
            "average_vote_average": {
                "$avg": "$vote_average"
            },
            "count": {
                "$sum": 1
            }
        }
    },
    {
        "$addFields": {
            "score": {
                "$avg": [
                    {
                        "$multiply": ["$average_popularity", 0.7]
                    },
                    {
                        "$multiply": ["$average_vote_average", 0.3]
                    }
                ]
            }
        }
    },
    {
        "$sort": {
            "score": -1
        }
    }
]




# Execute aggregation pipeline
result = list(collection.aggregate(pipeline))

# Write output to JSON file with readable formatting
with open('output2.json', 'w') as f:
    json.dump(result, f, indent=4)

client.close()