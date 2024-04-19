from pymongo import MongoClient
import json

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['movierec']
collection = db['movies']

# Aggregation pipeline
pipeline = [
    {
        '$project': {
            'production_countries': 1, 
            'genres': 1, 
            'revenue': 1, 
            'budget': 1
        }
    }, {
        '$unwind': '$production_countries'
    }, {
        '$unwind': '$genres'
    }, {
        '$project': {
            '_id': 0, 
            'production_country': '$production_countries.name', 
            'iso_3166_1': '$production_countries.iso_3166_1', 
            'genre': '$genres.name', 
            'revenue': 1, 
            'budget': 1
        }
    }, {
        '$match': {
            'revenue': {
                '$gt': 0
            }, 
            'budget': {
                '$gt': 0
            }
        }
    }, {
        '$group': {
            '_id': {
                'production_country': '$production_country', 
                'iso_3166_1': '$iso_3166_1', 
                'genre': '$genre'
            }, 
            'total_revenue': {
                '$sum': '$revenue'
            }, 
            'total_budget': {
                '$sum': '$budget'
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'production_country': '$_id.production_country', 
            'iso_3166_1': '$_id.iso_3166_1', 
            'genre': '$_id.genre', 
            'avg_roi': {
                '$divide': [
                    '$total_revenue', '$total_budget'
                ]
            }
        }
    }, {
        '$group': {
            '_id': {
                'production_country': '$production_country', 
                'iso_3166_1': '$iso_3166_1'
            }, 
            'genres': {
                '$push': {
                    'genre': '$genre', 
                    'avg_roi': '$avg_roi', 
                    'is_profitable': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    '$avg_roi', 1.12
                                ]
                            }, 
                            'then': True, 
                            'else': False
                        }
                    }
                }
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'production_country': '$_id.production_country', 
            'iso_3166_1': '$_id.iso_3166_1', 
            'genres': 1
        }
    }, {
        '$sort': {
            'production_country': 1
        }
    }
]

# Execute aggregation pipeline
result = list(collection.aggregate(pipeline))

# Write output to JSON file with readable formatting
with open('output1.json', 'w') as f:
    json.dump(result, f, indent=4)

client.close()
