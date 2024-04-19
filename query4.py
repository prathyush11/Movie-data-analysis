from pymongo import MongoClient
import json

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['movierec']
collection = db['movies']

# Aggregation pipeline
pipeline = [
    {
        '$unwind': '$production_companies'
    }, {
        '$addFields': {
            'season': {
                '$switch': {
                    'branches': [
                        {
                            'case': {
                                '$in': [
                                    {
                                        '$month': '$release_date'
                                    }, [
                                        3, 4, 5
                                    ]
                                ]
                            }, 
                            'then': 'Spring'
                        }, {
                            'case': {
                                '$in': [
                                    {
                                        '$month': '$release_date'
                                    }, [
                                        6, 7, 8
                                    ]
                                ]
                            }, 
                            'then': 'Summer'
                        }, {
                            'case': {
                                '$in': [
                                    {
                                        '$month': '$release_date'
                                    }, [
                                        9, 10, 11
                                    ]
                                ]
                            }, 
                            'then': 'Fall'
                        }, {
                            'case': {
                                '$in': [
                                    {
                                        '$month': '$release_date'
                                    }, [
                                        12, 1, 2
                                    ]
                                ]
                            }, 
                            'then': 'Winter'
                        }
                    ], 
                    'default': 'Unknown'
                }
            }
        }
    }, {
        '$group': {
            '_id': {
                'name': '$production_companies.name', 
                'id': '$production_companies.id', 
                'season': '$season'
            }, 
            'total_movies': {
                '$sum': 1
            }, 
            'total_popularity': {
                '$sum': '$popularity'
            }
        }
    }, {
        '$group': {
            '_id': {
                'name': '$_id.name', 
                'id': '$_id.id'
            }, 
            'seasons': {
                '$push': {
                    'season': '$_id.season', 
                    'total_movies': '$total_movies', 
                    'avg_popularity': {
                        '$avg': '$total_popularity'
                    }
                }
            }
        }
    }, {
        '$project': {
            '_id': 0, 
            'production_company': {
                'name': '$_id.name', 
                'id': '$_id.id'
            }, 
            'seasons': {
                '$map': {
                    'input': '$seasons', 
                    'in': {
                        'season': '$$this.season', 
                        'total_movies': '$$this.total_movies', 
                        'normalized_avg_popularity': {
                            '$divide': [
                                {
                                    '$subtract': [
                                        '$$this.avg_popularity', 0
                                    ]
                                }, 875.581305
                            ]
                        }
                    }
                }
            }
        }
    }, {
        '$unwind': '$seasons'
    }, {
        '$addFields': {
            'seasons.likelihood_of_success': {
                '$cond': [
                    {
                        '$gt': [
                            '$seasons.total_movies', 0
                        ]
                    }, {
                        '$divide': [
                            '$seasons.normalized_avg_popularity', '$seasons.total_movies'
                        ]
                    }, 0
                ]
            }
        }
    }, {
        '$group': {
            '_id': {
                'name': '$production_company.name', 
                'id': '$production_company.id'
            }, 
            'seasons': {
                '$push': '$seasons'
            }
        }
    }, {
        '$sort': {
            '_id.name': -1
        }
    }, {
        '$project': {
            '_id': 0, 
            'production_company': {
                'name': '$_id.name', 
                'id': '$_id.id'
            }, 
            'seasons': {
                '$map': {
                    'input': '$seasons', 
                    'in': {
                        'season': '$$this.season', 
                        'total_movies': '$$this.total_movies', 
                        'likelihood_of_success': '$$this.likelihood_of_success'
                    }
                }
            }
        }
    }
]

# Execute aggregation pipeline
result = list(collection.aggregate(pipeline))

# Write output to JSON file with readable formatting
with open('output4.json', 'w') as f:
    json.dump(result, f, indent=4)

client.close()
