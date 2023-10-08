from typing import List
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

index_name = 'hebrew_search'
documents = [
    {
        '_op_type': 'index',  # Operation type ('index' for insert)
        '_index': index_name,  # Target index
        '_source': {
            'title': 'Document 1',
            'content': 'מרכז סיוע לנפגעי נפש',
            "location": {
                "lat": 40.7128,
                "lon": -74.0060
            }
        },
    },
    {
        '_op_type': 'index',  # Operation type ('index' for insert)
        '_index': index_name,  # Target index
        '_source': {
            'title': 'Document 2',
            'content': 'מרכז חלוקת מזון',
            "location": {
                "lat": 40.7128,
                "lon": -74.0060
            }
        },
    },
    {
        '_op_type': 'index',  # Operation type ('index' for insert)
        '_index': index_name,  # Target index
        '_source': {
            'title': 'Document 3',
            'content': 'בית אבות',
            "location": {
                "lat": 40.7128,
                "lon": -74.0060
            }
        },
    },
    # Add more documents as needed
]


def init_index(es_connection: Elasticsearch):
    index_settings = {
        "settings": {
            "analysis": {
                "filter": {
                    "hebrew_stop": {
                        "type": "stop",
                        "stopwords": "_hebrew_"
                    },
                    "hebrew_keywords": {
                        "type": "keyword_marker",
                        "keywords": ["example"]
                    }
                },
                "analyzer": {
                    "hebrew_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "hebrew_stop",
                            "hebrew_keywords"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "content": {
                    "type": "text",
                    "analyzer": "hebrew_analyzer"
                },
                "title": {
                    "type": "text",
                    "analyzer": "hebrew_analyzer"
                },
                "location": {
                    "type": "geo_point"  # Define the location field as a geo_point
                }
            }
        }
    }

    # Create the index with the specified settings and mappings
    es_connection.indices.create(index=index_name, body=index_settings)


# def search_elastic_hebrew(string_to_search: str, es: Elasticsearch) -> List[dict]:
#     query = ''''''
#     search_results = es.search(index=index_name, body=query)
#     return search_results["hits"]["hits"]


def get_elastic_conn(host='localhost', port=9200, username=None, password=None) -> Elasticsearch:
    if username and password:
        # If username and password are provided, use basic authentication.
        es_connection = Elasticsearch([f"http://{username}:{password}@{host}:{port}"])
    else:
        # If no username and password, create a basic connection.
        es_connection = Elasticsearch([f"http://{host}:{port}"])

    return es_connection


# Now 'es' is an Elasticsearch client instance that you can use for Elasticsearch operations.


def insert_into_es(docs: List[dict], es_connection: Elasticsearch):
    # Use the Elasticsearch Bulk API to insert the documents in bulk
    success, failed = bulk(es_connection, docs)
    return success if success else failed


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    es_connection = get_elastic_conn()
    init_index(es_connection=es_connection)
    insert_into_es(docs=documents, es_connection=es_connection)
    # hits = search_elastic_hebrew(string_to_search="סיוע", es=es_connection)
