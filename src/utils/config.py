import os
# from typing import Dict, Any


CSV_FILE_PATH = os.getenv("CSV_PATH","C:/Users/brdwn/Desktop/my_projects/Python/Elastic/Week_12_ElasticSearch_Malicious_Text/data/tweets_injected 3.csv")
TXT_FILE_PATH = os.getenv("TXT_PATH","C:/Users/brdwn/Desktop/my_projects/Python/Elastic/Week_12_ElasticSearch_Malicious_Text/data/tweets_injected 3.txt")

ES_HOST = os.getenv("ES_HOST","http://localhost:9200")

ES_INDEX = os.getenv("ES_INDEX","tweets")

ES_MAPPING = {
    "mappings": {
        "properties": {
            "TweetID": {"type": "keyword"},
            "CreateDate": {"type": "date"},
            "Antisemitic": {"type": "boolean"},
            "text": {"type": "text"},
            "sentiment": {"type": "keyword"},
            "weapons": {"type": "array", "items": {"type": "keyword"}},
        }
    }
}