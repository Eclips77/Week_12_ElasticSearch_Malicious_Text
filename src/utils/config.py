import os
# from typing import Dict, Any

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DATA_DIR = os.path.join(BASE_DIR, "data")

CSV_FILE_PATH = os.path.join(DATA_DIR, "tweets3.csv")
WEAPONS_FILE_PATH = os.path.join(DATA_DIR, "weapon_list.txt")


# CSV_FILE_PATH = os.getenv("CSV_PATH","C:/Users/brdwn/Desktop/my_projects/Python/Elastic/Week_12_ElasticSearch_Malicious_Text/data/tweets_injected 3.csv")
# WEAPONS_FILE_PATH = os.getenv("WEAPONS_PATH","C:/Users/brdwn/Desktop/my_projects/Python/Elastic/Week_12_ElasticSearch_Malicious_Text/data/weapon_list (1).txt")

ES_HOST = os.getenv("ES_HOST","http://elasticsearch:9200")

ES_INDEX = os.getenv("ES_INDEX","tweets")

ES_MAPPING = {
  "mappings": {
    "properties": {
      "TweetID": { 
        "type": "text"
        # "ignore_above": 32766 
      },
      "CreateDate": { 
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ssXXX||strict_date_optional_time||epoch_millis",
        "ignore_malformed": True,
        "null_value": None
      },
      "Antisemitic": { 
        "type": "boolean" 
      },
      "text": {
        "type": "text",
        "fields": {
          "raw": { 
            "type": "keyword", 
            "ignore_above": 32766 
          }
        }
      },
      "sentiment": { 
        "type": "keyword" 
      }, 
      "weapons": { 
        "type": "keyword"
      }
    }
  }
}

DELETE_QUERY = {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": False}},
                        {"bool": {"must_not": {"exists": {"field": "weapons"}}}},
                        {"bool": {"must_not": {"term": {"sentiment": "negative"}}}}
                    ]
                }
            }
        