import os
# from typing import Dict, Any


CSV_FILE_PATH = os.getenv("CSV_PATH","C:/Users/brdwn/Desktop/my_projects/Python/Elastic/Week_12_ElasticSearch_Malicious_Text/data/tweets_injected 3.csv")
TXT_FILE_PATH = os.getenv("TXT_PATH","C:/Users/brdwn/Desktop/my_projects/Python/Elastic/Week_12_ElasticSearch_Malicious_Text/data/tweets_injected 3.txt")

ES_HOST = os.getenv("ES_HOST","http://localhost:9200")

ES_INDEX = os.getenv("ES_INDEX","tweets")

ES_MAPPING ={
  "mappings": {
    "properties": {
      "TweetID": { "type": "keyword", "ignore_above": 32766 },
      "CreateDate": { "type": "date", "format": "yyyy-MM-dd HH:mm:ssXXX||strict_date_optional_time||epoch_millis" },
      "Antisemitic": { "type": "boolean" },
      "text": {
        "type": "text",
        "fields": {
          "raw": { "type": "keyword", "ignore_above": 32766 }
        }
      },
      "sentiment": { "type": "keyword" }, 
      "weapons": { "type": "keyword" }
    }
  }
}
