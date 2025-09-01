import csv
from elasticsearch import helpers, Elasticsearch
from src.utils import config


class DataLoader:
    def __init__(self, es_client: Elasticsearch):
        self.es_client = es_client

    def load_data_from_csv(self, index_name: str):
        with open(config.CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            actions = [
                {
                    "_index": index_name,
                    "_source": row
                }
                for row in reader
            ]
            helpers.bulk(self.es_client, actions)

    def load_data_from_txt(self, index_name: str):
        with open(config.TXT_FILE_PATH, mode='r', encoding='utf-8') as file:
            lines = file.readlines()
            actions = [
                {
                    "_index": index_name,
                    "_source": {"text": line.strip()}
                }
                for line in lines
            ]
            helpers.bulk(self.es_client, actions)