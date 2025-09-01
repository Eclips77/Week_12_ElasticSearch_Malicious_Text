from json import load
from venv import logger
from elasticsearch import Elasticsearch,helpers
from ..utils import config
from .es_client import ESClient
import logging
import pandas as pd
from ..utils.loader import DataLoader
from src.services import es_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
class ESCrud():
    """"
    crud operations for Elasticsearch index management.
    """
    def __init__(self):
        self.es = ESClient().es
        self.index = config.ES_INDEX
        self.mapping = config.ES_MAPPING

    def create_index(self):
        """"
        Create an Elasticsearch index with the specified mapping."""
        try:
            if not self.es.indices.exists(index=self.index):
                self.es.indices.create(index=self.index, body=self.mapping)
                logger.info(f"Index '{self.index}' created successfully.")
            else:
                logger.info(f"Index '{self.index}' already exists.")
        except Exception as e:
            logger.error(f"Error creating index '{self.index}': {e}")
    
    def index_data(self,data:pd.DataFrame):
        """"
        Index data into the Elasticsearch index."""
        try:
            actions = [
                {
                    "_index": self.index,
                    "_source": record
                }
                for record in data.to_dict(orient='records')
            ]
            helpers.bulk(self.es, actions)
            logger.info(f"Indexed {len(actions)} records into '{self.index}'.")
        except Exception as e:
            logger.error(f"Error indexing data into '{self.index}': {e}")

    def update_data(self,field_name:str,update_value:str,query_value:str):
        """
        a method to update data in elastic."""
        try:
            script = {
                "source": f"ctx._source.{field_name} = params.value",
                "lang": "painless",
                "params": {"value": update_value}
            }
            query = {
                "query": {
                    "match": {
                        field_name: query_value
                    }
                }
            }
            response = self.es.update_by_query(index=self.index, body={**query, "script": script})
            logger.info(f"Updated {response['updated']} records in '{self.index}'.")
        except Exception as e:
            logger.error(f"Error updating data in '{self.index}': {e}")

    def delete_data(self):
        """
         a method to delete data in elastic."""
        pass
    def read_data(self):
        """
        a method to read data from elastic."""
        pass
    def search_data(self):
        """
        a method to search a specific data in elastic."""
        pass




if __name__ == "__main__":
    es_crud = ESCrud()
    data = DataLoader().load_from_csv(config.CSV_FILE_PATH)
    es_crud.create_index()
    es_crud.index_data(data)
    # es = ESClient().es
    # print(es.info())