from elasticsearch import Elasticsearch,helpers
from ..utils import config
from .es_client import ESClient
import logging
# import pandas as pd
from ..utils.loader import DataLoader

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
    
    def index_data(self,data:list[dict]):
        """"
        Index data into the Elasticsearch index.
        Args:
            data (List[Dict[str, Any]]): List of documents to index
        Returns:
            None"""
        try:
            actions = [
                {
                    "_index": self.index,
                    "_source": record
                }
                for record in data
            ]
            helpers.bulk(self.es, actions)
            logger.info(f"Indexed {len(actions)} records into '{self.index}'.")
        except Exception as e:
            logger.error(f"Error indexing data into '{self.index}': {e}")

    def delete_data(self,query:dict):
        """"
        Delete data from the Elasticsearch index based on a query.
        Args:
            query (dict): Elasticsearch query DSL
        Returns:
            None"""
        try:
            response = self.es.delete_by_query(index=self.index, body={"query": query})
            deleted = response.get('deleted', 0)
            logger.info(f"Deleted {deleted} records from '{self.index}'.")
        except Exception as e:
            logger.error(f"Error deleting data from '{self.index}': {e}")

    def search_data(self,query:dict)->list[dict]:
        """"
        Search data in the Elasticsearch index based on a query.
        Args:
            query (dict): Elasticsearch query DSL
        Returns:
            List[Dict[str, Any]]: List of documents matching the query"""
        try:
            response = self.es.search(index=self.index, body={"query": query})
            hits = response.get('hits', {}).get('hits', [])
            results = [hit['_source'] for hit in hits]
            logger.info(f"Found {len(results)} records in '{self.index}'.")
            return results
        except Exception as e:
            logger.error(f"Error searching data in '{self.index}': {e}")
            return []



if __name__ == "__main__":
    es_crud = ESCrud()
    data = DataLoader().load_csv(config.CSV_FILE_PATH)
    es_crud.create_index()
    es_crud.index_data(data)
    # es = ESClient().es
    # print(es.info())