from elasticsearch import Elasticsearch, helpers
from ..utils import config
from .es_client import ESClient
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ESCrud:
    """
    CRUD operations for Elasticsearch index management.
    """

    def __init__(self):
        self.es = ESClient().es
        self.index = config.ES_INDEX
        self.mapping = config.ES_MAPPING

    def create_index(self):
        """
        Create an Elasticsearch index with the specified mapping.
        """
        try:
            if not self.es.indices.exists(index=self.index):
                self.es.indices.create(index=self.index, body=self.mapping)
                logger.info(f"Index '{self.index}' created successfully.")
            else:
                logger.info(f"Index '{self.index}' already exists.")
        except Exception as e:
            logger.error(f"Error creating index '{self.index}': {e}")

    def index_data(self, data: list[dict]):
        """
        Index a batch of documents into Elasticsearch.
        If a document already has an '_id', it will overwrite the existing document.
        Otherwise, Elasticsearch will generate a new '_id'.
        """
        try:
            actions = []
            for record in data:
                action = {
                    "_index": self.index,
                    "_source": record
                }
                if "_id" in record:
                    action["_id"] = record["_id"]
                    record.pop("_id", None)
                actions.append(action)

            response = helpers.bulk(
                self.es,
                actions,
                refresh=True,
                raise_on_error=False,
                raise_on_exception=False
            )
            success_count = response[0]
            failed_items = response[1]

            if failed_items and isinstance(failed_items, list):
                logger.error(f"Failed to index {len(failed_items)} documents")
                for item in failed_items[:5]:
                    logger.error(f"Error detail: {item}")

            logger.info(f"Successfully indexed {success_count} records into '{self.index}'.")
            return success_count
        except Exception as e:
            logger.error(f"Error indexing data into '{self.index}': {e}")
            return 0

    def delete_data(self, query: dict):
        """
        Delete data from the Elasticsearch index based on a query.
        """
        try:
            response = self.es.delete_by_query(index=self.index, body={"query": query})
            deleted = response.get('deleted', 0)
            logger.info(f"Deleted {deleted} records from '{self.index}'.")
        except Exception as e:
            logger.error(f"Error deleting data from '{self.index}': {e}")

    def search_data(self, query: dict, size: int = 10000) -> list[dict]:
        """
        Search for documents in Elasticsearch.
        Returns both _id and _source for each document.
        """
        try:
            if not query:
                query = {"match_all": {}}

            response = self.es.search(
                index=self.index,
                body={"query": query},
                size=size
            )
            hits = response.get('hits', {}).get('hits', [])
            results = [{"_id": hit["_id"], **hit["_source"]} for hit in hits]
            logger.info(f"Found {len(results)} records in '{self.index}'.")
            return results
        except Exception as e:
            logger.error(f"Error searching data in '{self.index}': {e}")
            return []
