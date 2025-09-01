from venv import logger
from elasticsearch import Elasticsearch,helpers
from ..utils import config
from .es_client import ESClient
import logging
import json

logger = logging.getLogger(__name__)


class ESCrud():
    def __init__(self):
        self.es = ESClient()
        self.index = config.ES_INDEX
        self.mapping = config.ES_MAPPING

    def create_index(self):
        try:
            if not self.es.indices.exists(index=self.index):
                self.es.indices.create(index=self.index, body=self.mapping)
                logger.info(f"Index '{self.index}' created successfully.")
            else:
                logger.info(f"Index '{self.index}' already exists.")
        except Exception as e:
            logger.error(f"Error creating index '{self.index}': {e}")