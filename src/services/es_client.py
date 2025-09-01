from venv import logger
from elasticsearch import Elasticsearch
import logging
from ..utils import config

logger = logging.getLogger(__name__)

class ESClient:
    def __init__(self):
        self.es = Elasticsearch(
            hosts=[config.ES_HOST],
            http_auth=(config.ES_USER, config.ES_PASSWORD)
        )
        if not self.es.ping():
            logger.error("Elasticsearch cluster is down!")
            raise ConnectionError("Elasticsearch cluster is down!")
        else:
            logger.info("Connected to Elasticsearch cluster")