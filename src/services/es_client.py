from elasticsearch import Elasticsearch
import logging
from ..utils import config

logger = logging.getLogger(__name__)

class ESClient:
    """"
    Elasticsearch client for connecting to the Elasticsearch cluster."""
    def __init__(self):
        self.es = Elasticsearch(
            hosts=[config.ES_HOST],
            request_timeout=30
            # http_auth=(config.ES_USER, config.ES_PASSWORD)
        )
        if not self.es.ping():
            logger.error("Elasticsearch cluster is down!")
            raise ConnectionError("Elasticsearch cluster is down!")
        else:
            logger.info("Connected to Elasticsearch cluster")
        
    



