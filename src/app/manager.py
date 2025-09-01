from ..utils.loader import DataLoader
from ..utils import config
from ..services.es_crud import ESCrud
from ..services.sentimenter import SentimentEnhancer
from ..services.weapon_finder import WeaponsDetector
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class DataManager:
    def __init__(self):
        self.es_crud = ESCrud()
        self.sentiment_enhancer = SentimentEnhancer()
        self.weapons_detector = WeaponsDetector()

    def _load_tweets_data(self):
        tweets_data = DataLoader.load_csv(config.CSV_FILE_PATH)
        return tweets_data
    
    def index_to_es(self):
        self.es_crud.create_index()  # Added missing parentheses
        self.es_crud.index_data(self._load_tweets_data())

    def add_sentiment(self):
        self.sentiment_enhancer.enrich_documents()
    
    def detect_weapons(self):
        self.weapons_detector.detect_weapons()
    
    def run_full_pipeline(self):
        """Execute the complete project workflow"""
        logger.info("Starting full data processing pipeline")
        
        try:
            # Step 1: Index data to Elasticsearch
            logger.info("Step 1: Indexing data to Elasticsearch")
            self.index_to_es()
            
            # Step 2: Add sentiment analysis
            logger.info("Step 2: Adding sentiment analysis")
            self.add_sentiment()
            
            # Step 3: Detect weapons/malicious content
            logger.info("Step 3: Detecting weapons/malicious content")
            self.detect_weapons()
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise