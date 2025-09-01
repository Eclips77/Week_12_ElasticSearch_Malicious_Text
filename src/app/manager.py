from ..utils.loader import DataLoader
from ..utils import config
from ..services.es_crud import ESCrud
from ..services.sentimenter import SentimentEnhancer
from ..services.weapon_finder import WeaponsDetector

class DataManager:
    def __init__(self):
        self.es_crud = ESCrud()
        self.sentiment_enhancer = SentimentEnhancer()
        self.weapons_detector = None

    def run_full_pipeline(self):
        tweets_data = DataLoader.load_csv(config.CSV_FILE_PATH)
        
        self.es_crud.create_index()
        self.es_crud.index_data(tweets_data)
        
        indexed_data = self.es_crud.search_data({"match_all": {}})
        
        sentiment_enriched = self.sentiment_enhancer.enrich_documents(indexed_data)
        self.es_crud.index_data(sentiment_enriched)
        
        updated_data = self.es_crud.search_data({"match_all": {}})
        
        if self.weapons_detector:
            weapons_enriched = self.weapons_detector.detect_weapons(updated_data)
            self.es_crud.index_data(weapons_enriched)
        
        return len(tweets_data)

    def setup_weapons_detector(self):
        weapons_list = DataLoader.load_txt(config.WEAPONS_FILE_PATH)
        self.weapons_detector = WeaponsDetector(weapons_list)

    def search_tweets(self, query):
        return self.es_crud.search_data(query)

    def delete_index_data(self, query):
        self.es_crud.delete_data(query)