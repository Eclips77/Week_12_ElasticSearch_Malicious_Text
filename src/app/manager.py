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
        self.loader = DataLoader()
        self.es_crud = ESCrud()
        self.sentiment_enhancer = SentimentEnhancer()
        self.weapons_detector = None

    def load_data(self):
        tweets_data = self.loader.load_csv(config.CSV_FILE_PATH)
        return tweets_data