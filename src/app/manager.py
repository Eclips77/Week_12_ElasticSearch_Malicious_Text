from ..utils.loader import DataLoader
from ..utils import config
from ..services.es_crud import ESClient
from ..services.sentimenter import SentimentEnhancer
from ..services.weapon_finder import WeaponsDetector
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class DataManager:
    def __init__(self,)