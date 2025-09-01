import logging
import pandas as pd

logger = logging.getLogger(__name__)

class DataLoader:
    """"
    loader class for svc files or txt
    """
    @staticmethod
    def load_from_csv(file_path: str)->pd.DataFrame:
        """"
        a method to load data from csv file
        """
        try:
            df = pd.DataFrame(file_path)
            logger.info("data loaded secssefuly")
            return df
        except Exception as e:
            logger.error(f"error loading csv file {e}")
            raise e

    @staticmethod
    def load_from_txt(file_path: str)->pd.DataFrame:
        """"
        a method to load data from txt file
        """
        try:
            df = pd.read_csv(file_path, delimiter="\t", header=None)
            logger.info("data loaded secssefuly")
            return df
        except Exception as e:
            logger.error(f"error loading txt file {e}")
            raise e