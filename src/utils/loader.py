import logging
import pandas as pd

logger = logging.getLogger(__name__)

class DataLoader:
    """
    loader class for csv files or txt
    """
    @staticmethod
    def load_from_csv(file_path: str)->pd.DataFrame:
        """
        a method to load data from csv file
        """
        try:
            df = pd.read_csv(file_path)
            logger.info("data loaded successfully")
            s = pd.to_datetime(df["CreateDate"], errors="coerce", utc=True)
            df["CreateDate"] = s.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            return df
        except Exception as e:
            logger.error(f"error loading csv file {e}")
            raise

    @staticmethod
    def load_from_txt(file_path: str)->pd.DataFrame:
        """
        a method to load data from txt file (tab-delimited)
        """
        try:
            df = pd.read_csv(file_path, delimiter="\t", header=None)
            logger.info("data loaded successfully")
            return df
        except Exception as e:
            logger.error(f"error loading txt file {e}")
            raise