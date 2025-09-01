import pandas as pd
import logging
logger = logging.getLogger(__name__)

class DataLoader:
    """"
    DataLoader class for loading CSV and TXT files.
    """
    @staticmethod
    def load_data(file_path:str)-> pd.DataFrame:
        """Load a CSV file.

        Args:
            file_path (str): Path to the CSV file.

        Returns:
            pandas.DataFrame: Loaded DataFrame.

        Usage:
            df = DataLoader.load_data("data.csv")
        """
        try:
            df = pd.read_csv(file_path)
            logger.info("data loaded secssefuly")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    @staticmethod
    def load_txt(file_path:str)->list:
        """Load a TXT file.

        Args:
            file_path (str): Path to the TXT file.

        Returns:
            list: List of lines from the TXT file.

        Usage:
            lines = DataLoader.load_txt("data.txt")
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
            logger.info("TXT data loaded secssefuly")
            return lines
        except Exception as e:
            logger.error(f"Error loading TXT data: {e}")
            raise
