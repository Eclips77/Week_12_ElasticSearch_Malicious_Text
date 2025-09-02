import logging
import csv
logger = logging.getLogger(__name__)

class DataLoader:
    """"
    a class for loading data from files.
    """
    
    @staticmethod
    def load_csv(file_path: str)->list[dict]:
        """
        Load data from a CSV file and return it as a list of dictionaries.
        Args:
            file_path (str): Path to the CSV file.
        Returns:
            List[Dict[str, Any]]: List of records as dictionaries.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = list(csv.DictReader(f))
                logger.info("CSV file loaded successfully.")
            return data
        except Exception as e:  
            logger.error(f"Error loading CSV file: {e}")
            raise
    
    @staticmethod
    def load_txt(file_path: str)->list:
        """
        Load data from a TXT file and return it as a list of strings.
        Args:
            file_path (str): Path to the TXT file.
        Returns:
            List[str]: List of lines from the TXT file.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = [line.strip() for line in f if line.strip()]
                logger.info("TXT file loaded successfully.")
            return data
        except Exception as e:
            logger.error(f"Error loading TXT file: {e}")
            raise
