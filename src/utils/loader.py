import logging
import pandas as pd
import csv
logger = logging.getLogger(__name__)

# class DataLoader:
#     """
#     loader class for csv files or txt
#     """
#     @staticmethod
#     def load_from_csv(file_path: str)->pd.DataFrame:
#         """
#         a method to load data from csv file
#         """
#         try:
#             df = pd.read_csv(file_path)
#             logger.info("data loaded successfully")
#             return df
#         except Exception as e:
#             logger.error(f"error loading csv file {e}")
#             raise

#     @staticmethod
#     def load_from_txt(file_path: str)->pd.DataFrame:
        # """
        # a method to load data from txt file (tab-delimited)
        # """
        # try:
        #     df = pd.read_csv(file_path, delimiter="\t", header=None)
        #     logger.info("data loaded successfully")
        #     return df
        # except Exception as e:
        #     logger.error(f"error loading txt file {e}")
        #     raise

class DataLoader:
    
    @staticmethod
    def load_csv(file_path: str)->list[dict]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = csv.DictReader(f)
                logger.info("CSV file loaded successfully.")
            return list(data)
        except Exception as e:  
            logger.error(f"Error loading CSV file: {e}")
            raise
    
    @staticmethod
    def load_txt(file_path: str)->list:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = [line.strip() for line in f if line.strip()]
                logger.info("TXT file loaded successfully.")
            return data
        except Exception as e:
            logger.error(f"Error loading TXT file: {e}")
            raise
