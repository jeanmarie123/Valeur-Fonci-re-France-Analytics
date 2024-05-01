"""
    Ce module permet d'extraire les données du site internet open source du gouvernement
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s') 
fh = logging.FileHandler('etl_log.log')

def extract(filepath: object, select_col: list) -> object :
    """
        params : filepath de type str qui est le lien du site ou les données se trouvent
        output : Dataframe pandas extrait sous format CSV
    """
    try:
        # Lecture du fichier text
        logger.info('Stard extraction process.')
        df_val = pd.read_csv(filepath, delimiter = "|", low_memory = False)
        df_val = df_val[select_col]
        logger.info(f'{filepath} : extracted correctly {filepath} record from csv file')

    except Exception as e:
        logger.exception(f'{filepath} : exception {e} encountered while extracting the csv_file_name file')
        df_val = pd.DataFrame()
        logger.info('The process of extraction completed.')
    return df_val 
