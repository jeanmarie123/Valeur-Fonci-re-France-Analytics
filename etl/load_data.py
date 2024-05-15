#import datetime
import pandas as pd

import logging

logger = logging.getLogger(__name__)


# Load data in bronze layer           
def load_bronze(data_from_website, name_file_from_wb):
    """
        This function load data in bronze layer couche.
        :params: dataframe transformed and name of file
    """
    logger.info('The process of loading data in bronze layer stard.')

    file_name = f'{name_file_from_wb}'+'.csv'

    data_from_website.to_csv(
        'abfs://bronze@cmptvaleurfoncierefrance.dfs.core.windows.net/data-raw-from-website/' + file_name,
        storage_options = {'account_key' : 'mRK2LEofR0TZ54DVwymTtfmwqK1fiFs7FhJJ8I9FBwRfExBaWf44bn6ktR76UGLlLRZsDmRJEIKf+ASt/Yu2OA=='},
        index = False
    )
    logger.info(f'Data of {name_file_from_wb} load correctly in bronze layer.')


# Load data in silver layer
def load_silver(df_tranformed: object, name_file_transformed: str):
    """
        This function load data in silver layer couche.
        :params: dataframe transformed and name of file
    """

    logger.info('The process of loading data in silver layer stard.')

    file_name = f'{name_file_transformed}'+'.csv'

    df_tranformed.to_csv(
        'abfs://silver@cmptvaleurfoncierefrance.dfs.core.windows.net/row-transfomed/' + file_name,
        storage_options = {'account_key' : 'mRK2LEofR0TZ54DVwymTtfmwqK1fiFs7FhJJ8I9FBwRfExBaWf44bn6ktR76UGLlLRZsDmRJEIKf+ASt/Yu2OA=='},
        index = False
    )
    logger.info(f'Data of {name_file_transformed} load correctly in silver layer.')


# Load data in gold layer
def load_gold(df_tranformed_for_business: object, name_file_transformed: str):
    """
        This function load data in gold layer couche.
        :params: dataframe transformed and name of file
    """

    logger.info('The process of loading data in gold layer stard.')

    file_name = f'{name_file_transformed}'+'.csv'

    df_tranformed_for_business.to_csv(
        'abfs://gold@cmptvaleurfoncierefrance.dfs.core.windows.net/data-for-business/' + file_name,
        storage_options = {'account_key' : 'mRK2LEofR0TZ54DVwymTtfmwqK1fiFs7FhJJ8I9FBwRfExBaWf44bn6ktR76UGLlLRZsDmRJEIKf+ASt/Yu2OA=='},
        index = False
    )
    logger.info(f'Data of {name_file_transformed} load correctly in gold layer.')
