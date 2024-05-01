"""
    This code allows to transform data.
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform(df_1: object, df_2: object) -> object:
    """
        This function transform data and take two parameters
        :params: df_1 and df_2 are the data in csv file
        :output: data tranformed
    """

    logger.info('The process of transformation stard.')

    col_drop = ['No Volume', '1er lot', 'Surface Carrez du 1er lot', '2eme lot', 
                'Surface Carrez du 2eme lot', '3eme lot', 'Surface Carrez du 3eme lot', 
                '4eme lot', 'Surface Carrez du 4eme lot', '5eme lot', 'Surface Carrez du 5eme lot']
    
    col_rename = {"no disposition": "no_disposition", "date mutation": "date_mutation", "nature mutation": "nature_mutation", 
                  "valeur fonciere": "valeur_fonciere", "code voie": "code_voie", "code postal": "code_postal", 
                  "code departement": "code_departement", "code commune": "code_commune", "nombre de lots": "nombre_de_lots", 
                  "code type local": "code_type_local", "type local": "type_local", "surface reelle bati": "surface_reelle_bati", 
                  "nombre pieces principales": "nombre_pieces"}

    # Concat data for combine to have one dataframe with nany year
    df_val_g = pd.concat([df_1, df_2], ignore_index = True)

    # Drop the duplicate rows
    df_val_g = df_val_g.drop_duplicates()

    # Delate the columns which have more 90% missing values
    df_val_g = df_val_g.drop(col_drop, axis = 1)

    # Convert the columns to appropriate data type
    df_val_g['Date mutation'] = pd.to_datetime(df_val_g['Date mutation'], format ='%d/%m/%Y')
    df_val_g['Valeur fonciere'] = df_val_g['Valeur fonciere'].str.replace(',', '.')
    df_val_g['Valeur fonciere'] = df_val_g['Valeur fonciere'].astype(float)

    # Replace missing values in numeric columns with mean
    #df_val_g.fillna(df_val_g.mean(), inplace = True)

    # Write columns names in lower
    df_val_g.columns = df_val_g.columns.str.lower()

    # Rename columns
    df_val_g = df_val_g.rename(columns = col_rename)

    logger.info('The process of transformation completed.')

    return df_val_g
