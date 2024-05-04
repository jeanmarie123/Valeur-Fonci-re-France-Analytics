# Import modules
import yaml

#from etl import*

from etl.extract_data import (
    extract,
    extract_dpt
)
from etl.transform_data import transform
from etl.load_data import (
    load_bronze,
    load_silver,
    load_gold
)

# Import pipeline configuration
with open('config.yaml', 'r') as file:
    config_data = yaml.safe_load(file)


def run_pipeline():

    # Extract data
    df_vaf_21 = extract(config_data['val_fon_21'], config_data['select_col_list'])
    df_vaf_22 = extract(config_data['val_fon_22'], config_data['select_col_list'])
    df_dpt_fr = extract_dpt(config_data['france_dept'])


    # Transform data
    df_val_gb = transform(df_vaf_21, df_vaf_22)

    # Load data
    load_bronze(df_vaf_21, 'Valeurs_foncières_2021')
    load_bronze(df_vaf_21, 'Valeurs_foncières_2022')
    
    load_silver(df_val_gb, 'valeur_fonciere_transfome')

    load_gold(df_val_gb, 'valeur_fonciere_gold')
    load_gold(df_dpt_fr, 'departement_france')


if __name__=="__main__":
    run_pipeline()
