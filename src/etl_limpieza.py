# Pseudo codigo
#1. Leer archivo .csv
#2. realizar limpieza
#3. guardar nuevo archivo en formato .csv

from fileinput import filename
from pkgutil import get_data
import pandas as pd
import os
from pathlib import Path

def main():

    filename  = "llamadas123_julio_2022.csv"
    # leer archivo
    data = get_data(filename = filename)
    # extraer resumen
    df_resumen = get_summary(data)
    # garde el resumen
    save_data(df_resumen, filename)

def save_data(df, filename):
    #Guardar la tabla

    out_name = 'Etl_Limpieza_' + filename
    root_dir = Path(".").resolve()
    out_path = os.path.join(root_dir, 'data', 'processed', out_name)
    #print(out_path)

    df.to_csv(out_path)

def get_summary(data):
    # Craer unn diccionario vacio
    dict_resume= dict()

    for col in data.columns:

        data = data.drop_duplicates()

        data = data.fillna({'UNIDAD': 'SIN_DATO'})

        df_resumen = data

        return df_resumen

def get_data(filename):
    data_dir = "raw"
    root_dir = Path(".").resolve()
    file_path = os.path.join(root_dir, "data", data_dir, filename)

    data = pd.read_csv(file_path, encoding='latin-1', sep=';')
    #print(data.shape)
    return data

if __name__ == '__main__':
    main()