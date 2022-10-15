# Pseudo codigo
#1. Leer archivo .csv
#2. extraer el resumen
#3. guardar el resumen en formato .csv

from fileinput import filename
from pkgutil import get_data
import pandas as pd
import os
from pathlib import Path

#root_dir = Path(".").reolve()
root_dir = 'gs://jsanchez_bucket_llamadas123'

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

    out_name = 'resumen_' + filename
    #root_dir = Path(".").resolve()
    out_path = os.path.join(root_dir, 'data', 'processed', out_name)
    #print(out_path)

    df.to_csv(out_path)
    
    print('Guardando en BQ')
    #Guardar la tabla en BigQuery
    df.to_gbq(destination_table = 'EspBigData.llamadas_123', )

def get_summary(data):
    # Craer unn diccionario vacio
    dict_resume= dict()

    for col in data.columns:
        valores_unicos = data[col].unique()
        n_valores = len(valores_unicos)
        #print(col, n_valores)
        dict_resume[col] = n_valores

        df_resumen = pd.DataFrame.from_dict(dict_resume, orient='index')
        df_resumen.rename({0: 'Count'}, axis=1, inplace=True)
        
        print('get_summary')
        print(df_resumen.head())
        
        return df_resumen

def get_data(filename):
    data_dir = "raw"
    #root_dir = Path(".").resolve()
    file_path = os.path.join(root_dir, "data", data_dir, filename)

    data = pd.read_csv(file_path, encoding='latin-1', sep=';')
    print('get_data')
    print('La tabla contiene', data.shape[0],'filas', data.shape[1],'columnas')
    return data

if __name__ == '__main__':
    main()