# Pseudo codigo
#1. Leer archivo .csv
#2. realizar limpieza
#3. guardar nuevo archivo en formato .csv

import pandas as pd
import os
import numpy as np
from pathlib import Path
from dateutil.parser import parse
import logging

def main():
    
    #Basic configuration for logging
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    logging.captureWarnings(True)
    logging.basicConfig(
        level=logging.INFO, 
        format=log_fmt,
    filename='data/logs/etl_llamadas.log'
    )
    
    filename  = "datos_abiertos_abril_2022.csv"
    # leer archivo
    data = get_data(filename = filename)
    # extraer resumen
    df_resumen = get_summary(data)
    # garde el resumen
    save_data(df_resumen, filename)

def save_data(df, filename):
    #Guardar la tabla
    logger = logging.getLogger('save_data')

    out_name = 'Etl_Limpieza_' + filename
    #root_dir = Path(".").resolve()
    bucket = "gs://jsanchez_bucket_llamadas123"
    out_path = os.path.join(bucket, 'data', 'processed', out_name)
    
    logger.info(f'Saving data in {out_path}')
    df.to_csv(out_path)
    
    print('Guardando en BQ')
    #Guardar la tabla en BigQuery
    table_name = 'composite-haiku-364223.EspBigData.llamadas_123'
    logger.info(f'Saving table {table_name} into BigQuery')
    
    df.to_gbq(table_name, if_exists='replace')

def get_summary(data):
    # Craer unn diccionario vacio
    dict_resume= dict()
    
    #Unificar nombre se las columnas
    data.columns = ['NUMERO_INCIDENTE', 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL', 'CODIGO_LOCALIDAD', 'LOCALIDAD', 'EDAD', 
                    'UNIDAD', 'GENERO','RED','TIPO_INCIDENTE','PRIORIDAD', 'RECEPCION']

    for col in data.columns:

        data = data.drop_duplicates()

        data = data.fillna({'UNIDAD': 'SIN_DATO'})

        col = 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'
        data[col] = pd.to_datetime(data[col], errors='coerce')
            
        data['RECEPCION'] = pd.to_datetime(data['RECEPCION'], errors='coerce')

        data['EDAD'] = data['EDAD'].replace({'SIN_DATO' : np.nan})

        f = lambda x: x if pd.isna(x) == True else int(x)
        data['EDAD'] = data['EDAD'].apply(f)

        df = pd.DataFrame(data,columns=['CODIGO_LOCALIDAD','LOCALIDAD'])

        df.loc[df['CODIGO_LOCALIDAD']==1,'LOCALIDAD']='Usaquen'
        df.loc[df['CODIGO_LOCALIDAD']==2,'LOCALIDAD']='Chapinero'
        df.loc[df['CODIGO_LOCALIDAD']==3,'LOCALIDAD']='Santa Fe'
        df.loc[df['CODIGO_LOCALIDAD']==4,'LOCALIDAD']='San Cristobal'
        df.loc[df['CODIGO_LOCALIDAD']==5,'LOCALIDAD']='Usme'
        df.loc[df['CODIGO_LOCALIDAD']==6,'LOCALIDAD']='Tunjuelito'
        df.loc[df['CODIGO_LOCALIDAD']==7,'LOCALIDAD']='Bosa'
        df.loc[df['CODIGO_LOCALIDAD']==8,'LOCALIDAD']='Kennedy'
        df.loc[df['CODIGO_LOCALIDAD']==9,'LOCALIDAD']='Fontibon'
        df.loc[df['CODIGO_LOCALIDAD']==10,'LOCALIDAD']='Engativa'
        df.loc[df['CODIGO_LOCALIDAD']==11,'LOCALIDAD']='Suba'
        df.loc[df['CODIGO_LOCALIDAD']==12,'LOCALIDAD']='Barrios Unidos'
        df.loc[df['CODIGO_LOCALIDAD']==13,'LOCALIDAD']='Teusaquillo'
        df.loc[df['CODIGO_LOCALIDAD']==14,'LOCALIDAD']='Los Martires'
        df.loc[df['CODIGO_LOCALIDAD']==15,'LOCALIDAD']='Antonio Nariño'
        df.loc[df['CODIGO_LOCALIDAD']==16,'LOCALIDAD']='Puente Aranda'
        df.loc[df['CODIGO_LOCALIDAD']==17,'LOCALIDAD']='Candelaria'
        df.loc[df['CODIGO_LOCALIDAD']==18,'LOCALIDAD']='Rafael Uribe Uribe'
        df.loc[df['CODIGO_LOCALIDAD']==19,'LOCALIDAD']='Ciudad Bolivar'
        df.loc[df['CODIGO_LOCALIDAD']==20,'LOCALIDAD']='Sumapaz'

        data['LOCALIDAD'] = df['LOCALIDAD']
        
    for col in data.columns:
        valores_unicos = data[col].unique()
        n_valores = len(valores_unicos)
        #print(col, n_valores)
        dict_resume[col] = n_valores

        df_resumen = pd.DataFrame.from_dict(dict_resume, orient='index')
        df_resumen.rename({0: 'Count'}, axis=1, inplace=True)

        df_resumen = data

        return df_resumen

def get_data(filename):
    logger = logging.getLogger('get_data')
    
    data_dir = "raw"
    #root_dir = Path(".").resolve()
    bucket = "gs://jsanchez_bucket_llamadas123"
    file_path = os.path.join(bucket, "data", data_dir, filename)
    
    logger.info(f'Reading  file: {file_path}')
    
    data = pd.read_csv(file_path, encoding='latin-1', sep=';')
    #print(data.shape)
    
    logger.info(f'La tabla contiene {data.shape[0]} filas y {data.shape[1]} columnas')
        
    logging.info('DONE!!!')
    return data
if __name__ == '__main__':
    main()