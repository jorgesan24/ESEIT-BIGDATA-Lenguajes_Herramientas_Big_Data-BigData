# Pseudo codigo
#1. Leer archivo .csv
#2. realizar limpieza
#3. guardar nuevo archivo en formato .csv

import pandas as pd
import os
import numpy as np
from pathlib import Path
from dateutil.parser import parse

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
        df.loc[df['CODIGO_LOCALIDAD']==9,'LOCALIDAD']='Fontibon'
        df.loc[df['CODIGO_LOCALIDAD']==10,'LOCALIDAD']='Engativa'
        df.loc[df['CODIGO_LOCALIDAD']==14,'LOCALIDAD']='Los Martires'
        df.loc[df['CODIGO_LOCALIDAD']==15,'LOCALIDAD']='Antonio Nariño'
        df.loc[df['CODIGO_LOCALIDAD']==19,'LOCALIDAD']='Ciudad Bolivar'

        data['LOCALIDAD'] = df['LOCALIDAD']

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