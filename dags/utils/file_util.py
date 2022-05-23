import pandas as pd

def cargar_datos(name):
    df = pd.read_csv("/opt/airflow/data/" + name + ".csv", sep=',', encoding='utf-8', encoding_errors='ignore', index_col=False)
    return df

def guardar_datos(df, nombre):
    df.to_csv('/opt/airflow/data/' + nombre + '.csv' , encoding='utf-8', encoding_errors='ignore', sep=',', index=False)