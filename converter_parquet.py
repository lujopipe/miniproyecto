import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import math
import os

def csv_to_parquet_by_chunks(csv_file_path, parquet_file_path, chunk_size=5000000):
    # Carga los datos desde un archivo CSV
    df = pd.read_csv(csv_file_path)

    # Optimiza los tipos de datos
    df = optimize_data_types(df)

    # Número total de registros
    total_records = len(df)
    num_chunks = math.ceil(total_records / chunk_size)

    # Divide los datos en trozos y escribe cada trozo a un archivo Parquet
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        chunk = df.iloc[start_idx:end_idx]

        # Convierte el DataFrame de pandas a una tabla de PyArrow
        table = pa.Table.from_pandas(chunk)

        # Construye el nombre del archivo para cada trozo
        chunk_file_path = os.path.join(parquet_file_path, f'chunk_{i}.parquet')

        # Escribe la tabla a un archivo Parquet
        pq.write_table(table, chunk_file_path)
        print(f"Datos del trozo {i} escritos en formato Parquet en '{chunk_file_path}'")

def optimize_data_types(df):
    """Optimiza los tipos de datos de las columnas para mejorar el rendimiento y reducir el tamaño del archivo."""
    for col in df.columns:
        if df[col].dtype == 'object':
            num_unique_values = df[col].nunique()
            num_total_values = len(df[col])
            if num_unique_values / num_total_values < 0.5:
                df[col] = df[col].astype('category')
        elif col in ['Fecha_Transaccion']:  # Columna Fecha transaccion
            df[col] = pd.to_datetime(df[col])
    return df

if __name__ == '__main__':
    csv_file = 'data/generated_data.csv'
    parquet_file = 'data/'
    csv_to_parquet_by_chunks(csv_file, parquet_file)
