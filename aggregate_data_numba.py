import pandas as pd
import numba
import time
from google.cloud import storage

def list_parquet_files(bucket_name, prefix, file_pattern):
    """Lista todos los archivos Parquet que coincidan con el patrón en el bucket."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)  # Lista los blobs en el bucket
    files = []
    for blob in blobs:
        if file_pattern in blob.name:
            files.append(f"gs://{bucket_name}/{blob.name}")
    if not files:
        raise ValueError("No files found matching the pattern.")
    return files

def load_parquet_from_gcs(files):
    """Carga múltiples archivos Parquet desde Google Cloud Storage utilizando Pandas."""
    dfs = []
    for file in files:
        dfs.append(pd.read_parquet(file, engine='pyarrow'))
    return pd.concat(dfs)

@numba.jit
def calculate_aggregations(data):
    """Función optimizada para calcular sumas y promedios, asumiendo data como Pandas DataFrame."""
    categories = data['Categoria_Producto'].unique()
    sums = {}
    for category in categories:
        mask = data['Categoria_Producto'] == category
        sums[category] = data.loc[mask, 'Cantidad_Venta'].sum()  # Suma de Cantidad_Venta
    return sums

def main(bucket_name, prefix, file_pattern):
    files = list_parquet_files(bucket_name, prefix, file_pattern)
    df = load_parquet_from_gcs(files)

    start_time = time.time()
    results = calculate_aggregations(df)
    operation_time = time.time() - start_time

    print(f"Optimized calculations completed in {operation_time} seconds")
    print("Results:", results)

if __name__ == '__main__':
    bucket_name = 'sintetico99'
    prefix = ''  # Subdirectorio, si los archivos están directamente en el bucket
    file_pattern = 'chunk_'  # Asegúrate de que este es el patrón correcto del archivo
    main(bucket_name, prefix, file_pattern)
