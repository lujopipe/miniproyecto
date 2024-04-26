import time
import dask.dataframe as dd
from google.cloud import storage

def list_parquet_files(bucket_name, prefix):
    """Lista todos los archivos Parquet en el bucket de GCP."""
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    files = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith('.parquet')]
    return files

def load_data_dask_categoria(files):
    start_time = time.time()
    ddf = dd.read_parquet(files)
    load_time = time.time() - start_time

    start_time = time.time()
    df_grouped = ddf.groupby('Categoria_Producto', observed=True).agg({'Cantidad_Venta': 'sum'}).compute().reset_index()
    aggregation_time = time.time() - start_time

    return df_grouped, load_time, aggregation_time

def load_data_dask_region(files):
    start_time = time.time()
    ddf = dd.read_parquet(files)
    load_time = time.time() - start_time

    start_time = time.time()
    df_grouped = ddf.groupby('Region_Venta', observed=True).agg({'Cantidad_Venta': 'sum'}).compute().reset_index()
    aggregation_time = time.time() - start_time

    return df_grouped, load_time, aggregation_time

def main(bucket_name, prefix):
    # Listar archivos Parquet en el bucket de GCP
    files = list_parquet_files(bucket_name, prefix)

    # Carga de datos y cálculo de agregaciones por Categoria_Producto usando Dask
    df_grouped_categoria, load_time_categoria, aggregation_time_categoria = load_data_dask_categoria(files)

    # Carga de datos y cálculo de agregaciones por Region_Venta usando Dask
    df_grouped_region, load_time_region, aggregation_time_region = load_data_dask_region(files)

    # Mostrar resultados de agregaciones por Categoria_Producto
    print("Dask - Agregación por Categoria_Producto:")
    print("Load Time:", load_time_categoria)
    print("Aggregation Time:", aggregation_time_categoria)
    print("Results:", df_grouped_categoria)
    print()

    # Mostrar resultados de agregaciones por Region_Venta
    print("Dask - Agregación por Region_Venta:")
    print("Load Time:", load_time_region)
    print("Aggregation Time:", aggregation_time_region)
    print("Results:", df_grouped_region)
    print()

if __name__ == "__main__":
    bucket_name = 'sintetico99'
    prefix = ''
    main(bucket_name, prefix)
