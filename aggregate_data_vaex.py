import time
import vaex
from google.cloud import storage

def list_parquet_files(bucket_name, prefix):
    """Lista todos los archivos Parquet en el bucket de GCP."""
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    files = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith('.parquet')]
    return files

def load_data_vaex(files):
    start_time = time.time()
    df = vaex.open_many(files)
    load_time = time.time() - start_time

    start_time = time.time()
    df_grouped_categoria = df.groupby('Categoria_Producto', agg={'Cantidad_Venta_sum': vaex.agg.sum('Cantidad_Venta')}).to_pandas_df().reset_index()
    aggregation_time_categoria = time.time() - start_time

    start_time = time.time()
    df_grouped_region = df.groupby('Region_Venta', agg={'Cantidad_Venta_sum': vaex.agg.sum('Cantidad_Venta')}).to_pandas_df().reset_index()
    aggregation_time_region = time.time() - start_time

    return df_grouped_categoria, df_grouped_region, load_time, aggregation_time_categoria, aggregation_time_region

def main(bucket_name, prefix):
    # Listar archivos Parquet en el bucket de GCP
    files = list_parquet_files(bucket_name, prefix)

    # Carga de datos y cálculo de agregaciones usando Vaex
    df_grouped_categoria, df_grouped_region, load_time, aggregation_time_categoria, aggregation_time_region = load_data_vaex(files)

    # Mostrar resultados
    print("Vaex - Agregación por Categoria_Producto:")
    print("Load Time:", load_time)
    print("Aggregation Time:", aggregation_time_categoria)
    print("Results:", df_grouped_categoria)
    print()

    print("Vaex - Agregación por Region_Venta:")
    print("Load Time:", load_time)
    print("Aggregation Time:", aggregation_time_region)
    print("Results:", df_grouped_region)
    print()

if __name__ == "__main__":
    bucket_name = 'sintetico99'
    prefix = ''
    main(bucket_name, prefix)
