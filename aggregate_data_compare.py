import time
import pandas as pd
import dask.dataframe as dd
import vaex
from google.cloud import storage

def list_parquet_files(bucket_name, prefix):
    """Lista todos los archivos Parquet en el bucket de GCP."""
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    files = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith('.parquet')]
    return files

def load_data_pandas(files):
    start_time = time.time()
    df = pd.concat([pd.read_parquet(file) for file in files])
    load_time = time.time() - start_time

    # Ordenar por 'Categoria_Producto'
    df_sorted = df.sort_values(by='Categoria_Producto')

    # Agregar datos por 'Categoria_Producto'
    start_time = time.time()
    df_grouped_categoria = df_sorted.groupby('Categoria_Producto', observed=True).agg({'Cantidad_Venta': 'sum'}).reset_index()
    aggregation_time_categoria = time.time() - start_time

    # Agregar datos por 'Region_Venta' y calcular promedio de 'Cantidad_Venta'
    start_time = time.time()
    df_grouped_region = df_sorted.groupby('Region_Venta', observed=True).agg({'Cantidad_Venta': 'mean'}).reset_index()
    aggregation_time_region = time.time() - start_time

    return df_grouped_categoria, df_grouped_region, load_time, aggregation_time_categoria, aggregation_time_region

def load_data_dask(files):
    start_time = time.time()
    ddf = dd.read_parquet(files)
    load_time = time.time() - start_time

    # Ordenar por 'Categoria_Producto'
    ddf_sorted = ddf.sort_values(by='Categoria_Producto')

    # Agregar datos por 'Categoria_Producto'
    start_time = time.time()
    df_grouped_categoria = ddf_sorted.groupby('Categoria_Producto', observed=True).agg({'Cantidad_Venta': 'sum'}).compute().reset_index()
    aggregation_time_categoria = time.time() - start_time

    # Agregar datos por 'Region_Venta' y calcular promedio de 'Cantidad_Venta'
    start_time = time.time()
    df_grouped_region = ddf_sorted.groupby('Region_Venta', observed=True).agg({'Cantidad_Venta': 'mean'}).compute().reset_index()
    aggregation_time_region = time.time() - start_time

    return df_grouped_categoria, df_grouped_region, load_time, aggregation_time_categoria, aggregation_time_region

def load_data_vaex(files):
    start_time = time.time()
    df = vaex.open_many(files)
    load_time = time.time() - start_time

    # Convertir 'Categoria_Producto' a cadena de texto para ordenamiento
    df['Categoria_Producto'] = df['Categoria_Producto'].astype(str)

    # Ordenar por 'Categoria_Producto'
    start_time = time.time()
    df_sorted = df.sort('Categoria_Producto')
    sort_time = time.time() - start_time

    # Agregar datos por 'Categoria_Producto'
    start_time = time.time()
    df_grouped_categoria = df_sorted.groupby('Categoria_Producto', agg={'Cantidad_Venta': 'sum'}).to_pandas_df().reset_index()
    aggregation_time_categoria = time.time() - start_time

    # Agregar datos por 'Region_Venta' y calcular promedio de 'Cantidad_Venta'
    start_time = time.time()
    df_grouped_region = df_sorted.groupby('Region_Venta', agg={'Cantidad_Venta': 'mean'}).to_pandas_df().reset_index()
    aggregation_time_region = time.time() - start_time

    return df_grouped_categoria, df_grouped_region, load_time, sort_time, aggregation_time_categoria, aggregation_time_region


def main(bucket_name, prefix):
    # Listar archivos Parquet en el bucket de GCP
    files = list_parquet_files(bucket_name, prefix)

    # Carga y agregación de datos usando Pandas
    df_grouped_categoria_pandas, df_grouped_region_pandas, load_time_pandas, aggregation_time_categoria_pandas, aggregation_time_region_pandas = load_data_pandas(files)

    # Carga y agregación de datos usando Dask
    df_grouped_categoria_dask, df_grouped_region_dask, load_time_dask, aggregation_time_categoria_dask, aggregation_time_region_dask = load_data_dask(files)

    # Carga y agregación de datos usando Vaex
    df_grouped_categoria_vaex, df_grouped_region_vaex, load_time_vaex, sort_time_vaex, aggregation_time_categoria_vaex, aggregation_time_region_vaex = load_data_vaex(files)

    # Mostrar resultados
    print("Pandas:")
    print("Load Time:", load_time_pandas)
    print("Aggregation Time (Categoria_Producto):", aggregation_time_categoria_pandas)
    print("Results (Categoria_Producto):", df_grouped_categoria_pandas)
    print("Aggregation Time (Region_Venta):", aggregation_time_region_pandas)
    print("Results (Region_Venta):", df_grouped_region_pandas)
    print()

    print("Dask:")
    print("Load Time:", load_time_dask)
    print("Aggregation Time (Categoria_Producto):", aggregation_time_categoria_dask)
    print("Results (Categoria_Producto):", df_grouped_categoria_dask)
    print("Aggregation Time (Region_Venta):", aggregation_time_region_dask)
    print("Results (Region_Venta):", df_grouped_region_dask)
    print()

    print("Vaex:")
    print("Load Time:", load_time_vaex)
    print("Sort Time:", sort_time_vaex)
    print("Aggregation Time (Categoria_Producto):", aggregation_time_categoria_vaex)
    print("Results (Categoria_Producto):", df_grouped_categoria_vaex)
    print("Aggregation Time (Region_Venta):", aggregation_time_region_vaex)
    print("Results (Region_Venta):", df_grouped_region_vaex)
    print()

    # Comparar tiempos de carga y agregación
    load_times = {
        "Pandas": load_time_pandas,
        "Dask": load_time_dask,
        "Vaex": load_time_vaex
    }
    best_load_time = min(load_times, key=load_times.get)

    aggregation_times_categoria = {
        "Pandas": aggregation_time_categoria_pandas,
        "Dask": aggregation_time_categoria_dask,
        "Vaex": aggregation_time_categoria_vaex
    }
    best_aggregation_time_categoria = min(aggregation_times_categoria, key=aggregation_times_categoria.get)

    aggregation_times_region = {
        "Pandas": aggregation_time_region_pandas,
        "Dask": aggregation_time_region_dask,
        "Vaex": aggregation_time_region_vaex
    }
    best_aggregation_time_region = min(aggregation_times_region, key=aggregation_times_region.get)

    print(f"The best library in terms of load time is: {best_load_time}")
    print(f"The best library in terms of aggregation time (Categoria_Producto) is: {best_aggregation_time_categoria}")
    print(f"The best library in terms of aggregation time (Region_Venta) is: {best_aggregation_time_region}")

if __name__ == "__main__":
    bucket_name = 'sintetico99'
    prefix = ''
    main(bucket_name, prefix)
