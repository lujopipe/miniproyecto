import pandas as pd
import time

#AGREGACION DE PANDAS

def read_and_aggregate_parquet(bucket_name, prefix=''):
    """Lee y agrega datos de archivos Parquet almacenados en GCS."""
    start_time = time.time()  # Comienza a medir el tiempo
     # Path completo al bucket y prefijo donde están almacenados los archivos Parquet
    gcs_path = f'gs://{bucket_name}/{prefix}'
    
    try:
        df = pd.read_parquet(gcs_path, engine='pyarrow')
        print(df.head())
        print(df.columns)
        # Ejemplo de agregación: suma de 'Cantidad_Venta' por 'Categoria_Producto'
        # Asegurando el comportamiento futuro con observed=True
        aggregated_data = df.groupby('Categoria_Producto', observed=True)['Cantidad_Venta'].sum().reset_index()
        print(aggregated_data)
    except Exception as e:
        print(f"Error al leer archivos Parquet: {e}")

    end_time = time.time()  # Finaliza la medición del tiempo
    print(f"Tiempo de ejecución del script: {end_time - start_time} segundos.")

if __name__ == '__main__':
    bucket_name = 'sintetico99' # Nombre correcto del bucket
    prefix = '' #subdirectorio
    read_and_aggregate_parquet(bucket_name, prefix)
