from google.cloud import bigquery
import os

def load_data_to_bigquery(table_id, source_file_path):
    """Carga datos desde un archivo CSV a una tabla de BigQuery, creando la tabla si no existe."""
    client = bigquery.Client()
    
    # Definir el esquema de la tabla
    schema = [
        bigquery.SchemaField("ID_Cliente", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Fecha_Transaccion", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("Cantidad_Venta", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("Categoria_Producto", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Region_Venta", "STRING", mode="REQUIRED"),
    ]
    
    try:
        table = client.get_table(table_id)  # Intenta obtener la tabla
        print(f"Tabla ya existe: {table.table_id}")
    except bigquery.NotFound:
        # Si la tabla no existe, crea una nueva con el esquema definido
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"Tabla creada {table.table_id}")

    # Configuración del job para cargar los datos
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema
    )

    # Cargar los datos en la tabla
    with open(source_file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Espera a que la carga se complete
    print(f"Se han cargado {job.output_rows} filas en {table_id}.")

if __name__ == '__main__':
    load_data_to_bigquery('data-pipeline-project-419820.dataset_sintetico.FACT_VENTAS', 'data/generated_data.csv')
