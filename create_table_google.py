from google.cloud import bigquery

def create_table():
    # Configura tus credenciales de GCP y el proyecto
    client = bigquery.Client(project='data-pipeline-project-419820')

    # Define el ID de la tabla. 
    table_id = "data-pipeline-project-419820.dataset_sintetico.FACT_VENTAS"

    # Define la estructura de la tabla
    schema = [
        bigquery.SchemaField("ID_Cliente", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("Fecha_Transaccion", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("Cantidad_Venta", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("Categoria_Producto", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Region_Venta", "STRING", mode="REQUIRED"),
    ]

    # Crea una configuración de tabla con la estructura definida
    table = bigquery.Table(table_id, schema=schema)

    # Crea la tabla en BigQuery
    table = client.create_table(table)
    print(f"Tabla {table.table_id} creada.")

if __name__ == '__main__':
    create_table()
