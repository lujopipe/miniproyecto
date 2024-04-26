import pandas as pd
from faker import Faker
import random

def generate_data(num_records):
    fake = Faker()
    data = [{
        'ID_Cliente': i,
        'Fecha_Transaccion': fake.date_between(start_date='-2y', end_date='today'),
        'Cantidad_Venta': round(random.uniform(10.0, 5000.0), 2),
        # Definir las categorías de productos y las regiones de venta
        'Categoria_Producto': random.choice(['Electronica', 'Alimentos', 'Ropa', 'Hogar', 'Juguetes']),
        'Region_Venta': random.choice(['Norte', 'Sur', 'Este', 'Oeste', 'Centro'])
    } for i in range(num_records)]
    # Crear un DataFrame de pandas con los datos
    df = pd.DataFrame(data)
    df.to_csv('data/generated_data.csv', index=False)

if __name__ == '__main__':
    generate_data(10000000)  # Asegura la generación de 10 millones de registros únicos
