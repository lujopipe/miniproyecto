import pandas as pd

def transform_data(filepath):
    df = pd.read_csv(filepath)
    # Aplica algunas transformaciones, como normalizar nombres de columnas, convertir fechas, etc.
    df['Cantidad Venta'] = df['Cantidad Venta'] * 1.1  # Incrementa ventas en un 10%
    df.to_csv('data/transformed_data.csv', index=False)

if __name__ == '__main__':
    transform_data('data/generated_data.csv')
