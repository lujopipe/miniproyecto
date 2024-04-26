from google.cloud import storage
import os

def create_bucket_if_not_exists(bucket_name):
    """Crea un bucket en Google Cloud Storage si no existe."""
    client = storage.Client()
    bucket = client.lookup_bucket(bucket_name)  # Verifica si el bucket ya existe

    if bucket is None:
        # Si el bucket no existe, lo crea
        bucket = client.create_bucket(bucket_name)
        print(f"Bucket {bucket_name} creado.")
    else:
        print(f"Bucket {bucket_name} ya existe.")

    return bucket

def upload_to_gcs(bucket_name, source_file_dir):
    """Sube archivos desde un directorio local a Google Cloud Storage."""
    bucket = create_bucket_if_not_exists(bucket_name)  # Crea el bucket si no existe

    for filename in os.listdir(source_file_dir):
        if filename.endswith('.parquet'):
            local_file_path = os.path.join(source_file_dir, filename)
            blob = bucket.blob(filename)
            blob.upload_from_filename(local_file_path)
            print(f"Archivo {filename} subido a {bucket_name}.")

if __name__ == '__main__':
    bucket_name = 'sintetico99'  # El nombre del bucket
    source_file_dir = 'data/'  # Ruta al directorio de los archivos Parquet
    upload_to_gcs(bucket_name, source_file_dir)
