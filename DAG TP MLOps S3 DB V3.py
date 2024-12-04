from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import boto3
from io import BytesIO, StringIO
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.base import BaseHook


# Leer archivos de entrada desde S3
def read_csv_from_s3(bucket_name, s3_path):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_path)
    return pd.read_csv(BytesIO(response['Body'].read()))

# Guardar resultados en S3
def save_csv_to_s3(dataframe, s3_path):
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_buffer.getvalue())

# Función para filtrar datos
def filtrar_datos(input_ads_views, input_product_views, input_advertisers, output_folder, bucket_name, execution_date):
    """
    Filtra los datos eliminando filas cuyo advertiser_id no esté en la lista de advertisers,
    y mantiene solo los datos del día corriente.
    """
    # Leer archivos de entrada
    ads_views = read_csv_from_s3(bucket_name, input_ads_views)
    product_views = read_csv_from_s3(bucket_name, input_product_views)
    advertisers = read_csv_from_s3(bucket_name, input_advertisers)
    
    # Obtener la lista de IDs válidos
    valid_advertisers = advertisers['advertiser_id'].unique()
    
    # Filtrar datos por advertiser_id
    ads_views_filtered = ads_views[ads_views['advertiser_id'].isin(valid_advertisers)]
    product_views_filtered = product_views[product_views['advertiser_id'].isin(valid_advertisers)]
    
    ads_views['date'] = pd.to_datetime(ads_views['date'])
    product_views['date'] = pd.to_datetime(product_views['date'])
    
    # Filtrar por fecha del día corriente
    today = execution_date.strftime('%Y-%m-%d')
    ads_views_filtered = ads_views_filtered[ads_views_filtered['date'] <= today]
    product_views_filtered = product_views_filtered[product_views_filtered['date'] <= today]
    
    # Guardar resultados en S3
    save_csv_to_s3(ads_views_filtered, f"{output_folder}/filtered_ads_views_{today}.csv")
    save_csv_to_s3(product_views_filtered, f"{output_folder}/filtered_product_views_{today}.csv")

# Función para calcular los top 20 productos más vistos
def calcular_top_productos(input_product_views, output_folder, bucket_name, execution_date):
    
    today = execution_date.strftime('%Y-%m-%d')
    # Cargar el archivo CSV filtrado
    product_views = read_csv_from_s3(bucket_name, f"{input_product_views}_{today}.csv")

    # Asegurarse de que los datos necesarios están presentes
    if not {'advertiser_id', 'product_id', 'date'}.issubset(product_views.columns):
        raise ValueError("El archivo CSV debe contener las columnas 'advertiser_id', 'product_id', 'date'.")

    # Agrupar por advertiser_id y product_id, y contar las vistas
    product_counts = (
        product_views.groupby(['advertiser_id', 'product_id'])
        .size()
        .reset_index(name='views')  # Renombrar el conteo a 'views'
    )

    # Obtener los 20 productos más vistos para cada advertiser_id
    top_products = (
        product_counts.sort_values(['advertiser_id', 'views'], ascending=[True, False])
        .groupby('advertiser_id')
        .head(20)  # Seleccionar los top 20 productos por advertiser
    )
    # Agregar la columna con la fecha de hoy
    top_products['date'] = today

    # Guardar resultados en S3
    save_csv_to_s3(top_products, f"{output_folder}/top_products_{today}.csv")

# Función para calcular los top 20 productos por CTR
def calcular_top_ctr(input_ads_views, output_folder, bucket_name, execution_date):
    
    today = execution_date.strftime('%Y-%m-%d')
    # Cargar el archivo CSV filtrado
    ads_views = read_csv_from_s3(bucket_name, f"{input_ads_views}_{today}.csv")

    # Asegurarse de que los datos necesarios están presentes
    if not {'advertiser_id', 'product_id', 'type'}.issubset(ads_views.columns):
        raise ValueError("El archivo CSV debe contener las columnas 'advertiser_id', 'product_id', y 'type'.")

    # Filtrar clics e impresiones
    clicks = ads_views[ads_views['type'] == 'click']
    impressions = ads_views[ads_views['type'] == 'impression']

    # Contar clics e impresiones por advertiser_id y product_id
    clicks_count = clicks.groupby(['advertiser_id', 'product_id']).size().reset_index(name='clicks')
    impressions_count = impressions.groupby(['advertiser_id', 'product_id']).size().reset_index(name='impressions')

    # Combinar clics e impresiones
    ctr_data = pd.merge(clicks_count, impressions_count, on=['advertiser_id', 'product_id'], how='inner')

    # Calcular CTR
    ctr_data['ctr'] = ctr_data['clicks'] / ctr_data['impressions']

    # Obtener los 20 productos con mejor CTR por advertiser_id
    top_ctr = (
        ctr_data.sort_values(['advertiser_id', 'ctr'], ascending=[True, False])
        .groupby('advertiser_id')
        .head(20)  # Seleccionar los top 20 productos por advertiser
    )
    
    # Agregar la columna con la fecha de hoy
    top_ctr['date'] = today
    
    # Guardar resultados en S3
    save_csv_to_s3(top_ctr, f"{output_folder}/top_ctr_products_{today}.csv")

# Función para escribir en DB
def escribir_en_db(top_products_path, top_ctr_path, db_config, bucket_name, execution_date):
    # Formatear la fecha
    today = execution_date.strftime('%Y-%m-%d')
    
    def read_csv_from_s3(bucket_name, key):
        """Lee un archivo CSV de S3 y lo retorna como un DataFrame de pandas."""
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return pd.read_csv(response['Body'])
    
    # Rutas completas en S3
    top_products_key = f"{top_products_path}_{today}.csv"
    top_ctr_key = f"{top_ctr_path}_{today}.csv"
    
    # Cargar datos desde S3
    top_products = read_csv_from_s3(bucket_name, top_products_key)
    top_ctr = read_csv_from_s3(bucket_name, top_ctr_key)
    
    # Función para cargar un DataFrame en la base de datos
    def upload_dataframe_to_db(df, table_name, db_config):
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
            	# Extraer nombres de columnas y valores del DataFrame
            	columns = list(df.columns)
            	values = df.to_records(index=False).tolist()

            	# Crear una consulta INSERT dinámica
            	insert_query = f"""
            	INSERT INTO {table_name} ({', '.join(columns)})
            	VALUES ({', '.join(['%s'] * len(columns))})
		ON CONFLICT DO NOTHING
            	"""

            	# Ejecutar la consulta con múltiples filas
            	cursor.executemany(insert_query, values)

            	# Confirmar los cambios
            	conn.commit()

    # Subir los DataFrames a sus tablas correspondientes
    upload_dataframe_to_db(top_products, 'top_products', db_config)
    upload_dataframe_to_db(top_ctr, 'top_ctr_products', db_config)

bucket_name = 'grupo-15-mlops-bucket'

# Crear cliente S3 usando credenciales
s3_client = boto3.client(
    's3',
    aws_access_key_id='AKIAVY2PG64LY7U654ST',
    aws_secret_access_key='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',
    region_name='us-east-1'
)

# Argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
with DAG(
    'AdTechPipelineS3-V2',
    default_args=default_args,
    description='Filtrar datos, calcular top productos más vistos, CTR y escribir en base de datos',
    schedule_interval='0 23 * * *',  # Ejecución diaria a las 23:00 (por la noche)
    start_date=datetime(2024, 11, 29),  # Fecha inicial
    catchup=True,
) as dag:

    # Tarea para filtrar datos
    filtrar_datos_task = PythonOperator(
        task_id='FiltrarDatos',
        python_callable=filtrar_datos,
        op_kwargs={
            'input_ads_views': 'input/ads_views.csv',
            'input_product_views': 'input/product_views.csv',
            'input_advertisers': 'input/advertiser_ids.csv',
            'output_folder': 'output2',
            'bucket_name': 'grupo-15-mlops-bucket',
        },
        provide_context=True,
    )

    # Tarea para calcular los productos más vistos
    top_products_task = PythonOperator(
        task_id='TopProducts',
        python_callable=calcular_top_productos,
        op_kwargs={
            'input_product_views': 'output2/filtered_product_views',
            'output_folder': 'output2',
            'bucket_name': 'grupo-15-mlops-bucket',
        },
        provide_context=True,
    )

    # Tarea para calcular los productos con mejor CTR
    top_ctr_task = PythonOperator(
        task_id='TopCTR',
        python_callable=calcular_top_ctr,
        op_kwargs={
            'input_ads_views': 'output2/filtered_ads_views',
            'output_folder': 'output2',
            'bucket_name': 'grupo-15-mlops-bucket',
        },
        provide_context=True,
    )
    
    # Tarea para escribir en PostgreSQL
    db_writing_task = PythonOperator(
        task_id='DBWriting',
        python_callable=escribir_en_db,
        op_kwargs={
            'top_products_path': 'output2/top_products',
            'top_ctr_path': 'output2/top_ctr_products',
            'db_config': {
                'host': 'grupo-15-rds.cf4i6e6cwv74.us-east-1.rds.amazonaws.com',
                'port': 5432,
                'database': 'postgres',
                'user': 'postgres',
                'password': 'nvmQVqg3d46tCJa',  
            },
            'bucket_name': 'grupo-15-mlops-bucket',  
        },
        provide_context=True,
    )

    # Configuración de dependencias
    filtrar_datos_task >> [top_products_task, top_ctr_task] >> db_writing_task
    #filtrar_datos_task >> [top_products_task, top_ctr_task]