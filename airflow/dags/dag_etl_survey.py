from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from urllib.parse import urlparse
import re

# Importações das funções Python do seu ETL
from tasks.g_sheets_extracao_dk import google_sheet_to_minio_etl
from tasks.transformacao_survey import process_survey_to_silver
from tasks.transformacao_gold_survey import main as process_gold_layer

# ----------------- CONFIGURAÇÕES GERAIS -----------------
default_args = {
    'owner': 'samantha',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _extract_bucket_name(value: str) -> str:
    """
    Recebe algo como:
      - 'bucket-name'
      - 'bucket-name/some/path'
      - 's3://bucket-name/some/path/file.parquet'
    Retorna apenas o bucket-name (sem s3://, sem caminho).
    Levanta ValueError se não conseguir extrair um nome válido.
    """
    if not value:
        raise ValueError("Bucket value está vazio")

    # remove prefixo s3:// se existir
    if value.startswith("s3://"):
        parsed = urlparse(value)
        bucket = parsed.netloc
    else:
        bucket = value.split("/", 1)[0]

    # validação simples do nome do bucket (aceita letras, números, . - _)
    if not re.match(r'^[a-zA-Z0-9.\-_]{1,255}$', bucket):
        raise ValueError(f"Nome de bucket inválido extraído: '{bucket}' (a partir de '{value}')")

    return bucket


def normalize_and_call_process_survey(bucket_bronze_raw, bucket_silver_raw,
                                      endpoint_url, access_key, secret_key, **context):
    
    bucket_bronze = _extract_bucket_name(bucket_bronze_raw)
    bucket_silver = _extract_bucket_name(bucket_silver_raw)

    # CORREÇÃO: Usando argumentos NOMEADOS para que os valores de conexão (endpoint, keys)
    # sejam mapeados para os parâmetros corretos na função process_survey_to_silver.
    return process_survey_to_silver(
        bucket_bronze=bucket_bronze,
        bucket_silver=bucket_silver,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key
    )

@dag(
    dag_id='dag_etl_survey_v1',
    default_args=default_args,
    description='Pipeline ETL completo: Google Sheets → MinIO (Bronze → Silver → Gold)',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'google_sheets', 'minio', 'bronze', 'silver', 'gold']
)
def dag_etl_survey():
    # ----------------- PARÂMETROS -----------------
    sheet_id = '1qElDdURG1_c65sqOFvK0Wr3hqPT5fJkTnu5GYDaBNcs'
    sheet_names = ['survey']

    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'
    # certifique-se de que aqui são **apenas** nomes de buckets (ex: 'raw'), mas o wrapper tolera 's3://...' ou 'bucket/path'
    bucket_bronze = 'raw'
    bucket_silver = 'silver'
    bucket_gold = 'gold'

    # ----------------- CAMADA BRONZE -----------------
    with TaskGroup('camada_bronze', tooltip='Extração do Google Sheets e upload em Parquet no MinIO (Bronze)') as camada_bronze:
        for sheet_name in sheet_names:
            PythonOperator(
                task_id=f'extrair_{sheet_name}_para_bronze',
                python_callable=google_sheet_to_minio_etl,
                op_args=[sheet_id, sheet_name, bucket_bronze, endpoint_url, access_key, secret_key]
            )

    # ----------------- CAMADA SILVER -----------------
    with TaskGroup('camada_silver', tooltip='Transformação dos dados da camada Bronze para Silver') as camada_silver:
        # Chamamos o wrapper normalize_and_call_process_survey para prevenir bucket inválido
        PythonOperator(
            task_id='transformar_survey_para_silver',  # Nome da task que falhou no log
            python_callable=normalize_and_call_process_survey,
            op_args=[bucket_bronze, bucket_silver, endpoint_url, access_key, secret_key],
            provide_context=True,
        )

    # ----------------- CAMADA GOLD -----------------
    with TaskGroup('camada_gold', tooltip='Transformação dos dados da camada Silver para Gold') as camada_gold:
        PythonOperator(
            task_id='gerar_gold_survey',
            python_callable=process_gold_layer
        )

    # ----------------- DEPENDÊNCIAS -----------------
    camada_bronze >> camada_silver >> camada_gold


dag_instance = dag_etl_survey()
