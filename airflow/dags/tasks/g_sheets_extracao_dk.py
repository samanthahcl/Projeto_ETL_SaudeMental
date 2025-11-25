import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
import io
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_google_sheet_data(sheet_id, sheet_name):
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_file('/opt/airflow/config_airflow/credentials.json', scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
        try:
            data = sheet.get_all_records()
        except gspread.exceptions.GSpreadException as e:
            if 'A linha de cabeçalho na planilha não é única.' in str(e):
                logging.warning(f"Erro ao usar get_all_records() (cabeçalhos duplicados): {e}")
                expected_headers = {
                    'survey': [
        'Timestamp', 'Age', 'Gender', 'Country', 'state', 'self_employed',
        'family_history', 'treatment', 'work_interfere', 'no_employees',
        'remote_work', 'tech_company', 'benefits', 'care_options',
        'wellness_program', 'seek_help', 'anonymity', 'leave',
        'mental_health_consequence', 'phys_health_consequence', 'coworkers',
        'supervisor', 'mental_health_interview', 'phys_health_interview',
        'mental_vs_physical', 'obs_consequence', 'comments']
                }.get(sheet_name, None)
                if expected_headers:
                    data = sheet.get_all_records(expected_headers=expected_headers)
                else:
                    raise
        if not data:
            raise ValueError(f"Nenhum dado foi retornado para a planilha {sheet_name}")

        df = pd.DataFrame(data)
        return df
    except Exception as e:
        logging.error(f"Erro ao obter dados da planilha do Google: {e}")
        raise


def google_sheet_to_minio_etl(sheet_id, sheet_name, bucket_name, endpoint_url, access_key, secret_key):
    # Configuração do cliente MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        df = get_google_sheet_data(sheet_id, sheet_name)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        logger.info(f"📦 Tamanho do buffer antes do upload: {parquet_buffer.getbuffer().nbytes} bytes")
        minio_client.put_object(Bucket=bucket_name, Key=f"{sheet_name}/data.parquet", Body=parquet_buffer.getvalue())
    except Exception as e:
        logging.error(f"Erro ao processar a planilha {sheet_name}: {e}")
        raise

if __name__ == "__main__":
    SHEET_ID = "1qElDdURG1_c65sqOFvK0Wr3hqPT5fJkTnu5GYDaBNcs"
    SHEET_NAME = "survey"

    ENDPOINT_URL = "http://minio:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minio@1234!"
    BUCKET_NAME = "raw"

    print("🔄 Lendo dados da planilha, aguarde...")
    google_sheet_to_minio_etl(SHEET_ID, SHEET_NAME, BUCKET_NAME, ENDPOINT_URL, ACCESS_KEY, SECRET_KEY)
    print(f"✅ Dados da planilha '{SHEET_NAME}' enviados com sucesso para o bucket '{BUCKET_NAME}' no MinIO!")
