#!/usr/bin/env python3

from datetime import datetime
import io
import logging
import re
import sys
from typing import Optional

import pandas as pd
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# --- Configurações padrão (pode sobrescrever por env vars se quiser) ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minio@1234!"
BUCKET_RAW = "raw"
BUCKET_SILVER = "silver"
DEFAULT_SOURCE_KEY = "survey/data.parquet"
TARGET_PREFIX = "survey/"

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("transformacao_survey")

# --- Helpers ---
def _s3_client(endpoint_url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def _normalize_yes_no(v: Optional[str]) -> Optional[str]:
    if pd.isna(v):
        return None
    s = str(v).strip().lower()
    if s in {"yes", "y", "true", "1", "sim", "s"}:
        return "yes"
    if s in {"no", "n", "false", "0", "não", "nao"}:
        return "no"
    if s in {"maybe", "sometimes", "unsure", "don't know", "dont know"}:
        return "maybe"
    return "unknown"

def _normalize_gender(v: Optional[str]) -> str:
    if pd.isna(v):
        return "unknown"
    s = re.sub(r'[^a-z]', '', str(v).lower())
    if s in {"male", "m", "man"}:
        return "male"
    if s in {"female", "f", "woman", "w"}:
        return "female"
    if s in {"trans", "nonbinary", "nb", "other"}:
        return "other"
    return "other" if len(s) > 0 else "unknown"

def _age_group(age):
    try:
        a = int(age)
        if a < 18:
            return "child"
        if a < 30:
            return "young_adult"
        if a < 60:
            return "adult"
        return "senior"
    except Exception:
        return "unknown"

# --- Transformação específica para survey ---
def transform_survey_df(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Iniciando transformações da aba 'survey'")

    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Timestamp -> datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["date"] = df["timestamp"].dt.date
        df["hour"] = df["timestamp"].dt.hour

    # Age
    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce")
        df["age_group"] = df["age"].apply(_age_group)

    # Gender normalization
    if "gender" in df.columns:
        df["gender_norm"] = df["gender"].apply(_normalize_gender)

    # Clean textual cols
    for col in ["country", "state", "comments"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"nan": None, "none": None, "na": None, "n/a": None})

    # Yes/no/maybe normalizations
    yn_cols = [
        "self_employed", "family_history", "treatment", "remote_work", "tech_company",
        "benefits", "care_options", "wellness_program", "seek_help", "anonymity",
        "leave", "mental_health_consequence", "phys_health_consequence",
        "coworkers", "supervisor", "mental_health_interview", "phys_health_interview",
        "mental_vs_physical", "obs_consequence"
    ]
    for c in yn_cols:
        if c in df.columns:
            df[c + "_std"] = df[c].apply(_normalize_yes_no)

    # no_employees bucket
    if "no_employees" in df.columns:
        def bucket_emp(v):
            try:
                s = str(v).lower()
                if "-" in s or "+" in s:
                    return s
                n = int(re.sub(r'\D', '', s)) if re.search(r'\d', s) else None
                if n is None:
                    return s
                if n <= 5:
                    return "1-5"
                if n <= 25:
                    return "6-25"
                if n <= 100:
                    return "26-100"
                if n <= 500:
                    return "101-500"
                return "500+"
            except:
                return s
        df["no_employees_bucket"] = df["no_employees"].apply(bucket_emp)

    # work_interfere normalization
    if "work_interfere" in df.columns:
        df["work_interfere_std"] = df["work_interfere"].astype(str).str.lower().str.strip().replace({
            "never":"never","rarely":"rarely","sometimes":"sometimes","often":"often","na":"unknown","none":"unknown"
        })

    # flags and risk_score
    df["_family_history_flag"] = df.get("family_history_std", "").apply(lambda x: 1 if x == "yes" else 0)
    df["_treatment_flag"] = df.get("treatment_std", "").apply(lambda x: 1 if x == "yes" else 0)
    df["_work_interfere_flag"] = df.get("work_interfere_std", "").apply(lambda x: 1 if x in ("often","sometimes") else 0)
    df["_mental_cons_flag"] = df.get("mental_health_consequence_std", "").apply(lambda x: 1 if x == "yes" else 0)
    df["_phys_cons_flag"] = df.get("phys_health_consequence_std", "").apply(lambda x: 1 if x == "yes" else 0)

    df["risk_score"] = df[["_family_history_flag","_treatment_flag","_work_interfere_flag","_mental_cons_flag","_phys_cons_flag"]].sum(axis=1)
    df["needs_follow_up"] = ((df["risk_score"] >= 2) & (df["_treatment_flag"]==0)).astype(int)

    # cleanup helpers
    df = df.drop(columns=[c for c in df.columns if c.startswith("_")], errors='ignore')
    df = df.drop_duplicates()
    df["processed_at"] = datetime.utcnow()

    logger.info(f"Transformação completada: {len(df)} linhas finais")
    return df

# --- Processo principal ---
def process_survey_to_silver(
    bucket_bronze: str = BUCKET_RAW,
    bucket_silver: str = BUCKET_SILVER,
    source_key: Optional[str] = None,
    target_prefix: Optional[str] = None,
    endpoint_url: str = MINIO_ENDPOINT,
    access_key: str = MINIO_ACCESS_KEY,
    secret_key: str = MINIO_SECRET_KEY,
):
    s3 = _s3_client(endpoint_url, access_key, secret_key)

    if not source_key:
        # detecta arquivos .parquet em raw/survey/ e prefere data.parquet
        objs = s3.list_objects_v2(Bucket=bucket_bronze, Prefix='survey/').get('Contents', [])
        keys = [o['Key'] for o in objs if o['Key'].lower().endswith('.parquet')]
        if not keys:
            raise FileNotFoundError("Nenhum arquivo .parquet encontrado em raw/survey/")
        source_key = DEFAULT_SOURCE_KEY if DEFAULT_SOURCE_KEY in keys else keys[0]
    if not target_prefix:
        target_prefix = target_prefix or TARGET_PREFIX

    logger.info(f"Usando fonte: s3://{bucket_bronze}/{source_key}")

    # garantir bucket silver
    try:
        s3.head_bucket(Bucket=bucket_silver)
    except ClientError:
        logger.info(f"Bucket '{bucket_silver}' não existe. Criando...")
        s3.create_bucket(Bucket=bucket_silver)

    # download
    try:
        resp = s3.get_object(Bucket=bucket_bronze, Key=source_key)
        data = resp['Body'].read()
        logger.info(f"Bytes baixados: {len(data)}")
        buf = io.BytesIO(data)
    except Exception as e:
        logger.exception(f"Erro ao baixar objeto fonte: {e}")
        raise

    # read parquet
    try:
        buf.seek(0)
        df = pd.read_parquet(buf)
        logger.info(f"DataFrame carregado: {len(df)} linhas, {len(df.columns)} colunas")
    except Exception as e:
        logger.exception(f"Erro ao ler parquet: {e}")
        raise

    # transform
    try:
        df_t = transform_survey_df(df)
    except Exception as e:
        logger.exception(f"Erro durante transformação: {e}")
        raise

    # serialize parquet to memory
    out = io.BytesIO()
    try:
        df_t.to_parquet(out, index=False, engine='pyarrow')
        out.seek(0)
        logger.info(f"Parquet transformado gerado, bytes={out.getbuffer().nbytes}")
    except Exception as e:
        logger.exception(f"Erro ao serializar parquet: {e}")
        raise

    # build target key and upload
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    target_key = f"{target_prefix}data_silver_{ts}.parquet"
    try:
        out.seek(0)
        s3.upload_fileobj(Fileobj=out, Bucket=bucket_silver, Key=target_key)
        logger.info(f"Upload concluído: s3://{bucket_silver}/{target_key}")
    except Exception as e:
        logger.exception(f"Erro ao enviar para silver: {e}")
        raise

    return target_key

# CLI
if __name__ == "__main__":
    # permite passar source_key via CLI args (ex: python transformacao_survey.py survey/data_from_test.parquet)
    src = sys.argv[1] if len(sys.argv) > 1 else None
    key = process_survey_to_silver(source_key=src)
    logger.info(f"Arquivo salvo em: {key}")
