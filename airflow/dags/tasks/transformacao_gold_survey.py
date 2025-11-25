#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Transformacao GOLD (versão corrigida)
- detecta arquivo mais recente na camada silver (MinIO)
- calcula métricas descritivas solicitadas
- grava resumo + tabelas detalhadas no bucket GOLD (MinIO)
- grava resumo e detalhes no MariaDB (evitando constraints com UUID por execução)

Coloque este arquivo em: ./airflow/dags/tasks/transformacao_gold_survey.py (substituindo o existente)
Execute dentro do container airflow:
  docker exec -it airflow bash -c "python -u /opt/airflow/dags/tasks/transformacao_gold_survey.py"
"""

from datetime import datetime
import io
import logging
import re
import json
import uuid
from typing import Optional, Any

import pandas as pd
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from airflow.providers.mysql.hooks.mysql import MySqlHook

# ---- CONFIGURAÇÕES ----
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minio@1234!"
BUCKET_SILVER = "silver"
BUCKET_GOLD = "gold"
SILVER_PREFIX = "survey/"
GOLD_PREFIX = "survey/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("transformacao_gold_survey_pt")

# ---- CLIENTE S3 (MinIO) ----
def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

# ---- LIMPEZA DE COMENTÁRIOS ----
def _limpar_comentario(texto: Optional[str]) -> Optional[str]:
    if pd.isna(texto):
        return None
    s = str(texto).strip()
    s = re.sub(r'\s+', ' ', s)
    s = s if len(s) > 0 and s.lower() not in {"nan","none","na","n/a"} else None
    return s

# ---- LOCALIZAR ARQUIVO MAIS RECENTE NA CAMADA SILVER ----
def _obter_ultimo_arquivo_silver(s3, bucket=BUCKET_SILVER, prefix=SILVER_PREFIX):
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    keys = [o for o in objs if o['Key'].lower().endswith('.parquet')]
    if not keys:
        raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em {bucket}/{prefix}")
    keys_ordenados = sorted(keys, key=lambda x: x.get("LastModified", x.get("Key")), reverse=True)
    return keys_ordenados[0]['Key']

# ---- CÁLCULO DE MÉTRICAS ----
def calcular_metricas(df: pd.DataFrame) -> dict:
    resultados = {}

    resultados['total_respostas'] = int(len(df))
    resultados['timestamps_unicos'] = int(df['timestamp'].nunique()) if 'timestamp' in df.columns else None
    resultados['idades_faltantes'] = int(df['age'].isna().sum()) if 'age' in df.columns else None

    # Distribuição por faixa etária
    if 'age_group' in df.columns:
        idade = df['age_group'].value_counts(dropna=False).rename_axis('faixa_etaria').reset_index(name='quantidade')
        idade['metrica'] = 'distribuicao_faixa_etaria'
        resultados['faixa_etaria'] = idade

    # Distribuição por gênero
    if 'gender_norm' in df.columns:
        genero = df['gender_norm'].value_counts(dropna=False).rename_axis('genero').reset_index(name='quantidade')
        genero['metrica'] = 'distribuicao_genero'
        resultados['genero'] = genero

    # Taxa de tratamento
    if 'treatment_std' in df.columns:
        tratamento = df['treatment_std'].value_counts(dropna=False).rename_axis('tratamento').reset_index(name='quantidade')
        total = int(tratamento['quantidade'].sum())
        sim = int(tratamento.loc[tratamento['tratamento'] == 'yes', 'quantidade'].sum()) if 'yes' in list(tratamento['tratamento']) else 0
        resultados['tratamento'] = {
            'total': total,
            'quantidade_sim': sim,
            'taxa_tratamento': float(sim / total) if total > 0 else None
        }

    # Estatísticas do risco
    if 'risk_score' in df.columns:
        resultados['media_risco'] = float(df['risk_score'].mean()) if df['risk_score'].notna().any() else None
        resultados['mediana_risco'] = float(df['risk_score'].median()) if df['risk_score'].notna().any() else None
        resultados['desvio_padrao_risco'] = float(df['risk_score'].std()) if df['risk_score'].notna().any() else None

    # Distribuição por tamanho da empresa
    if 'no_employees_bucket' in df.columns:
        empresa = df['no_employees_bucket'].value_counts(dropna=False).rename_axis('tamanho_empresa').reset_index(name='quantidade')
        empresa['metrica'] = 'distribuicao_tamanho_empresa'
        resultados['tamanho_empresa'] = empresa

    # Interferência no trabalho
    if 'work_interfere_std' in df.columns:
        interferencia = df['work_interfere_std'].value_counts(dropna=False).rename_axis('interferencia_trabalho').reset_index(name='quantidade')
        interferencia['metrica'] = 'interferencia_trabalho'
        resultados['interferencia_trabalho'] = interferencia

    # Porcentagem de valores faltantes
    faltantes = (df.isna().sum() / len(df)).reset_index()
    faltantes.columns = ['coluna', 'percentual_faltante']
    resultados['faltantes'] = faltantes

    return resultados

# ---- UTILITÁRIOS PARA JSON/DB SAFE ----
def _to_str_or_none(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        if isinstance(x, float) and pd.isna(x):
            return None
        if isinstance(x, (dict, list)):
            return json.dumps(x, default=str)
        return str(x)
    except Exception:
        return str(x)


def _to_json_safe_value(v: Any):
    """Converte valores pandas/numpy para tipos nativos para json."""
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    # numpy scalar -> python builtin
    try:
        if hasattr(v, 'item'):
            return v.item()
    except Exception:
        pass
    # builtin types
    if isinstance(v, (int, float, bool, str)):
        return v
    # fallback to string
    try:
        return str(v)
    except Exception:
        return None

# ---- SALVAR NA CAMADA GOLD (e retornar chaves) ----
def salvar_gold_e_db(s3, resultados, df, bucket_gold=BUCKET_GOLD, prefixo=GOLD_PREFIX):
    try:
        s3.head_bucket(Bucket=bucket_gold)
    except ClientError:
        logger.info(f"Bucket {bucket_gold} não existe. Criando...")
        s3.create_bucket(Bucket=bucket_gold)

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    tabelas = []
    for chave in ['faixa_etaria', 'genero', 'tamanho_empresa', 'interferencia_trabalho', 'faltantes']:
        if chave in resultados:
            df_tabela = resultados[chave].copy()
            df_tabela['tabela'] = chave
            tabelas.append(df_tabela)

    resumo = pd.DataFrame([{
        'metrica': 'total_respostas', 'valor': resultados.get('total_respostas')
    }, {
        'metrica': 'timestamps_unicos', 'valor': resultados.get('timestamps_unicos')
    }, {
        'metrica': 'idades_faltantes', 'valor': resultados.get('idades_faltantes')
    }, {
        'metrica': 'total_tratamentos', 'valor': (resultados.get('tratamento') or {}).get('total')
    }, {
        'metrica': 'tratamentos_sim', 'valor': (resultados.get('tratamento') or {}).get('quantidade_sim')
    }, {
        'metrica': 'taxa_tratamento', 'valor': (resultados.get('tratamento') or {}).get('taxa_tratamento')
    }, {
        'metrica': 'media_risco', 'valor': resultados.get('media_risco')
    }, {
        'metrica': 'mediana_risco', 'valor': resultados.get('mediana_risco')
    }, {
        'metrica': 'desvio_padrao_risco', 'valor': resultados.get('desvio_padrao_risco')
    }])

    # --- NORMALIZAR A COLUNA 'valor' PARA STRING/NULL EXPLÍCITO (evita conversão automática para double) ---
    resumo['valor'] = resumo['valor'].apply(_to_str_or_none)

    if tabelas:
        todas_tabelas = pd.concat(tabelas, ignore_index=True, sort=False)
    else:
        todas_tabelas = pd.DataFrame(columns=['tabela'])

    # salvar resumo
    resumo_buf = io.BytesIO()
    # garantir que todas as colunas object/string estejam em tipo string (pandas StringDtype)
    for col in resumo.select_dtypes(include=['object']).columns:
        resumo[col] = resumo[col].astype('string')

    # garantir que todos os valores do resumo sejam strings (ou None)
    resumo['valor'] = resumo['valor'].apply(lambda x: None if x is None else str(x))

    resumo.to_parquet(resumo_buf, index=False, engine='pyarrow')
    resumo_buf.seek(0)
    resumo_key = f"{prefixo}metricas_resumo_{ts}.parquet"
    s3.upload_fileobj(Fileobj=resumo_buf, Bucket=bucket_gold, Key=resumo_key)
    logger.info(f"Resumo salvo em s3://{bucket_gold}/{resumo_key}")

    # salvar tabelas detalhadas
    tabelas_buf = io.BytesIO()
    if todas_tabelas.empty:
        todas_tabelas = pd.DataFrame(columns=['tabela'])
    else:
        # converter object -> string para evitar problemas com pyarrow
        for col in todas_tabelas.select_dtypes(include=['object']).columns:
            try:
                todas_tabelas[col] = todas_tabelas[col].astype('string')
            except Exception:
                todas_tabelas[col] = todas_tabelas[col].astype(str)

    todas_tabelas.to_parquet(tabelas_buf, index=False, engine='pyarrow')
    tabelas_buf.seek(0)
    tabelas_key = f"{prefixo}metricas_tabelas_{ts}.parquet"
    s3.upload_fileobj(Fileobj=tabelas_buf, Bucket=bucket_gold, Key=tabelas_key)
    logger.info(f"Tabelas salvas em s3://{bucket_gold}/{tabelas_key}")

    # comentários
    comentarios_key = None
    if 'comments' in df.columns:
        comentarios = df['comments'].astype(str).map(_limpar_comentario).dropna()
        comentarios = comentarios[comentarios.str.len() > 5]
        if not comentarios.empty:
            top_comentarios = comentarios.sort_values(key=lambda s: s.str.len(), ascending=False).head(200).reset_index(drop=True)
            comentarios_df = pd.DataFrame({'comentario': top_comentarios})

            comentarios_buf = io.BytesIO()
            comentarios_df.to_csv(comentarios_buf, index=False, encoding='utf-8')
            comentarios_buf.seek(0)
            comentarios_key = f"{prefixo}principais_comentarios_{ts}.csv"
            s3.upload_fileobj(Fileobj=comentarios_buf, Bucket=bucket_gold, Key=comentarios_key)
            logger.info(f"Comentários salvos em s3://{bucket_gold}/{comentarios_key}")

    return {
        'resumo_key': resumo_key,
        'tabelas_key': tabelas_key,
        'comentarios_key': comentarios_key,
        'ts': ts
    }

# ---- SALVAR MÉTRICAS NO MARIADB ----
def save_metrics_to_mariadb(resultados: dict, df: pd.DataFrame, run_ts: str, mysql_conn_id: str = "mariadb_local"):
    """
    Cria tabelas simples e insere o resumo + detalhes em formato JSON.
    Evita conflito de unique constraints gerando id único por execução (UUID).
    """
    logger.info("Salvando métricas no MariaDB (conn_id=%s)...", mysql_conn_id)
    hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # tabela de resumo
        create_summary = """
        CREATE TABLE IF NOT EXISTS metrics_summary (
            id VARCHAR(36) PRIMARY KEY,
            run_ts VARCHAR(30),
            metrica VARCHAR(255),
            valor TEXT,
            details JSON DEFAULT NULL,
            created_at DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_summary)

        # tabela de detalhes (linhas das tabelas detalhadas)
        create_details = """
        CREATE TABLE IF NOT EXISTS metrics_details (
            id VARCHAR(36) PRIMARY KEY,
            run_ts VARCHAR(30),
            tabela VARCHAR(100),
            row_json JSON,
            created_at DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(create_details)
        conn.commit()

        # inserir resumo (cada metrica como uma linha)
        now = datetime.utcnow()
        summary_rows = [
            {'metrica': 'total_respostas', 'valor': resultados.get('total_respostas')},
            {'metrica': 'timestamps_unicos', 'valor': resultados.get('timestamps_unicos')},
            {'metrica': 'idades_faltantes', 'valor': resultados.get('idades_faltantes')},
            {'metrica': 'total_tratamentos', 'valor': (resultados.get('tratamento') or {}).get('total')},
            {'metrica': 'tratamentos_sim', 'valor': (resultados.get('tratamento') or {}).get('quantidade_sim')},
            {'metrica': 'taxa_tratamento', 'valor': (resultados.get('tratamento') or {}).get('taxa_tratamento')},
            {'metrica': 'media_risco', 'valor': resultados.get('media_risco')},
            {'metrica': 'mediana_risco', 'valor': resultados.get('mediana_risco')},
            {'metrica': 'desvio_padrao_risco', 'valor': resultados.get('desvio_padrao_risco')}
        ]

        insert_sql = "INSERT INTO metrics_summary (id, run_ts, metrica, valor, details, created_at) VALUES (%s,%s,%s,%s,%s,%s)"
        for r in summary_rows:
            metrica = r['metrica']
            valor = r['valor']
            rec_id = str(uuid.uuid4())
            details = None
            if isinstance(valor, (dict, list)):
                try:
                    details = json.dumps(valor, default=_to_str_or_none)
                except Exception:
                    details = json.dumps(valor, default=str)
                valor = None

            # garantir que valor não seja NaN (MySQL chokes on NaN)
            try:
                if pd.isna(valor):
                    valor_db = None
                else:
                    valor_db = str(valor) if valor is not None else None
            except Exception:
                valor_db = str(valor) if valor is not None else None

            try:
                cursor.execute(insert_sql, (rec_id, run_ts, metrica, valor_db, details, now))
            except Exception as e:
                logger.warning("Erro ao inserir resumo %s: %s", metrica, e)
        conn.commit()

        # inserir detalhes (tabelas como linhas JSON)
        insert_d = "INSERT INTO metrics_details (id, run_ts, tabela, row_json, created_at) VALUES (%s,%s,%s,%s,%s)"
        for tabela_nome in ['faixa_etaria', 'genero', 'tamanho_empresa', 'interferencia_trabalho', 'faltantes']:
            if tabela_nome in resultados:
                df_t = resultados[tabela_nome].copy()
                for _, r in df_t.iterrows():
                    rec_id = str(uuid.uuid4())
                    # construir dict seguro para json
                    row_dict = {k: _to_json_safe_value(v) for k, v in r.items()}
                    try:
                        row_json = json.dumps(row_dict, default=str, ensure_ascii=False)
                    except Exception as e:
                        logger.warning("Falha ao serializar linha de %s: %s. Usando versão stringada.", tabela_nome, e)
                        row_json = json.dumps({k: str(v) for k, v in row_dict.items()})
                    try:
                        cursor.execute(insert_d, (rec_id, run_ts, tabela_nome, row_json, now))
                    except Exception as e:
                        logger.warning("Erro ao inserir detalhe tabela %s: %s", tabela_nome, e)
        conn.commit()

        logger.info("Métricas salvas no MariaDB com sucesso.")
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

# ---- MAIN ----
def main():
    s3 = _s3_client()

    try:
        ultimo = _obter_ultimo_arquivo_silver(s3)
        logger.info(f"Arquivo Silver detectado: {ultimo}")
    except Exception as e:
        logger.exception("Erro ao detectar arquivo Silver: %s", e)
        raise

    try:
        resp = s3.get_object(Bucket=BUCKET_SILVER, Key=ultimo)
        data = resp['Body'].read()
        logger.info(f"Bytes baixados do Silver: {len(data)}")
        buf = io.BytesIO(data)
        buf.seek(0)
        df = pd.read_parquet(buf)
        logger.info(f"DataFrame Silver carregado: {len(df)} linhas, {len(df.columns)} colunas")
    except Exception as e:
        logger.exception("Erro ao ler parquet Silver: %s", e)
        raise

    metricas = calcular_metricas(df)
    resultado = salvar_gold_e_db(s3, metricas, df)
    # salvar também no banco
    try:
        save_metrics_to_mariadb(metricas, df, resultado['ts'])
    except Exception as e:
        logger.exception("Falha ao salvar métricas no MariaDB: %s", e)

    logger.info("Arquivos da camada Gold gerados com sucesso: %s", resultado)
    return resultado

if __name__ == "__main__":
    main()
