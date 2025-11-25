    # Use a imagem base oficial do Airflow com a versão de Python que você deseja.
    FROM apache/airflow:2.9.2-python3.11

    # Copie o arquivo requirements.txt do seu diretório de configuração
    # para um local temporário dentro do contêiner.
    # O caminho de origem é relativo ao 'context' de build (que é a raiz do seu projeto JDEA).
    COPY ./airflow/config_airflow/requirements.txt /tmp/requirements.txt

    # Instale todas as dependências Python listadas no requirements.txt.
    RUN pip install --no-cache-dir -r /tmp/requirements.txt

    # Defina o usuário do Airflow para as próximas instruções.
    USER airflow
    