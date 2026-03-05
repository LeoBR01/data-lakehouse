from airflow.sdk import DAG, task
from datetime import datetime
import subprocess
import os

PROJECT_DIR = os.path.expanduser("~/projetos/data-lakehouse")

with DAG(
    dag_id="github_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # roda manualmente
    catchup=False,
    tags=["github", "dbt", "lakehouse"],
) as dag:

    @task
    def carregar_dados():
        import subprocess
        result = subprocess.run(
            ["python", f"{PROJECT_DIR}/scripts/load_raw.py"],
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)
        return "dados carregados"

    @task
    def rodar_dbt():
        result = subprocess.run(
            ["dbt", "run"],
            cwd=f"{PROJECT_DIR}/dbt",
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)
        return "dbt executado"

    @task
    def testar_dbt():
        result = subprocess.run(
            ["dbt", "test"],
            cwd=f"{PROJECT_DIR}/dbt",
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)
        return "testes passaram"

    # Define a ordem de execução
    carregar_dados() >> rodar_dbt() >> testar_dbt()