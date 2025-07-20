from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import BaseOperator

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project


class KedroOperator(BaseOperator):

    def __init__(
        self,
        package_name: str,
        pipeline_name: str,
        node_name: str | list[str],
        project_path: str | Path,
        env: str,
        conf_source: str,
        *args, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.package_name = package_name
        self.pipeline_name = pipeline_name
        self.node_name = node_name
        self.project_path = project_path
        self.env = env
        self.conf_source = conf_source

    def execute(self, context):
        configure_project(self.package_name)
        with KedroSession.create(self.project_path, env=self.env, conf_source=self.conf_source) as session:
            if isinstance(self.node_name, str):
                self.node_name = [self.node_name]
            session.run(self.pipeline_name, node_names=self.node_name)

# Kedro settings required to run your pipeline
env = "airflow"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "factory"
conf_source = "" or Path.cwd() / "conf"


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    dag_id="factory",
    start_date=datetime(2025, 1, 1),
    max_active_runs=3,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    schedule="@daily",
    catchup=False,
    # Default settings applied to all tasks
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        # retries=1,
        # retry_delay=timedelta(minutes=5)
    )
) as dag:
    tasks = {
        "etl-html-cds-node": KedroOperator(
            task_id="etl-html-cds-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="etl_html_cds_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "etl-html-ifix-node": KedroOperator(
            task_id="etl-html-ifix-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="etl_html_ifix_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "etl-html-infomoney-node": KedroOperator(
            task_id="etl-html-infomoney-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="etl_html_infomoney_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "etl-html-moneytimes-node": KedroOperator(
            task_id="etl-html-moneytimes-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="etl_html_moneytimes_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "etl-html-seudinheiro-node": KedroOperator(
            task_id="etl-html-seudinheiro-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="etl_html_seudinheiro_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "etl-html-valorinveste-node": KedroOperator(
            task_id="etl-html-valorinveste-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="etl_html_valorinveste_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "etl-ibov-node": KedroOperator(
            task_id="etl-ibov-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="etl_ibov_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "etl-ivvb11-vix-brasil-node": KedroOperator(
            task_id="etl-ivvb11-vix-brasil-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="etl_ivvb11_vix_brasil_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "indicador-atividade-mercado-node": KedroOperator(
            task_id="indicador-atividade-mercado-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="indicador_atividade_mercado_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "indicador-confianca-mercado-local-node": KedroOperator(
            task_id="indicador-confianca-mercado-local-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="indicador_confianca_mercado_local_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "indicador-retorno-mercado-node": KedroOperator(
            task_id="indicador-retorno-mercado-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="indicador_retorno_mercado_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "indicador-risco-credito-node": KedroOperator(
            task_id="indicador-risco-credito-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="indicador_risco_credito_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "indicador-sentimento-noticias-node": KedroOperator(
            task_id="indicador-sentimento-noticias-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="indicador_sentimento_noticias_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "indicador-volatilidade-mercado-node": KedroOperator(
            task_id="indicador-volatilidade-mercado-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="indicador_volatilidade_mercado_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "process-score-data-node": KedroOperator(
            task_id="process-score-data-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="process_score_data_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        )
    }
    tasks["etl-ibov-node"] >> tasks["indicador-atividade-mercado-node"]
    tasks["etl-html-ifix-node"] >> tasks["indicador-confianca-mercado-local-node"]
    tasks["etl-ibov-node"] >> tasks["indicador-retorno-mercado-node"]
    tasks["etl-html-cds-node"] >> tasks["indicador-risco-credito-node"]
    tasks["etl-html-valorinveste-node"] >> tasks["indicador-sentimento-noticias-node"]
    tasks["etl-html-seudinheiro-node"] >> tasks["indicador-sentimento-noticias-node"]
    tasks["etl-html-moneytimes-node"] >> tasks["indicador-sentimento-noticias-node"]
    tasks["etl-html-infomoney-node"] >> tasks["indicador-sentimento-noticias-node"]
    tasks["etl-ibov-node"] >> tasks["indicador-volatilidade-mercado-node"]
    tasks["etl-ivvb11-vix-brasil-node"] >> tasks["indicador-volatilidade-mercado-node"]
    tasks["indicador-retorno-mercado-node"] >> tasks["process-score-data-node"]
    tasks["indicador-volatilidade-mercado-node"] >> tasks["process-score-data-node"]
    tasks["indicador-confianca-mercado-local-node"] >> tasks["process-score-data-node"]
    tasks["indicador-risco-credito-node"] >> tasks["process-score-data-node"]
    tasks["indicador-atividade-mercado-node"] >> tasks["process-score-data-node"]
    tasks["indicador-sentimento-noticias-node"] >> tasks["process-score-data-node"]