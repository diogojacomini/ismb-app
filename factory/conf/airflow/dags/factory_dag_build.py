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
        # Acessa os parâmetros da DAG
        dag_params = context.get('dag_run').conf or {}
        dag_default_params = context.get('params', {})
        
        # Combina parâmetros do DAG run com parâmetros padrão
        all_params = {**dag_default_params, **dag_params}

        # Garante que macros do Airflow sejam resolvidas (ex.: "{{ ds }}")
        ds = context.get('ds')
        run_id = context.get('run_id') or (context.get('dag_run').run_id if context.get('dag_run') else None)

        def _needs_render(v):
            return v is None or (isinstance(v, str) and '{{' in v)
        if _needs_render(all_params.get('odate')):
            all_params['odate'] = ds
        if _needs_render(all_params.get('execution_date')):
            all_params['execution_date'] = ds
        if _needs_render(all_params.get('run_id')):
            all_params['run_id'] = run_id
        
        # Log dos parâmetros para debug
        self.log.info(f"Executing with parameters: {all_params}")
        self.log.info(f"Execution date (odate): {context.get('ds')}")

        configure_project(self.package_name)
        with KedroSession.create(
            self.project_path,
            env=self.env,
            conf_source=self.conf_source,
            # Passa os parâmetros para o Kedro via extra_params
            extra_params=all_params
        ) as session:
            if isinstance(self.node_name, str):
                self.node_name = [self.node_name]
            session.run(self.pipeline_name, node_names=self.node_name)


# Kedro settings required to run your pipeline
env = "airflow"  # Use 'airflow' environment for Docker (ismb-db:5432)
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "factory"
conf_source = "" or Path.cwd() / "conf"


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    dag_id="factory_ismb_build",
    start_date=datetime(2025, 1, 1),
    max_active_runs=5,
    # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    # schedule="",
    catchup=False,
    # Default settings applied to all tasks
    default_args=dict(
        owner="airflow",
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        # retries=1,
        # retry_delay=timedelta(minutes=5)
    ),
    params={
        "odate": "{{ ds }}",  # Data de execução no formato YYYY-MM-DD
        "execution_date": "{{ ds }}",
        "run_id": "{{ run_id }}",
        "environment": "production",
        "process_full_data": False
    }
) as dag:
    tasks = {
        "build-dim-fonte-noticia-node": KedroOperator(
            task_id="build-dim-fonte-noticia-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="build_dim_fonte_noticia_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "build-dim-indice-node": KedroOperator(
            task_id="build-dim-indice-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="build_dim_indice_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "build-dim-tempo-node": KedroOperator(
            task_id="build-dim-tempo-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="build_dim_tempo_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "build-dim-tipo-indicador-node": KedroOperator(
            task_id="build-dim-tipo-indicador-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="build_dim_tipo_indicador_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        ),
        "build-dim-noticia-node": KedroOperator(
            task_id="build-dim-noticia-node",
            package_name=package_name,
            pipeline_name=pipeline_name,
            node_name="build_dim_noticia_node",
            project_path=project_path,
            env=env,
            conf_source=conf_source,
        )
    }
    tasks["build-dim-fonte-noticia-node"] >> tasks["build-dim-noticia-node"]
