# config/airflow_local_settings.py

def configure_logging():
    from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

    DEFAULT_LOGGING_CONFIG["handlers"]["task"]["base_log_folder"] = "/opt/airflow/logs"
    DEFAULT_LOGGING_CONFIG["handlers"]["task"]["filename_template"] = (
        "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/"
        "{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}"
        "attempt={{ try_number|default(ti.try_number) }}.log"
    )
    DEFAULT_LOGGING_CONFIG["handlers"]["task"]["remote_base_log_folder"] = None
    return DEFAULT_LOGGING_CONFIG
