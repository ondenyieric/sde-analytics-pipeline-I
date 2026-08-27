from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_order_key_is_deterministic_and_text():
    config = yaml.safe_load((ROOT / "ingestion/config.yaml").read_text())
    orders = next(r for r in config["resources"] if r["name"] == "orders")
    assert orders["primary_key"] == "id"
    assert orders["columns"]["id"] == "TEXT"
    assert "order_ts" in orders["immutable_columns"]


def test_airflow_does_not_require_host_docker_socket():
    dag = (ROOT / "airflow/dags/pipeline_dag.py").read_text()
    assert "DockerOperator" not in dag
    assert "var/run/docker.sock" not in dag
    assert "/opt/ingestion/ingest.py" in dag


def test_cdc_delete_parsing_uses_before_image():
    sql = (ROOT / "clickhouse/init/03_kafka_ingest.sql").read_text()
    assert "JSONExtractRaw(message, 'after') != 'null'" in sql
    assert "JSONExtractString(message, 'before', 'id')" in sql


def test_prometheus_scrapes_pushgateway_not_ephemeral_ingestion():
    config = (ROOT / "observability/prometheus/prometheus.yml").read_text()
    assert 'targets: ["pushgateway:9091"]' in config
    assert 'targets: ["ingestion:8000"]' not in config
