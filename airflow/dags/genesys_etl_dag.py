from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract.genesys_api_client import GenesysAPIClient
from extract.dimensions.users import extract_users
from extract.dimensions.queues import extract_queues
from extract.dimensions.divisions import extract_divisions
from extract.dimensions.skills import extract_skills
from extract.dimensions.wrapup_codes import extract_wrapup_codes

from load.mongo_raw_writer import write_raw_entities
from load.mongo_writer import write_to_mongo_upsert

from transform.utils.common import read_raw_collection
from transform.dimensions.dim_users import build_dim_users
from transform.dimensions.dim_queue import build_dim_queue
from transform.dimensions.dim_division import build_dim_division
from transform.dimensions.dim_skills import build_dim_skills
from transform.dimensions.dim_qualification import build_dim_qualification
from transform.facts.fact_calls import build_fact_calls
from transform.facts.fact_call_skills import build_fact_call_skills

from pyspark.sql import SparkSession


def run_genesys_etl():
    # -------------------------
    # Near-real-time window
    # -------------------------
    end_dt = datetime.utcnow()
    start_dt = end_dt - timedelta(minutes=3)

    start_date = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # -------------------------
    # Extract RAW reference data
    # -------------------------
    write_raw_entities(extract_users(), "users_raw", "id")
    write_raw_entities(extract_queues(), "queues_raw", "id")
    write_raw_entities(extract_divisions(), "divisions_raw", "id")
    write_raw_entities(extract_skills(), "skills_raw", "id")
    write_raw_entities(extract_wrapup_codes(), "wrapup_codes_raw", "id")

    # -------------------------
    # Extract RAW conversations
    # -------------------------
    client = GenesysAPIClient()
    conversations = client.fetch_interactions(start_date, end_date)
    write_raw_entities(conversations, "conversations_raw", "conversationId")

    spark = SparkSession.builder.appName("genesys-analytics-etl").getOrCreate()

    # -------------------------
    # Read RAW collections
    # -------------------------
    raw_users = read_raw_collection(spark, "users_raw")
    raw_queues = read_raw_collection(spark, "queues_raw")
    raw_divisions = read_raw_collection(spark, "divisions_raw")
    raw_skills = read_raw_collection(spark, "skills_raw")
    raw_wrapups = read_raw_collection(spark, "wrapup_codes_raw")
    raw_conversations = read_raw_collection(spark, "conversations_raw")

    # -------------------------
    # Transform dimensions
    # -------------------------
    write_to_mongo_upsert(build_dim_users(raw_users), "dim_users", "User_Id")
    write_to_mongo_upsert(build_dim_queue(raw_queues), "dim_queue", "Queue_Id")
    write_to_mongo_upsert(build_dim_division(raw_divisions), "dim_division", "Division_Id")
    write_to_mongo_upsert(build_dim_skills(raw_skills), "dim_skills", "Skill_Id")
    write_to_mongo_upsert(build_dim_qualification(raw_wrapups), "dim_qualification", "WrapUpCode_Id")

    # -------------------------
    # Transform facts
    # -------------------------
    write_to_mongo_upsert(build_fact_calls(raw_conversations), "fact_calls", "Interaction_Id")
    write_to_mongo_upsert(
        build_fact_call_skills(raw_conversations),
        "fact_call_skills",
        "Interaction_Id"
    )

    spark.stop()


with DAG(
    dag_id="genesys_analytics_near_realtime_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/2 * * * *",
    catchup=False,
    tags=["genesys", "etl"]
) as dag:

    PythonOperator(
        task_id="run_genesys_etl",
        python_callable=run_genesys_etl
    )
