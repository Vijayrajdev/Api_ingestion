"""
Author: Vijay Raj C K
Date: 2024-05-23
Description: This script fetches data from an API, processes it, and loads it into Google BigQuery and Google Cloud Storage.
Version: 1.1.0
"""

import json
import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, date
from google.api_core.exceptions import NotFound
import time


def get_batch_date(bigquery_client, audit_database, batch_control_table_id):
    batch_control_query = f"""
    SELECT batch_date
    FROM `{audit_database}.{batch_control_table_id}`
    """
    batch_control_result = bigquery_client.query(batch_control_query).to_dataframe()

    if batch_control_result.empty or "batch_date" not in batch_control_result.columns:
        raise ValueError("Failed to retrieve batch date from batch control table")

    return batch_control_result["batch_date"][0].date()


def get_config(bigquery_client, config_tgt_dataset, config_table_id):
    config_query = f"""
    SELECT jobid, url_qualifier, tgt_table_name, temp_table_name, tgt_schema, GCS_filepath, GCS_filename, api_method, api_headers
    FROM `{config_tgt_dataset}.{config_table_id}`
    """
    return bigquery_client.query(config_query).to_dataframe()


def fetch_data(api_url, api_method, api_headers):
    response = requests.request(api_method, api_url)
    response_time = datetime.now()
    response_code = response.status_code
    if response_code == 200:
        try:
            return response.json(), response_code, response_time
        except json.JSONDecodeError:
            raise ValueError(f"Failed to decode JSON from {api_url}")
    else:
        return response.text, response_code, response_time


def get_table_schema(bigquery_client, tgt_dataset, table_id):
    table_ref = bigquery_client.dataset(tgt_dataset).table(table_id)
    table = bigquery_client.get_table(table_ref)
    return table.schema


def check_table_exists(bigquery_client, tgt_dataset, table_id):
    table_ref = bigquery_client.dataset(tgt_dataset).table(table_id)
    try:
        bigquery_client.get_table(table_ref)
        print(f"Table {table_id} exists in dataset {tgt_dataset}")
        return True
    except NotFound:
        print(f"Table {table_id} does not exist in dataset {tgt_dataset}")
        return False


def load_data_to_bigquery(
    bigquery_client, df, tgt_dataset, table_id, schema, write_disposition
):
    table_ref = bigquery_client.dataset(tgt_dataset).table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=schema, write_disposition=write_disposition
    )
    load_job = bigquery_client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )
    load_job.result()
    return len(df)


def upload_to_gcs(df, csv_file_path, gcs_file_path):
    df.to_csv(csv_file_path, index=False, sep="~")
    storage_client = storage.Client()
    bucket_name, blob_name = gcs_file_path.split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(csv_file_path)
    print(f"CSV file {csv_file_path} uploaded to {gcs_file_path}")
    os.remove(csv_file_path)


def log_audit_entry(
    bigquery_client, log_entry, log_table_id, log_dataset="audit_database"
):
    # Convert datetime objects to strings
    for key, value in log_entry.items():
        if isinstance(value, (datetime, pd.Timestamp)):
            log_entry[key] = value.isoformat()
        elif isinstance(value, date):
            log_entry[key] = value.isoformat()

    log_df = pd.DataFrame([log_entry])

    # Converting to datetime
    log_df["api_requested_time"] = pd.to_datetime(log_df["api_requested_time"])
    log_df["api_response_time"] = pd.to_datetime(log_df["api_response_time"])
    log_df["db_prcsd_dttm"] = pd.to_datetime(log_df["db_prcsd_dttm"])
    log_df["batch_date"] = pd.to_datetime(log_df["batch_date"])

    table_ref = bigquery_client.dataset(log_dataset).table(log_table_id)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    load_job = bigquery_client.load_table_from_dataframe(
        log_df, table_ref, job_config=job_config
    )
    load_job.result()

    if load_job.errors:
        print(f"Failed to log audit entry: {load_job.errors}")
    else:
        print(f"Audit log entry added: {log_entry}")


def main():
    bigquery_client = bigquery.Client()
    config_tgt_dataset = "audit_database"
    config_table_id = "ssot_api_setup"
    log_table_id = "ssot_api_log"
    batch_control_table_id = "batch_control"

    batch_date = get_batch_date(
        bigquery_client, config_tgt_dataset, batch_control_table_id
    )

    config_df = get_config(bigquery_client, config_tgt_dataset, config_table_id)
    if config_df.empty:
        audit_entry = {
            "jobid": None,
            "tgt_schema": None,
            "tgt_table_name": None,
            "processed_rec_count": 0,
            "api_requested_time": None,
            "api_response_time": None,
            "api_response_code": None,
            "exec_time": None,
            "exec_status": "FAILURE",
            "log_file_path": "",
            "error_message": "No configuration found in the configuration table",
            "failure_incident": "ConfigurationError",
            "db_prcsd_dttm": datetime.now(),
            "batch_date": batch_date,
        }
        log_audit_entry(bigquery_client, audit_entry, log_table_id)
        raise ValueError("No configuration found in the configuration table")

    for index, config in config_df.iterrows():
        api_url = config["url_qualifier"]
        api_method = config["api_method"]
        api_headers = (
            json.loads(config["api_headers"])
            if config["api_headers"]
            else {"User-Agent": "Api Ingestion", "Accept": "application/json"}
        )
        table_id = config["tgt_table_name"]
        table_id_temp = config["temp_table_name"]
        tgt_dataset = config["tgt_schema"]
        gcs_file_path = config["GCS_filepath"]
        gcs_file_name = config["GCS_filename"]
        jobid = config["jobid"]

        api_requested_time = datetime.now()
        try:
            if not check_table_exists(bigquery_client, tgt_dataset, table_id):
                audit_entry = {
                    "jobid": jobid,
                    "tgt_schema": tgt_dataset,
                    "tgt_table_name": table_id,
                    "processed_rec_count": 0,
                    "api_requested_time": api_requested_time,
                    "api_response_time": None,
                    "api_response_code": None,
                    "exec_time": None,
                    "exec_status": "FAILURE",
                    "log_file_path": "",
                    "error_message": f"Table {table_id} does not exist in dataset {tgt_dataset}",
                    "failure_incident": "TableNotFoundError",
                    "db_prcsd_dttm": datetime.now(),
                    "batch_date": batch_date,
                }
                log_audit_entry(bigquery_client, audit_entry, log_table_id)
                continue  # Skip to the next config if the table does not exist

            start_time = time.time()
            data, response_code, api_response_time = fetch_data(
                api_url, api_method, api_headers
            )
            exec_time = time.time() - start_time

            if response_code != 200:
                audit_entry = {
                    "jobid": jobid,
                    "tgt_schema": tgt_dataset,
                    "tgt_table_name": table_id,
                    "processed_rec_count": 0,
                    "api_requested_time": api_requested_time,
                    "api_response_time": api_response_time,
                    "api_response_code": response_code,
                    "exec_time": f"{exec_time:.2f}",
                    "exec_status": "FAILURE",
                    "log_file_path": "",
                    "error_message": f"API returned status code {response_code}",
                    "failure_incident": "APIResponseError",
                    "db_prcsd_dttm": datetime.now(),
                    "batch_date": batch_date,
                }
                log_audit_entry(bigquery_client, audit_entry, log_table_id)
                print(f"API returned status code {response_code}: {data}")
                continue

            if not data:
                audit_entry = {
                    "jobid": jobid,
                    "tgt_schema": tgt_dataset,
                    "tgt_table_name": table_id,
                    "processed_rec_count": 0,
                    "api_requested_time": api_requested_time,
                    "api_response_time": api_response_time,
                    "api_response_code": response_code,
                    "exec_time": f"{exec_time:.2f}",
                    "exec_status": "SUCCESS",
                    "log_file_path": "",
                    "error_message": "No records fetched from API",
                    "failure_incident": "NoRecordsError",
                    "db_prcsd_dttm": datetime.now(),
                    "batch_date": batch_date,
                }
                log_audit_entry(bigquery_client, audit_entry, log_table_id)
                print("No records fetched from API")
                continue

            if isinstance(data, dict):
                df = pd.DataFrame([data])
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                raise ValueError(f"Unexpected data format from {api_url}")

            schema = get_table_schema(bigquery_client, tgt_dataset, table_id)

            num_rows_main = load_data_to_bigquery(
                bigquery_client, df, tgt_dataset, table_id, schema, "WRITE_TRUNCATE"
            )

            if check_table_exists(bigquery_client, tgt_dataset, table_id_temp):
                num_rows_temp = load_data_to_bigquery(
                    bigquery_client,
                    df,
                    tgt_dataset,
                    table_id_temp,
                    schema,
                    "WRITE_APPEND",
                )
                print(f"Loaded {num_rows_temp} rows into {tgt_dataset}:{table_id_temp}")

            print(f"Loaded {num_rows_main} rows into {tgt_dataset}:{table_id}")

            date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file_path = os.path.join(r"tmp", f"{gcs_file_name}_{date_str}.csv")
            gcs_full_path = f"{gcs_file_path}/{gcs_file_name}_{date_str}.csv"
            upload_to_gcs(df, csv_file_path, gcs_full_path)

            audit_entry = {
                "jobid": jobid,
                "tgt_schema": tgt_dataset,
                "tgt_table_name": table_id,
                "processed_rec_count": num_rows_main,
                "api_requested_time": api_requested_time,
                "api_response_time": api_response_time,
                "api_response_code": response_code,
                "exec_time": f"{exec_time:.2f}",
                "exec_status": "SUCCESS",
                "log_file_path": gcs_full_path,
                "error_message": "",
                "failure_incident": "",
                "db_prcsd_dttm": datetime.now(),
                "batch_date": batch_date,
            }
            log_audit_entry(bigquery_client, audit_entry, log_table_id)

        except ValueError as e:
            audit_entry = {
                "jobid": jobid,
                "tgt_schema": tgt_dataset,
                "tgt_table_name": table_id,
                "processed_rec_count": 0,
                "api_requested_time": api_requested_time,
                "api_response_time": api_response_time,
                "api_response_code": response_code,
                "exec_time": None,
                "exec_status": "FAILURE",
                "log_file_path": "",
                "error_message": str(e),
                "failure_incident": "ValueError",
                "db_prcsd_dttm": datetime.now(),
                "batch_date": batch_date,
            }
            log_audit_entry(bigquery_client, audit_entry, log_table_id)
            print(e)
        except Exception as e:
            audit_entry = {
                "jobid": jobid,
                "tgt_schema": tgt_dataset,
                "tgt_table_name": table_id,
                "processed_rec_count": 0,
                "api_requested_time": api_requested_time,
                "api_response_time": None,
                "api_response_code": None,
                "exec_time": None,
                "exec_status": "FAILURE",
                "log_file_path": "",
                "error_message": str(e),
                "failure_incident": "Exception",
                "db_prcsd_dttm": datetime.now(),
                "batch_date": batch_date,
            }
            log_audit_entry(bigquery_client, audit_entry, log_table_id)
            print(f"An error occurred: {e}")

    print("Data loading completed successfully!")


if __name__ == "__main__":
    main()
