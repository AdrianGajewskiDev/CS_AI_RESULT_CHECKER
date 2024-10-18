from ast import Tuple

from results_checker.check_all_results_completed.check_all_results_completed import check_all_results_completed
from results_checker.dynamo_db.dynamo_db import update_task_status
from results_checker.logging.logger import InternalLogger
from results_checker.sns.sns import publish_to_sns
from cs_ai_common.dynamodb.resolver_task_table import update_resolver_task

def check_results(event: dict) -> dict:
    records = event.get("Records", [])
    InternalLogger.LogDebug(f"Received {len(records)} records.")
    for record in records:
        process_record(record)

def process_record(record: dict) -> None:
    InternalLogger.LogDebug(f"Processing record: {record}")
    key = record.get("s3", {}).get("object", {}).get("key", "")

    if not key:
        raise Exception("Key not found in record.")
    
    task_id, resolver = extract_task_id_and_resolver(key)
    InternalLogger.LogDebug(f"Task ID: {task_id}")
    InternalLogger.LogDebug(f"Resolver: {resolver}")

    InternalLogger.LogDebug("Saving results to DynamoDB.")
    update_resolver_task(task_id, resolver, status="COMPLETED")

    if not check_all_results_completed(task_id):
        InternalLogger.LogDebug("Not all results completed.")
        return
    
    InternalLogger.LogDebug("All results completed.")
    update_task_status(task_id, "DATA_GATHERED")

    InternalLogger.LogDebug("Task status updated to DATA_GATHERED.")

    InternalLogger.LogDebug("Publishing to SNS.")
    publish_to_sns({
        "task_id": task_id
    })
    InternalLogger.LogDebug("Published to SNS.")
    InternalLogger.LogDebug("Record processed.")
    return 0

def extract_task_id_and_resolver(key: str):
    key_parts = key.split("/")
    if len(key_parts) != 3:
        raise Exception(f"Invalid key format: {key}")
    
    return key_parts[0], key_parts[1]