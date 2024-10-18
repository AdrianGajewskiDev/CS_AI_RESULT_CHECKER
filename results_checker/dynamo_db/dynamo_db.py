import os
import boto3

RESULTS_TABLE_NAME = os.getenv("RESULTS_TABLE_NAME", "")
TASKS_TABLE_NAME = os.getenv("TASK_TABLE_NAME", "")

def get_processed_results(task_id: str, resolver: str):
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.query(
        TableName=RESULTS_TABLE_NAME,
        KeyConditionExpression="task_id = :task_id AND resolver = :resolver",
        ExpressionAttributeValues={
            ":task_id": {
                'S': task_id
            },
            ":resolver": {
                'S': resolver
            }
        }
    )

    return response.get("Items", [])