import os
import boto3

RESULTS_TABLE_NAME = os.getenv("RESULTS_TABLE_NAME", "")
TASKS_TABLE_NAME = os.getenv("TASK_TABLE_NAME", "")

def save_results(task_id: str, resolver: str, status: str):
    dynamodb = boto3.client('dynamodb')
    item = {
        'task_id': {
            'S': task_id
        },
        'resolver': {
            'S': resolver
        },
        'status': {
            'S': status
        }
    }

    dynamodb.put_item(TableName=RESULTS_TABLE_NAME, Item=item)

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

def update_task_status(task_id: str, status: str):
    dynamodb = boto3.client('dynamodb')
    dynamodb.update_item(
        TableName=TASKS_TABLE_NAME,
        Key={
            'task_id': {
                'S': task_id
            }
        },
        UpdateExpression="set #status = :status",
        ExpressionAttributeNames={
            "#status": "status"
        },
        ExpressionAttributeValues={
            ":status": {
                'S': status
            }
        }
    )