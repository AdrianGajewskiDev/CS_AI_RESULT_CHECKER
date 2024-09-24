import json
import os
import boto3

TOPIC_ARN = os.getenv("TASKS_COMPLETED_TOPIC_ARN", "")

def publish_to_sns(message: dict):
    sns = boto3.client('sns')
    sns.publish(
        TopicArn=TOPIC_ARN,
        Message=json.dumps({
            "default": json.dumps(message)
        }), 
        MessageStructure="json")