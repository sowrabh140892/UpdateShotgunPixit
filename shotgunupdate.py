import boto3
import json
import os
from os import environ
import re
import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)
TYPE=str(sys.argv[1])
ENTITY_ID=str(sys.argv[2])
ENTITY_TYPE=str(sys.argv[3])
ATTRIBUTE_NAME=str(sys.argv[4])
ATTRIBUTE_VALUE=str(sys.argv[5])
UAP_SNS_TOPIC="arn:aws:sns:ap-southeast-2:416748369696:UAPSNS"



def get_aws_session(region_name):
    """
    :param access_id:
    :param access_key:
    :param region_name:
    :return:
    """
    print "Creating AWS Session..."
    return boto3.Session(region_name=region_name)
def send_message_to_sns(topic_arn, message_body, message_attrs):
    """
    :param topic_arn:
    :param message_body:
    :param message_attrs:
    :return:
    """
    session = get_aws_session(region_name='ap-southeast-2', )
    sns = session.client('sns')
    response = sns.publish(TopicArn=topic_arn,
                           Message=message_body,
                           MessageAttributes=message_attrs
                           )
    return response

msg_attr={}
msg_body = 'Changing Delivery Status to Delivered'
msg_attr['Type'] = {"DataType": "String", "StringValue": TYPE}
msg_attr['entityId'] = {"DataType": "Number", "StringValue": ENTITY_ID}
msg_attr['entityType'] = {"DataType": "String", "StringValue": ENTITY_TYPE}
msg_attr['attributeName'] = {"DataType": "String", "StringValue": ATTRIBUTE_NAME}
msg_attr['attributeValue'] = {"DataType": "String", "StringValue": ATTRIBUTE_VALUE}


response = send_message_to_sns(topic_arn=UAP_SNS_TOPIC, message_body=msg_body, message_attrs=msg_attr)
print(response)
