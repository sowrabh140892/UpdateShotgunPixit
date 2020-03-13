import boto3
import json
import os
from os import environ
import time
import re
import logging
import sys
import sentry_sdk
import shotgun_api3
from sentry_sdk.integrations.logging import ignore_logger
sentry_sdk.init(dsn='https://2fee4ed938294813aeeb28f08e3614b8@sentry.io/1858927')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

UAP_SNS_TOPIC="arn:aws:sns:ap-southeast-2:416748369696:UAPSNS"

BUCKET='aws-batch-parameter'
KEY=str(sys.argv[1])
session = boto3.Session()
#print dir(session)

count=0

try:
    s3 = boto3.resource('s3')
    s3.Bucket(BUCKET).download_file(KEY, '/tmp/%s'%(KEY))
except Exception as e:
    raise ValueError('Error while downloading temmplate file %s from %s '%(BUCKET))
    logger.info(' ERROR while downloading template file')
finally:
    logger.info('Downloading Template Ended')


file=open('/tmp/%s'%(KEY)).read().splitlines()

print(file[count])

line = file[count].split(' ')

if len(line)==12:
	SHOTGUN_TYPE=str(line[7])
	SHOTGUN_ENTITY_ID=str(line[8])
	SHOTGUN_ENTITY_TYPE=str(line[9])
	SHOTGUN_ATTRIBUTE_NAME=str(line[10])
	SHOTGUN_ATTRIBUTE_VALUE=str(line[11])
	SHOTGUN_ATTRIBUTE_VALUE=SHOTGUN_ATTRIBUTE_VALUE.replace("-"," ")
elif len(line)==16:
	SHOTGUN_TYPE=str(line[7])
	SHOTGUN_ENTITY_ID=str(line[8])
	SHOTGUN_ENTITY_TYPE=str(line[9])
	SHOTGUN_ATTRIBUTE_NAME=str(line[10])
	SHOTGUN_ATTRIBUTE_VALUE=str(line[11])
	SHOTGUN_ATTRIBUTE_VALUE=SHOTGUN_ATTRIBUTE_VALUE.replace("-"," ")
	PIXIT_PROJECT_TYPE=str(line[12])
	PIXIT_TYPE=str(line[13])
	PIXIT_DELIVERY_ID=str(line[14])
	print(PIXIT_DELIVERY_ID)
	PIXIT_MANIFEST_PATH=str(line[15])


print(line)

try:
    SHOTGUN_SCRIPT_NAME=str(os.environ['SHOTGUN_SCRIPT_NAME'])
    SHOTGUN_SCRIPT_KEY=str(os.environ['SHOTGUN_SCRIPT_KEY'])
    SHOTGUN_HOST_NAME=str(os.environ['SHOTGUN_HOST_NAME'])	
except Exception as e:
    raise ValueError('Error while accessing Shotgun details from environment variables')
    logger.info('Error while accessing Shotgun details from environment variables')

def send_message_to_sns(topic_arn, message_body, message_attrs):
    """
    :param topic_arn:
    :param message_body:
    :param message_attrs:
    :return:
    """
    session = boto3.Session(region_name='ap-southeast-2')
    sns = session.client('sns')
    response = sns.publish(TopicArn=topic_arn,
                           Message=message_body,
                           MessageAttributes=message_attrs
                           )
    return response

def main():
    """
    :return:
    """
	
    if len(line)==12:
		msg_attr={}
		msg_body = 'Changing Delivery Status to Delivered'
		msg_attr['Type'] = {"DataType": "String", "StringValue": SHOTGUN_TYPE}
		msg_attr['entityId'] = {"DataType": "Number", "StringValue": SHOTGUN_ENTITY_ID}
		msg_attr['entityType'] = {"DataType": "String", "StringValue": SHOTGUN_ENTITY_TYPE}
		msg_attr['attributeName'] = {"DataType": "String", "StringValue": SHOTGUN_ATTRIBUTE_NAME}
		msg_attr['attributeValue'] = {"DataType": "String", "StringValue": SHOTGUN_ATTRIBUTE_VALUE}
		response = send_message_to_sns(topic_arn=UAP_SNS_TOPIC, message_body=msg_body, message_attrs=msg_attr)
		print(response)
    elif len(line)==16:
		msg_attr={}
		msg_body = 'Changing Delivery Status to Delivered'
		msg_attr['Type'] = {"DataType": "String", "StringValue": SHOTGUN_TYPE}
		msg_attr['entityId'] = {"DataType": "Number", "StringValue": SHOTGUN_ENTITY_ID}
		msg_attr['entityType'] = {"DataType": "String", "StringValue": SHOTGUN_ENTITY_TYPE}
		msg_attr['attributeName'] = {"DataType": "String", "StringValue": SHOTGUN_ATTRIBUTE_NAME}
		msg_attr['attributeValue'] = {"DataType": "String", "StringValue": SHOTGUN_ATTRIBUTE_VALUE}
		response = send_message_to_sns(topic_arn=UAP_SNS_TOPIC, message_body=msg_body, message_attrs=msg_attr)
		print(response)
		msg_attr={}
		msg_body = 'Changing Delivery Status to Delivered'
		msg_attr['ProjectType'] = {"DataType": "String", "StringValue": PIXIT_PROJECT_TYPE}
		msg_attr['Type'] = {"DataType": "String", "StringValue": PIXIT_TYPE}
		msg_attr['delivery_id'] = {"DataType": "Number", "StringValue": PIXIT_DELIVERY_ID}
		msg_attr['manifest_path'] = {"DataType": "String", "StringValue": PIXIT_MANIFEST_PATH}
		response = send_message_to_sns(topic_arn=UAP_SNS_TOPIC, message_body=msg_body, message_attrs=msg_attr)
		print(response)
		
    sg = shotgun_api3.Shotgun(SHOTGUN_HOST_NAME, SHOTGUN_SCRIPT_NAME, SHOTGUN_SCRIPT_KEY)
    sg.create("Reply", {"entity": {"type": SHOTGUN_ENTITY_TYPE, "id": int(SHOTGUN_ENTITY_ID)},"content": "AWS Copy Completed..."})
    
	
if __name__ == '__main__':
    main()
	
