import logging
import sys
import sentry_sdk
sentry_sdk.init(dsn="https://b2c77e36a6794ca08dd31681c645c876@sentry.io/1412782")

import boto3
from boto3.dynamodb.conditions import Key, Attr
import Utilities

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def send_message_to_sns(job_data):
    """
    Send a message to sns for pixit
    """
    msg_body = '%s'
    msg_attr = dict()
    msg_attr['ProjectType'] = {"DataType": "String", "StringValue": job_data['project_type']}
    msg_attr['Type'] = {"DataType": "String", "StringValue": job_data['forward_as_process_type']}
    msg_attr['delivery_id'] = {"DataType": "Number", "StringValue": str(job_data['entity_id'])}
    msg_attr['manifest_path'] = {"DataType": "String", "StringValue": job_data['manifest_path']}

    sns_session = boto3.Session(region_name=job_data['batch_region'])
    sns = sns_session.client('sns')
    sns_response = sns.publish(TopicArn=job_data['sns_topic'],
                               Message=msg_body,
                               MessageAttributes=msg_attr)
    return sns_response


def main():
    """
    :return:
    """
    bucket_name = str(sys.argv[1])
    json_key = str(sys.argv[2])
    logger.info('bucket_name - %s, json_key - %s' % (bucket_name, json_key))

    logger.info('downloading and parsing json')
    job_data = Utilities.get_json_data(bucket=bucket_name, obj_key=json_key)
    logger.info('downloaded and parsing json')

    logger.info("getting sg object")
    sg = Utilities.get_sg_object(job_data=job_data)
    # dict to create the kwargs for the copy process

    # send the sns to pixit
    if job_data['process_type'] == "FromVendorToPixit":
        logger.info("sending sns message")
        ret = send_message_to_sns(job_data=job_data)
        logger.info(ret)

    # create the reply to update delivery entity
    logger.info("creating shotgun reply")
    sg.create("Reply", {
        "entity": {"type": job_data['entity_type'], "id": job_data['entity_id']},
        "content": "AWS Copy Completed..."
    })

    logger.info("updating shotgun status")
    sg.update(job_data['entity_type'], job_data['entity_id'], job_data['entity_status_updates'])

    # upload the dynamo db logs
    logger.info("getting info from dynamo db and creating log file")
    dynamo_db = boto3.resource('dynamodb', region_name=job_data['batch_region'])
    dynamo_table = dynamo_db.Table(job_data['dynamo_table_name'])
    dynamo_db_log_file = '/tmp/batch_copy_log_%s.txt' % job_data['entity_id']
    dynamo_db_response = dynamo_table.query(KeyConditionExpression=Key('ProcessID').eq(str(job_data['entity_id'])))
    with open(dynamo_db_log_file, 'w') as _stream:
        for content in dynamo_db_response['Items']:
            _stream.write('%s,%s,%s,%s\n' % (str(content['ProcessNumber']), str(content['TargetPath']),
                                             str(content['Status']), str(content['ProcessTime'])))
            if content['Error']:
                _stream.write('         Error:-\n%s\n' % str(content['Error']))

    logger.info("uploading log file to shotgun")
    sg.upload(job_data['entity_type'], job_data['entity_id'], dynamo_db_log_file)


if __name__ == '__main__':
    main()
