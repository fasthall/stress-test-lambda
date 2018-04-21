import time
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('stress-output')

def lambda_handler(event, context):
    for ev in event['Records']:
        if ev['eventName'] == 'INSERT':
            obj = ev['dynamodb']['NewImage']
            record = {}
            record['woof'] = obj['woof']['S']
            record['posted'] = obj['posted']['N']
            record['fielded'] = int(time.time() * 1000.0)
            record['id'] = obj['Id']['S']
        
            response = table.put_item(
                    Item={
                        'woof': obj['woof']['S'],
                        'posted': int(obj['posted']['N']),
                        'fielded': int(time.time() * 1000.0),
                        'Id': obj['Id']['S']
                        }
                    )
    return len(event['Records'])
