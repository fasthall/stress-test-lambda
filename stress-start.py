import time
import threading
import uuid
import boto3

dynamodb = boto3.resource('dynamodb')
inputTable = dynamodb.Table('stress-input')
outputTable = dynamodb.Table('stress-output')

client = boto3.client('sns')
pLock = threading.Lock()
gLock = threading.Lock()
putRemain = 0
payloadSize = 106
ids = []
done = 0
output = ''

def put():
    global putRemain
    global ids

    pLock.acquire()
    while putRemain > 0:
        putRemain -= 1
        pLock.release()
        id = str(uuid.uuid4())
        response = inputTable.put_item(
            Item={
                'woof': 'stress-input',
                'posted': int(time.time() * 1000.0),
                'Id': id
                }
            )
        gLock.acquire()
        ids += [id]
        gLock.release()
        pLock.acquire()
    pLock.release()

def get():
    global ids
    global done
    global output

    while (done == 0) or (len(ids) > 0):
        gLock.acquire()
        try:
            id = ids[0]
        except IndexError:
            gLock.release()
            continue
        ids = ids[1:]
        gLock.release()
        retries = 0
        while retries < 30:
            response = outputTable.get_item(
                Key={
                    'Id': id
                }
            )
            if 'Item' not in response:
                retries += 1
            else:
                elapsed = response['Item']['fielded'] - response['Item']['posted']
                # output += 'Id {} elapsed {}\n'.format(response['Item']['Id'], elapsed)
                output += (str(elapsed) + ' ')
                break
        if retries == 30:
            print('you fail')
    
def lambda_handler(event, context):
    global putRemain
    global done
    global output

    putRemain = int(event['size'])
    pt = int(event['pt'])
    gt = int(event['gt'])
    output = ''
    done = 0

    pThreads = []
    for i in range(pt):
        t = threading.Thread(target=put)
        pThreads.append(t)
        t.start()

    gThreads = []
    for i in range(gt):
        t = threading.Thread(target=get)
        gThreads.append(t)
        t.start()
    
    for pth in pThreads:
        pth.join()
    done = 1
    for gth in gThreads:
        gth.join()
    print(output)

    return ''
