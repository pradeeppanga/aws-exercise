# Author - Pradeep Panga
# All rights reserved.

from flask import Flask,request
import json 
import boto3
import datetime
import uuid

def get_s3_key_name(timestamp):
'''
Generates the S3 key name to use, based on the timestamp information received from payload.
'''
    timestamp_formatted = datetime.datetime.strptime(timestamp, '%d/%m/%Y %H:%M:%S')
    timestamp_string = list(timestamp_formatted.strftime('%Y%m%d%H%M'))
    if int(timestamp_string[-1]) == 0:
        temp_a = "0"
        temp_b = timestamp_string[-2]

    if int(timestamp_string[-1]) - 5 <= 0:
        temp_a = "0"
        temp_b = timestamp_string[-2]
    else:
        temp_a = "5"
        temp_b = int(timestamp_string[-2])
    timestamp_string[-1] = str(temp_a)
    timestamp_string[-2] = str(temp_b)
    timestamp_string = "".join(timestamp_string)
    return str(timestamp_string)

def upload_json_to_s3(content, prefix):
'''
Uploads the JSON payload to S3 bucket.
'''
    client = boto3.client('s3')
    s3_key = get_s3_key_name(content['timestamp'])
    client.put_object(Body=json.dumps(content), Bucket='aws-webservice-panga', Key=str(s3_key) + '/' + prefix + '_payload.json') 

app = Flask(__name__)

@app.route("/upload", methods = ['POST'])
def upload():
'''
Receives the POST from the client and calls the upload_json_to_s3() if the JSON is valid. 
'''
    status = {
        "status": "uploaded",
        "error": ""
        }
    if request.is_json:
        try:
            content = request.get_json()
        except Exception as e:
            status['status'] = "not uploaded"
            status['error'] = "not valid json" + ", " + str(e)
            return json.dumps(status)
        upload_json_to_s3(content, str(uuid.uuid4()))
        return json.dumps(status)
    else:
        status['status'] = "not uploaded"
        status['error'] = "not valid json"
        return json.dumps(status)

@app.route("/", methods = ['GET'])
def health_check():
    return "OK"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port= 8090)
