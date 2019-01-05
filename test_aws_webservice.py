from aws_webservice import app
from aws_webservice import get_s3_key_name
from aws_webservice import upload_json_to_s3
from flask import json
import boto3
from moto import mock_s3

def test_upload():
    response = app.test_client().post(
        '/upload',
        data=json.dumps({"device":"TemperatureSensor", "value":"20", "timestamp":"25/01/2017 10:17:00"}),
        content_type='application/json',
    )
    data = json.loads(response.get_data(as_text=True))

    assert response.status_code == 200
    assert data['status'] == "uploaded"

def test_get_s3_key_name():
    assert get_s3_key_name("25/01/2017 10:17:00") == "201701251015"

@mock_s3
def test_upload_json_to_s3():
    conn = boto3.resource('s3', region_name='us-east-1')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket='ppanga-json')

    data={'device': 'TemperatureSensor', 'timestamp': '25/01/2017 10:17:00', 'value': u'20'}

    upload_json_to_s3(data)

    body = json.loads(conn.Object('ppanga-json', '201701251015/payload.json').get()['Body'].read().decode("utf-8"))

    assert body == data