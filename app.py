from flask import Flask,request
import json 
import boto3

status = { 
        "status": "uploaded",
        "error": ""
        }

app = Flask(__name__)

@app.route("/upload", methods = ['POST'])
def upload():
    print "Pradeep"
    print (request.is_json)
    content = request.get_json()
    print (content)
    client = boto3.client('s3')
    client.put_object(Body=json.dumps(content), Bucket='ppanga-json', Key='201812301020/payload.json')
    #return 'JSON posted'
    return json.dumps(status)

app.run(host='0.0.0.0', port= 8090)
