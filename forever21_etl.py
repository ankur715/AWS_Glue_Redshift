import requests
import boto3
import json
import pandas as pd

## Forever21 API to query for information about categories, products, etc.
url = "https://apidojo-forever21-v1.p.rapidapi.com/categories/v2/list"
headers = {
	"X-RapidAPI-Key": "2f16fd7b75msh78aedd85e4d98bep157157jsn36b644047edf",
	"X-RapidAPI-Host": "apidojo-forever21-v1.p.rapidapi.com"
}
response = requests.get(url, headers=headers)
# print(response.json())
data = pd.json_normalize(response.json())

## S3 to create bucket and store
import boto3
access_key = pd.read_csv("../ankur_accessKeys_s3.csv")
sess = boto3.Session(region_name="us-east-2")
client = sess.client('s3',
                     aws_access_key_id=access_key["Access key ID"][0],
                     aws_secret_access_key=access_key["Secret access key"][0])
bucket_name = 'forever21etl'
client.create_bucket(Bucket=bucket_name,
    CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
# s3 = boto3.client('s3')
# json_object = response.json()
# client.put_object(
#      Body=json.dumps(json_object),
#      Bucket=bucket_name,
#      Key=access_key["Secret access key"][0]
# )
obj_name = "forever21-items.csv"
data.to_csv(obj_name, index=False)
client.upload_file(obj_name, bucket_name, obj_name)