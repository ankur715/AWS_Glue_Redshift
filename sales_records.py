import pandas as pd

data = pd.read_csv("data/sales_records.csv")
print(data.shape)

## S3 to create bucket and store
import boto3
access_key = pd.read_csv("../ankur_accessKeys_s3.csv")
sess = boto3.Session(region_name="us-east-2")
client = sess.client('s3',
                     aws_access_key_id=access_key["Access key ID"][0],
                     aws_secret_access_key=access_key["Secret access key"][0])
bucket_name = 'forever21etl'
obj_name = "sales-records.csv"
data.to_csv(obj_name, index=False)
client.upload_file(obj_name, bucket_name, obj_name)