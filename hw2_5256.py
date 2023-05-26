#!/usr/bin/env python
# coding: utf-8

# In[1]:


import boto3
import botocore
import pandas as pd
from IPython.display import display, Markdown

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')


# In[2]:


def create_bucket(bucket):
    import logging

    try:
        s3.create_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return 'Bucket ' + bucket + ' could not be created.'
    return 'Created or already exists ' + bucket + ' bucket.'

create_bucket('oat-cs650-5256')


# In[3]:


def list_buckets(match=''):
    response = s3.list_buckets()
    if match:
        print(f'Existing buckets containing "{match}" string:')
    else:
        print('All existing buckets:')
    for bucket in response['Buckets']:
        if match:
            if match in bucket["Name"]:
                print(f'  {bucket["Name"]}')
                
list_buckets(match='oat')


# In[5]:


def list_bucket_contents(bucket, match='', size_mb=0):
    bucket_resource = s3_resource.Bucket(bucket)
    total_size_gb = 0
    total_files = 0
    match_size_gb = 0
    match_files = 0
    for key in bucket_resource.objects.all():
        key_size_mb = key.size/1024/1024
        total_size_gb += key_size_mb
        total_files += 1
        list_check = False
        if not match:
            list_check = True
        elif match in key.key:
            list_check = True
        if list_check and not size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print(f'{key.key} ({key_size_mb:3.0f}MB)')
        elif list_check and key_size_mb <= size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print(f'{key.key} ({key_size_mb:3.0f}MB)')

    if match:
        print(f'Matched file size is {match_size_gb/1024:3.1f}GB with {match_files} files')            
    
    print(f'Bucket {bucket} total size is {total_size_gb/1024:3.1f}GB with {total_files} files')
    
    list_bucket_contents(bucket='oat', match='2017', size_mb=250)


# In[9]:


pip install pyarrow


# In[15]:


def preview(bucket, key):
    data_source = {
            'Bucket': bucket,
            'Key': key
        }
    # Generate the URL to get Key from Bucket
    url = s3.generate_presigned_url(
        ClientMethod = 'get_object',
        Params = data_source
    )

    data = pd.read_parquet(url , engine='pyarrow')
    return data

df = preview(bucket='nyc-tlc', key='trip data/yellow_tripdata_2017-02.parquet')
df.head(6)


# In[16]:


def key_exists(bucket, key):
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            return(False)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The key does exist.
        return(True)

def copy_among_buckets(from_bucket, from_key, to_bucket, to_key):
    if not key_exists(to_bucket, to_key):
        s3_resource.meta.client.copy({'Bucket': from_bucket, 'Key': from_key}, 
                                        to_bucket, to_key)        
        print(f'File {to_key} saved to S3 bucket {to_bucket}')
    else:
        print(f'File {to_key} already exists in S3 bucket {to_bucket}') 


# In[17]:


for i in range(1,6):
    copy_among_buckets(from_bucket='nyc-tlc', from_key=f'trip data/yellow_tripdata_2017-0{i}.parquet',
                      to_bucket='oat-cs650-5256', to_key=f'yellow_tripdata_2017-0{i}.parquet')


# In[21]:


import numpy as np
yellow_jan_PULocationID = df['PULocationID'].unique()
np.sort(yellow_jan_PULocationID)


# In[13]:


import boto3
s3 = boto3.client('s3')
sum = 0
for i in range(1,6):
    resp = s3.select_object_content(
        Bucket='oat-cs650-5256',
        Key='yellow_tripdata_2017-01.parquet',
        
        ExpressionType='SQL',
        
        Expression=f"SELECT COUNT(payment_type) FROM s3object s WHERE payment_type= {i}",
        InputSerialization={'Parquet': {}},
        OutputSerialization = {'CSV': {}},
    )
    
    for event in resp['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            sum = sum + int(records)
            print(f"จำนวน yellow taxi ที่มี payment_type={i} เท่ากับ {records}")
            print(f"มี yellow taxi รวมทั้งสิ้น {sum} คัน")


# In[29]:


import boto3

s3 = boto3.client('s3')

def cal_rides_each_month(month):
    sum = 0 
    for type in range(1,6):
        resp=s3.select_object_content(
        Bucket='oat-cs650-5256',
        Key=f'yellow_tripdata_2017-0{month}.parquet',
        ExpressionType='SQL',
        Expression=f"SELECT COUNT(payment_type) FROM s3object s WHERE payment _type={type}",
        InputSerialization={'Parquet':{}},
        OutputSerialization={'CSV':{}},
    )

    for event in resp['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
        
            records = int(records)
            sum = sum + records
            if type==1:
                type1.append(records)
            elif type==2:
                type2.append(records)
            elif type==3:
                type3.append(records)
            elif type==4:
                type4.append(records)
            else:
                type5.append(records)
            
print(f"จำนวน yellow taxi rides เดือน ที่มี payment_type={type} เท่ากับ {records}")
print(f"rides เดือน มี yellow taxi rides รวมทั้งสิ้น {sum} เที่ยว")
print()


# In[31]:


data = {
"month": ['Jan', 'Feb', 'Mar', 'April', 'May'],
'payment type 1': type1,
'payment type 2': type2,
'payment type 3': type3,
'payment type 4': type4,
'payment type 5': type5,
'sum': sum_rides
}

hw_item3 = pd.DataFrame(data)
hw_item3


# In[ ]:




