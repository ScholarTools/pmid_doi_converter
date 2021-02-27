#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Approach
1) Is the file in the DB? - call Lambda that answers that ...
2) 
"""

#TODO: Get list of all files

from ftplib import FTP
import os
import wget
import boto3
import botocore
import json

BUCKET_NAME = 'pubmed2021'

#https://stackoverflow.com/questions/44147352/invoking-lambda-with-boto-doesnt-respect-timeout
cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=300, 
                             connect_timeout=300, region_name="us-east-2" )


session = boto3.Session(
    aws_access_key_id=os.environ['aws_access_key'],
    aws_secret_access_key=os.environ['aws_secret'],
    region_name='us-east-2'
)

#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html
lambda_client = session.client('lambda',config=cfg)

#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
s3_client = session.client('s3')

print('Retrieving list of files in S3')
response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
s3_files = [x['Key'] for x in response['Contents']]
if 'NextContinuationToken' in response:
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME,ContinuationToken=response['NextContinuationToken'])
    s3_files.extend([x['Key'] for x in response['Contents']])  
    
    
s3_files = sorted([x for x in s3_files if x.endswith('gz')])



"""
        response = lambda_client.invoke(
            FunctionName='db_updater',
            Payload=json.dumps('new')
        )
"""

"""
        response = lambda_client.invoke(
            FunctionName='db_updater',
            Payload=json.dumps('get')
        )
        wtf = json.loads(response['Payload'].read())
        wtf2 = json.loads(wtf['body'])
        
        response = lambda_client.invoke(
            FunctionName='db_updater',
            Payload=json.dumps(s3_files[0])
        )

"""

#timeout issue
#https://github.com/boto/boto3/issues/2424

response = lambda_client.invoke(
    FunctionName='db_updater',
    Payload=json.dumps('get')
)
wtf = json.loads(response['Payload'].read())
db_files_added = json.loads(wtf['body'])

for file_name in s3_files:
    if file_name not in db_files_added:   
        print('Adding file %s :' % (file_name), end =" ")
        response = lambda_client.invoke(
            FunctionName='db_updater',
            Payload=json.dumps(file_name)
        )
    
        wtf = json.loads(response['Payload'].read())
        if 'errorMessage' in wtf:
            raise Exception(wtf['errorMessage'])
        else:
            #yikes ...
            wtf2 = json.loads(wtf['body'])
            if wtf2['add_status'] == 1:
                print('file already added')
            else:
                print('file added')
            
