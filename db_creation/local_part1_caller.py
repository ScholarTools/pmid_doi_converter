#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Running this code forces all pubmed files to be put on S3
"""
#Standard
from ftplib import FTP
import json
import os

#Third
import boto3


BUCKET_NAME = 'pubmed2021'

session = boto3.Session(
    aws_access_key_id=os.environ['aws_access_key'],
    aws_secret_access_key=os.environ['aws_secret'],
    region_name='us-east-2'
)

#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html
lambda_client = session.client('lambda')

#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
s3_client = session.client('s3')

def get_ftp_conn(is_update=True):
    
    FTP_ROOT = 'ftp.ncbi.nlm.nih.gov'

    ftp = FTP(FTP_ROOT)
    
    ftp.login()
    
    if is_update:
        ftp.cwd('pubmed/updatefiles/')
    else:
        ftp.cwd('pubmed/baseline/')
    
    return ftp


def get_file_list(is_update=True):
    
    ftp = get_ftp_conn(is_update=is_update)
    
    files = ftp.nlst()
    files = sorted(filter (lambda x:x.endswith(".gz") , files))
    
    return files
    
    
print('retrieving baseline file list')
files = get_file_list(is_update=False)
print('retrieving update file list')
files.extend(get_file_list(is_update=True))

#TODO: Note this only handles 2000 which is fine
#for 2021 ...
response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
s3_files = [x['Key'] for x in response['Contents']]
if 'NextContinuationToken' in response:
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME,ContinuationToken=response['NextContinuationToken'])
    s3_files.extend([x['Key'] for x in response['Contents']])  
    
for file_name in files:
    
    if file_name in s3_files:
        print('%s already exists' % (file_name))
    else:
        print('Requesting file %s' % (file_name))
        data = file_name
        
        response = lambda_client.invoke(
            FunctionName='db_updater_part1',
            Payload=json.dumps(data)
        )

        wtf = json.loads(response['Payload'].read())
        if 'errorMessage' in wtf:
            raise Exception(wtf['errorMessage'])
        
"""
response = client.invoke(
    FunctionName='db_updater_part1',
    InvocationType='Event'|'RequestResponse'|'DryRun',
    LogType='None'|'Tail',
    ClientContext='string',
    Payload=b'bytes'|file,
    Qualifier='string'
)
"""