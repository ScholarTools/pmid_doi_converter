#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

#db_updater_part1

Algorithm
---------
1) Download file to S3
2) Trigger other Lambda

"""


from ftplib import FTP
import os
import wget
import boto3
import json


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
    
    
def is_update_file(file_name):
    
    #file_name = 'pubmed21n0007.xml.gz'
    
    year = int(file_name[6:8])
    file_id = int(file_name[9:13])
    
    #1062 - last baseline
    if year == 21:
        flag = file_id > 1062
    else:
        raise Exception('Unhandled year')
        
    return flag

def transfer_file_to_s3(s3_client, file_name):
    
    BUCKET_NAME = "pubmed2021"
    
    if is_update_file(file_name):
        URL_ROOT = 'https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/'
    else:
        URL_ROOT = 'https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/'

    
    src = URL_ROOT + file_name
    local_dest = '/tmp/' + file_name
    
    wget.download(src,out=local_dest)
   
    response = s3_client.upload_file(local_dest, BUCKET_NAME, file_name)
        
    os.remove(local_dest)
    
    return response
    

def lambda_handler(event, context):
    
    #TODO: Eventually get the filename from event
    #file_name = 'pubmed21n1085.xml.gz'
    
    s3_client = boto3.client('s3')
    
    if len(event) < 10:
        files = get_file_list(is_update=True)
 
        #TODO: 1000 should be fine for update ...
        #eventually could run into problems if baseline is too long
        #
        #TODO: I think we can use lexographical order - use last baseline file
        response = s3_client.list_objects_v2(Bucket='pubmed2021',StartAfter='pubmed21n1')
        s3_files = [x['Key'] for x in response['Contents']]
        for file_name in files:

            if file_name not in s3_files:
                print('Retrieving: %s' % (file_name))
                response = transfer_file_to_s3(s3_client, file_name)
            
            
        #invocation from event bridge
        return {
            'statusCode': 200,
            'body': json.dumps(event)
            }
    else:
        #manual invocation
        file_name = event
        response = transfer_file_to_s3(s3_client, file_name)
        
        return {
            'statusCode': 200,
            'body': json.dumps(response)
        }