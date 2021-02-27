#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

Interface:

/pmid_to_doi
/pmids_to_dois
/dois_to_pmids
/doi_to_pmid

"""
import os
import time
#import mysql.connector
import pymysql
import json

BUCKET_NAME = 'pubmed2021'
DB_USER = os.environ['mysql_user']
DB_PASS = os.environ['mysql_pass']
try:
    DB_HOST = os.environ['mysql_host']
except:
    DB_HOST = 'localhost'


#EC2 & Lambda support
my_user = os.environ.get("USER") #for EC2
if my_user is None:
    my_user = ''
#                   EC2                     LAMBDA
running_aws = ("ec2" in my_user) or (os.environ.get("AWS_EXECUTION_ENV") is not None)

def connect():
    
    conn = pymysql.connect(
      host=DB_HOST,
      user=DB_USER,
      password=DB_PASS,
      database="mydb",
      port=3306
    )
    
    return conn


mydb = connect()
cursor = mydb.cursor()

def lambda_handler(event, context):
    
    """
    error
    error_message
    
    
    /pmid_to_doi
        status: 0 - success
        status: 1 - missing_pmid_input
        status: 2 - missing_pmid_in_db
    /pmids_to_dois
    /dois_to_pmids
    /doi_to_pmid
    
    """
    
    target = event['rawPath']
    if 'queryStringParameters' in event:
        params = event['queryStringParameters']
    else:
        params = {}
        
    if target == '/pmid_to_doi':
        if 'pmid' not in params:
            return {
                'statusCode': 400,
                'body': json.dumps({'error':'pmid_to_doi:pmid_missing',
                                    'message':'"pmid" parameter missing from in request',
                                    'status':1})
            }
        
        cursor.execute("SELECT doi FROM ids where pmid=%s",(params['pmid'],))
        if cursor.rowcount == 0:
            status = 2
            message = 'pmid value not found in database'
            doi = None
        else:   
            status = 0
            message = 'success'
            myresult = cursor.fetchone()
            doi = myresult[0]
            
        out = {'status':status,
               'message':message,
               'doi':doi}
            
        return {
            'statusCode': 200,
            'body': json.dumps(out)
        }
     
    elif target == '/pmids_to_dois':
        
        pass
    elif target == '/dois_to_pmids':
        print(params)
        return {
            'statusCode': 200,
            'body': json.dumps(1)
        }
    elif target == '/doi_to_pmid':
        if 'doi' not in params:
            return {
                'statusCode': 400,
                'body': json.dumps({'error':'doi_to_pmid:doi_missing',
                                    'message':'"doi" parameter missing from in request',
                                    'status':1})
            }
        
        cursor.execute("SELECT pmid FROM ids where doi=%s",(params['doi'],))
        if cursor.rowcount == 0:
            status = 2
            message = 'doi value not found in database'
            pmid = None
        else:   
            status = 0
            message = 'success'
            myresult = cursor.fetchone()
            pmid = myresult[0]
            
        out = {'status':status,
               'message':message,
               'pmid':pmid}
            
        return {
            'statusCode': 200,
            'body': json.dumps(out)
        }
        pass
    
    
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
