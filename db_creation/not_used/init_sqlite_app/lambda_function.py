#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""

#Questions
#----------------------------------
#1) Does it make sense to use some other json library?
#2) 


#Functions
#------------------------
#- pmid list - get dois
#- doi list - get pmids

import time
import json
import sqlite3

sql_file_path = "/Users/jim/Desktop/pubmed_db.sql"

#How does 'in' compare to multiple queries???


def find_doi(c,pmid):
    """
    
    pmid = 2994283

    """
    
    
   
    t = (pmid,)
    #TODO: How to say match 1 only ...
    c.execute('SELECT doi FROM ids WHERE pmid=?', t)
    temp = c.fetchone()
    if temp is None:
        return ''
    else:
        return temp[0]
    
def find_dois1(c,pmids):
    
    #Format:
    #- match input
    dois = []
    for pmid in pmids:
        dois.append(find_doi(c,pmid))
        
    return dois
    
def load_db():
    conn = sqlite3.connect(sql_file_path)
    c = conn.cursor()
    return c

#-------------------------------------------------------------------
#
#   get pmids_from_dois(dois)
#   get dois_from_pmids(pmids)
#
#-------------------------------------------------------------------
#
#   event: from printing, can't find documentation ...
#   {"version": "2.0", "routeKey": "ANY /", "rawPath": "/", "rawQueryString": "", "headers": {"accept": 
#   {"version": "2.0", "routeKey": "GET /pmids_from_dois", "rawPath": "/pmids_from_dois", "rawQueryString": "", "headers": {"accept":
#
#   "queryStringParameters": {"test": "1"},
#
#   context : https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
#

def pmids_from_dois_hanlder(event, context):
    
    pass


def lambda_handler(event, context):
    
    #??? What is event
    #??? What is context
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

if __name__ == "__main__":
    #In the DB
    t1 = time.time()
    c = load_db()
    t2 = time.time()
    print('Loading time: %g' % (t2-t1))
    result = find_doi(c,3024130)
    t3 = time.time()
    print(result)
    #Not in the DB
    result = find_doi(c,1234)
    print(result)
    t3 = time.time()
    result = find_dois1(c,[1e3, 1e4, 1e5, 1e6, 2e6, 2e7, 2994282, 2994284, 2994270, 2994139])
    t4 = time.time()
    print(result)
    print(t4-t3)
    
    
