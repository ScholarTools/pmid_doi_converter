#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
???? What was this file for?


pubmed21n1091.xml.gz - only 7.3 MB

xml:
    Billed Duration: 7814 ms	Memory Size: 256 MB	Max Memory Used: 130 MB
    Billed Duration: 7835 ms	Memory Size: 256 MB	Max Memory Used: 131 MB
gz:
    Billed Duration: 7918 ms	Memory Size: 256 MB	Max Memory Used: 130 MB
    Billed Duration: 8317 ms	Memory Size: 256 MB	Max Memory Used: 96 MB
    Billed Duration: 7885 ms	Memory Size: 256 MB	Max Memory Used: 97 MB
    
    
xml:
    Billed Duration: 14685 ms	Memory Size: 2000 MB	Max Memory Used: 606 MB	Init Duration: 314.28 ms
    Billed Duration: 14194 ms	Memory Size: 2000 MB	Max Memory Used: 611 MB
gz:
    Billed Duration: 14122 ms	Memory Size: 2000 MB	Max Memory Used: 188 MB	Init Duration: 319.91 ms
"""

#https://stackoverflow.com/questions/9856163/using-lxml-and-iterparse-to-parse-a-big-1gb-xml-file

from lxml import etree

import gzip
import shutil
import boto3
import botocore
import os
import json


SOURCE_BUCKET_NAME = 'pubmed2021'


xml_file_name = 'pubmed21n1091.xml.gz'
xml_file_name = 'pubmed21n1078.xml.gz'


def iterate_xml(xmlfile):
    if xmlfile.endswith('gz'):
        doc = etree.iterparse(gzip.GzipFile(xmlfile), events=('start', 'end'))
    else:
        doc = etree.iterparse(xmlfile, events=('start', 'end'))
        
    _, root = next(doc)
    start_tag = None
    for event, element in doc:
        if event == 'start' and start_tag is None:
            start_tag = element.tag
        if event == 'end' and element.tag == start_tag:
            yield element
            start_tag = None
            root.clear()

def run_main(unzip):
    
    """

    """
    
    s3_client = boto3.client('s3','us-east-2',config=botocore.config.Config(s3={'addressing_style': 'path'}))
    
    local_xml_gz_path = '/tmp/' + xml_file_name;
    s3_client.download_file(SOURCE_BUCKET_NAME, xml_file_name, local_xml_gz_path)

    
    if unzip:
        #https://stackoverflow.com/questions/48466421/python-how-to-decompress-a-gzip-file-to-an-uncompressed-file-on-disk
        
        file_path = '/tmp/' + xml_file_name[:-3]
        with gzip.open(local_xml_gz_path, 'r') as f_in, open(file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    else:
        file_path = local_xml_gz_path
        

    i = 0            
    for elem in iterate_xml(file_path):
        i+=1
        #print('%d: %s'%(i,elem.tag))
     
    print(i)    
     
    os.remove(local_xml_gz_path)
    if unzip:
        os.remove(file_path)

def lambda_handler(event, context):
    # TODO implement
    
    if event == 'gz':
        run_main(False)
    else:
        run_main(True)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }     
        
