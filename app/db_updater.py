#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""
import os
import zipfile

with open('./opt/last_file.txt', 'r') as file:
    data = int(file.read())
    


file_path = './opt/temp/last_file.txt'
os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
with open(file_path,'w') as file:
    file.write(str(data+1))    
    
with zipfile.ZipFile('temp.zip','w') as zip: 
    #- write the file to the zip
    #- 2nd input specifies where in the archive to go
    #   otherwise the relative structure is preserved
    zip.write(file_path,'last_file.txt') 


with open(file_path,'rb') as file:
    bytes = file.read()