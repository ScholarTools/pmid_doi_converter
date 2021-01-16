#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 08:37:46 2021

@author: jim
"""

import mysql.connector
import os

mydb = mysql.connector.connect(
  host="localhost",
  user=os.environ['mysql_user'],
  password=os.environ['mysql_pass'],
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE mydb")