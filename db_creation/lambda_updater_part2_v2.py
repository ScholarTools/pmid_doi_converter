#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
- part 2 used to load from xml.gz and throw into DB
- with 1.5, we have dumped the xml.gz to .tsv, so
this code only needs to load tsv and throw into DB


TODO
-----------------------
1. 
"""

import wget
import os
import time
import glob
#import mysql.connector
import pymysql
import json
import boto3
import botocore

BUCKET_NAME = 'pubmed2021tsv'


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


if running_aws:
    root_path = "/tmp/"
else:
    root_path = "/Users/jim/Desktop/pubmed/"   

def connect():
    
    conn = pymysql.connect(
      host=DB_HOST,
      user=DB_USER,
      password=DB_PASS,
      database="mydb",
      port=3306,
      local_infile=True
    )
    
    return conn
    
    
mydb = connect()
mycursor = mydb.cursor()

table_names = ['updates','deleted']

#Note, main must be first ...
other_table_names = ['main','abstract','authors','mesh','supp_mesh',
               'chem','keywords','comments_corrections','personal_names']

table_names.extend(other_table_names)
    
def recreate_db():

    for table_name in table_names:    
        if table_name != 'main':
            mycursor.execute("DROP TABLE IF EXISTS " + table_name)
    
    mycursor.execute("DROP TABLE IF EXISTS " + 'main')
    
    #Without the auto-incrementing primary key (that we don't use) everything
    #slows down a ton ...
    #
    #
    mycursor.execute("CREATE TABLE main (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL UNIQUE," +
                     "doi VARCHAR(255), INDEX(doi)," +
                     "pii VARCHAR(255), INDEX(pii),"
                     "pmcid VARCHAR(20)," + 
                     "journal_name VARCHAR(255), INDEX(journal_name)," + 
                     "journal_volume VARCHAR(255)," + 
                     "journal_year INT," + 
                     "journal_issue VARCHAR(255)," +
                     "journal_month VARCHAR(20)," +
                     "title VARCHAR(1000)," +
                     "pages VARCHAR(255)," +
                     "n_chems INT," +
                     "n_supp_mesh INT," +
                     "n_mesh INT," +
                     "n_authors INT," +
                     "n_keywords INT" +
                     ") CHARACTER SET utf8mb4")
    
    
    # abstract -------------------------------------------------    
    #For large data sets, it is much faster to load your data into a table that 
    #has no FULLTEXT index and then create the index after that, than to load 
    #data into a table that has an existing FULLTEXT index.
    
    
    #For large data sets, it is much faster to load your data into a table that has no FULLTEXT index and then create the index after that, than to load data into a table that has an existing FULLTEXT index.
    mycursor.execute("CREATE TABLE abstract (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL," +
                     "element_id INT," +
                     "label varchar(100)," +
                     "category varchar(15)," +
                     "abstract TEXT," + 
                     "FOREIGN KEY (pmid) " +
                     "REFERENCES main (pmid) " +
                     "ON DELETE CASCADE " +
                     ") CHARACTER SET utf8mb4")
    
    
    #abstract_data.append((None,pmid_text,0,'full','full',full_abstract))
        
    # authors ------------------------------------------
    #- metafields can't be formatted in for execute
    #- order is a reserved word ...
    
    mycursor.execute("CREATE TABLE authors (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL," +
                     "auth_order INT NOT NULL," + 
                     "is_collective BOOLEAN NOT NULL," +
                     "is_first BOOLEAN NOT NULL," +
                     "is_last BOOLEAN NOT NULL," +
                     "last_name VARCHAR(255), INDEX(last_name)," +
                     "fore_name VARCHAR(255)," +
                     "initials VARCHAR(20)," +
                     "suffix VARCHAR(20)," +
                     "orcid VARCHAR(19), INDEX(orcid)," +
                     "affiliation VARCHAR(255)," +
                     "ringgold_id INT," +
                     "isni_id VARCHAR(255)," +
                     "grid_id VARCHAR(20)," +
                     "FOREIGN KEY (pmid) " +
                     "REFERENCES main (pmid) " +
                     "ON DELETE CASCADE " +
                     ") CHARACTER SET utf8mb4")
        
    #https://orcid.org/0000-0002-3843-3472
    #https://www.ringgold.com/ringgold-identifier/
    #ring: Currently they’re between 4 and 6 digits long.
    #https://www.grid.ac/
    


    
    # chem -------------------------------------------------------
    mycursor.execute("CREATE TABLE chem (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL," +
                     "registry_number VARCHAR(15)," +
                     "name VARCHAR(100)," +
                     "ui_id VARCHAR(10)," +
                     "FOREIGN KEY (pmid) " +
                     "REFERENCES main (pmid) " +
                     "ON DELETE CASCADE " +
                     ") CHARACTER SET utf8mb4")
    
    
    """
    
                                (pmid_text,
                                reg_num_text,
                                name_text,
                                name_ui))
    """
    
    # comments corrections  --------------------
    #TODO: ref type is an enum
    mycursor.execute("CREATE TABLE comments_corrections (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL," +
                     "ref_type VARCHAR(30)," +
                     "ref_source VARCHAR(100)," +
                     "note VARCHAR(255)," + 
                     "ref_pmid INT," +
                     "FOREIGN KEY (pmid) " +
                     "REFERENCES main (pmid) " +
                     "ON DELETE CASCADE " +
                     ") CHARACTER SET utf8mb4")
    
    
    """
                cc_output.append((None,
                              pmid_text,
                              cc_type,
                              ref_source,
                              note,
                              cc_pmid))
    """
    
    # keywords -------------------------------------------------
    #owner: IE, PIP, NOTNLM
    #
    #TODO: Index value
    mycursor.execute("CREATE TABLE keywords (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL," +
                     "owner VARCHAR(10)," +
                     "is_major INT," +
                     "keyword VARCHAR(100)," + 
                     "FOREIGN KEY (pmid) " +
                     "REFERENCES main (pmid) " +
                     "ON DELETE CASCADE " +
                     ") CHARACTER SET utf8mb4")
    
    """
                                (pmid_text,
                                 owner,
                                 int(keyword.get('MajorTopicYN')=='Y'),
                                 keyword.text))
    """

    
    # mesh ---------------------------------------------------
    mycursor.execute("CREATE TABLE mesh (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL," +
                     "heading_id INT," +
                     "is_major BOOLEAN," +
                     "ui_id VARCHAR(10)," +
                     "value VARCHAR(50)," +
                     "FOREIGN KEY (pmid) " +
                     "REFERENCES main (pmid) " +
                     "ON DELETE CASCADE " +
                     ") CHARACTER SET utf8mb4")
    
    """
                                    pmid_text,
                                    i,
                                    mesh_elem.get('MajorTopicYN')=='Y',
                                    mesh_elem.get('UI'),
                                    mesh_elem.text))
    """
    
    
    # personal name list ------------------------------
    mycursor.execute("CREATE TABLE personal_names (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL," +
                     "last_name VARCHAR(50)," +
                     "fore_name VARCHAR(50)," +
                     "initials VARCHAR(10)," +
                     "suffix VARCHAR(10)," +
                     "FOREIGN KEY (pmid) " +
                     "REFERENCES main (pmid) " +
                     "ON DELETE CASCADE " +
                     ") CHARACTER SET utf8mb4")    
    
    
    
    """
    (None,
                              pmid_text,
                              last_name,
                              fore_name,
                              initials,
                              suffix))
    
    """
    
    # supplemental mesh ------------------------------------------
    
    #TODO: I think type is either disease, organism, or protocol, so enum would be better
    mycursor.execute("CREATE TABLE supp_mesh (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL," +
                     "type VARCHAR(10)," + 
                     "ui_id VARCHAR(10)," +
                     "value VARCHAR(50)," +
                     "FOREIGN KEY (pmid) " +
                     "REFERENCES main (pmid) " +
                     "ON DELETE CASCADE " +
                     ") CHARACTER SET utf8mb4")
    
    """
                                pmid_text,
                                supp_mesh_name.get('Type'),
                                supp_mesh_name.get('UI'),
                                supp_mesh_name.text))
    """
           
    mycursor.execute("CREATE TABLE updates ("
                     "id INT AUTO_INCREMENT PRIMARY KEY,"
                     "file_name VARCHAR(255))")

    mycursor.execute("CREATE TABLE deleted ("
                     "id INT AUTO_INCREMENT PRIMARY KEY,"
                     " pmid INT, file_name VARCHAR(255))")
     
def add_baseline_files_to_db():
    
    print("Adding baseline files via local disk")
            
    files = sorted(glob.glob(os.path.join(root_path,'*.gz')))
    
    for file_name in files:
        if file_name[-1] == 'z':
            download_and_add_file(file_name)
                
def log_file_added(mycursor,file_name):
    mycursor.execute("INSERT INTO updates (file_name) VALUES(%s)",(file_name))
    
def add_file_to_db(file_name):
    """
    
    Parameters
    ----------
    file_path : file path, .tsv extension

    """
        
    t2 = time.time()
    print('Processing: %s' % (file_name,))
    
    name_root = file_name[:-4]
    
    #JAH TODO: At this point ...
    
    #Step 1, see if deleted exists
    
    #TODO
    
    
    #Step 2, add all tables
    
    for table_name in other_table_names:
        tsv_path = root_path + name_root + '_' + table_name + '.tsv'
        mycursor.execute(
            "LOAD DATA LOCAL INFILE %s " +
            "INTO TABLE " + table_name + ' '
            "CHARACTER SET UTF8MB4 " +
            "FIELDS OPTIONALLY ENCLOSED BY '\"' " +
            "TERMINATED BY '\t' " +
            "LINES TERMINATED BY '\n'",(tsv_path,))
    
    
    """
    mycursor.execute(
        "LOAD DATA LOCAL INFILE %s " +
        "INTO TABLE " + 'authors ' + 
        "CHARACTER SET UTF8MB4 " +
        "FIELDS OPTIONALLY ENCLOSED BY '\"' " +
        "TERMINATED BY '\t' " +
        "LINES TERMINATED BY '\n'",(authors_path,))
    """
        
    mydb.commit()
    t3 = time.time()

      
    print('t3-t2: %g' % (t3-t2)) 

def download_and_add_file(file_name,s3_client=None):
    
    flag = 1 #already exists    
    mycursor.execute("SELECT id FROM updates WHERE file_name = %s",(file_name,))
    myresult = mycursor.fetchone()
    if myresult is None:
        out_file_path = os.path.join(root_path, file_name)
        if os.path.exists(out_file_path):
            print('File already exists locally: %s' % file_name) 
            pass
        else:
            print('Downloading: %s' % file_name)
            #This needs to be an s3 fetch ...
            
            if s3_client is not None:
                t1 = time.time()
                s3_client.download_file(BUCKET_NAME, file_name, out_file_path)
                t2 = time.time()
                print('Download time, t2-t1: %g' % (t2-t1)) 
                #os.remove(out_file_path)  
                #return 2 
 
     

            
        add_file_to_db(out_file_path)
        flag = 2
        
        if running_aws:
            os.remove(out_file_path)   
     
    print('Flag: %d' % (flag))        
    return flag
        
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

def get_added_file_list():
    
     mycursor.execute("SELECT * FROM updates")
     myresult = mycursor.fetchall()
     
     file_list = [x[1] for x in myresult]
     
     return file_list
    
    
     
def lambda_handler(event, context):
    
    """
    - new
    - get
    - <file_name>
    - update
    """
    
    if event == 'new':
        recreate_db()
        return {
        'statusCode': 200,
        'body': 'new'
        }
    elif event == 'get':
        file_list = get_added_file_list()
        return {
        'statusCode': 200,
        'body': json.dumps(file_list)
        }
    elif event == 'update':
        file_list = get_added_file_list()
        #This should be the entry method where we see how S3
        #differs from what is in the DB
        #TODO: cross reference with added
        return {
        'statusCode': 200,
        'body': 'update'
        }
    else:
        file_name = event
        s3_client = boto3.client('s3','us-east-2',config=botocore.config.Config(s3={'addressing_style': 'path'}))
        print('Calling add for: %s' % (file_name))
        flag = download_and_add_file(file_name,s3_client)
        print('Done with flag: %d' % (flag))
        output = {'add_status':flag}
        return {
        'statusCode': 200,
        'body': json.dumps(output)
        }
    
if not running_aws:
    file_name = 'pubmed21n1078.tsv'
    recreate_db()
    add_file_to_db(file_name)
    