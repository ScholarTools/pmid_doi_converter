#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
"""

import wget
import os
import time
import glob
from ftplib import FTP
from lxml import etree
#import mysql.connector
import pymysql
import json
import boto3
import botocore

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


if running_aws:
    updates_root_path = "/tmp/"
    root_path = "/tmp/"
else:
    updates_root_path = os.path.abspath("./tmp/")
    root_path = "/Users/jim/Desktop/pubmed/"   


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
mycursor = mydb.cursor()
    
def add_update_files_to_db():
    
    print("Adding update files via ftp")
    
    #1 - get file list
    #2 - 
    
    ftp = get_ftp_conn()
    
    files = ftp.nlst()
    files = sorted(filter (lambda x:x.endswith(".gz") , files))

    
    for file_name in files:
        download_and_add_file(file_name)
    

def get_ftp_conn(is_update=True):
    
    FTP_ROOT = 'ftp.ncbi.nlm.nih.gov'

    ftp = FTP(FTP_ROOT)
    
    ftp.login()
    
    if is_update:
        ftp.cwd('pubmed/updatefiles/')
    else:
        ftp.cwd('pubmed/baseline/')
    
    return ftp

def recreate_db():

    mycursor.execute("DROP TABLE IF EXISTS ids")
    mycursor.execute("DROP TABLE IF EXISTS updates")
    mycursor.execute("DROP TABLE IF EXISTS deleted")
    
    #Without the auto-incrementing primary key (that we don't use) everything
    #slows down a ton ...
    #
    #
    mycursor.execute("CREATE TABLE ids (" + 
                     "id INT AUTO_INCREMENT PRIMARY KEY," + 
                     "pmid INT NOT NULL UNIQUE," +
                     "doi VARCHAR(255), INDEX(doi)," +
                     "journal_name VARCHAR(255)," + 
                     "journal_volume VARCHAR(255)," + 
                     "journal_year INT," + 
                     "journal_issue VARCHAR(255)," +
                     "journal_month VARCHAR(20)" +
                     ") CHARACTER SET utf8mb4")
    
    mycursor.execute("CREATE TABLE updates (id INT AUTO_INCREMENT PRIMARY KEY, file_name VARCHAR(255))")

    mycursor.execute("CREATE TABLE deleted (id INT AUTO_INCREMENT PRIMARY KEY, pmid INT, file_name VARCHAR(255))")
 
def add_baseline_files_to_db_ftp():
    
    print("Adding baseline files via ftp")
    

    ftp = get_ftp_conn(is_update=False)
    
    files = ftp.nlst()
    files = sorted(filter (lambda x:x.endswith(".gz") , files))

    
    for file_name in files:          
        download_and_add_file(file_name)

    
def add_baseline_files_to_db():
    
    print("Adding baseline files via local disk")
            
    files = sorted(glob.glob(os.path.join(root_path,'*.gz')))
    
    for file_name in files:
        if file_name[-1] == 'z':
            download_and_add_file(file_name)
                
    
#Additional bits to add
# - -----------------------------------------------
#- journal name
#- year
#- vol

def log_file_added(mycursor,file_name):
    mycursor.execute("INSERT INTO updates (file_name) VALUES(%s)",(file_name))
    
def add_file_to_db(file_path,tree):
    """
    

    Parameters
    ----------
    file_path : file path, .gz extension

    Returns
    -------
    None.
    
    Things to Add
    -------------
    - pmid
    - doi
    - pmcid
    - journal
    - year
    
    - last names
    - mesh
    - 
    

    """
        
    
    file_name = os.path.basename(file_path)
    print('Processing: %s at %s' % (file_name,file_path))
    
    

    #DTD
    #http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd
    #
    #Definitions
    #https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html

    #We may want to dump all ids, not just doi ...
    """
    	<Item Name="ArticleIds" Type="List">
    		<Item Name="pubmed" Type="String">32022941</Item>
    		<Item Name="doi" Type="String">10.1002/nau.24300</Item>
    		<Item Name="rid" Type="String">32022941</Item>
    		<Item Name="eid" Type="String">32022941</Item>
    """

    #<PMID Version="1">1</PMID>
    #ArticleIdList
    #<ArticleIdList>
    #    <ArticleId IdType="pubmed">101</ArticleId>
    #</ArticleIdList>
    
    #PubmedArticleSet
    #  - PubmedArticle
    
    #Quicker to iterate with while next than findall?

    #Note, it is much faster to iterate over articles rather than finding all
    #and iterating over the result.
    article = tree.find('PubmedArticle')
    
    
    t2 = time.time()
    #mapping = {}
    i = 0
    while article is not None:
        i += 1
        if i % 10000 == 0:
            print('-- %d' % i)
            
        #MedlineCitation, PubmedData?
        
        """
        
        <!ELEMENT	PubmedArticle (MedlineCitation, PubmedData?)>
        
        <!ELEMENT	MedlineCitation (PMID, 
                                   DateCompleted?, 
                                   DateRevised?,
                                   Article, 
                             MedlineJournalInfo, 
                             ChemicalList?, 
                             SupplMeshList?,
                             CitationSubset*, 
                             CommentsCorrectionsList?, 
                             GeneSymbolList?,
                             MeshHeadingList?, 
                             NumberOfReferences?, 
                             PersonalNameSubjectList?, 
                             OtherID*, 
                             OtherAbstract*, 
                             KeywordList*, 
                             CoiStatement?, 
                             SpaceFlightMission*, 
                             InvestigatorList?, 
                             GeneralNote*)>
        
        
        
        <!ELEMENT	Article (Journal,
                           ArticleTitle,
                        ((Pagination, ELocationID*) | ELocationID+),
                        Abstract?,
                        AuthorList?, 
                        Language+,
                        DataBankList?,
                        GrantList?,
                        PublicationTypeList,
                        VernacularTitle?,
                        ArticleDate*) >
        <!ATTLIST	Article 
		    PubModel (Print | Print-Electronic | Electronic | Electronic-Print | Electronic-eCollection) #REQUIRED >
        
        """
        
        """
        PubmedData (History?, 
                    PublicationStatus, 
                    ArticleIdList, 
                    ObjectList?, 
                    ReferenceList*) 
        """
        
        medline_citation = article.find('MedlineCitation')
        
        try:
            article2 = medline_citation.find('Article')
            #TODO: Should do a more specific catch ...
        except:
            print("Executing delete code")
            #article may really be a DeleteCitation element
            #
            ##<!ELEMENT	DeleteCitation (PMID+) >
            #
            #TODO: Should verify 
            #article.tag == 'DeleteCitation'
            for pmid_element in article:
                pmid = pmid_element.text
                mycursor.execute("DELETE FROM ids WHERE pmid = %s",(pmid,))
                mycursor.execute("INSERT INTO deleted (pmid,file_name) VALUES(%s,%s)",(pmid,file_name))

            article = article.getnext()  
            continue
            
        # - -----------------------------------------------------------------
            
        journal = article2.find('Journal')
        
        
        #<!ELEMENT	Journal (ISSN?, JournalIssue, Title?, ISOAbbreviation?)>
        #<!ELEMENT	JournalIssue (Volume?, Issue?, PubDate) >
        #<!ELEMENT	PubDate ((Year, ((Month, Day?) | Season)?) | MedlineDate) >

        

        journal_title = journal.find('Title')
        journaL_text = journal_title.text
        
        journal_issue = journal.find('JournalIssue')
        
        journal_volume = journal_issue.find('Volume')
        if journal_volume is None:
            journal_volume_text = ''
        else:
            journal_volume_text = journal_volume.text
        
        journal_issue2 = journal_issue.find('Issue')
        if journal_issue2 is None:
            journal_issue_text = ''
        else:
            journal_issue_text = journal_issue2.text
        
        
        journal_date = journal_issue.find('PubDate')
        

        journal_year = journal_date.find('Year')
        if journal_year is None:
            journal_year_text = '0'
        else:
            journal_year_text = journal_year.text
        
        journal_month = journal_date.find('Month')
        if journal_month is None:
            journal_month_text = ''
        else:
            journal_month_text = journal_month.text       
        
        pmid = medline_citation.find('PMID')
        pmid_text = pmid.text
        
        
        #article2
        #((Pagination, ELocationID*) | ELocationID+),
        #
        
        
        #<!ELEMENT	PubmedData (History?, PublicationStatus, ArticleIdList, ObjectList?, ReferenceList*) >
        
        pubmed_data = article.find('PubmedData')
        if pubmed_data is None:
            doi_text = ''
        else:
            article_ids = pubmed_data.find('ArticleIdList')
            doi = article_ids.find('ArticleId[@IdType="doi"]')
            if doi is None:
                doi_text = ''
            else:
                doi_text = doi.text
        
        try:                 
            mycursor.execute("INSERT INTO ids (pmid,doi,journal_name,journal_volume,journal_year,journal_issue,journal_month) " + 
                         "VALUES(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE " + 
                         "doi=%s,journal_name=%s,journal_volume=%s,journal_year=%s,journal_issue=%s,journal_month=%s",
              (pmid_text,doi_text,journaL_text,journal_volume_text,journal_year_text,
               journal_issue_text,journal_month_text,doi_text,journaL_text,
               journal_volume_text,journal_year_text,journal_issue_text,journal_month_text))
        except:
            import pdb
            pdb.set_trace()
        
        article = article.getnext()
    
    log_file_added(mycursor,file_name)
    
    mydb.commit()
    t3 = time.time()

      
    print('t3-t2: %g' % (t3-t2)) 
    
def get_ftp_url(file_name,is_update):
    if is_update:
        url = 'https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/' + file_name
    else:
        url = 'https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/' + file_name
    
    return url

def bar_progress(current, total, width=80):
  progress_message = "Downloading: %d%% [%d / %d] bytes" % (current / total * 100, current, total)
  # Don't use print() as it will print in new line every time.
  sys.stdout.write("\r" + progress_message)
  sys.stdout.flush()


def download_and_add_file(file_name,s3_client=None):
    
    is_update = is_update_file(file_name)
    if is_update:
        base_path = updates_root_path
    else:
        base_path = root_path
        
        
    flag = 1 #already exists    
    mycursor.execute("SELECT id FROM updates WHERE file_name = %s",(file_name,))
    myresult = mycursor.fetchone()
    if myresult is None:
        out_file_path = os.path.join(base_path, file_name)
        if os.path.exists(out_file_path):
            print('File already exists locally: %s' % file_name) 
            pass
        else:
            print('Downloading: %s' % file_name)
            #This needs to be an s3 fetch ...
            
            if s3_client is not None:
                #print("Downloading via s3")
                #print("File name %s" % (file_name))
                #print("out: %s" % (out_file_path))
                t1 = time.time()
                s3_client.download_file(BUCKET_NAME, file_name, out_file_path)
                t2 = time.time()
                print('Download time, t2-t1: %g' % (t2-t1)) 
                #os.remove(out_file_path)  
                #return 2
            else:
                print("Downloading via wget")
                t1 = time.time()
                url = get_ftp_url(file_name,is_update)
                wget.download(url,out=out_file_path,bar=bar_progress)
                t2 = time.time()
                print('t2-t1: %g' % (t2-t1)) 
 
        t1 = time.time()
        tree = etree.parse(out_file_path)
        t2 = time.time()
        print('tree load time, t2-t1: %g' % (t2-t1))  

            
        add_file_to_db(out_file_path, tree)
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