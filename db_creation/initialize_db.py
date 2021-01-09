#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Steps
-----
0. initialize table
1. read through the files
2. extract pmid and doi
3. write to sql database


#Global TODO
#---------------------------------------------------------------
#- verify updates work
#- upload database to function or layer
#- create update and upload function
#


"""
import pubmed_parser as pp
import os
import time
import gzip
import glob
from lxml import etree
from ftplib import FTP
from io import BytesIO

from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import Column, String, Integer, Boolean, ForeignKey, BigInteger
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, event


#TODO: Log the files we've used ...


prefix = 'pubmed21n'
suffix = '.xml.gz'

#Max # of files in the baseline
#TODO: We could eventually pull this from the files ...
n_max = 6



root_path = "/Volumes/Pubmed/Pubmed/"
root_path = "/Users/jim/Desktop/pubmed/"
sql_file_path = "/Users/jim/Desktop/pubmed_db.sql"



#Update code
#---------------------------------------------------------------
FTP_ROOT = 'ftp.ncbi.nlm.nih.gov'

ftp = FTP(FTP_ROOT)

ftp.login()

ftp.cwd('pubmed/updatefiles/')

files = ftp.nlst()

for file_name in files:
    out_file_path = os.path.join(root_path,file_name)
    if os.path.exists(out_file_path):
        pass
    else:
        print('Downloading: %s' % file_name)
        with open(out_file_path, 'wb' ) as file :
            ftp.retrbinary('RETR %s' % file_name, file.write)
            file.close()

ftp.quit()

#DB Code ...
#---------------------------------------------------------------
Base = declarative_base()
class ID(Base):
    
    __tablename__ = "ids"
    pmid = Column(Integer,primary_key=True)
    doi = Column(String,index=True)


def init_db():
    if os.path.exists(sql_file_path):
        os.remove(sql_file_path)
    engine = create_engine('sqlite:///' + sql_file_path)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker()
    Session.configure(bind=engine)
    session = Session()
    
    return session


#year 2021 -> 21

def add_baseline_files_to_db():
    
    
    session = init_db()
    
    files = sorted(glob.glob(os.path.join(root_path,'*.gz')))
    
    id_set = set()
    for name in files:
        if name[-1] == 'z':
            file_path = os.path.join(root_path,name)
            print('Processing: %s' % (name))
            id_set = add_file_to_db(file_path,session,id_set)
    
    """
    for i in range(n_max):
        value = i + 1
        int_string = '%04d' % (value)
        print('Processing: %d' % (value))
        file_path = root_path + prefix + int_string + suffix
        add_file_to_db(file_path,session)
    """
    
def add_file_to_db(file_path,session,id_set):
    """
    

    Parameters
    ----------
    file_path : file path, .gz extension
    session : sqlalchemy session (class name?)

    Returns
    -------
    None.

    """
    t1 = time.time()
    tree = etree.parse(file_path)
    #DTD
    #http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd

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
    while article is not None:
        article_ids = article.find('PubmedData/ArticleIdList')
        try:
            doi = article_ids.find('ArticleId[@IdType="doi"]')
            pmid = article_ids.find('ArticleId[@IdType="pubmed"]')
            doi_text = doi.text
        except:
            pmid = article.find('MedlineCitation/PMID')
            doi_text = ''
            if article_ids is None:
                #Get pubmed id the other way, not sure where this is ...
                pass
            pass
        pmid_int = int(pmid.text)
        #mapping[pmid_int] = doi_text
        
        #
        
        #TODO: We could compare temp to id
        
        id = ID()
        id.pmid = pmid_int
        id.doi = doi_text
        
        if pmid_int in id_set:
            #TODO: Should this be one????c
            temp = session.query(ID).filter(ID.pmid == pmid_int).first()
            session.delete(temp)
            session.flush()
        else:
            id_set.add(pmid_int)

        
        #if temp:
        #    print('Updating object for %d' % (pmid_int))
        #    session.delete(temp)
        #    session.flush()
        
        
        #NOTES:
        #1) Querying in the loop increasese execution time from like 4-5 seconds 
        #to roughly 40 seconds per loop
        #
        #2) Commit to force uniqueness instead is SUPER slow , 113 seconds
        

        session.add(id)
        
        
        article = article.getnext()

    session.commit()
    t3 = time.time()

    print('t2-t1: %g' % (t2-t1))   
    print('t3-t2: %g' % (t3-t2)) 
    
    return id_set
    
    
if __name__ == "__main__":
    add_baseline_files_to_db() 
        
"""
t1 = time.time()
dicts_out = pp.parse_medline_xml(file_path,
                                 year_info_only=False,
                                 nlm_category=False,
                                 author_list=False,
                                 reference_list=False)

t2 = time.time()
print(t2-t1)
#32 seconds ...
#43 seconds for 100

"""


"""
FTP_ROOT = 'ftp://ftp.ncbi.nlm.nih.gov/'
FTP_ROOT = 'ftp.ncbi.nlm.nih.gov'


ftp_baseline_root = 'ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline'

ftp = FTP(FTP_ROOT)

ftp.login()

ftp.cwd('pubmed/baseline')

t1 = time.time()
#https://stackoverflow.com/questions/11208957/is-it-possible-to-read-ftp-files-without-writing-them-using-python
r = BytesIO()
ftp.retrbinary('RETR ' + 'pubmed21n0100.xml.gz', r.write)

t2 = time.time()
print(t2-t1)

wtf = r.getvalue()
"""


