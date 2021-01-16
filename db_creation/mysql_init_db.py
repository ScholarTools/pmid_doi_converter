#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""



import os
import time
import glob
from lxml import etree
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user=os.environ['mysql_user'],
  password=os.environ['mysql_pass'],
  database="mydb"
)

mycursor = mydb.cursor()

mycursor.execute("DROP TABLE IF EXISTS ids")

#Without the auto-incrementing primary key (that we don't use) everything
#slows down a ton ...
mycursor.execute("CREATE TABLE ids (" + 
                 "id INT AUTO_INCREMENT PRIMARY KEY," + 
                 "pmid INT NOT NULL UNIQUE," +
                 "doi VARCHAR(255), INDEX(doi)) " +
                 "CHARACTER SET utf8mb4")
#mycursor.execute("CREATE TABLE ids (pmid INT PRIMARY KEY, doi VARCHAR(255), INDEX(doi))")
#mydb.commit()

root_path = "/Users/jim/Desktop/pubmed/"

def add_baseline_files_to_db():
        
    files = sorted(glob.glob(os.path.join(root_path,'*.gz')))
    
    for name in files:
        if name[-1] == 'z':
            file_path = os.path.join(root_path,name)
            print('Processing: %s' % (name))
            add_file_to_db(file_path)
    
    """
    for i in range(n_max):
        value = i + 1
        int_string = '%04d' % (value)
        print('Processing: %d' % (value))
        file_path = root_path + prefix + int_string + suffix
        add_file_to_db(file_path,session)
    """
    
    
#Additional bits to add
#--------------------------------------------------
#- journal name
#- year
#- vol
# 
    
def add_file_to_db(file_path):
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
    print('t2-t1: %g' % (t2-t1)) 
    t2 = time.time()
    #mapping = {}
    i = 0
    while article is not None:
        i += 1
        if i % 10000 == 0:
            print('-- %d' % i)
            
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
        
        mycursor.execute('INSERT INTO ids (pmid,doi) VALUES(%s,%s) ON DUPLICATE KEY UPDATE doi=%s',
              (pmid_int,doi_text,doi_text))
        
        article = article.getnext()
    
    """
    temp = c.fetchone()
    if temp is None:
        return ''
    else:
        return temp[0]
    
    #https://stackoverflow.com/questions/4205181/insert-into-a-mysql-table-or-update-if-exists
    #INSERT INTO table (id, name, age) VALUES(1, "A", 19) ON DUPLICATE KEY UPDATE name="A", age=19
        
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
    """

    mydb.commit()
    t3 = time.time()

      
    print('t3-t2: %g' % (t3-t2)) 
    

if __name__ == "__main__":
    add_baseline_files_to_db() 