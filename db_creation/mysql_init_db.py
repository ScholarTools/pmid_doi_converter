#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""



import os
import time
import glob
from ftplib import FTP
from lxml import etree
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user=os.environ['mysql_user'],
  password=os.environ['mysql_pass'],
  database="mydb"
)

mycursor = mydb.cursor()

root_path = "/Users/jim/Desktop/pubmed/"
root_path = 'G:/pubmed_2021'

if os.environ.get("AWS_EXECUTION_ENV") is not None:
    updates_root_path = "/tmp/"
else:
    updates_root_path = "./tmp/"

def add_update_files_to_db():
    
    #1 - get file list
    #2 - 
    
    ftp = get_ftp_conn()
    
    files = ftp.nlst()
    files = sorted(filter (lambda x:x.endswith(".gz") , files))

    
    for file_name in files:
        mycursor.execute("SELECT id FROM updates WHERE file_name = %s",(file_name,))
        myresult = mycursor.fetchone()
        if myresult is None:
            #download file locally
            out_file_path = os.path.join(updates_root_path, file_name)
            if os.path.exists(out_file_path):
                pass
            else:
                print('Downloading: %s' % file_name)
                with open(out_file_path, 'wb' ) as file:
                    try:
                        ftp.retrbinary('RETR %s' % file_name, file.write)
                    except:
                        ftp = get_ftp_conn()
                        ftp.retrbinary('RETR %s' % file_name, file.write)
                    
                    file.close()
            
            print('Processing: %s' % (file_name))
            add_file_to_db(out_file_path)
    

def get_ftp_conn():
    
    FTP_ROOT = 'ftp.ncbi.nlm.nih.gov'

    ftp = FTP(FTP_ROOT)
    
    ftp.login()
    
    ftp.cwd('pubmed/updatefiles/')
    
    return ftp
    
    
    
    """
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
    """

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
    

def add_baseline_files_to_db():
        
    files = sorted(glob.glob(os.path.join(root_path,'*.gz')))
    
    i = 0
    for name in files:
        i = i + 1
        #For debugging a certain file, change i
        if i > 0:
            if name[-1] == 'z':
                file_path = os.path.join(root_path,name)
                print('Processing: %s' % (name))
                add_file_to_db(file_path)
    
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
    
    #file_name = file_path
    
    file_name = os.path.splitext(os.path.basename(file_path))[0]
    
    
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
            
        #------------------------------------------------------------------
            
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
    
    mydb.commit()
    t3 = time.time()

      
    print('t3-t2: %g' % (t3-t2)) 
    

if __name__ == "__main__":
    recreate_db()
    
    add_baseline_files_to_db()
    
    add_update_files_to_db()
    
     