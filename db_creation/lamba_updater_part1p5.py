#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Goal is to read xml and save as tsv
http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd
https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html

#TODO: I would be interested in seeing how performance compares
if instead of doing finds we just iterated elements and handled them

#TODO: Create a bucket with json entries

Remaining TODOs
- flush out remaining entries 
- create lambda that looks for differences in files between
 the two buckets (source and destination) and updates accordingly


TODO:
    - author, support ValidYN and EqualContrib  
"""


#Standard  --------------
import csv
import gzip
import os
import json
import re
import time


#Third --------------
from lxml import etree
import boto3
import botocore


SOURCE_BUCKET_NAME = 'pubmed2021'
DEST_BUCKET_NAME = 'pubmed2021tsv'

#Keep main first
table_names = ['main',
               'abstract',
               'authors',
               'chem',
               'comments_corrections',
               'data_bank',
               'grants',
               'keywords',
               'languages',
               'mesh',
               'notes',
               'personal_names',
               'pub_types',
               'refs',
               'space',
               'supp_mesh']  

#EC2 & Lambda support
my_user = os.environ.get("USER") #for EC2
if my_user is None:
    my_user = ''
#                   EC2                     LAMBDA
running_aws = ("ec2" in my_user) or (os.environ.get("AWS_EXECUTION_ENV") is not None)

def get_date_entry(elem):
    if elem is None:
        return None
    else:
        year = elem.find('Year').text
        month = int(elem.find('Month').text)
        day = int(elem.find('Day').text)
        return "%s-%02d-%02d"%(year,month,day)

def get_text(elem,default):
    if elem is None:
        return default
    else:
        return elem.text

#Grabbed from stackoverflow somewhere ...
class CSVWriter():

    filename = None
    fp = None
    writer = None

    def __init__(self, filename):
        self.filename = filename
        self.fp = open(self.filename, 'w', encoding='utf8')
        self.writer = csv.writer(self.fp, 
                                 delimiter='\t', 
                                 quotechar='"', 
                                 quoting=csv.QUOTE_ALL, 
                                 lineterminator='\n')

    def close(self):
        self.fp.close()

    def write(self, elems):
        self.writer.writerow(elems)

    def size(self):
        return os.path.getsize(self.filename)

    def fname(self):
        return self.filename


#https://stackoverflow.com/questions/9856163/using-lxml-and-iterparse-to-parse-a-big-1gb-xml-file
def iterate_xml(xmlfile):
    if xmlfile.endswith('gz'):
        doc = etree.iterparse(gzip.GzipFile(xmlfile), events=('start', 'end'),encoding="utf-8")
    else:
        doc = etree.iterparse(xmlfile, events=('start', 'end'),encoding="utf-8")
        
    _, root = next(doc)
    start_tag = None
    for event, element in doc:
        if event == 'start' and start_tag is None:
            start_tag = element.tag
        if event == 'end' and element.tag == start_tag:
            yield element
            start_tag = None
            root.clear()


def populate_row(elem):
    """
    
    This code looks through a PubmedArticle xml element and extract tuples 
    that get added to various delimited files (later to become tables).
    Occasionally 
    
    Returns
    -------
    deleted_pmids,1
    OR
    output,0
    
    Where output is a dictionary with entries that go to differnt files.
       
    Parameters
    ----------
    elem : XML element with the following structure:         
        <PubmedArticle>
            <MedlineCitation>
                <Article>
            <PubmedData>?
        OR
        <DeleteCitation>?
     
    """
    
    #This only occurs once ...
    if elem.tag[0] == 'D':
        pmids = [x.text for x in elem]
        return (pmids,1)
    
    output = {x:None for x in table_names}
    
    #   MedlineCitation ---------------------------------------------------
    medline_citation = elem.find('MedlineCitation')
    
    pmid_text = medline_citation.find('PMID').text
    
    """
    (
    X PMID, 
    S DateCompleted?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#datecompleted
        Not particularly interested in this
    S DateRevised?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#daterevised
        Not particularly interested in this
    X Article, 
    S MedlineJournalInfo,   #( Country?, MedlineTA, NlmUniqueID?, ISSNLinking? )
        Skipping for now, not described in the element descriptions
    X ChemicalList?,  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#chemicallist
    X SupplMeshList?,
    S CitationSubset*,  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#citationsubset
        - For some articles has a tag that indicates it belongs to a special group
        - Does not seem all that particularly useful
    X CommentsCorrectionsList?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#commentscorrections
    S GeneSymbolList?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#genesymbollist
            Sounds like it was only used for a brief time in the 90s
    X MeshHeadingList?, 
    X NumberOfReferences?, - not accurate for >2010
    X PersonalNameSubjectList?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#personalnamesubjectlist
        When an article is about someone
    OtherID*,  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#otherid
        Doesn't really look like it is used
    S OtherAbstract*, 
    X KeywordList*, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#keywordlist
    X CoiStatement?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#coistatement
        started in 2017
    X SpaceFlightMission*, 
    X InvestigatorList?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#investigatorlist
            Can be used to describe people that contributed that are not authors
    X GeneralNote* https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#generalnote
        - Catchall for extra information
    )>
    """
    
    # MedlineCitation.DateCompleted?
    date_completed_string = get_date_entry(medline_citation.find('DateCompleted'))

    # MedlineCitation.DateRevised?
    date_revised_string = get_date_entry(medline_citation.find('DateRevised'))
    
    # MedlineCitation.ChemicalList?
    output['chem'],n_chems = get_chem_list(medline_citation,pmid_text)
    
    # MedlineCitation.SupplMeshList?
    output['supp_mesh'],n_supp_mesh = get_supp_mesh_list(medline_citation,pmid_text)

    # MedlineCitation.CommentsCorrectionsList?
    output['comments_corrections'] = get_comments_corrections(medline_citation,pmid_text)
    
    # MedlineCitation.MeshHeadingList?
    output['mesh'],n_mesh = get_mesh(medline_citation,pmid_text)
    
    # MedlineCitation.NumberOfReferences?
    n_references = get_text(medline_citation.find('NumberOfReferences'),'None')
        
    # MedlineCitation.PersonalNameSubjectList?
    output['personal_names'] = get_personal_names(medline_citation,pmid_text)
    
    # MedlineCitation.KeywordList
    output['keywords'],n_keywords = get_keywords(medline_citation,pmid_text)
        
    # MedlineCitation.CoiStatement?
    coi_text = get_coi_statement(medline_citation)
    
    # MedlineCitat.SpaceFlightMission* -----------------------------
    output['space'] = get_space_flight(medline_citation,pmid_text)

    # MedlineCitation.InvestigatorList?  --------------------------
    # <!ELEMENT	InvestigatorList (Investigator+) >
    # <!ELEMENT	Investigator (LastName, ForeName?, Initials?, Suffix?, Identifier*, AffiliationInfo*) >
    # <!ATTLIST	Investigator 
	#	    ValidYN (Y | N) "Y" >

    #TODO: finish this
    
    
    # MedlineCitation.GeneralNote* --------------------------------
    output['notes'] = get_general_notes(medline_citation,pmid_text)
    
    # MedlineCitation.Article ------------------------------------------    
    """
    <!ELEMENT	Article (
                   X Journal, 
                   X ArticleTitle,
                   X ((Pagination, ELocationID*) | ELocationID+),
                       - pagination is handled, elocationID is not (doi or pii)
                   X Abstract?,
                   X AuthorList?, 
                   X Language+,  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#language
                   X DataBankList?, https://www.nlm.nih.gov/bsd/medline_databank_source.html
                       TODO: This contains important things like clinical trial IDs
                   X GrantList?,
                   X PublicationTypeList, 
                   X VernacularTitle?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#verniculartitle
                   S ArticleDate*
                   ) >
    """

    article = medline_citation.find('Article')
    
    # MedlineCitation.Article.Journal
    journal_info = get_journal_info(article.find('Journal'))
       
    # MedlineCitation.Article.ArticleTitle
    article_title_text = article.find('ArticleTitle').text
    
    # MedlineCitation.Article.Pagination
    medline_pgn = get_pagination(article)
    
    # MedlineCitation.Article.Abstract?
    output['abstract'] = get_abstract(article,pmid_text)

    # MedlineCitation.Article.AuthorList?
    output['authors'],n_authors = get_author_list(article,pmid_text)
   
    # MedlineCitation.Article.Language+
    output['languages'] = get_languages(article,pmid_text)
    
    # MedlineCitation.Article.DataBankList? ------------------------------
    output['data_bank'] = get_data_bank_list(article,pmid_text)
   
    # MedlineCitation.Article.GrantList? ----------------------------
    output['grants'] = get_grant_list(article,pmid_text)
   
    # MedlineCitation.Article.PublicationTypeList --------------------
    output['pub_types'] = get_publication_type_list(article,pmid_text)
    
    # MedlineCitation.Article.VernacularTitle?
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#verniculartitle
    vernacular_title_text = get_text(article.find('VernacularTitle'),'')
    
    # MedlineCitation.Article.ArticleDate*
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#articledate
    # skipping
    
  
    # PubmedArticle.PubmedData? -------------------------------------------------------
    
    """
    #<!ELEMENT	PubmedData (
                    S History?,
                    S PublicationStatus,
                    X ArticleIdList, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#articleidlist
                    ObjectList?,  ???? - these two are not in the description ...
                    ReferenceList*)>
    """
    
    pubmed_data = elem.find('PubmedData')
    
    
    # PubmedArticle.PubmedData.History?
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#history
    # skipping
    
    # PubmedArticle.PubmedData.PublicationStatus
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#publicationstatus
    # ppublish, epublcih, or ahead of print
    
    if pubmed_data is None:
        doi_text = ''
        pmcid_text = ''
        pii_text = ''
    else:
        
        # PubmedArticle.PubmedData.ObjectList?
        obj_list = pubmed_data.find('ObjectList')
        if obj_list is not None:
            import pdb
            pdb.set_trace()
        
        # PubmedArticle.PubmedData.ReferenceList*
        output['refs'] = get_references(pubmed_data,pmid_text)

        #   PubmedArticle.PubmedData.ArticleIdList  -------------------------
        
        article_ids = pubmed_data.find('ArticleIdList')
        
        #TODO: reformat? as iteration over ids???
        
        #We may want to dump all ids, not just doi ...
        #<!ELEMENT	ArticleId (#PCDATA) >
        #<!ATTLIST   ArticleId
        #          IdType (doi | pii | pmcpid | pmpid | pmc | mid |
        #               sici | pubmed | medline | pmcid | pmcbook | bookaccession) "pubmed" >
        
        doi_text = get_text(article_ids.find('ArticleId[@IdType="doi"]'),'')
        pii_text = get_text(article_ids.find('ArticleId[@IdType="pii"]'),'')
        pmcid_text = get_text(article_ids.find('ArticleId[@IdType="pmcid"]'),'')
     
    """
    journal_info
    return {'issn_value':issn_value,
            'issn_type':issn_type,
            'volume':journal_volume,
            'issue':journal_issue,
            'pub_date':pub_date,
            'title':title,
            'iso_abbrev':iso_abbrev}
    """    
     
    output['main'] = (None,
                      pmid_text, #string
                      doi_text,  #string
                      pii_text,  #string
                      pmcid_text, #string
                      date_completed_string, #date string yyyy-mm-dd
                      date_revised_string, #date string
                      journal_info['issn_value'], #text
                      journal_info['issn_type'], 
                      journal_info['title'],
                      journal_info['volume'],
                      journal_info['issue'],
                      journal_info['pub_date'],
                      journal_info['iso_abbrev'],
                      article_title_text,
                      vernacular_title_text,
                      medline_pgn,
                      coi_text,
                      n_references,
                      n_chems,
                      n_supp_mesh,
                      n_mesh,
                      n_authors,
                      n_keywords)
    
    #journal_info
            
    return (output,0)

def get_journal_info(journal):
    # <!ELEMENT	Journal (ISSN?, JournalIssue, Title?, ISOAbbreviation?)>
    #
    #     <!ELEMENT	ISSN (#PCDATA) >
    #     <!ATTLIST	ISSN 
	#    	    IssnType  (Electronic | Print) #REQUIRED >
    #
    #     <!ELEMENT	JournalIssue (Volume?, Issue?, PubDate) >
    #         <!ATTLIST	  JournalIssue 
	#	                  CitedMedium (Internet | Print) #REQUIRED >
    #           <!ELEMENT	Volume (#PCDATA) >
    #           <!ELEMENT	Issue (#PCDATA) >
    #     <!ELEMENT	PubDate ((Year, ((Month, Day?) | Season)?) | MedlineDate) >
    #                       12      34          4         3  2              1    
    #
    #                       Year &
    #                              Month, Day | Season
    #                                                       OR MedlineDate
    #           <!ELEMENT	Season (#PCDATA) >
    #           <!ELEMENT	MedlineDate (#PCDATA) >
    #
    #     <!ELEMENT	Title (#PCDATA) >
    #
    #     <!ELEMENT	ISOAbbreviation (#PCDATA) >
    #     
    
    """
    <PubDate>
<MedlineDate>1998 Dec-1999 Jan</MedlineDate>
</PubDate>

<PubDate>
<MedlineDate>2000 Spring</MedlineDate>
</PubDate>
    """
    
    months = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,
              'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
    
    #ISSN --------
    issn_element = journal.find('ISSN')
    if issn_element is not None:
        issn_value = issn_element.text
        issn_type = issn_element.attrib['IssnType']
    else:
        issn_value = None
        issn_type = None
        
    #Journal Issue ------
    journal_issue = journal.find('JournalIssue')
    volume = get_text(journal_issue.find('Volume'),None)
    issue = get_text(journal_issue.find('Issue'),None)
    
    date = journal_issue.find('PubDate')
    year_elem = date.find('Year')

    if year_elem is not None:
        year = year_elem.text
        month_elem = date.find('Month')
        try:
            month = int(get_text(month_elem,'01'))
        except:
            month = months[month_elem.text]
        day = int(get_text(date.find('Day'),'01'))
        season = get_text(date.find('Season'),None)

        if season is not None:
            day = 1
            if season == 'Spring':
                month = 4
            elif season == 'Winter':
                month = 1
            elif season == 'Summer':
                month = 7
            elif season == 'Fall':
                month = 10
            else:
                raise Exception('Unhandled case')
        
        pub_date = '%s-%02d-%02d'%(year,month,day)
    else:
        medline_date = date.find('MedlineDate').text
        
        #We could support starte and stop dates ...
        
        #1) 4 digit year
        #2) 3 char month
        
        year = medline_date[0:4]
        if len(medline_date) >= 8:
            month = months[medline_date[5:8]]
            if len(medline_date) >= 10:
                try:
                    day = int(re.findall("\d+", medline_date[9:])[0])
                    #import pdb
                    #pdb.set_trace()
                except:
                    day = 1
            else:
                day = 1
        else:
            month = 1
            day = 1
            
        #print(medline_date)
        pub_date = '%s-%02d-%02d'%('2000',1,1)
        #Observed types
        #1998 Dec 7-21
        #2006 Feb-Mar
        #2016
        #2018 Oct/Dec
        #2021 Jan/Feb 01
            
    #Title ------------
    title = journal.find('Title').text  
    
    #IsoAbbreviation ------------
    iso_abbrev = journal.find('ISOAbbreviation').text
    
    return {'issn_value':issn_value,
            'issn_type':issn_type,
            'volume':volume,
            'issue':issue,
            'pub_date':pub_date,
            'title':title,
            'iso_abbrev':iso_abbrev}
    

def get_coi_statement(medline_citation):
    #https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#coistatement
    # - Introduced in 2017
    #<!ELEMENT   CoiStatement   (%text;)*>
    return get_text(medline_citation.find('CoiStatement'),None)

def get_references(pubmed_data,pmid_text):
    # PubmedArticle.PubmedData.ReferenceList*
    #<!ELEMENT	ReferenceList (Title?, Reference*, ReferenceList*) >
    #<!ELEMENT	Reference (Citation, ArticleIdList?) >
	    #<!ELEMENT	ArticleIdList (ArticleId+)>
    #<!ELEMENT	ArticleId (#PCDATA) >
    #<!ATTLIST   ArticleId
	    #    IdType (doi | pii | pmcpid | pmpid | pmc | mid |
    #           sici | pubmed | medline | pmcid | pmcbook | bookaccession) "pubmed" >
    ref_lists = pubmed_data.findall('ReferenceList')
    if len(ref_lists) > 0:
        ref_output = []
        #??? When are we going to have multiple reference lists???
        for i,ref_list in enumerate(ref_lists):
            title = get_text(ref_list.find('Title'),'')
            
            ref_list2 = ref_list.find('ReferenceList')
            if ref_list2 is not None:
                raise Exception('Unhandled case, nested reference list')
            
            references = ref_list.findall('Reference')
            for j,reference in enumerate(references):
                citation = reference.find('Citation').text
                article_id_list = reference.find('ArticleIdList')
                local_pmid_text = ''
                local_doi_text = ''
                local_pii_text = ''
                #local_pmcid_text = ''
                if article_id_list is not None:
                    for article_id in article_id_list:
                        id_type = article_id.attrib['IdType']
                        id_value = article_id.text
                        if id_type == 'pubmed':
                            local_pmid_text = id_value
                        elif id_type == 'doi':
                            local_doi_text = id_value
                        elif id_type == 'pii':
                            local_pii_text = id_value
                            #elif id_type == 'pmcid':
                            #pass
                            #local_pmcid_text = id_value
                        else:
                            pass
                            #raise Exception('Unhandled case: %s'%(id_type,))
                            
                ref_output.append((None,pmid_text,title,i,j,
                                   citation,local_pmid_text,
                                   local_doi_text,local_pii_text))
                
        return ref_output
    else:
        return None
                                        

def get_general_notes(medline_citation,pmid_text):
    # MedlineCitation.GeneralNote* --------------------------------
    #<!ELEMENT	GeneralNote (#PCDATA) >
    #<!ATTLIST	GeneralNote
	#	     Owner (NLM | NASA | PIP | KIE | HSR | HMD) "NLM" >
    
    general_notes = medline_citation.findall('GeneralNote')
    if len(general_notes) > 0:
        notes_output = []
        for note in general_notes:
            if 'Owner' in note.attrib:
                note_owner = note.attrib['Owner']
            else:
                import pdb
                pdb.set_trace()
                note_owner = 'nlm'
            notes_output.append((None,
                                  pmid_text,
                                  note.text,
                                  note_owner))
            
        return notes_output
    else:
        return None

def get_space_flight(medline_citation,pmid_text):
    # MedlineCitat.SpaceFlightMission*
    # output['space'] = get_space_flight(medline_citation,pmid_text)
    """
    <SpaceFlightMission>Biosatellite 2 Project</SpaceFlightMission>
    <SpaceFlightMission>Flight Experiment</SpaceFlightMission>
    <SpaceFlightMission>Project Gemini 11</SpaceFlightMission>
    <SpaceFlightMission>manned</SpaceFlightMission>
    """
    # <!ELEMENT	SpaceFlightMission (#PCDATA) >
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#spaceflightmission
    
    space_flights = medline_citation.findall('SpaceFlightMission')
    if len(space_flights) > 0:
        space_output = []
        for space in space_flights:
            space_output.apppend((None,
                                  pmid_text,
                                  space.text))
            
        return space_output
    else:
        return None

def get_keywords(medline_citation,pmid_text):
    # MedlineCitation.KeywordList*
    """
    <KeywordList Owner="KIE">
    <Keyword MajorTopicYN="N">Birth Rate</Keyword>
    <Keyword MajorTopicYN="N">Doe v. Bolton</Keyword>
    """
    
    keyword_lists = medline_citation.findall('KeywordList')
    if len(keyword_lists) > 0:
        n_keywords = len(keyword_lists)
        keywords = []
        for keyword_list in keyword_lists:
            owner = keyword_list.get('Owner')
            for keyword in keyword_list:
                keywords.append((None,
                                 pmid_text,
                                 owner,
                                 int(keyword.get('MajorTopicYN')=='Y'),
                                 keyword.text))
                
        return keywords,n_keywords
    else:
        return None,0
    
    
def get_personal_names(medline_citation,pmid_text):
    #   MedlineCitation.PersonalNameSubjectList?
    #   Personal name subject is when an article is about a person ...
    #
    #   PersonalNameSubjectList (PersonalNameSubject+) 
    #   <!ELEMENT	PersonalNameSubject (LastName, ForeName?, Initials?, Suffix?)
    """
    <PersonalNameSubjectList>
    <PersonalNameSubject>
    <LastName>Koop</LastName>
    <ForeName>C Everett</ForeName>
    <Initials>CE</Initials>
    </PersonalNameSubject>
    </PersonalNameSubjectList>
    """
    pn_list = medline_citation.find('PersonalNameSubjectList')
    if pn_list is None:
        return None
    else:
        pn_output = []
        for pn in pn_list:
            last_name = pn.find('LastName').text
            fore_name = get_text(pn.find('ForeName'),'')
            initials = get_text(pn.find('Initials'),'')
            suffix = get_text(pn.find('Suffix'),'')
            pn_output.append((None,
                              pmid_text,
                              last_name,
                              fore_name,
                              initials,
                              suffix))
            
            
        return pn_output

def get_mesh(medline_citation,pmid_text):
    # MedlineCitation.MeshHeadingList? ---------------------------------    
    """
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D000328">Adult</DescriptorName>
    </MeshHeading>
    <MeshHeading>
    <DescriptorName MajorTopicYN="N" UI="D002318">Cardiovascular Diseases</DescriptorName>
    <QualifierName MajorTopicYN="N" UI="Q000209">etiology</QualifierName>
    <QualifierName MajorTopicYN="Y" UI="Q000401">mortality</QualifierName>
    </MeshHeading>
    """
    
    mesh_list = medline_citation.find('MeshHeadingList')
    if mesh_list is None:
        return None,0
    else:
        n_mesh = len(mesh_list)
        mesh_output = []
        for i, mesh_heading in enumerate(mesh_list):
            #DescriptorName, QualifierName*
            for mesh_elem in mesh_heading:
                #index,type,is_major,ui,str
                mesh_output.append((None,
                                    pmid_text,
                                    i,
                                    int(mesh_elem.get('MajorTopicYN')=='Y'),
                                    mesh_elem.get('UI'),
                                    mesh_elem.text))
        return mesh_output,n_mesh

def get_comments_corrections(medline_citation,pmid_text):
    # MedlineCitation.CommentsCorrectionsList? --------------------------    
    # <!ELEMENT	CommentsCorrectionsList (CommentsCorrections+) >
    # <!ELEMENT	CommentsCorrections (RefSource,PMID?,Note?) >
    # <!ELEMENT	RefSource (#PCDATA) >
    # <!ELEMENT	Note (#PCDATA) >
    # <!ELEMENT	PMID (#PCDATA) >
    # <!ATTLIST	PMID 
    #       Version CDATA #REQUIRED >
    """
    <!ATTLIST	CommentsCorrections 
		     RefType (AssociatedDataset | 
		             AssociatedPublication | 
		             CommentIn | CommentOn | 
		             CorrectedandRepublishedIn | CorrectedandRepublishedFrom |
		             ErratumIn | ErratumFor | 
		             ExpressionOfConcernIn | ExpressionOfConcernFor | 
		             RepublishedIn | RepublishedFrom |  
		             RetractedandRepublishedIn | RetractedandRepublishedFrom |
		             RetractionIn | RetractionOf |  
		             UpdateIn | UpdateOf | 
		             SummaryForPatientsIn | 
		             OriginalReportIn | 
		             ReprintIn | ReprintOf |  
		             Cites)      #REQUIRED    >
             
             
        <CommentsCorrections RefType="ErratumIn">
            <RefSource>J Infect Dis 1998 Aug;178(2):601</RefSource>
            <Note>Whitely RJ [corrected to Whitley RJ]</Note>
        </CommentsCorrections>
        <CommentsCorrections RefType="RetractionOf">
            <RefSource>Dunkel EC, de Freitas D, Scheer DI, Siegel ML, Zhu Q, Whitley RJ, Schaffer PA, Pavan-Langston D. J Infect Dis. 1993 Aug;168(2):336-44</RefSource>
            <PMID VersionID = "1">8393056</PMID>
        </CommentsCorrections>
             
             
             
    """
    cc_list = medline_citation.find('CommentsCorrectionsList')
    if cc_list is None:
        return None
    else:
        cc_output = []
        for cc in cc_list:
            #type,source,PMID?,note?
            cc_type = cc.attrib['RefType']
            ref_source = cc.find('RefSource').text
            note = get_text(cc.find('Note'),'')
            cc_pmid = get_text(cc.find('PMID'),'0')
            cc_output.append((None,
                              pmid_text,
                              cc_type,
                              ref_source,
                              note,
                              cc_pmid))
            
        return cc_output

def get_supp_mesh_list(medline_citation,pmid_text):    
    # MedlineCitation.SupplMeshList? ---------------------------------    
    # (Disease | Protocol | Organism)
    """
    <SupplMeshName Type="Protocol" UI="C040721">ABDIC protocol</SupplMeshName>
    <SupplMeshName Type="Disease" UI="C538248">Amyloid angiopathy</SupplMeshName>
    <SupplMeshName Type="Organism" UI="C000623891">Tomato yellow leaf curl virus</SupplMeshName>
    """
    
    n_supp_mesh = 0
    supp_mesh_list = medline_citation.find('SupplMeshList')
    if supp_mesh_list is None:
        return None,n_supp_mesh
    else:
        n_supp_mesh = len(supp_mesh_list)
        supp_output = []
        for supp_mesh_name in supp_mesh_list:
            #Note, could save space by only storing 1 char for type
            supp_output.append((None,
                                pmid_text,
                                supp_mesh_name.get('Type'),
                                supp_mesh_name.get('UI'),
                                supp_mesh_name.text))
        return supp_output,n_supp_mesh

def get_chem_list(medline_citation,pmid_text):
    # MedlineCitation.ChemicalList? ----------------------------
    #<!ELEMENT	ChemicalList (Chemical+) >
    #<!ELEMENT	Chemical (RegistryNumber, NameOfSubstance) >
    """
    <Chemical List>
    <Chemical>
    <RegistryNumber>69-93-2</RegistryNumber>
    <NameOfSubstance UI="D014527">Uric Acid</NameOfSubstance>
    </Chemical>
    <Chemical>
    <RegistyNumber>6964-20-1</RegistryNumber>
    <NameOfSubstance UI="C004568">tiadenol</NameOfSubstance>
    </Chemical>
    """
    
    chem_list = medline_citation.find('ChemicalList')
    if chem_list is None:
        return None,0
    else:
        n_chems = len(chem_list)
        chem_output = []
        for chem in chem_list:
            reg_number = chem.find('RegistryNumber')
            name = chem.find('NameOfSubstance')
            reg_num_text = reg_number.text
            name_text = name.text
            name_ui = name.get('UI')
            chem_output.append((None,
                                pmid_text,
                                reg_num_text,
                                name_text,
                                name_ui))
        return chem_output,n_chems
    

def get_pagination(article):
    #  MedlineCitation.Article.Pagination -----------------------------------
    #((Pagination, ELocationID*) | ELocationID+)
    #
    #   Start and End not currently used by NLM (reserved for future use)
    #
    #<!ELEMENT	Pagination ((StartPage, EndPage?, MedlinePgn?) | MedlinePgn) >
    #<!ELEMENT	MedlinePgn (#PCDATA) >
    #<!ELEMENT	ELocationID (#PCDATA) >
    #<!ATTLIST	ELocationID 
    #        EIdType (doi | pii) #REQUIRED 
	#	     ValidYN  (Y | N) "Y">
    pagination = article.find('Pagination')
    if pagination is None:
        medline_pgn = ''
        #<MedlinePgn>12-9</MedlinePgn>
    else:
        medline_pgn_elem = pagination.find('MedlinePgn')
        medline_pgn = medline_pgn_elem.text
     
    return medline_pgn

def get_abstract(article,pmid_text):
    # MedlineCitation.Article.Abstract? ------------------------------
    # Abstract (AbstractText+, CopyrightInformation?)
    # <!ELEMENT	CopyrightInformation (#PCDATA) >
    # <!ELEMENT	AbstractText   (%text; | mml:math | DispFormula)* >
    # <!ATTLIST	AbstractText
	#	    Label CDATA #IMPLIED
	#	    NlmCategory (BACKGROUND | OBJECTIVE | METHODS | RESULTS | CONCLUSIONS | UNASSIGNED) #IMPLIED >
		
    #AbstractText - tag in abstract
    
    abstract = article.find('Abstract')
    if abstract is None:
        return None
    else:
        abstract_parts = abstract.findall('AbstractText')
        abstract_data = []
        if len(abstract_parts) == 1:
            #if 'NlmCategory' in abstract_parts[0].attrib:
            #31232872
            #<AbstractText Label="ABSTRACT" NlmCategory="UNASSIGNED">
            #
            #Design decision, just use full,full
            #else:
            abstract_data.append((None,pmid_text,0,'full','full',abstract_parts[0].text))
        else:
            full_abstract = ''
            for i, abstract_part in enumerate(abstract_parts):
                
                abstract_text = abstract_part.text
                if abstract_text is None:
                    abstract_text = ''
                    
                try:
                    label = abstract_part.attrib['Label']
                except:
                    #27412096 - missing label for one element.
                    label = ''
                    
                try:
                    category = abstract_part.attrib['NlmCategory']
                except:
                    #11556437  , Label="OBJECTIVE" with no NlmCategory ...
                    #27412096 Label="AVAILABILITY AND IMPLEMENTATION"
                    #       -> let's set to '' otherwise category could
                    #   be anything
                    category = ''
                    
                abstract_data.append((None,pmid_text,i+1,label,category,abstract_text))
                
                """
                #Example of abstract text is none. Comes after "accessible summary"
                #26283005
                health nursing practitioners can play a pivotal role in this.
                </AbstractText>\n          
                <AbstractText Label="ABSTRACT" NlmCategory="UNASSIGNED"/>\n          
                <AbstractText Label="AIM" NlmCategory="OBJECTIVE">To establish
                """
                
                try:
                    if i > 0:
                        full_abstract = full_abstract + '\n' + label + ': ' + abstract_text
                    else:
                        full_abstract = label + ': ' + abstract_text
                except:
                    print('wtf full')
                    import pdb
                    pdb.set_trace()
                            
            abstract_data.append((None,pmid_text,0,'full','full',full_abstract))
            
        return abstract_data 

def get_languages(article,pmid_text):
    #  MedlineCitation.Article.Language+ --------------------------------
    #https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#language
    #<!ELEMENT	Language (#PCDATA) >
    #<Language>eng</Language>
    #
    #   3 letters
    #   some are 'und' for undetermined ...        
    languages = article.findall('Language')
    if len(languages) > 0:
        lang_data = []
        for language in languages:
            lang_data.append((None,pmid_text,language.text))
        
        return lang_data
    else:
        return None

def get_author_list(article,pmid_text):
    #   MedlineCitation.Article.AuthorList?   -----------------------
    """
    <!ELEMENT	AuthorList (Author+) >
    <!ELEMENT	Author (
                      ((LastName, ForeName?, Initials?, Suffix?) | CollectiveName), 
                      Identifier*, 
                      AffiliationInfo*) >  
    <!ATTLIST	Author 
            ValidYN (Y | N) "Y" 
            EqualContrib    (Y | N)  #IMPLIED >
    
    <!ELEMENT	Identifier (#PCDATA) >
    <!ATTLIST	Identifier 
		    Source CDATA #REQUIRED >
            
    <!ELEMENT	AffiliationInfo (Affiliation, Identifier*)>

    <Identifier Source="ORCID">0000000179841889</Identifier>.
    
    <AffiliationInfo>
    <Affiliation>Harvard Medical School, Boston, Massachusetts</Affiliation>
    <Identifier Source=”Ringgold”>123456</Identifier>
    </AffiliationInfo>
    
    https://www.ringgold.com/ringgold-identifier/
    
    """
    n_authors = 0
    author_list = article.find('AuthorList')
    if author_list is None:
        return None,n_authors
    else:
        author_data = []
        #How does this compare to a dict or ordered dict approach?
        for i, author in enumerate(author_list):
            n_authors += 1
            is_first = int(i == 0)
            is_last = int(i == len(author_list)-1)
            #last name etc. vs collective --------
            last_name = author.find('LastName')
            if last_name is None:
                is_collective = 1
                last_name_text = get_text(author.find('CollectiveName'),'')
                fore_name_text = ''
                initials_text = ''
                suffix_text = ''
            else:
                is_collective = 0
                last_name_text = last_name.text
                fore_name_text = get_text(author.find('ForeName'),'')
                initials_text = get_text(author.find('Initials'),'')
                suffix_text = get_text(author.find('Suffix'),'')
                
            orcid_text = ''
               
            #Identifiers ------------
            identifiers = author.findall('Identifier')
            for identifier in identifiers:
                source = identifier.get('Source')
                if source == 'ORCID':
                    orcid_text = identifier.text
                else:
                    print('Unhandled author identifier')
                    import pdb
                    pdb.set_trace()

            aff_text = ''
            ring_text = '' 
            isni_text = ''
            grid_text = ''                   
                    
            #Affiliation ------------
            aff_info = author.find('AffiliationInfo')
            if aff_info is not None:
                for entry in aff_info:
                    if entry.tag[0] == 'A':
                        aff_text = entry.text
                    else:
                        #Identifier
                        #ISNI
                        #GRID
                        source = entry.get('Source')
                        if source == 'RINGGOLD':
                            ring_text = entry.text
                        elif source == 'ISNI':
                            isni_text = entry.text
                        elif source == 'GRID':
                            grid_text = entry.text
                        elif source == 'ORCID':
                            #33355784
                            #<Identifier Source="ORCID">orcid</Identifier>
                            pass
                        else:
                            import pdb
                            pdb.set_trace()
            
                             
            author_data.append((None,
                                pmid_text,
                                i,
                                is_collective,
                                is_first,
                                is_last,
                                last_name_text,
                                fore_name_text,
                                initials_text,
                                suffix_text,
                                orcid_text,
                                aff_text,
                                ring_text,
                                isni_text,
                                grid_text))        
            
        return author_data,n_authors

def get_data_bank_list(article,pmid_text):
    # MedlineCitation.Article.DataBankList? 
    #   https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#databanklist
    #   https://www.nlm.nih.gov/bsd/medline_databank_source.html
    
    """
    <!ELEMENT	DataBankList (DataBank+) >
    <!ATTLIST	DataBankList 
            CompleteYN (Y | N) "Y" >
        <!ELEMENT	DataBank (DataBankName, AccessionNumberList?) >
            <!ELEMENT	DataBankName (#PCDATA) >
            <!ELEMENT	AccessionNumberList (AccessionNumber+) >
                <!ELEMENT	AccessionNumber (#PCDATA) >
                                       
    <DataBankList CompleteYN="N">
    <DataBank>
    <DataBankName>GENBANK</DataBankName>
    <AccessionNumberList>
    <AccessionNumber>AF078607</AccessionNumber>
    <AccessionNumber>AF078608</AccessionNumber>
    <AccessionNumber>AF078609</AccessionNumber>
    
    <DataBankList CompleteYN="Y">
    <DataBank>
    <DataBankName>ClinicalTrials.gov</DataBankName>
    <AccessionNumberList>
    <AccessionNumber>NCT00000161</AccessionNumber>
    </AccessionNumberList>
    """
    
    data_bank_list = article.find('DataBankList')
    if data_bank_list is not None:
        data_bank_output = []
        is_complete = int(data_bank_list.attrib['CompleteYN'] == 'Y')
        for data_bank in data_bank_list:
            data_bank_name = data_bank.find('DataBankName').text
            acc_list = data_bank.find('AccessionNumberList')
            if acc_list is None:
                data_bank_output.append((None,pmid_text,is_complete,data_bank_name,''))
            else:
                for acc_number in acc_list:
                    data_bank_output.append((None,pmid_text,is_complete,data_bank_name,acc_number.text))
        
        return data_bank_output
    else:
        return None

def get_grant_list(article,pmid_text):
    #  MedlineCitation.Article.GrantList? ----------------------------
    #
    #https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#grantlist
    #
    #
    """
    <GrantList CompleteYN="N">
    <Grant>
    <GrantID>CA 59327</GrantID>
    <Acronym>CA</Acronym>
    <Agency>NCI NIH HHS</Agency>
    <Country>United States</Country>
    </Grant>
    """
    """
    <!ELEMENT	GrantList (Grant+)>
    <!ATTLIST	GrantList 
                CompleteYN (Y | N) "Y">
        <!ELEMENT	Grant (GrantID?, Acronym?, Agency, Country)>
            <!ELEMENT	GrantID (#PCDATA) >
            <!ELEMENT	Acronym (#PCDATA) >
            <!ELEMENT	Agency (#PCDATA) >
            <!ELEMENT	Country (#PCDATA) >
    """
    grant_list = article.find('GrantList')
    if grant_list is not None:
        grants_output = []
        is_complete = int(grant_list.attrib['CompleteYN'] == 'Y')
        for grant in grant_list:
            grant_id = get_text(grant.find('GrantID'),'')
            acronym = get_text(grant.find('Acronym'),'')
            agency = grant.find('Agency').text
            country = grant.find('Country').text
            grants_output.append((None,pmid_text,is_complete,
                                  grant_id,acronym,agency,country))
            
        return grants_output
    else:
        return None

def get_publication_type_list(article,pmid_text):
    #  MedlineCitation.Article.PublicationTypeList --------------------
    #  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#publicationtypelist
    #
    #  https://www.nlm.nih.gov/mesh/pubtypes.html
    #  ??? - where is the translation between UI and value
    
    
    
    """
    <PublicationTypeList>
    <PublicationType UI="D016428">Journal Article</PublicationType>
    <PublicationType UI="D052061">Research Support, N.I.H., Extramural</PublicationType>
    <PublicationType UI="D016441">Retracted Publication</PublicationType>
    <PublicationType UI="D016454">Review</PublicationType>
    </PublicationTypeList
    """
    
    pub_type_list = article.find("PublicationTypeList")
    if pub_type_list is not None:
        pub_type_output = []
        for pub_type in pub_type_list:
            pub_type_output.append((None,
                                    pmid_text,
                                    pub_type.attrib['UI'],
                                    pub_type.text))
    
        return pub_type_output
    else:
        return None

    
"""
---------------------------------------------------------------------
---------------------------------------------------------------------
---------------------------------------------------------------------
---------------------------------------------------------------------
---------------------------------------------------------------------
---------------------------------------------------------------------
---------------------------------------------------------------------
"""
def run_main(xml_file_name,run_local):
    
    """

    """
    if run_local:
        root_path = '/Users/jim/Desktop/pubmed/'
        local_xml_gz_path = root_path + xml_file_name
    else:
        root_path = '/tmp/'
        s3_client = boto3.client('s3','us-east-2',config=botocore.config.Config(s3={'addressing_style': 'path'}))
    
        local_xml_gz_path = '/tmp/' + xml_file_name;
        s3_client.download_file(SOURCE_BUCKET_NAME, xml_file_name, local_xml_gz_path)
        
    root_file_name = xml_file_name[:-7]   
        
    tsv_name = root_file_name + '_main.tsv'
    tsv_name2 = root_file_name + '_deleted.tsv'
    
    other_file_names = table_names[1:]
    
    #other_file_names = ['supp_mesh','mesh','keywords','abstract','authors','chem']
    
    tsv_path = root_path + tsv_name
    tsv_path2 = root_path + tsv_name2
    deleted_exists = False
    
    other_file_paths = [root_path + root_file_name + '_' + x + '.tsv' for x in other_file_names]    
    other_file_csvs = {x:CSVWriter(y) for (x,y) in zip(other_file_names,other_file_paths)}
    
    mycsv = CSVWriter(tsv_path)
    
    t2 = time.time()
    i = 0            
    for elem in iterate_xml(local_xml_gz_path):
        if i % 5000 == 0:
            print('-- %d' % i)
        i+=1
        row,row_type = populate_row(elem)
        if row_type == 0:
            #Write main, and maybe write other things
            mycsv.write(row['main'])
            for name in other_file_names:
                data = row[name]
                if data is not None:
                    writer = other_file_csvs[name]
                    for row2 in data:
                        writer.write(row2)
        else:
            #This will only happen once ...
            deleted_exists = True
            mycsv2 = CSVWriter(tsv_path2)
            for item in row:
                mycsv2.write(item)
            mycsv2.close()
        
        #print('%d: %s'%(i,elem.tag))
    
    mycsv.close()
    print("Written %d bytes to %s" % (mycsv.size(), mycsv.fname()))
    print(i) 
        
    t3 = time.time()

    print('t3-t2: %g' % (t3-t2)) 
    
    #DEST_BUCKET_NAME
    if run_local:
        pass
    else: 
        os.remove(local_xml_gz_path)
        
        s3_client.upload_file(tsv_path, DEST_BUCKET_NAME, tsv_name)
        os.remove(tsv_path)
        
        #TODO: Handle other file uploads ...
        
        if deleted_exists:
            s3_client.upload_file(tsv_path2, DEST_BUCKET_NAME, tsv_name2)
            os.remove(tsv_path2)

def lambda_handler(event, context):
    # TODO implement
    
    file_name = 'pubmed21n1091.xml.gz'
    #file_name = event;
    run_main(file_name,False)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }  

if not running_aws:
    #UTF8 issue - 15380367
    file_name = 'pubmed21n1091.xml.gz'
    """
    for value in range(1000,1090):
        file_name = 'pubmed21n' + str(value) + '.xml.gz'
        print(file_name)
        run_main(file_name,True)
    """
    file_name = 'pubmed21n1078.xml.gz'
    #file_name = 'pubmed21n0001.xml.gz'
    run_main(file_name,True)
        
