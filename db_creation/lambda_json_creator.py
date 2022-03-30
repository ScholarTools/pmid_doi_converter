#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


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

def get_text(elem,default=None):
    if elem is None:
        return default
    else:
        #https://stackoverflow.com/questions/42633089/how-to-remove-all-n-in-xml-payload-by-using-lxml-librarys
        return elem.text.strip()

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
    
    Converts element to json 
    
    Parameters
    ----------
    elem : XML element with the following structure:         
        <PubmedArticle>
            <MedlineCitation>
            <PubmedData>?
        OR
        <DeleteCitation>?
     
    """
    
    #This only occurs once ...
    #TODO: When this occurs we need to created deleted entries ...
    if elem.tag[0] == 'D':
        pmids = [x.text for x in elem]
        return (pmids,1)
    
    output = {}
    
    #   MedlineCitation ---------------------------------------------------
    medline_citation = elem.find('MedlineCitation')
    
    mc_dict = {}
    
    mc_dict['Owner'] = medline_citation.get('Owner')
    if mc_dict['Owner'] is None:
        import pdb
        pdb.set_trace()
    
    mc_dict['Status'] = medline_citation.get('Status')
    mc_dict['VersionID'] = medline_citation.get('VersionID')
    mc_dict['VersionDate'] = medline_citation.get('VersionID')
    mc_dict['IndexingMethod'] = medline_citation.get('IndexingMethod')
        
    """
    

    <!ATTLIST	MedlineCitation 
		Owner  (NLM | NASA | PIP | KIE | HSR | HMD | NOTNLM) "NLM"
		Status (Completed | In-Process | PubMed-not-MEDLINE |  In-Data-Review | Publisher | 
		        MEDLINE | OLDMEDLINE) #REQUIRED 
		VersionID CDATA #IMPLIED
		VersionDate CDATA #IMPLIED 
		IndexingMethod    CDATA  #IMPLIED >
    (
      X Article,
      X ChemicalList?
      X CitationSubset*
      X CoiStatement?
      X CommentsCorrectionsList?
      X DateCompleted?,
      X DateRevised?,
      X GeneralNote*,
      X GeneSymbolList?,
      X InvestigatorList?,
      X KeywordList*,
      MedlineJournalInfo,
      X MeshHeadingList?
      X NumberOfReferences?
      X OtherAbstract*
      X OtherID*
      X PersonalNameSubjectList?
      X PMID, 
      X SpaceFlightMission*,
      X SupplMeshList?
    )>
    """
    # MedlineCitation.Article ------------------------------------------    
    mc_dict['Article'] = get_article(medline_citation.find('Article'))
    
    # MedlineCitation.ChemicalList?
    mc_dict['ChemicalList'] = get_chem_list(medline_citation)
    
    #MedlineCitation.CitationSubset*
    #<!ELEMENT	CitationSubset (#PCDATA) >
    list1 = []
    c_subsets = medline_citation.findall('CitationSubset')
    for c_subset in c_subsets:
        list1.append(get_text(c_subset))
    mc_dict['CitationSubsets'] = list1

    
    # MedlineCitation.CoiStatement?
    mc_dict['CoiStatement'] = get_coi_statement(medline_citation)
    
    # MedlineCitation.CommentsCorrectionsList?
    mc_dict['CommentsCorrectionsList'] = get_comments_corrections(medline_citation)
    
    # MedlineCitation.DateCompleted?
    #TODO: I'm just seeing newline, is this the Python parser ...
    mc_dict['DateCompleted'] = get_text(medline_citation.find('DateCompleted'))

    # MedlineCitation.DateRevised?
    mc_dict['DateRevised'] = get_text(medline_citation.find('DateRevised'))
    
    # MedlineCitation.GeneralNote*
    mc_dict['GeneralNotes'] = get_general_notes(medline_citation)
    
    #GeneSymbolList?
    #<!ELEMENT	GeneSymbol (#PCDATA) >
    #<!ELEMENT	GeneSymbolList (GeneSymbol+)>
    gene_symbol_list = medline_citation.find('GeneSymbolList')
    if gene_symbol_list:
        gene_symbols = gene_symbol_list.findall('GeneSymbol')
        list1 = []
        for gene_symbol in gene_symbols:
            list1.append(get_text(gene_symbol))
        mc_dict['GeneSymbolList'] = list1
    else:
        mc_dict['GeneSymbolList'] = None
    
    # MedlineCitation.InvestigatorList?
    mc_dict['InvestigatorList'] = get_investigator_list(medline_citation)
    
    # MedlineCitation.KeywordList*
    mc_dict['KeywordLists'] = get_keywords(medline_citation)
    
    # MedlineCitation.MedlineJournalInfo
    #<!ELEMENT	MedlineJournalInfo (Country?, MedlineTA, NlmUniqueID?, ISSNLinking?) >
    #<!ELEMENT	Country (#PCDATA) >
    #<!ELEMENT	MedlineTA (#PCDATA) >
    #<!ELEMENT	NlmUniqueID (#PCDATA) >
    #<!ELEMENT	ISSNLinking (#PCDATA) >

    medline_journal_info = medline_citation.find('MedlineJournalInfo')
    temp1 = {}
    names = ['Country','MedlineTA','NlmUniqueID','ISSNLinking']
    for name in names:
        temp1[name] = get_text(medline_journal_info.find(name))
    mc_dict['MedlineJournalInfo'] = temp1
    
    
    # MedlineCitation.MeshHeadingList?
    mc_dict['MeshHeadingList'] = get_mesh(medline_citation)
    
    # MedlineCitation.NumberOfReferences?
    mc_dict['NumberOfReferences'] = get_text(medline_citation.find('NumberOfReferences'),None)
       
    # MedlineCitation.OtherAbstract*
    # <!ELEMENT	OtherAbstract (AbstractText+, CopyrightInformation?) >
    # <!ELEMENT	AbstractText   (%text; | mml:math | DispFormula)* >
    # <!ATTLIST	AbstractText
	#	    Label CDATA #IMPLIED
	#	    NlmCategory (BACKGROUND | OBJECTIVE | METHODS | RESULTS | CONCLUSIONS | UNASSIGNED) #IMPLIED >
    #
    # <!ELEMENT	CopyrightInformation (#PCDATA) >
    other_abstracts = medline_citation.findall('OtherAbstract')
    list1 = []
    for other_abstract in other_abstracts:
        temp1 = {}
        abstract_texts = other_abstract.findall('AbstractText')
        list2 = []
        for abstract_text in abstract_texts:
            temp2 = {}
            temp2['Label'] = abstract_text.get('Label')
            temp2['NlmCategory'] = abstract_text.get('NlmCategory')
            temp2['Value'] = get_text(abstract_text)
            list2.append(temp2)
        temp1['AbstractTexts'] = list2
        temp1['CopyrightInformation'] = get_text(other_abstract.find('CopyrightInformation'))
        list1.append(temp1)
    mc_dict['OtherAbstracts'] = list1
    
    # MedlineCitation.OtherID*
    #<!ELEMENT	OtherID (#PCDATA) >
    #<!ATTLIST	OtherID 
	#	    Source (NASA | KIE | PIP | POP | ARPL | CPC | IND | CPFH | CLML |
	#	            NRCBL | NLM | QCIM) #REQUIRED >
    other_ids = medline_citation.findall('OtherID')
    list1 = []
    for other_id in other_ids:
        temp1 = {}
        temp1['Source'] = other_id.get('Source')
        temp1['Value'] = get_text(other_id)
        list1.append(temp1)
    mc_dict['OtherIDs'] = list1
    
    # MedlineCitation.PersonalNameSubjectList?
    mc_dict['PersonalNameSubjectList'] = get_personal_names(medline_citation)
    
    #MedlineCitation.PMID
    #<!ELEMENT	PMID (#PCDATA) >
    #<!ATTLIST	PMID 
    #       Version CDATA #REQUIRED >
    pmid = medline_citation.find('PMID')
    temp1 = {}
    temp1['Version'] = pmid.get('Version')
    temp1['Value'] = get_text(pmid)
    mc_dict['PMID'] = temp1
    
    # MedlineCitat.SpaceFlightMission*
    mc_dict['SpaceFlightMissions'] = get_space_flight(medline_citation)
    
    # MedlineCitation.SupplMeshList?
    mc_dict['SupplMeshList'] = get_supp_mesh_list(medline_citation)
    
    
    
    import pdb
    pdb.set_trace()




    
  
    # PubmedArticle.PubmedData? -------------------------------------------------------
    
    """
    #<!ELEMENT	PubmedData (
                      History?,
                     PublicationStatus,
                    X ArticleIdList, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#articleidlist
                    ObjectList?,  ???? - these two are not in the description ...
                    ReferenceList*)>
    """
    
    pubmed_data = elem.find('PubmedData')
    
    
    # PubmedArticle.PubmedData.History?
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#history
    # 
    
    # PubmedArticle.PubmedData.PublicationStatus
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#publicationstatus
    # ppublish, epublcih, or ahead of print
    
    pd_dict = {}
    

    # PubmedArticle.PubmedData.ObjectList?
    #<!ELEMENT	ObjectList (Object+) >
    #<!ELEMENT	Object (Param*)>
    #   <!ATTLIST	Object 
    #     Type CDATA #REQUIRED >
    #     
    #<!ELEMENT	Param  (%text;)*>
    #   <!ATTLIST	Param 
    #         Name CDATA #REQUIRED >
    obj_list = pubmed_data.find('ObjectList')
    if obj_list:
        objects = []
        list1 = []
        for _object in objects:
            params = _object.findall('Param')
            list2 = []
            for param in params:
                temp2 = {}
                temp2['Name'] = param.get('Name')
                temp2['Value'] = get_text(param)
                list2.append(temp2)
            list1.append(list2)
                
        pd_dict['ObjectList'] = list1
    else:
        pd_dict['ObjectList'] = None
    
    
    # PubmedArticle.PubmedData.ReferenceList*
    pd_dict['ReferenceList'] = get_references(pubmed_data,pmid_text)

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
     
    return mc_dict

def get_article(article):
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
    
    art = {}
    # MedlineCitation.Article.Journal
    art['Journal'] = get_journal_info(article.find('Journal'))
       
    # MedlineCitation.Article.ArticleTitle
    art['ArticleTitle'] = get_text(article.find('ArticleTitle'))
    
    # MedlineCitation.Article.
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
    temp = {}
    pagination = article.find('Pagination')
    if pagination is None:
        art['Pagination'] = None
    else:
        medline_pgn_elem = pagination.find('MedlinePgn')
        temp['MedlinePgn'] = get_text(medline_pgn_elem)
        temp['StartPage'] = get_text(pagination.find('StartPage'))
        temp['EndPage'] = get_text(pagination.find('EndPage'))
        art['Pagination'] = temp
        
    #TODO: ELocationIDs
    eids = article.findall('ELocationID')
    output = []
    for eid in eids:
        temp = {}
        temp['ValidYN'] = eid.get('ValidYN')
        if temp['ValidYN'] is None:
            import pdb
            pdb.set_trace()
        
        temp['EIdType'] = eid.get('EIdType')
        temp['Value'] = get_text(eid)
        output.append(temp)
    art['ELocationIDs'] = output
    
    
    # MedlineCitation.Article.Abstract?
    art['Abstract'] = get_abstract(article)
    
    # MedlineCitation.Article.AuthorList?
    art['AuthorList'] = get_author_list(article)
       
    # MedlineCitation.Article.Language+
    art['Language'] = get_languages(article)
    
    # MedlineCitation.Article.DataBankList? ------------------------------
    art['DataBankList'] = get_data_bank_list(article)
    
    # MedlineCitation.Article.GrantList? ----------------------------
    art['GrantList'] = get_grant_list(article)
   
    # MedlineCitation.Article.PublicationTypeList --------------------
    art['PublicationTypeList'] = get_publication_type_list(article)
    
    # MedlineCitation.Article.VernacularTitle?
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#verniculartitle
    #
    #<!ELEMENT	VernacularTitle     (%text; | mml:math)*>
    art['VernacularTitle'] = get_text(article.find('VernacularTitle'))
    
    # MedlineCitation.Article.ArticleDate*
    # https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#articledate
    # skipping
    #
    #TODO: do this
    #<!ELEMENT	ArticleDate (Year, Month, Day) >
    #<!ATTLIST	ArticleDate 
    #        DateType CDATA  #FIXED "Electronic" >
    
    ad_elems = article.findall('ArticleDate')
    article_dates = []
    for art_date in ad_elems:  
        temp = {}
        temp['Year'] = get_text(art_date.find('Year'))
        temp['Month'] = get_text(art_date.find('Month'))
        temp['Day'] = get_text(art_date.find('Day'))
        temp['DateType'] = art_date.get('DateType')
        article_dates.append(temp)
    
    art['ArticleDates'] = article_dates
    
    return art

def get_investigator_list(medline_citation):
    # MedlineCitation.InvestigatorList?  --------------------------
    # <!ELEMENT	InvestigatorList (Investigator+) >
    # <!ELEMENT	Investigator (LastName, ForeName?, Initials?, Suffix?, Identifier*, AffiliationInfo*) >
    # <!ATTLIST	Investigator 
	#	    ValidYN (Y | N) "Y" >
    #<!ELEMENT	Affiliation  (%text;)*>
    #<!ELEMENT	AffiliationInfo (Affiliation, Identifier*)>
    #<!ELEMENT	Identifier (#PCDATA) >
    #<!ATTLIST	Identifier 
	#	    Source CDATA #REQUIRED >

    ilist = medline_citation.find('InvestigatorList')
    if ilist is None:
        return None
    else:
        investigators = ilist.findall('Investigator')
        output = []
        for investigator in investigators:
            idict = {}
            idict['ValidYN'] = investigator.get('ValidYN')
            idict['LastName'] = get_text(investigator.find('LastName'))
            idict['ForeName'] = get_text(investigator.find('ForeName'))
            idict['Initials'] = get_text(investigator.find('Initials'))
            idict['Suffix'] = get_text(investigator.find('Suffix'))
            
            ids = investigator.findall('Identifier')
            out2 = []
            for identifier in ids:
                temp = {}
                temp['Source'] = identifier.get('Source')
                temp['Value'] = get_text(identifier)
                out2.append(temp)
            idict['Identifiers'] = out2
            
            aff_info = investigator.findall('AffiliationInfo')
            out3 = []
            for aff in aff_info:
                temp = {}
                temp['Affiliation'] = get_text(aff.find('Affiliation'))
                ids = aff.findall('Identifier')
                out2 = []
                for identifier in ids:
                    temp = {}
                    temp['Source'] = identifier.get('Source')
                    temp['Value'] = get_text(identifier)
                    out2.append(temp)
                temp['Identifiers'] = out2
                out3.append(temp)
            idict['AffiliationInfos'] = out3
            output.append(idict)
            
        return output


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

    output = {}
    
    #ISSN --------
    #     <!ELEMENT	ISSN (#PCDATA) >
    #     <!ATTLIST	ISSN 
	#    	    IssnType  (Electronic | Print) #REQUIRED >
    issn_element = journal.find('ISSN')
    if issn_element is None:
        output['ISSN'] = None
    else:
        temp = {}
        temp['IssnType'] = issn_element.get('IssnType')
        temp['Value'] = get_text(issn_element)
        output['ISSN'] = temp
        
    #Journal Issue ------
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
    journal_issue = journal.find('JournalIssue')
    
    temp = {}
    temp['Volume'] = get_text(journal_issue.find('Volume'))
    temp['Issue'] = get_text(journal_issue.find('Issue'))
    temp['CitedMedium'] = journal_issue.get('CitedMedium')
    
    date_elem = journal_issue.find('PubDate')    
    date = {}
    date['Year'] = get_text(date_elem.find('Year'))
    date['Month'] = get_text(date_elem.find('Month'))
    date['Day'] = get_text(date_elem.find('Day'))
    date['Season'] = get_text(date_elem.find('Season'))
    date['Year'] = get_text(date_elem.find('Year'))
    date['MedlineDate'] = get_text(date_elem.find('MedlineDate'))

    temp['PubDate'] = date

    output['JournalIssue'] = temp    

    #Title ------------
    output['Title'] = get_text(journal.find('Title'))
    
    #IsoAbbreviation ------------
    output['ISOAbbreviation'] = get_text(journal.find('ISOAbbreviation'))
    
    return output
    

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
                                        

def get_general_notes(medline_citation):
    # MedlineCitation.GeneralNote* --------------------------------
    #<!ELEMENT	GeneralNote (#PCDATA) >
    #<!ATTLIST	GeneralNote
	#	     Owner (NLM | NASA | PIP | KIE | HSR | HMD) "NLM" >
    
    general_notes = medline_citation.findall('GeneralNote')
    output = []
    for note in general_notes:
        temp = {}
        temp['Owner'] = note.get('Owner')
        if output['Owner'] is None:
            import pdb
            pdb.set_trace()
        temp['Value'] = get_text(note)
        output.append(temp)
            
    return output

def get_space_flight(medline_citation):
    # MedlineCitat.SpaceFlightMission*
    #<!ELEMENT	SpaceFlightMission (#PCDATA) >
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
            space_output.apppend(get_text(space))
            
        return space_output
    else:
        return None

def get_keywords(medline_citation):
    # MedlineCitation.KeywordList*
    """
    <KeywordList Owner="KIE">
    <Keyword MajorTopicYN="N">Birth Rate</Keyword>
    <Keyword MajorTopicYN="N">Doe v. Bolton</Keyword>
    """
    
    keyword_lists = medline_citation.findall('KeywordList')
    if len(keyword_lists) > 0:
        k_lists = []
        for keyword_list in keyword_lists:
            temp = {}
            temp['Owner'] = keyword_list.get('Owner')
            keywords = []
            for keyword in keyword_list:
                temp2 = {}
                temp2['MajorTopicYN'] = keyword.get('MajorTopicYN')
                temp2['Value'] = keyword.text
                keywords.append(temp2)
            temp['Keywords'] = temp2
            k_lists.append(temp)
                
        return k_lists
    else:
        return []
    
    
def get_personal_names(medline_citation):
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
        return []
    else:
        pn_output = []
        for pn in pn_list:
            temp = {}
            temp['LastName'] = pn.find('LastName').text
            temp['ForeName'] = get_text(pn.find('ForeName'),None)
            temp['Initials'] = get_text(pn.find('Initials'),None)
            temp['Suffix'] = get_text(pn.find('Suffix'),None)
            
            pn_output.append(temp)
            
            
        return pn_output

def get_mesh(medline_citation):
    # MedlineCitation.MeshHeadingList? ---------------------------------  
    #
    #<!ELEMENT	MeshHeadingList (MeshHeading+)>
    #   <!ELEMENT	MeshHeading (DescriptorName, QualifierName*)>
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
        return []
    else:
        mesh_output = []
        for i, mesh_heading in enumerate(mesh_list):
            
            qualifiers = []
            descriptor = None
            #DescriptorName, QualifierName*
            for mesh_elem in mesh_heading:
                #index,type,is_major,ui,str
                
                #Descriptor
                #Qualifiers
                #   - is major
                #   - ui
                #   - value

                temp = {'MajorTopicYN':mesh_elem.get('MajorTopicYN'),
                        'UI':mesh_elem.get('UI'),
                        'Value':mesh_elem.text}
                
                if mesh_elem.tag == 'DescriptorName':
                    descriptor = temp
                else:
                    qualifiers.append(temp)
            
            mesh_output.append({'Descriptor':descriptor,'Qualifiers':qualifiers})   

        return mesh_output

def get_comments_corrections(medline_citation):
    # MedlineCitation.CommentsCorrectionsList? --------------------------    
    # <!ELEMENT	CommentsCorrectionsList (CommentsCorrections+) >
    #   <!ELEMENT	CommentsCorrections (RefSource,PMID?,Note?) >
    #       <!ELEMENT	RefSource (#PCDATA) >
    #       <!ELEMENT	Note (#PCDATA) >
    #       <!ELEMENT	PMID (#PCDATA) >
    #           <!ATTLIST	PMID Version CDATA #REQUIRED >
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
        return []
    else:
        cc_output = []
        for cc in cc_list:
            cc_pmid = cc.find('PMID')
            if cc_pmid is None:
                pmid = None
            else:
                pmid = {'value':int(cc_pmid.text),
                        'Version':cc_pmid.attrib['VersionID']}
                pass
            cc_output.append({'RefType':cc.attrib['RefType'],
                             'RefSource':cc.find('RefSource').text,
                             'Note':get_text(cc.find('Note'),''),
                             'PMID':pmid})
            
        return cc_output

def get_supp_mesh_list(medline_citation):    
    # MedlineCitation.SupplMeshList? ---------------------------------    
    # (Disease | Protocol | Organism)
    """
    <SupplMeshName Type="Protocol" UI="C040721">ABDIC protocol</SupplMeshName>
    <SupplMeshName Type="Disease" UI="C538248">Amyloid angiopathy</SupplMeshName>
    <SupplMeshName Type="Organism" UI="C000623891">Tomato yellow leaf curl virus</SupplMeshName>
    """
    
    supp_mesh_list = medline_citation.find('SupplMeshList')
    if supp_mesh_list is None:
        return []
    else:
        supp_output = []
        for supp_mesh_name in supp_mesh_list:
            #Note, could save space by only storing 1 char for type
            supp_output.append({'Name':supp_mesh_name.text,
                                'Type':supp_mesh_name.get('Type'),
                                'UI':supp_mesh_name.get('UI')})
        return supp_output

def get_chem_list(medline_citation):
    # MedlineCitation.ChemicalList? ----------------------------
    #<!ELEMENT	ChemicalList (Chemical+) >
    #<!ELEMENT	Chemical (RegistryNumber, NameOfSubstance) >
    #<!ELEMENT	RegistryNumber (#PCDATA) >
    #<!ELEMENT	NameOfSubstance (#PCDATA) >
    #<!ATTLIST	NameOfSubstance 
	#	    UI CDATA #REQUIRED >

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
        return []
    else:
        chem_output = []
        for chem in chem_list:
            name_tag = chem.find('NameOfSubstance');
            chem_output.append(
                {'RegistryNumber':chem.find('RegistryNumber').text,
                'NameOfSubstance':name_tag.text,
                'UI':name_tag.get('UI')})
                
        return chem_output
    
def get_abstract(article):
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
        output = {}
        output['CopyrightInformation'] = get_text(abstract.find('CopyrightInformation'))
        abstract_parts = abstract.findall('AbstractText')
        abstract_data = []
        for abstract_text in abstract_parts:
            temp = {}
            temp['Value'] = get_text(abstract_text)
            temp['Label'] = abstract_text.get('Label')
            abstract_data.append(temp)
        output['Parts'] = abstract_data
            
        return output 

def get_languages(article):
    #  MedlineCitation.Article.Language+ --------------------------------
    #https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#language
    #<!ELEMENT	Language (#PCDATA) >
    #<Language>eng</Language>
    #
    #   3 letters
    #   some are 'und' for undetermined ...   
    output = []    
    languages = article.findall('Language')
    for language in languages:
        output.append(get_text(language))
        
    return output

def get_author_list(article):
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
    <!ELEMENT	Affiliation  (%text;)*>


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
        return []
    else:
        author_data = []
        #How does this compare to a dict or ordered dict approach?
        for i, author in enumerate(author_list):
            n_authors += 1
            #last name etc. vs collective --------
            temp = {}
            temp['CollectiveName'] = get_text(author.find('CollectiveName'))
            temp['LastName'] = get_text(author.find('LastName'))
            temp['ForeName'] = get_text(author.find('ForeName'))
            temp['Initials'] = get_text(author.find('Initials'))
            temp['Suffix'] = get_text(author.find('Suffix'))
                           
            #Identifiers ------------
            identifiers = author.findall('Identifier')
            id_list = []
            for identifier in identifiers:
                temp2 = {}
                temp2['Source'] = identifier.get('Source')
                temp2['Value'] = get_text(identifier)
                id_list.append(temp2)
                
            temp['Identifiers'] = id_list

            #Affiliation ------------
            aff_info = author.find('AffiliationInfo')
            aff_list = []
            if aff_info is not None:
                temp2 = {}
                temp2['Affiliation'] = get_text(aff_info.find('Affiliation'))
                identifiers = author.findall('Identifier')
                id_list = []
                for identifier in identifiers:
                    temp3 = {}
                    temp3['Source'] = identifier.get('Source')
                    temp3['Value'] = get_text(identifier)
                    id_list.append(temp2)
                    
                temp2['Identifiers'] = id_list
                aff_list.append(temp2)
            
            temp['Affiliations'] = aff_list
            author_data.append(temp)

        return author_data

def get_data_bank_list(article):
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
        temp = {}
        temp['CompleteYN'] = data_bank_list.get('CompleteYN')
        data_banks = DataBankList.find('DataBank')
        data_bank_output = []
        for data_bank in data_banks:
            temp2 = {}
            temp2['DataBankName'] = get_text(data_bank.find('DataBankName'))
            acc_list = data_bank.find('AccessionNumberList')
            acc_numbers = acc_list.find_all('AccessionNumber')
            acc_output = []
            for acc_number in acc_numbers:
                acc_output.append(get_text(acc_number))
                
            temp2['AccessionNumberList'] = acc_output
            data_bank_output.append(temp2)
              
        temp['DataBankList'] = data_bank_output
        
        return temp
    else:
        return None

def get_grant_list(article):
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
        temp = {}
        temp['CompleteYN'] = grant_list.get('CompleteYN')
        grants_output = []
        grants = grant_list.findall('Grant')
        for grant in grants:
            temp2 = {}
            temp2['GrantID'] = get_text(grant.find('GrantID'))
            temp2['Acronym'] = get_text(grant.find('Acronym'))
            temp2['Agency'] = get_text(grant.find('Agency'))
            temp2['Country'] = get_text(grant.find('Country'))
            grants_output.append(temp2)
         
        temp['Grants'] = grants_output
            
        return temp
    else:
        return None

def get_publication_type_list(article):
    #  MedlineCitation.Article.PublicationTypeList --------------------
    #  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#publicationtypelist
    #
    #  https://www.nlm.nih.gov/mesh/pubtypes.html
    #  ??? - where is the translation between UI and value
    #
    #<!ELEMENT	PublicationType (#PCDATA) >
    #<!ATTLIST	PublicationType 
	#	    UI CDATA #REQUIRED >
	#      <!ELEMENT	PublicationTypeList (PublicationType+) >
    
    
    
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
        pub_types = pub_type_list.findall('PublicationType')
        pub_type_output = []
        for pub_type in pub_type_list:
            temp = {}
            temp['UI'] = pub_type.get('UI')
            temp['Value'] = get_text(pub_type)
            pub_type_output.append(temp)
    
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
        
   
    
    t2 = time.time()
    i = 0            
    for elem in iterate_xml(local_xml_gz_path):
        if i % 5000 == 0:
            print('-- %d' % i)
        i+=1
        populate_row(elem)


    """
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
    """

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
        
