#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Goal is to read xml and save as tsv
http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd
https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html

#TODO: I would be interested in seeing how performance compares
if instead of doing finds we just iterated elements and handled them

"""


#Standard  --------------
import csv
import gzip
import os
import json
import time


#Third --------------
from lxml import etree
import boto3
import botocore


SOURCE_BUCKET_NAME = 'pubmed2021'
DEST_BUCKET_NAME = 'pubmed2021tsv'

#Keep main first
table_names = ['main','abstract','authors','chem','keywords','languages',
               'mesh','supp_mesh','comments_corrections','personal_names']  

#EC2 & Lambda support
my_user = os.environ.get("USER") #for EC2
if my_user is None:
    my_user = ''
#                   EC2                     LAMBDA
running_aws = ("ec2" in my_user) or (os.environ.get("AWS_EXECUTION_ENV") is not None)


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
    
    """
    cc_list = medline_citation.find('CommentsCorrectionsList')
    if cc_list is not None:
        print("found it")
        import pdb
        pdb.set_trace()
        
    return ('test','test')
    """
    
    pmid = medline_citation.find('PMID')
    pmid_text = pmid.text
    
    """
    (
    X PMID, 
    S DateCompleted?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#datecompleted
        Not particularly interested in this
    S DateRevised?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#daterevised
        Not particularly interested in this
    X Article, 
    S MedlineJournalInfo,   #( Country?, MedlineTA, NlmUniqueID?, ISSNLinking? )
        Skipping for now
    X ChemicalList?,  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#chemicallist
    X SupplMeshList?,
    S CitationSubset*,  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#citationsubset
        - For some articles has a tag that indicates it belongs to a special group
        - Does not seem all that particularly useful
    X CommentsCorrectionsList?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#commentscorrections
    S GeneSymbolList?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#genesymbollist
            Sounds like it was only used for a brief time in the 90s
    X MeshHeadingList?, 
    S NumberOfReferences?, - not accurate for >2010
    X PersonalNameSubjectList?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#personalnamesubjectlist
        When an article is about someone
    OtherID*,  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#otherid
        Doesn't really look like it is used
    S OtherAbstract*, 
    X KeywordList*, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#keywordlist
    S CoiStatement?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#coistatement
        started in 2017
        TODO: This could be in the main table since only text, does large text
        impact table performance?
    SpaceFlightMission*, 
    S InvestigatorList?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#investigatorlist
            Can be used to describe people that contributed that are not authors
    S GeneralNote* https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#generalnote
        - Catchall for extra information
    )>
    """
    
    #    MedlineCitation.ChemicalList ----------------------------
    
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
    
    n_chems = 0
    chem_list = medline_citation.find('ChemicalList')
    if chem_list is not None:
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
        output['chem'] = chem_output


    
    #    MedlineCitation.SupplMeshList ----------------------------------------
    #   (Disease | Protocol | Organism)
    """
    <SupplMeshName Type="Protocol" UI="C040721">ABDIC protocol</SupplMeshName>
    <SupplMeshName Type="Disease" UI="C538248">Amyloid angiopathy</SupplMeshName>
    <SupplMeshName Type="Organism" UI="C000623891">Tomato yellow leaf curl virus</SupplMeshName>
    """
    
    n_supp_mesh = 0
    supp_mesh_list = medline_citation.find('SupplMeshList')
    if supp_mesh_list is not None:
        n_supp_mesh = len(supp_mesh_list)
        supp_output = []
        for supp_mesh_name in supp_mesh_list:
            #Note, could save space by only storing 1 char for type
            supp_output.append((None,
                                pmid_text,
                                supp_mesh_name.get('Type'),
                                supp_mesh_name.get('UI'),
                                supp_mesh_name.text))
        output['supp_mesh'] = supp_output
        
        
    #    MedlineCitation.CommentsCorrectionsList -----------------------------------------
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
    #print(str(etree.tostring(cc_list).decode('utf8')))
    cc_list = medline_citation.find('CommentsCorrectionsList')
    if cc_list is not None:
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
            
        output['comments_corrections'] = cc_output
        
    
    #    MedlineCitation.MeshHeadingList ---------------------------------
    
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
    
    n_mesh = 0
    mesh_list = medline_citation.find('MeshHeadingList')
    if mesh_list is not None:
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
        output['mesh'] = mesh_output
        
        
    #   MedlineCitation.PersonalNameSubjectList
    #PersonalNameSubjectList (PersonalNameSubject+) 
    #<!ELEMENT	PersonalNameSubject (LastName, ForeName?, Initials?, Suffix?)
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
    if pn_list is not None:
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
            
            
        output['personal_names'] = pn_output
    
    #PersonalNameSubjectList
    
    
    #   MedlineCitation.KeywordList  ---------------------------------
    
    """
    <KeywordList Owner="KIE">
    <Keyword MajorTopicYN="N">Birth Rate</Keyword>
    <Keyword MajorTopicYN="N">Doe v. Bolton</Keyword>
    """
    
    n_keywords = 0
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
                
        output['keywords'] = keywords
     
    #   --------------------------------------
    #   MedlineCitation.Article      ------------------------------------------
    #   --------------------------------------
    
    """
    <!ELEMENT	Article (
                   X Journal, 
                   X ArticleTitle,
                   X ((Pagination, ELocationID*) | ELocationID+),
                       - pagination is handled, elocationID is not (doi or pii)
                   X Abstract?,
                   X AuthorList?, 
                   X Language+,  https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#language
                   DataBankList?, https://www.nlm.nih.gov/bsd/medline_databank_source.html
                       TODO: This contains important things like clinical trial IDs
                   GrantList?,
                   PublicationTypeList, 
                   VernacularTitle?, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#verniculartitle
                   ArticleDate*
                   ) >
    """

    article = medline_citation.find('Article')
    
    #   MedlineCitation.Article.Journal   ------------------------
    journal = article.find('Journal')
    
    #<!ELEMENT	Journal (ISSN?, JournalIssue, Title?, ISOAbbreviation?)>
    #<!ELEMENT	JournalIssue (Volume?, Issue?, PubDate) >
    #<!ELEMENT	PubDate ((Year, ((Month, Day?) | Season)?) | MedlineDate) >
    
    journal_title = journal.find('Title')
    journal_text = journal_title.text
    
    journal_issue = journal.find('JournalIssue')
    journal_volume_text = get_text(journal_issue.find('Volume'),'')
    journal_issue_text = get_text(journal_issue.find('Issue'),'')
    
    journal_date = journal_issue.find('PubDate')
    journal_year_text = get_text(journal_date.find('Year'),'0')
    journal_month_text = get_text(journal_date.find('Month'),'')
    
    #   MedlineCitation.Article.ArticleTitle   -------------------
    article_title = article.find('ArticleTitle')
    article_title_text = article_title.text
    
    
    #   MedlineCitation.Article.Pagination -----------------------------------
    #((Pagination, ELocationID*) | ELocationID+)
    #
    #   Start and End not currently used ...
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
    
    """
    #Yikes, how does this compare to article IDs ????
    elocation_ids = article.findall('ELocationID')
    if len(elocation_ids) > 0:
        pass
    """
    
    
    
    #   MedlineCitation.Article.Abstract  ------------------------------
    #Abstract (AbstractText+, CopyrightInformation?)
    #<!ELEMENT	CopyrightInformation (#PCDATA) >
    #<!ELEMENT	AbstractText   (%text; | mml:math | DispFormula)* >
    #<!ATTLIST	AbstractText
	#	    Label CDATA #IMPLIED
	#	    NlmCategory (BACKGROUND | OBJECTIVE | METHODS | RESULTS | CONCLUSIONS | UNASSIGNED) #IMPLIED >
		
    #AbstractText - tag in abstract
    
    abstract = article.find('Abstract')
    if abstract is not None:
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
            
        output['abstract'] =  abstract_data   

        
    #   MedlineCitation.Article.AuthorList?   -----------------------
    """
    <!ELEMENT	AuthorList (Author+) >
    <!ELEMENT	Author (
                      ((LastName, ForeName?, Initials?, Suffix?) | CollectiveName), 
                      Identifier*, 
                      AffiliationInfo*) >  
    
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
    if author_list is not None:
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
            
        output['authors'] = author_data
        
    #  LanguageList ---------------------------------------------
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
        
        output['languages'] = lang_data
    
    #  DataBankList ---------------------------------------------
    #https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#databanklist
  
    #  Pubmed Data -------------------------------------------------------
    
    """
    #<!ELEMENT	PubmedData (
                    History?,
                    PublicationStatus,
                    X ArticleIdList, https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html#articleidlist
                    ObjectList?,  ???? - these two are not in the description ...
                    ReferenceList*) ???? >
    """
    
    pubmed_data = elem.find('PubmedData')
    
    if pubmed_data is None:
        doi_text = ''
        pmcid_text = ''
        pii_text = ''
    else:
        
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
            
    output['main'] = (None,
                      pmid_text,
                      doi_text,
                      pii_text,
                      pmcid_text,
                      journal_text,
                      journal_volume_text,
                      journal_year_text,
                      journal_issue_text,
                      journal_month_text,
                      article_title_text,
                      medline_pgn,
                      n_chems,
                      n_supp_mesh,
                      n_mesh,
                      n_authors,
                      n_keywords)
    
    
    
    return (output,0)
    
"""
#---------------------------------------------------------------------
#---------------------------------------------------------------------
#---------------------------------------------------------------------
#---------------------------------------------------------------------
#---------------------------------------------------------------------
#---------------------------------------------------------------------
#---------------------------------------------------------------------
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
        
