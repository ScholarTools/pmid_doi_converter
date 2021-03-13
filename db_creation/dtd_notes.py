#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

http://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd
https://www.nlm.nih.gov/bsd/licensee/elements_descriptions.html

#1-------------------------------------------------
#PubmedArticleSet is the top most element
<!ELEMENT	PubmedArticleSet ((PubmedArticle | PubmedBookArticle)+, DeleteCitation?) >

- ****Important, my understanding is that this means we will have the articles
  before the singular DeleteCitation (if present)

    #2-------------------------------------------------
    <!ELEMENT	PubmedArticle (MedlineCitation, PubmedData?)>

        #3-------------------------------------------------
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
                                   NumberOfReferences?, - not accurate for >2010
                                   PersonalNameSubjectList?, 
                                   OtherID*, 
                                   OtherAbstract*, 
                                   KeywordList*, 
                                   CoiStatement?, 
                                   SpaceFlightMission*, 
                                   InvestigatorList?, 
                                   GeneralNote*)>
        <!ATTLIST	MedlineCitation 
		Owner  (NLM | NASA | PIP | KIE | HSR | HMD | NOTNLM) "NLM"
		Status (Completed | In-Process | PubMed-not-MEDLINE |  In-Data-Review | Publisher | 
		        MEDLINE | OLDMEDLINE) #REQUIRED 
		VersionID CDATA #IMPLIED
		VersionDate CDATA #IMPLIED 
		IndexingMethod    CDATA  #IMPLIED >
        
            #4-------------------------------------------------
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
        
        #-------------------------------------------------
        #<!ELEMENT	PubmedData (History?,
                                PublicationStatus,
                                ArticleIdList, 
                                ObjectList?, 
                                ReferenceList*) >


PubmedArticleSet
    .PubmedArticle+
        .MedlineCitation
            .Article
        .PubmedData?
    .DeleteCitation?
    


"""
