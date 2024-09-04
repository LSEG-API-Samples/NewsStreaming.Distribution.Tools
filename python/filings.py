#=============================================================================
# Refinitiv Data Platform demo app to get filings data
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import requests
import json
import rdpToken

# Application Constants
RDP_version = "/v1"
base_URL = "https://api.refinitiv.com"
DOC_CLIENT_ID = "my_application_name"
DOC_API_KEY = "155d9dbf-f0ac-46d9-8b77-f7f6dcd238f8"

 
#==============================================
def requestSearch(searchPayload):   
#==============================================
	category_URL = "/data-store"
	endpoint_URL = "/graphql"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	
	requestData = {
		"query": searchPayload
	}

	# get the latest access token
	accessToken = rdpToken.getToken()
	hdrs = {
		'Authorization': "Bearer " + accessToken,
		'Content-Type': "application/json",
		'cache-control': "no-cache"
	}

	sResp = requests.post(RESOURCE_ENDPOINT, headers=hdrs, data = json.dumps(requestData))

	if sResp.status_code != 200:
		raise ValueError("Unable to search. Code %s, Message: %s" % (sResp.status_code, sResp.text))
	else:
		jResp = json.loads(sResp.text)
		documentID = jResp["data"]["FinancialFiling"][0]["FilingDocument"]["DocId"]
		return documentID


#==============================================
def retrieveDocURL(documentID):   
#==============================================
	category_URL = "/data/filings/"
	endpoint_URL = "/retrieval/search/docId/"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL + documentID

	# get the latest access token
	accessToken = rdpToken.getToken()
	hdrs = {
		'Authorization': "Bearer " + accessToken,
		"X-API-Key": DOC_API_KEY,
		"ClientID" : DOC_CLIENT_ID
	}

	rResp = requests.get(RESOURCE_ENDPOINT, headers = hdrs)

	if rResp.status_code != 200:
		raise ValueError("Unable to get document URL. Code %s, Message: %s" % (rResp.status_code, rResp.text))
	else:
		jResp = json.loads(rResp.text)
		fName = list(jResp.keys())[0]
		sURL = jResp[list(jResp.keys())[0]]["signedUrl"]
		return fName, sURL


#==============================================
def retrieveSaveDoc(fileName, signedUrl):
#==============================================
	dResp = requests.get(signedUrl, allow_redirects=True)

	if dResp.status_code != 200:
		raise ValueError("Unable to download the document. Response: " % (dResp.status_code, dResp.text))
	else:
		with open(fileName, 'wb') as f:
			f.write(dResp.content)
			f.close()
		
		print("The document [%s], has been downloaded" % fileName)



#==============================================
if __name__ == "__main__":
#==============================================
	documentSearchText = """
	{
		FinancialFiling( 
			sort: {FilingDocument: {DocumentSummary: {FilingDate: DESC}}}, 
			filter: {FilingDocument: {DocumentSummary: {FilingDate: {BETWN: {FROM: "2020-07-01T00:00:00Z", TO: "2020-08-01T00:00:00Z"}}}}}, 
			keywords: {searchstring: "FinancialFiling.FilingDocument.DocumentText:COVID-19"}, 
			limit: 5) { 
			_metadata { 
				totalCount 
				} 
			FilingOrganization { 
				Names { 
					Name { 
						OrganizationName(  
						filter: {AND: [ {
							OrganizationNameLanguageId: {EQ: "505062"}}, {
							OrganizationNameTypeCode: {EQ: "LNG"}}]}) 
						{ 
							OrganizationName 
						} 
					} 
				} 
			}             
			FilingDocument { 
				DocId
				DocumentSummary { 
					DocumentTitle 
					FilingDate 
					FormType 
					FeedName                     
				} 
				DocumentText 
			} 
		}
	} 
	"""

	# search for the document first
	print("Performing a document search using document-text...")
	docId = requestSearch(documentSearchText)
	print("Document ID is: %s " % str(docId))
	
	# get a signed download link for this document
	print("")
	print("Retrieving the document URL for this DocID...")
	fileName, signedUrl = retrieveDocURL(docId)
	print("Document fileName is: %s" % fileName)
	print("Retrieval signedUrl is: %s" % signedUrl)

	# download the document
	print("")
	print("Downloading the document: %s..." % fileName)
	retrieveSaveDoc(fileName, signedUrl)

	# Search by OrgId Example
	"""
	{
		FinancialFiling(filter: {
			AND: [
				{FilingDocument: {Identifiers: {OrganizationId: {EQ: "4297089638"}}}}, 
				{FilingDocument: {DocumentSummary: {FilingDate: {BETWN: {FROM: "2020-01-01T00:00:00Z", TO: "2020-12-31T00:00:00Z"}}}}}
			]
		}, limit: 25) 
		{
			FilingDocument {
				_metadata {
					totalCount
				}
				DocId
				DocumentSummary {
					DocumentTitle
					FilingDate
					DocumentType
					FeedName
				}
			}
		}
	}
	"""
	
	# Search by section (Edgar) Example
	"""
	{
		FinancialFiling(sort: {
			FilingDocument: {DocumentSummary: {FilingDate: DESC}}
		}, filter: {FilingDocument: {DocumentSummary:
			{FormType: {EQ: "10-K"}}}}, 
		keywords: {
			searchstring:
				"FinancialFiling.FilingDocument.Sections.ManagementDiscussion.Text:(expense increase)"}) {
					_metadata {
						totalCount
					}
					FilingDocument {
						DocId
						DocumentSummary {
							DocumentTitle
							FilingDate
							FormType
							FeedName
							ValueAddedTags {
								ValueAddedTag
							}
						}
						Sections {
							ManagementDiscussion {
								Text
							}
						}
					}
		}
	}
	"""
