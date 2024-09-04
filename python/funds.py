#=============================================================================
# Elektron Data Platform demo app to get funds data
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import requests
import json
import rdpToken

# Application Constants
EDP_version = "/v1"
base_URL = "https://api.refinitiv.com"
category_URL = "/data/funds"
endpoint_URL = "/assets"


#==============================================
def getFundData(requestPayload):
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + EDP_version + endpoint_URL
	
	# Get latest access token
	print("Getting OAuth access token...")
	accessToken = rdpToken.getToken()
	
	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestPayload)

	if dResp.status_code != 200:
		print("Unable to get data. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("Funds data received:")
		# Display data
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))



#==============================================
if __name__ == "__main__":
#==============================================

	# Get assets data for fund
	lipperID = "40003333"
	print("Getting assets data for fund: %s" % lipperID)

	requestData = {
		"symbols": lipperID,
		"properties": "names"
	}
	
	getFundData(requestData)
		
	# Get ratings data for fund
	lipperID = "60003333"
	print("Getting ratings data for fund: %s" % lipperID)

	requestData = {
		"symbols": lipperID,
		"properties": "llsratings"
	}
	
	getFundData(requestData)
