#=============================================================================
# Refinitiv Data Platform demo app to do Search
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import requests
import json
import rdpToken

# Application Constants
RDP_version = "/beta1"
base_URL = "https://api.refinitiv.com"
category_URL = "/search"
endpoint_URL = "/"


#==============================================
if __name__ == "__main__":
#==============================================
	# Get latest access token
	print("Getting OAuth access token...")
	accessToken = rdpToken.getToken()
	print("Invoking data request")

	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

	requestData = {
	  "View": "SearchAll",
	  "Filter": "RIC eq 'IBM'",
	  "Select": "_debugall",
	  "Top": 2000
	}

	dResp = requests.post(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, data = json.dumps(requestData))

	if dResp.status_code != 200:
		print("Unable to get data. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("Resource access successful")
		# Display data
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))
