#=============================================================================
# Refinitiv Data Platform demo app to get timeSeries data
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
category_URL = "/data/historical-pricing"
endpoint_URL = "/views/events"
universe_parameter_URL = "/"

#==============================================
def prettyPrintData(vData):
#==============================================
	#print(json.dumps(vData, indent=2))

	line = ""
	for i in vData["headers"]:
		line = line + i["name"] + ", "
	line = line [:-2]
	print(line)
	
	print("---------------")

	for d in vData["data"]:
		line = ""
		for pt in d:
			line = line + str(pt) + ", "
		line = line [:-2]
		print(line)


#==============================================
if __name__ == "__main__":
#==============================================
	print("Intraday quote request")
	
	# Get latest access token
	accessToken = rdpToken.getToken()

	# Make data request
	RIC="IBM.N"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL + universe_parameter_URL + RIC

	#requestData = {
	#	"eventTypes": "trade,quote,correction",
	#	"start": "2021-05-11T18:30:12.000000000Z",
	#	"end": "2021-05-11T18:30:13.000000000Z"
	#	"adjustments": "exchangeCorrection,manualCorrection",
	#	"maxpoints": 30
	#}

	requestData = {
		"eventTypes": "trade,quote",
		"start": "2021-05-11T18:30:12.000000000Z",
		"count": "20"
	}

	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestData)

	if dResp.status_code != 200:
		print("Unable to get data. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("Resource access successful")
		# Display data
		jResp = json.loads(dResp.text)
		prettyPrintData(jResp[0])
