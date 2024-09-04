#=============================================================================
# Refinitiv Data Platform demo app to view environmental social governance data
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import requests
import json
import rdpToken
import sys

# Application Constants
RDP_version = "/v1"
base_URL = "https://api.refinitiv.com"
category_URL = "/data/environmental-social-governance"

basicView = "/views/basic"
standardMeasureView = "/views/measures-standard"
fullMeasureView = "/views/measures-full"
standardScoreView = "/views/scores-standard"
fullScoreView = "/views/scores-full"

universe_parameter_URL = "?universe="


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
	RIC="IBM.N"
	viewURL = basicView
	
	if len(sys.argv) > 1:
		if sys.argv[1] == '-1':
			viewURL = basicView
		elif sys.argv[1] == '-2':
			viewURL = standardMeasureView
		elif sys.argv[1] == '-3':
			viewURL = fullMeasureView
		elif sys.argv[1] == '-4':
			viewURL = standardScoreView
		elif sys.argv[1] == '-5':
			viewURL = fullScoreView
	else:
		print("Optional arguments:")
		print("  -1 Basic View <default>")
		print("  -2 Standard Measures")
		print("  -3 Full Measures")
		print("  -4 Standard Scores")
		print("  -5 Full Scores")
	
	# Get latest access token
	print("Getting OAuth access token...")
	accessToken = rdpToken.getToken()
	print("Invoking data request for: " + RIC)
	# Make data request
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + viewURL + universe_parameter_URL + RIC

	# optional parameters to be sent with request
	requestData = {
		#"start": -20,
		#"end": "0",
		#"format": "noMessages"
	}

	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestData)

	if dResp.status_code != 200:
		print("Unable to get data. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("Resource access successful")
		# Display data
		jResp = json.loads(dResp.text)
		prettyPrintData(jResp)
