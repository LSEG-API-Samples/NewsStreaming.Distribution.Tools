#=============================================================================
# Refinitiv Data Platform demo app to get pricing analytics for an instrument
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
category_URL = "/data/quantitative-analytics"
endpoint_URL = "/financial-contracts"


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
	# Get latest access token
	print("Getting OAuth access token...")
	accessToken = rdpToken.getToken()
	print("Invoking the analytics request")
	# Make data request
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL

	# optional parameters to be sent with request
	requestData = {
		"fields": ["InstrumentTag", "StartDate", "EndDate", "FxSpot", "FxSwapsCcy1Ccy2", "FxOutrightCcy1Ccy2"],
		"outputs": ["Data", "Headers"],
		"universe": [{
				"instrumentType": "FxCross",
				"instrumentDefinition": {
					"instrumentTag": "FX_deal_001",
					"fxCrossType": "FxForward",
					"fxCrossCode": "EURGBP",
					"legs": [{
							"tenor": "3M10D"
						}
					]
				},
				"pricingParameters": {
					"valuationDate": "2019-02-02T00:00:00Z",
					"priceSide": "Mid"
				}
			}
		]
	}
	
	hdrs = {
		"Authorization": "Bearer " + accessToken,
		"Content-Type": "application/json"
	}

	dResp = requests.post(RESOURCE_ENDPOINT, headers = hdrs, data = json.dumps(requestData))

	if dResp.status_code != 200:
		print("Unable to get data. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("Resource access successful")
		# Display data
		jResp = json.loads(dResp.text)
		prettyPrintData(jResp)
