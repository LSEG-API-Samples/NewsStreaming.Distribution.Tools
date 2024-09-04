#=============================================================================
# Refinitiv Data Platform demo app to subscribe to Research messages
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import requests
import json
import rdpToken
import sqsQueue
import atexit
import sys
import boto3
import os
from botocore.exceptions import ClientError

# Application Constants
base_URL = "https://api.refinitiv.com"
RDP_version = "/v1"
REPORTS_DIR_NAME = "reports"
currentSubscriptionID = None

#==============================================
def subscribeToResearch():
#==============================================
	# get the latest access token first
	accessToken = rdpToken.getToken()

	category_URL = "/message-services"
	endpoint_URL = "/research/subscriptions"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	requestData = {
		"transport": {
			"transportType": "AWS-SQS"
		},
		"userID": rdpToken.UUID
	}

	hdrs = {
		"Authorization": "Bearer " + accessToken,
		"Content-Type": "application/json"
	}

	dResp = requests.post(RESOURCE_ENDPOINT, headers = hdrs, data = json.dumps(requestData))
	if dResp.status_code != 200:
		raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["transportInfo"]["endpoint"], jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"]


#==============================================
def getCloudCredentials(endpoint):
#==============================================
	category_URL = "/auth/cloud-credentials"
	endpoint_URL = "/"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	requestData = {
		"endpoint": endpoint
	}

	# get the latest access token
	accessToken = rdpToken.getToken()
	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestData)
	if dResp.status_code != 200:
		raise ValueError("Unable to get credentials. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"]["sessionToken"]


#==============================================
def downloadReport(rMessage):
#==============================================
	print('--------- Received research message -----------')
	#print(json.dumps(rMessage, indent=2))
	pl = rMessage['payload']
	fileType = pl['DocumentFileType']
	docID = pl['DocumentId']
	filename = pl['DocumentFileName']

	print('Headline: %s' % pl['Headline']['DocumentHeadlineValue'])
	print('Document name: %s of type: %s, size: %s' % (filename, fileType, pl['DocumentFileSize']))

	if fileType == 'pdf':
		print('Downloading the file: %s' % filename)
		category_URL = "/data/research"
		endpoint_URL = "/documents/"
		requestData = {
			"uuid": rdpToken.UUID,
			"doNotRedirect": False
		}
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL + str(docID) + "/" + fileType

		# get the latest access token
		accessToken = rdpToken.getToken()
		dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestData)
		if dResp.status_code != 200:
			print("Error - Unable to get the research report. Code %s, Message: %s" % (dResp.status_code, dResp.text))
		else:
			with open(REPORTS_DIR_NAME + "/" + filename, 'wb') as f:
				f.write(dResp.content)
				f.close()

	elif (fileType == 'txt') or (fileType == 'htm'):
		print('Downloading the file: %s' % filename)
		category_URL = "/data/research"
		endpoint_URL = "/documents/"
		requestData = {
			"uuid": rdpToken.UUID
		}
		RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL + str(docID) + "/text"

		# get the latest access token
		accessToken = rdpToken.getToken()
		dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = requestData)
		if dResp.status_code != 200:
			print("Error - Unable to get the research report. Code %s, Message: %s" % (dResp.status_code, dResp.text))
		else:
			# parse the signed url response from the JSON message
			pl = json.loads(dResp.text)
			signedURL = pl['signedUrl']
			print("Downloading text file from: %s" % signedURL)
			dResp2 = requests.get(signedURL)
			if dResp2.status_code != 200:
				print("Error - Unable to get the research report. Code %s, Message: %s" % (dResp2.status_code, dResp2.text))
			else:
				with open(REPORTS_DIR_NAME + "/" + filename, 'wb') as f:
					f.write(dResp2.content)
					f.close()

	elif (fileType == 'URL'):
		print('Saving the link to URL: %s' % filename)
		with open(REPORTS_DIR_NAME + "/" + str(docID) + ".link", 'wb') as f:
			f.write(filename.encode())
			f.close()

	else:
		print(json.dumps(rMessage, indent=2))


#==============================================
def startResearchAlerts(downloadReports):
#==============================================
	global currentSubscriptionID
	try:
		print("Subscribing to research stream ...")
		endpoint, cryptographyKey, currentSubscriptionID = subscribeToResearch()
		print("  Queue endpoint: %s" % (endpoint) )
		print("  Subscription ID: %s" % (currentSubscriptionID) )

		# unsubscribe before shutting down
		atexit.register(removeSubscription)

		while 1:
			try:
				print("Getting credentials to connect to AWS Queue...")
				accessID, secretKey, sessionToken = getCloudCredentials(endpoint)
				print("Queue access ID: %s" % (accessID) )
				print("Getting research, press BREAK to exit and delete subscription...")
				if downloadReports:
					sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey, downloadReport)
				else:
					sqsQueue.startPolling(accessID, secretKey, sessionToken, endpoint, cryptographyKey)

			except ClientError as e:
				print("Cloud credentials exprired!")
	except KeyboardInterrupt:
		print("User requested break, cleaning up...")
		sys.exit(0)


#==============================================
def removeSubscription():
#==============================================
	category_URL = "/message-services"
	endpoint_URL = "/research/subscriptions"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	# get the latest access token
	accessToken = rdpToken.getToken()

	if currentSubscriptionID:
		print("Deleting the open research subscription")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = {"subscriptionID": currentSubscriptionID, "userID": rdpToken.UUID})
	else:
		print("Deleting ALL open research subscriptions")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken}, params = {"userID": rdpToken.UUID})

	if dResp.status_code > 299:
		print(dResp)
		print("Warning: unable to remove subscription. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print("Research unsubscribed!")


#==============================================
def showActiveSubscriptions():
#==============================================
	category_URL = "/message-services"
	endpoint_URL = "/research/subscriptions"
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + endpoint_URL
	# get the latest access token
	accessToken = rdpToken.getToken()

	print("Getting all open research subscriptions")
	dResp = requests.get(RESOURCE_ENDPOINT, headers = {"Authorization": "Bearer " + accessToken})

	if dResp.status_code != 200:
		raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))



#==============================================
if __name__ == "__main__":
#==============================================
	if len(sys.argv) > 1:
		if sys.argv[1] == '-l':
			showActiveSubscriptions()
		elif sys.argv[1] == '-d':
			removeSubscription()
		elif sys.argv[1] == '-s':
			startResearchAlerts(False)
		elif sys.argv[1] == '-S':
			try:
				os.mkdir(REPORTS_DIR_NAME)
			except:
				pass
			startResearchAlerts(True)
	else:
		print("Arguments:")
		print("  -l List active subscriptions")
		print("  -d Delete all subscriptions")
		print("  -s Subscribe to research")
		print("  -S Subscribe to research and download into %s/ directory" % REPORTS_DIR_NAME)

