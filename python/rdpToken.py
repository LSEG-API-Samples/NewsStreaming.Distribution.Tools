#=============================================================================
# Refinitiv Data Platform demo app to get OAuth access tokens
# Following sequence is followed:
#	1. Read access token from the already created token file, if the token is not expired
#	2. Try using refresh token, if using v1 authentication and the token is expired
#	3. Try to use password grant or Client Credentials, if refresh token fails or no token file exists
#	For password grant or Client Credentials:
#		a. Use credentials file, if available
#		b. Use the hardcoded USERNAME, PASSWORD parameters from this module
#
# APP_KEY see instructions at https://developers.refinitiv.com/en/api-catalog/refinitiv-data-platform/refinitiv-data-platform-apis/quick-start
# UUID is required for research messages only and will be provided by Refinitiv
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import requests, json, time, getopt, sys, configparser


# fill in the parameters in the credentials file or hardcode them here
AUTHENTICATION_TYPE = "---CHOOSE EITHER v1 OR v2 DEPENDING ON YOUR ACCOUNT TYPE---"
# Password Grant Credentials (old type)
USERNAME  = "---YOUR PROVIDED RDP MACHINE ID---"
PASSWORD  = "---YOUR PASSWORD---"
APP_KEY = "---YOUR GENERATED APP KEY---"
# Client Credentials (new type)
CLIENT_ID = "---YOUR SELF GENERATED CLIENT ID---"
CLIENT_SECRET = "---YOUR SELF GENERATED CLIENT SECRET---"
# UUID is provided by Refinitiv for Research API only
UUID 	  = "---YOUR PROVIDED UUID ID---"


# Application Constants
base_URL = "https://api.refinitiv.com"
category_URL = "/auth/oauth2"
endpoint_URL = "/token"
SCOPE = "trapi"

CREDENTIALS_FILE = "credentials.ini"
TOKEN_FILE = "token.txt"


#==============================================
def _loadCredentialsFromFile():
#==============================================
	global AUTHENTICATION_TYPE, USERNAME, PASSWORD, APP_KEY, CLIENT_ID, CLIENT_SECRET, UUID
	try:
		config = configparser.ConfigParser()
		config.read(CREDENTIALS_FILE)
		
		AUTHENTICATION_TYPE = config['RDP']['authenticationType']

		USERNAME = config['RDP']['username']
		PASSWORD = config['RDP']['password']
		APP_KEY = config['RDP']['appKey']

		CLIENT_ID = config['RDP']['clientId']
		CLIENT_SECRET = config['RDP']['clientSecret']

		UUID = config['RDP']['uuid']

		print("Read credentials from file")
	except Exception:
		# ignore if no creds file
		pass

	return USERNAME


#==============================================
def _requestNewToken(refreshToken):
#==============================================
	# try to read user credentials from a file
	_loadCredentialsFromFile()
	TOKEN_ENDPOINT = base_URL + category_URL + "/" + AUTHENTICATION_TYPE + endpoint_URL
	
	if AUTHENTICATION_TYPE == "v1":
		print("Getting a new token using Password Grant...")
		return _requestNewToken_v1(refreshToken, TOKEN_ENDPOINT)
	else:
		print("Getting a new token using Client Credentials...")
		return _requestNewToken_v2(TOKEN_ENDPOINT)



#==============================================
def _requestNewToken_v1(refreshToken, TOKEN_ENDPOINT):
#==============================================
	if refreshToken is None:
		# formulate the request payload
		tData = {
			"username": USERNAME,
			"password": PASSWORD,
			"grant_type": "password",
			"scope": SCOPE,
			"takeExclusiveSignOnControl": "true"
		}
	else:
		tData = {
			"refresh_token": refreshToken,
			"grant_type": "refresh_token",
		}

	# Make a REST call to get latest access token
	response = requests.post(
		TOKEN_ENDPOINT,
		headers={
			"Accept": "application/json"
		},
		data=tData,
		auth=(
			APP_KEY,
			""
		)
	)

	if (response.status_code == 400) and ('invalid_grant' in response.text):
		print("Sleep 5 seconds, Failed to get access token {0} - {1}".format(response.status_code, response.text))
		time.sleep(5)
		return None
	
	if response.status_code != 200:
		print("Sleep 5 seconds, Failed to get access token {0} - {1}".format(response.status_code, response.text))
		time.sleep(5)
		raise Exception("Failed to get access token {0} - {1}".format(response.status_code, response.text))
	
	# return the new token
	return json.loads(response.text)


#==============================================
def _requestNewToken_v2(TOKEN_ENDPOINT):
#==============================================
	tData = {
		"client_id": CLIENT_ID,
		"client_secret": CLIENT_SECRET,
		"grant_type": "client_credentials",
		"scope": SCOPE
	}

	# Make a REST call to get latest access token
	response = requests.post(TOKEN_ENDPOINT, 
		headers = {'Accept' : 'application/json'}, 
		data = tData,
		verify = True,
		allow_redirects = False)
	
	if (response.status_code == 400) and ('invalid_grant' in response.text):
		print("Sleep 5 seconds, Failed to get access token {0} - {1}".format(response.status_code, response.text))
		time.sleep(5)
		return None
	
	if response.status_code != 200:
		print("Sleep 5 seconds, Failed to get access token {0} - {1}".format(response.status_code, response.text))
		time.sleep(5)
		raise Exception("Failed to get access token {0} - {1}".format(response.status_code, response.text))

	# return the new token
	return json.loads(response.text)


#==============================================
def _saveToken(tknObject):
#==============================================
	with open(TOKEN_FILE, "w+") as tf:
		print("Saving the new token")
		# append the expiry time to token
		tknObject["expiry_tm"] = time.time() + int(tknObject["expires_in"]) - 10
		# store it in the file
		json.dump(tknObject, tf, indent=4)

	username = _loadCredentialsFromFile()
	user_data = {
        "username": username
    }
	with open("current_user.json", "w+") as user_file:
		print("Saving the current user: {}".format(username))
		# store it in the file
		json.dump(user_data, user_file, indent=2)
		user_file.close()


#==============================================
def _loadToken():
#==============================================
	tknObject = None
	try:
		# read the token from a file
		tf = open(TOKEN_FILE, "r+")
		tknObject = json.load(tf)
		tf.close()
		print("Existing token read from: " + TOKEN_FILE)
	except Exception:
		pass

	return tknObject


#==============================================
def changePassword(user, oldPass, appKey, newPass):
#==============================================
	TOKEN_ENDPOINT = base_URL + category_URL + "/v1" + endpoint_URL
	
	tData = {
		"username": user,
		"password": oldPass,
		"grant_type": "password",
		"scope": SCOPE,
		"takeExclusiveSignOnControl": "true",
		"newPassword": newPass
	}

	# make a REST call to get latest access token
	response = requests.post(
		TOKEN_ENDPOINT,
		headers={
			"Accept": "application/json"
		},
		data=tData,
		auth=(
			appKey,
			""
		)
	)

	if response.status_code != 200:
		raise Exception("Failed to change password {0} - {1}".format(response.status_code, response.text))

	tknObject = json.loads(response.text)
	# persist this token for future queries
	_saveToken(tknObject)
	# return access token
	return tknObject["access_token"]


#==============================================
def getToken():
#==============================================
	tknObject = _loadToken()

	if tknObject is not None:
		# is access token valid
		if tknObject["expiry_tm"] > time.time():
			# return access token
			return tknObject["access_token"]

		print("Token expired, refreshing a new one...")
		
		# get a new token using refresh token
		tknObject = _requestNewToken(tknObject["refresh_token"])
		# if refresh grant failed
		if tknObject is None:
			print("Refresh token expired, using Password Grant...")
			# use password grant
			tknObject = _requestNewToken(None)
	else:
		tknObject = _requestNewToken(None)

	# persist this token for future queries
	_saveToken(tknObject)
	# return access token
	return tknObject["access_token"]


#==============================================
if __name__ == "__main__":	
#==============================================
	print("Getting OAuth access token...")
	accessToken = getToken()
	print("Received an access token")

