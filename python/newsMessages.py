#=============================================================================
# Refinitiv Data Platform demo app to subscribe to NEWS messages
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================
import os
import time
import traceback
from urllib.parse import urlparse

import boto3
import requests
import json
import rdpToken
import sqsQueue
import atexit
import sys
from botocore.exceptions import ClientError
import argparse

REGION = 'us-east-1'
# Application Constants
base_URL = "https://api.refinitiv.com"
RDP_version = "/v1"
category_URL = "/message-services"
endpoint_URL_headlines = "/news-headlines/subscriptions"
endpoint_URL_stories = "/news-stories/subscriptions"
supported_api_content_type = ["news-stories", "news-headlines"]
currentSubscriptionID = None
gHeadlines = True
api_content_type = "news-headlines"
news_endpoint = endpoint_URL_headlines
demo_subscription_id = None

#==============================================
def subscribe_to_news():
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + news_endpoint

	requestData = {
		"transport": {
			"transportType": "AWS-SQS"
		},
		"payloadVersion": "2.0"
	}

	# Optional filters can be applied to subscription. For e.g. following gets the english language top news only
	'''
	requestData = {
		"transport": {
			"transportType": "AWS-SQS"
		},
		"filter": {
			"query": {
				"type": "operator",
				"operator": "and",
				"operands": [{
						"type": "freetext",
						"match": "contains",
						"value": "TOP NEWS"
					}, {
						"type": "language",
						"value": "L:en"
					}
				]
			}
		},
		"payloadVersion": "2.0"
	}
	'''

	# get the latest access token
	accessToken = rdpToken.getToken()
	hdrs = {
		"Authorization": "Bearer " + accessToken,
		"Content-Type": "application/json"
	}

	dResp = requests.post(RESOURCE_ENDPOINT, headers=hdrs, data=json.dumps(requestData))
	if dResp.status_code != 200:
		raise ValueError("Unable to subscribe. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["transportInfo"]["endpoint"], jResp["transportInfo"]["cryptographyKey"], jResp["subscriptionID"]


#==============================================
def get_cloud_credentials(queue_endpoint):
#==============================================
	CC_category_URL = "/auth/cloud-credentials"
	CC_endpoint_URL = "/"
	RESOURCE_ENDPOINT = base_URL + CC_category_URL + RDP_version + CC_endpoint_URL
	request_data = {
		"endpoint": queue_endpoint
	}

	print(f"start getting cloud credentials from resource endpoint {RESOURCE_ENDPOINT}, queueEndpoint {queue_endpoint}")
	# get the latest access token
	accessToken = rdpToken.getToken()
	dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken}, params=request_data)
	if dResp.status_code != 200:
		raise ValueError("Unable to get credentials. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		return jResp["credentials"]["accessKeyId"], jResp["credentials"]["secretKey"], jResp["credentials"]["sessionToken"]


#==============================================
def start_news_messages(is_poll=False, is_demo=False):
#==============================================
	try:
		print(f"Subscribing to {api_content_type} messages...")

		queue_endpoint, cryptography_key, current_subscription_id = subscribe_to_news()
		print(f"Subscription ID: {current_subscription_id}")
		print(f"Queue endpoint: {queue_endpoint}")
		print(f"CryptographyKey: {cryptography_key}")

		if not is_poll:
			return

		if is_demo:
			global demo_subscription_id
			demo_subscription_id = current_subscription_id
			# unsubscribe before shutting down
			atexit.register(remove_subscription)

		polling_queue(queue_endpoint, cryptography_key, is_demo=False)

	except KeyboardInterrupt:
		print("User requested break, cleaning up...")
		sys.exit(0)


def extract_subscription_data(subscription_response):
	queue_endpoint = subscription_response["transportInfo"]["endpoint"]
	cryptography_key = subscription_response["transportInfo"]["cryptographyKey"]
	subscription_id = subscription_response["subscriptionID"]
	return queue_endpoint, cryptography_key, subscription_id


def get_subscription_info(subscription_id):
	subscription_responses = show_active_subscriptions(subscription_id=subscription_id)
	if subscription_responses is None or len(subscription_responses) == 0:
		return None, None, None
	else:
		return extract_subscription_data(subscription_responses[0])


#==============================================
def poll_news_messages(subscription_id=None):
#==============================================
	try:
		prefix = f"{'with subscriptionId {} '.format(subscription_id) if subscription_id is not None else ''}"
		print(f"polling {prefix}to {api_content_type} messages...")

		queue_endpoint, cryptography_key, current_subscription_id = get_subscription_info(subscription_id)
		if subscription_id is not None and current_subscription_id is None:
			raise Exception(f"SubscriptionId {subscription_id} for {api_content_type} does not exist")

		if current_subscription_id is None:
			print(f"No subscription to poll messages then start subscribing to {api_content_type} before polling...")
			queue_endpoint, cryptography_key, current_subscription_id = subscribe_to_news()

		print(f"Queue endpoint: {queue_endpoint}")
		print(f"Subscription ID: {current_subscription_id}")

		polling_queue(queue_endpoint, cryptography_key, is_demo=False)

	except KeyboardInterrupt:
		print("User requested break, cleaning up...")
		sys.exit(0)


def polling_queue(queue_endpoint, cryptography_key, is_demo=False):
	while 1:
		try:
			print("Getting credentials to connect to AWS Queue...")
			access_id, secret_key, session_token = get_cloud_credentials(queue_endpoint)
			print(f"Queue access ID: {access_id}")
			print(f"Getting {api_content_type}, press BREAK to exit {'and delete subscription' if is_demo else ''}...")
			sqsQueue.start_polling(access_id, secret_key, session_token, queue_endpoint, cryptography_key, callback=process_news_messages)
		except ClientError as e:
			print(f"Cloud credentials expired!, reason {e}")


def process_news_messages(pl):
	print(json.dumps(pl))
	if "href" in pl:
		try:
			file_stream_url = pl['href']
			subscription_id = pl["subscriptionID"]
			downloaded_result = download_messages_from_cfs(file_stream_url, subscription_id)
			if downloaded_result:
				json_data = json.loads(downloaded_result)
				print(json.dumps(json_data))
		except Exception as ex:
			traceback.print_exc()
			print(f"Could not process news large messages, reason {ex}, pl {pl}")


def download_messages_from_cfs(file_stream_url, subscription_id, save_to_file=False):
	requestData = {
		"doNotRedirect": True
	}
	# get the latest access token
	accessToken = rdpToken.getToken()
	resp = requests.get(file_stream_url, headers={"Authorization": "Bearer " + accessToken}, params=requestData, stream=True)
	if resp.ok:
		jResp = json.loads(resp.text)
		url = jResp["url"]
		a = urlparse(url)
		s3_path = a.path  # s3 file path
		s3_file_name = os.path.basename(s3_path)
		output_file_path = f"outputs/{subscription_id}_large_msg_{s3_file_name}"
		download_file_result_msg = f"Start downloading file {s3_file_name} and save to {output_file_path}"
		generate_asterisks = generate_asterisk_upper_lower(download_file_result_msg)

		print(generate_asterisks)
		print(download_file_result_msg)
		print(generate_asterisks)
		start_time_sec = time.time()
		fileResp = requests.get(url, stream=True)
		if fileResp.ok:
			if save_to_file:
				with open(output_file_path, 'wb') as f:
					for chunk in fileResp.iter_content(chunk_size=1024 * 8):
						if chunk:
							f.write(chunk)
							f.flush()
							os.fsync(f.fileno())
				with open(output_file_path, "r", encoding='utf-8') as f:
					output_data = f.read()
			else:
				chunks = []
				for chunk in fileResp.iter_content(chunk_size=4096):
					chunks.append(chunk)

				output_data = b''.join(chunks)
			print(f"Successfully download file {s3_file_name} and save to {output_file_path}, responseTime {time.time() - start_time_sec} sec, subscriptionId {subscription_id}")

			return output_data
		else:  # HTTP status code 4XX/5XX
			print(f"Failed to download file {s3_file_name}, responseTime {time.time() - start_time_sec} sec, subscriptionId {subscription_id}")
			print(f"status code {fileResp.status_code}, response {fileResp.text}\n")

			error_message = f"subscriptionId {subscription_id}, Failed to download s3_filename {s3_file_name}, file_stream_url {file_stream_url}, url {url}, status code {fileResp.status_code}\n{fileResp.text}"
			print(error_message)
			return None
	else:
		print(f"Failed to get s3 presigned url file_stream_url {file_stream_url}, subscriptionId {subscription_id}")
		print(f"status code {resp.status_code}, response {resp.text}\n")

		error_message = f"subscriptionId {subscription_id}, Failed to get s3 presigned url to download file_stream {file_stream_url}, status code {resp.status_code}\n{resp.text}"
		print(error_message)

	return None


def generate_asterisk_upper_lower(input):
	generate_asterisks = ""
	for idx in range(0, len(input)):
		generate_asterisks = generate_asterisks + "*"
	return generate_asterisks


def get_queue_attributes(subscription_id):
	try:
		subscription_responses = show_active_subscriptions(subscription_id=subscription_id)
		if subscription_responses is None or len(subscription_responses) == 0:
			raise Exception(f"No {api_content_type} subscriptions")

		result = []

		for subscription_response in subscription_responses:
			current_subscription_id = None
			queue_endpoint = None
			try:
				queue_endpoint, cryptography_key, current_subscription_id = extract_subscription_data(subscription_response)
				if current_subscription_id is None:
					raise Exception(f"subscriptionID {current_subscription_id} is not found")

				print("\n")
				print("*************************** Getting SQS Queue Attributes ***********************")
				print(f"subscriptionID: {subscription_id}, Endpoint: {queue_endpoint}, CryptographyKey: {cryptography_key}")
				print("********************************************************************************")

				access_id, secret_key, session_token = get_cloud_credentials(queue_endpoint)
				session = boto3.Session(
				    aws_access_key_id=access_id,
				    aws_secret_access_key=secret_key,
				    aws_session_token=session_token,
				    region_name=REGION
				)

				sqs = session.client('sqs')
				resp = sqs.get_queue_attributes(QueueUrl=queue_endpoint, AttributeNames=['ApproximateNumberOfMessages'])
				approx_num_msg_in_queue = resp['Attributes']['ApproximateNumberOfMessages']

				print("-------------------- Successfully get sqs queue attributes ----------------------")
				print(f"ApproximateNumberOfMessages: {approx_num_msg_in_queue}, statusCode: {resp['ResponseMetadata']['HTTPStatusCode']}")
				print(f"response: {resp}\n")

				result.append({
					"queueEndpoint": queue_endpoint,
					"approximateNumberOfMessages": approx_num_msg_in_queue
				})
			except Exception as e:
				traceback.print_exc()
				error_message = f"Could not get queue attributes from subscriptionId {current_subscription_id}, queueEndpoint {queue_endpoint} reason: {e}"
				print(error_message)

		return result
	except Exception as ex:
		traceback.print_exc()
		suffix = f" from subscriptionId {subscription_id}" if subscription_id is not None else ''
		error_message = f"Could not get queue attributes{suffix}, reason: {ex}"
		raise Exception(error_message)


#==============================================
def remove_subscription(subscription_id=None):
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + news_endpoint

	# get the latest access token
	accessToken = rdpToken.getToken()

	if demo_subscription_id is not None:
		subscription_id = demo_subscription_id

	if subscription_id:
		print(f"Deleting {api_content_type} subscriptionId {subscription_id}")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken}, params={"subscriptionID": subscription_id})
	else:
		print(f"Deleting all {api_content_type} subscription")
		dResp = requests.delete(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

	if dResp.status_code > 299:
		print(dResp)
		print("Warning: unable to remove subscription. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		print(f"{api_content_type} unsubscribed!")


#==============================================
def show_active_subscriptions(subscription_id=None):
#==============================================
	RESOURCE_ENDPOINT = base_URL + category_URL + RDP_version + news_endpoint

	# get the latest access token
	accessToken = rdpToken.getToken()

	if subscription_id is None:
		print(f"Getting all {api_content_type} subscriptions, endpoint {RESOURCE_ENDPOINT}")
	else:
		RESOURCE_ENDPOINT = RESOURCE_ENDPOINT + "?subscriptionID={}".format(subscription_id)
		print(f"Getting {api_content_type} subscription from subscriptionId {subscription_id}, endpoint {RESOURCE_ENDPOINT}")

	dResp = requests.get(RESOURCE_ENDPOINT, headers={"Authorization": "Bearer " + accessToken})

	if dResp.status_code != 200:
		raise ValueError("Unable to get subscriptions. Code %s, Message: %s" % (dResp.status_code, dResp.text))
	else:
		jResp = json.loads(dResp.text)
		print(json.dumps(jResp, indent=2))

	return jResp['subscriptions'] if 'subscriptions' in jResp else None


def load_current_user():
	user_object = None
	try:
		'''
        {
            "username": "GE-XXX"
        }
        '''
		# read the token from a file
		with open("current_user.json", "r+") as tf:
			user_object = json.load(tf)["username"]
			print("Successfully get current user: {}".format(user_object))
	except Exception:
		pass

	return user_object


#==============================================
if __name__ == "__main__":
#==============================================

	description = """RDP News Streaming Tool description
		1) Get all subscriptions 
		   1.1) news-stories
		        - python newsMessages.py -ct news-stories -g
		   1.2) news-headlines
		        - python newsMessages.py -ct news-headlines -g
		 
		2) Get specific subscriptions
		   2.1) news-stories
		        - python newsMessages.py -ct news-stories -g -s <subscriptionId>
		   2.2) news-headlines
		        - python newsMessages.py -ct news-headlines -g -s <subscriptionId>

		3) create a new subscription only
		   3.1) news-stories
		        - python newsMessages.py -ct news-stories -c
		   3.2) news-headlines
		        - python newsMessages.py -ct news-headlines -c 
		
		4) create a new subscription and then keep polling queue
		   4.1) news-stories
		        - python newsMessages.py -ct news-stories -c -p
		   4.2) news-headlines
		        - python newsMessages.py -ct news-headlines -c -p
		        
		5) [Demo Mode] create a new subscription and then keep polling queue but delete subscription after exit application
		   5.1) news-stories
		        - python newsMessages.py -ct news-stories -c -p -d
		   5.2) news-headlines
		        - python newsMessages.py -ct news-headlines -c -p -d

		6) poll message queue from existing subscription
		   6.1) news-stories
		        - python newsMessages.py -ct news-stories -p -s <subscriptionId>
		   6.2) news-headlines
		        - python newsMessages.py -ct news-headlines -p -s <subscriptionId>

		7) poll message queue by using default subscription and create new subscription if it does not exist
		   7.1) news-stories
		        - python newsMessages.py -ct news-stories -p
		   7.2) news-headlines
		        - python newsMessages.py -ct news-headlines -p

		8) Delete all subscriptions
		   8.1) news-stories
		        - python newsMessages.py -ct news-stories -r
		   8.2) news-headlines
		        - python newsMessages.py -ct news-headlines -r
		   
		9) Delete specific subscription
		   9.1) news-stories
		        - python newsMessages.py -ct news-stories -r -s <subscriptionId>
		   9.2) news-headlines
		        - python newsMessages.py -ct news-headlines -r -s <subscriptionId>
		        
		10) Get all subscription queue statistics
		   10.1) news-stories
		         - python newsMessages.py -ct news-stories -q
		   10.2) news-headlines
		         - python newsMessages.py -ct news-headlines -q
		
		11) Get specific subscription queue statistics
		   11.1) news-stories
		         - python newsMessages.py -ct news-stories -q -s <subscriptionId>
		   11.2) news-headlines
		         - python newsMessages.py -ct news-headlines -q -s <subscriptionId>

		"""
	# Initialize parser
	parser = argparse.ArgumentParser(description=description, formatter_class=argparse.RawTextHelpFormatter)

	# Adding optional argument
	parser.add_argument("-ct", "--contentType", help="Data contentType ex: news-stores, news-headlines", required=True)
	parser.add_argument("-g", "--get", action="store_true", help="get all of subscriptions information")
	parser.add_argument("-s", "--subId", help="specify subscriptionId")
	parser.add_argument("-c", "--create", action="store_true", help="create a new subscription")
	parser.add_argument("-d", "--demo", action="store_true", help="create a new subscription with demo mode (unsub subscription after exiting program")
	parser.add_argument("-p", "--poll", action="store_true", help="resume polling message queue from existing subscription")
	parser.add_argument("-r", "--remove", action="store_true", help="remove subscriptions")
	parser.add_argument("-q", "--queue", action="store_true", help="queue subscriptions statistics")

	# Read arguments from command line
	args = parser.parse_args()

	args_dict = vars(parser.parse_args())
	print(args_dict)

	api_content_type = args.contentType

	if api_content_type not in supported_api_content_type:
		raise Exception(f"-ct or --contentType parameter is invalid {api_content_type}, supported values {supported_api_content_type}")

	gHeadlines = True if "news-headlines" == api_content_type else False
	news_endpoint = endpoint_URL_headlines if gHeadlines else endpoint_URL_stories

	try:
		os.mkdir("outputs")
	except:
		pass

	try:
		print("Program is started")
		username = rdpToken._loadCredentialsFromFile()
		user_results = load_current_user()
		if user_results is None or str(user_results).strip() == '' or str(user_results) != username:
			if os.path.exists("token.txt"):
				print("Remove token.txt because user_results {} match with criteria compare with {}".format(user_results, username))
				os.remove("token.txt")
	except Exception as err:
		traceback.print_exc()
		print(f"Could not load username from current_user.json, reason {err}")

	if args.get:
		if args.subId:
			show_active_subscriptions(subscription_id=args.subId)
		else:
			show_active_subscriptions()
	elif args.create or (args.create and args.poll) or (args.create and args.demo):
		if args.poll:
			is_demo_mode = True if args.demo else False
			start_news_messages(is_poll=True, is_demo=is_demo_mode)
		else:
			start_news_messages()
	elif args.poll:
		poll_news_messages(args.subId)
	elif args.remove:
		remove_subscription(args.subId)
	elif args.queue:
		get_queue_attributes(args.subId)
	else:
		raise Exception(f"Command is invalid, please run python newsMessages.py -h")


