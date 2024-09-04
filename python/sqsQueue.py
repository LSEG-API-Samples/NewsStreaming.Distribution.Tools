#=============================================================================
# Module for polling and extracting news or research alerts from an AWS SQS Queue
# This module uses boto3 library from Anazon for fetching messages
# It uses pycryptodome - for AES GCM decryption
#-----------------------------------------------------------------------------
#   This source code is provided under the Apache 2.0 license
#   and is provided AS IS with no warranty or guarantee of fit for purpose.
#   Copyright (C) 2021 Refinitiv. All rights reserved.
#=============================================================================

import boto3
import json
import base64
from Crypto.Cipher import AES
import html

REGION = 'us-east-1'

#==============================================
def decrypt(key, source):
#==============================================
	GCM_AAD_LENGTH = 16
	GCM_TAG_LENGTH = 16
	GCM_NONCE_LENGTH = 12
	key = base64.b64decode(key)
	cipherText = base64.b64decode(source)
	
	aad = cipherText[:GCM_AAD_LENGTH]
	nonce = aad[-GCM_NONCE_LENGTH:] 
	tag = cipherText[-GCM_TAG_LENGTH:]
	encMessage = cipherText[GCM_AAD_LENGTH:-GCM_TAG_LENGTH]
	
	cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
	cipher.update(aad)
	decMessage = cipher.decrypt_and_verify(encMessage, tag)
	return decMessage


#==============================================
def process_payload(payloadText, callback):
#==============================================
	pl = json.loads(payloadText)
	# handover the decoded message to calling module
	if callback is not None:
		callback(pl)
	else:
		print(json.dumps(pl))


#==============================================
def start_polling(access_id, secret_key, session_token, queue_endpoint, cryptography_key, callback=None):
#==============================================
	# create a SQS session
	session = boto3.Session(
		aws_access_key_id=access_id,
		aws_secret_access_key=secret_key,
		aws_session_token=session_token,
		region_name=REGION
	)

	sqs = session.client('sqs')

	print(f'Polling messages from queue {queue_endpoint}...')
	while 1: 
		resp = sqs.receive_message(QueueUrl=queue_endpoint, WaitTimeSeconds=20, MaxNumberOfMessages=10, AttributeNames=["All"])
		if 'Messages' in resp:
			messages = resp['Messages']
			# print and remove all the nested messages
			for message in messages:
				sqs_message_id = message['MessageId'] if 'MessageId' in message else None
				sqs_metadata = message['Attributes'] if 'Attributes' in message else None
				print(f"sqs_message_id {sqs_message_id}, sqs_metadata {sqs_metadata}")
				body = message['Body']
				# decrypt this message
				m = decrypt(cryptography_key, body)
				process_payload(m, callback)
				# *** accumulate and remove all the nested message
				sqs.delete_message(QueueUrl=queue_endpoint, ReceiptHandle=message['ReceiptHandle'])


#==============================================
if __name__ == "__main__":
#==============================================
	print("SQS module cannot run standalone. Please use newsAlerts or researchAlerts")
