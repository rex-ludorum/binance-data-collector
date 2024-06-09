import asyncio
import websockets
import json
import requests
import boto3
import time
import traceback
import os
import dateutil.parser
import datetime
import argparse
from coinbase import jwt_generator
from functools import cmp_to_key

DATABASE_NAME = "coinbase-websocket-data"

COINBASE_WEBSOCKET_ARN = "arn:aws:sns:us-east-2:471112880949:coinbase-websocket-notifications"
ACCESS_KEY = "AKIAW3MEECM242BBX6NJ"
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

COINBASE_API_KEY_NAME = os.environ.get("COINBASE_API_KEY_NAME")
COINBASE_API_PRIVATE_KEY = os.environ.get("COINBASE_API_PRIVATE_KEY")

REGION_NAME = "us-east-2"

# Empirically determined number of records over which the write size will exceed 1 kB
NUM_RECORDS = 30

MAX_REST_API_TRADES = 1000

def publishAndPrintError(mysns, error, subject, symbol):
	errorMessage = repr(error) + " encountered for " + symbol + " at " + str(time.strftime("%H:%M:%S", time.localtime()))
	print(errorMessage)
	try:
		mysns.publish(
			TopicArn = COINBASE_WEBSOCKET_ARN,
			Message = errorMessage,
			Subject = subject + " Exception",
		)
	except Exception as e:
		print(repr(e), "encountered at", str(time.strftime("%H:%M:%S", time.localtime())), "during publishing")

def prepareRecord(response):
	# These two fields are only present in REST API responses
	if 'bid' in response:
		response.pop('bid')
	if 'ask' in response:
		response.pop('ask')

	makerSide = response['side']
	if (makerSide != 'BUY' and makerSide != 'SELL'):
		print("Unknown maker side for " + json.dumps(response))
		return {}
	isBuyerMaker = makerSide == 'BUY'

	formattedDate = dateutil.parser.isoparse(response['time'])
	microseconds = int(datetime.datetime.timestamp(formattedDate) * 1000000)

	try:
		# Coinbase sometimes sends trades with non-integer IDs
		# Skip the trade if that's the case
		tradeId = int(response['trade_id'])
		record = {
			'Time': str(microseconds),
			'MeasureValues': [
				prepareMeasure('tradeId', tradeId, 'BIGINT'),
				prepareMeasure('price', response['price'], 'DOUBLE'),
				prepareMeasure('size', response['size'], 'DOUBLE'),
				prepareMeasure('isBuyerMaker', isBuyerMaker, 'BOOLEAN')
			]
		}
		return record
	except ValueError:
		print("Unknown trade_id for " + json.dumps(response))
		return {}

def prepareMeasure(name, value, measureType):
	measure = {
		'Name': name,
		'Value': str(value),
		'Type': measureType
	}
	return measure

# Obtain the range of aggregate IDs we are writing
def getTradeIds(records):
	tradeIds = []
	firstTradeMeasureValues = records[0]['MeasureValues']
	for measure in firstTradeMeasureValues:
		if measure['Name'] == 'tradeId':
			tradeIds.append(measure['Value'])
			break
	lastTradeMeasureValues = records[-1]['MeasureValues']
	for measure in lastTradeMeasureValues:
		if measure['Name'] == 'tradeId':
			tradeIds.append(measure['Value'])
			break
	return tradeIds

def writeRecords(symbol, writeClient, records, commonAttributes, mysns):
	try:
		tradeIds = getTradeIds(records)
		print("Writing %d %s records (%s - %s) at %s" % (len(records), symbol, tradeIds[0], tradeIds[1], str(datetime.datetime.now())))
		gap = int(tradeIds[1]) - int(tradeIds[0])
		if gap != 29:
			print("Only writing %d records" % (gap + 1))
		result = writeClient.write_records(DatabaseName=DATABASE_NAME, TableName=symbol, CommonAttributes=commonAttributes, Records=records)
		status = result['ResponseMetadata']['HTTPStatusCode']
		print("Processed %d %s records (%s - %s). WriteRecords HTTPStatusCode: %s" % (len(records), commonAttributes['Dimensions'][0]['Value'], tradeIds[0], tradeIds[1], status))
	except writeClient.exceptions.RejectedRecordsException as e:
		# print("RejectedRecords at", str(time.strftime("%H:%M:%S", time.localtime())), ":", e)
		publishAndPrintError(mysns, e, "RejectedRecords", symbol)
		for rr in e.response["RejectedRecords"]:
			print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
			print(json.dumps(records[rr['RecordIndex']], indent=2))
			if "ExistingVersion" in rr:
				print("Rejected record existing version: ", rr["ExistingVersion"])
	except Exception as e:
		publishAndPrintError(mysns, e, "Other WriteClient", symbol)

# Timestream does not allow two records with the same timestamp and dimensions to have different measure values
# Therefore, add one us to the later timestamp
def updateRecordTime(record, lastTrade, recordList, symbol):
	recordTime = record['Time']
	if lastTrade and int(record['Time']) <= int(lastTrade['Time']) + int(lastTrade['offset']):
		record['Time'] = str(int(lastTrade['Time']) + int(lastTrade['offset']) + 1)
		# print("Time %s for %s conflicts with last trade time (%s with offset %s, tradeId %s), updating to %s" % (recordTime, symbol, lastTrade['Time'], lastTrade['offset'], lastTrade['tradeId'], record['Time']))
		lastTrade['offset'] = str(int(lastTrade['offset']) + 1)
	else:
		lastTrade['Time'] = recordTime
		lastTrade['offset'] = str(0)
	lastTrade['tradeId'] = getTradeIds([record])[0]
	recordList.append(record)

# Check if we have reached the 1 kB write size and write the records
def checkWriteThreshold(symbol, writeClient, trades, commonAttributes, mysns):
	if len(trades) == NUM_RECORDS:
		# print(json.dumps(trades, indent=2))
		writeRecords(symbol, writeClient, trades, commonAttributes, mysns)
		trades.clear()

async def collectData(symbol):
	url = "wss://advanced-trade-ws.coinbase.com"
	headers = {"Sec-WebSocket-Extensions": "permessage-deflate"}
	trades = []
	lastTrade = {'Time': '0', 'offset': '0', 'tradeId': '0'}
	handleFirstGap = False

	commonAttributes = {
		'Dimensions': [
			{'Name': 'symbol', 'Value': symbol}
		],
		'MeasureName': 'price',
		'MeasureValueType': 'MULTI',
		'TimeUnit': 'MICROSECONDS'
	}

	writeClient = boto3.client('timestream-write', region_name=REGION_NAME, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

	mysns = boto3.client("sns", region_name=REGION_NAME, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

	async for websocket in websockets.connect(url, extra_headers=headers):
		jwtToken = jwt_generator.build_ws_jwt(COINBASE_API_KEY_NAME, COINBASE_API_PRIVATE_KEY)
		tradesRequest = {
			"type": "subscribe",
			"product_ids": [
				symbol
			],
			"channel": "market_trades",
			"jwt": jwtToken
		}
		heartbeatsRequest = {
			"type": "subscribe",
			"product_ids": [
				symbol
			],
			"channel": "heartbeats",
			"jwt": jwtToken
		}

		try:
			await websocket.send(json.dumps(heartbeatsRequest))
			await websocket.send(json.dumps(tradesRequest))
			while True:
				response = json.loads(await websocket.recv())
				if response['channel'] == 'market_trades' and response['events'][0]['type'] == 'update':
					responseTrades = response['events'][0]['trades']
					cleanTrades(responseTrades)
					for trade in responseTrades:
						record = prepareRecord(trade)
						if handleFirstGap or lastTrade['tradeId'] != '0':
							handleGap(trade, trades, lastTrade, writeClient, commonAttributes, mysns)
						updateRecordTime(record, lastTrade, trades, trade['product_id'])
						checkWriteThreshold(trade['product_id'], writeClient, trades, commonAttributes, mysns)
		except websockets.ConnectionClosedOK as e:
			traceback.print_exc()
			publishAndPrintError(mysns, e, "Websocket ConnectionClosedOK", symbol)
		except websockets.ConnectionClosedError as e:
			traceback.print_exc()
			publishAndPrintError(mysns, e, "Websocket ConnectionClosedError", symbol)
		except Exception as e:
			traceback.print_exc()
			print(trade)
			print(lastTrade)
			print(response)
			publishAndPrintError(mysns, e, "Other Websocket", symbol)
			break

# If we have to reconnect after a websocket exception, get any trades we might have missed
def handleGap(response, trades, lastTrade, writeClient, commonAttributes, mysns):
	if int(response['trade_id']) > int(lastTrade['tradeId']) + 1:
		endId = int(response['trade_id'])
		endDate = dateutil.parser.isoparse(response['time'])
		endTime = int(datetime.datetime.timestamp(endDate)) + 1
		startTime = int(lastTrade['Time']) // 1000000
		startMicros = str(int(lastTrade['Time']) % 1000000)
		startDate = datetime.datetime.fromtimestamp(startTime).strftime('%Y-%m-%dT%H:%M:%S') + '.' + startMicros.zfill(6)
		print("Gap found: %s - %s (%s - %s)" % (lastTrade['tradeId'], response['trade_id'], startDate, response['time']))
		endTimeOffset = 0
		prevLastTradeId = ""
		while True:
			# Check old last trade time and increase the gap if it's still the same
			# Use a three-second window since the max observed trades in a one-second window was 260 on Feb 28 2024
			getGap(response['product_id'], endId, min(startTime + 3 + endTimeOffset, endTime), trades, startTime, lastTrade, writeClient, commonAttributes, mysns)
			if startTime + 3 + endTimeOffset >= endTime or int(response['trade_id']) == int(lastTrade['tradeId']) + 1:
				break
			if prevLastTradeId == lastTrade['tradeId']:
				endTimeOffset += 10
			else:
				endTimeOffset = 0
			prevLastTradeId = lastTrade['tradeId']
			startTime = int(lastTrade['Time']) // 1000000
		if int(response['trade_id']) != int(lastTrade['tradeId']) + 1:
			publishAndPrintError(mysns, RuntimeError("Only closed gap up to " + lastTrade['tradeId'] + ", but current trade ID is " + response['trade_id']), "Requests", symbol)

def getGap(symbol, endId, endTime, trades, startTime, lastTrade, writeClient, commonAttributes, mysns):
	url = "https://api.coinbase.com/api/v3/brokerage/products/%s/ticker" % (symbol)
	params = {"limit": MAX_REST_API_TRADES, "start": str(startTime), "end": str(endTime)}
	jwt_uri = jwt_generator.format_jwt_uri("GET", "/api/v3/brokerage/products/%s/ticker" % (symbol))
	jwt_token = jwt_generator.build_rest_jwt(jwt_uri, COINBASE_API_KEY_NAME, COINBASE_API_PRIVATE_KEY)
	headers = {"Authorization": "Bearer " + jwt_token}
	startDate = datetime.datetime.fromtimestamp(startTime).strftime('%Y-%m-%dT%H:%M:%S')
	endDate = datetime.datetime.fromtimestamp(endTime).strftime('%Y-%m-%dT%H:%M:%S')
	try:
		print("Sending HTTP request for %s trades from %s to %s (lastTradeId: %s)" % (symbol, startDate, endDate, lastTrade['tradeId']))
		response = requests.get(url, params=params, headers=headers)
		response.raise_for_status()
		responseTrades = response.json()['trades']
		cleanTrades(responseTrades)
		print("HTTP response contains %d trades from %s to %s (%s - %s)" % (len(responseTrades), responseTrades[0]['trade_id'], responseTrades[-1]['trade_id'], responseTrades[0]['time'], responseTrades[-1]['time']))
		# print(responseTrades)

		'''
		windows = []
		idx = 0
		currWindow = 0
		while idx < len(responseTrades):
			windows.append(currWindow)
			currWindow = 0
			formattedDate = dateutil.parser.isoparse(responseTrades[idx]['time'])
			print(int(datetime.datetime.timestamp(formattedDate)))
			window = int(datetime.datetime.timestamp(formattedDate)) * 1000000
			microseconds = window
			print(window)
			print(window + 1000000)
			print("follows")
			while (microseconds < window + 1000000):
				print(microseconds)
				currWindow += 1
				idx += 1
				if idx >= len(responseTrades):
					windows.append(currWindow)
					break;
				formattedDate = dateutil.parser.isoparse(responseTrades[idx]['time'])
				microseconds = int(datetime.datetime.timestamp(formattedDate) * 1000000)
		print(max(windows))
		print(windows)
		'''

		tradeId = int(lastTrade['tradeId'])
		idx = next((i for i, x in enumerate(responseTrades) if int(x['trade_id']) == tradeId), -1)
		if (idx == -1):
			raise LookupError("Last trade not found for ID " + str(tradeId))
		tradeId += 1
		while (tradeId < endId):
			if (idx == len(responseTrades) - 1):
				break;
			idx = next((i for i, x in enumerate(responseTrades) if int(x['trade_id']) == tradeId), -1)
			if (idx != -1):
				record = prepareRecord(responseTrades[idx])
				updateRecordTime(record, lastTrade, trades, symbol)
				checkWriteThreshold(symbol, writeClient, trades, commonAttributes, mysns)
			else:
				publishAndPrintError(mysns, LookupError("Trade ID " + str(tradeId) + " not found"), "Requests", symbol)
			tradeId += 1
	except Exception as e:
		publishAndPrintError(mysns, e, "Requests", symbol)

def cleanTrades(trades):
	for idx, _ in enumerate(trades):
		MoreWeirdTradeIds = True
		while MoreWeirdTradeIds:
			try:
				if (idx >= len(trades)):
					break
				int(trades[idx]['trade_id'])
				MoreWeirdTradeIds = False
			except ValueError:
				trades.pop(idx)
	trades.sort(key=cmp_to_key(lambda item1, item2: int(item1['trade_id']) - int(item2['trade_id'])))
	first = True
	for i, e in reversed(list(enumerate(trades))):
		if first:
			first = False
			continue
		if int(e['trade_id']) != int(trades[i + 1]['trade_id']) - 1:
			del(trades[:i+1])
			break

parser = argparse.ArgumentParser(description='Collect trading data from Coinbase and send it to AWS Timestream.')
parser.add_argument('symbol', help='the trading pair to collect data from', choices=['BTC-USD', 'ETH-USD'])
args = parser.parse_args()
symbol = vars(args)['symbol']

asyncio.run(collectData(symbol))

# Test commands for handling gaps
'''
commonAttributes = {
	'Dimensions': [
		{'Name': 'symbol', 'Value': symbol}
	],
	'MeasureName': 'price',
	'MeasureValueType': 'MULTI',
	'TimeUnit': 'MICROSECONDS'
}
'''

# writeClient = boto3.client('timestream-write', region_name=REGION_NAME, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# mysns = boto3.client("sns", region_name=REGION_NAME, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
# handleGap({'trade_id': '999999999', 'time': '2024-06-02T00:00:00.000000Z', 'product_id': 'BTC-USD'}, [], {'Time': '1617216665966502', 'offset': '0', 'tradeId': '151436694'}, writeClient, commonAttributes, mysns)
# handleGap({'trade_id': '151436698', 'time': '2024-06-02T00:00:00.000000Z', 'product_id': 'BTC-USD'}, [], {'Time': '1617216665966502', 'offset': '0', 'tradeId': '151436694'}, {}, {}, {})

# Used to find the max window size (260)
# getGap(symbol, 999999999, 1709144520, [], 1705311931, {}, [], [], [])
