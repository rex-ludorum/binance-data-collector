import asyncio
import websockets
import json
import requests
import boto3
import time
import traceback
import os

DATABASE_NAME = "binance-websocket-data"
BTC_TABLE_NAME = "btcusdt"
ETH_TABLE_NAME = "ethusdt"

BINANCE_WEBSOCKET_ARN = "arn:aws:sns:us-east-2:471112880949:binance-websocket-notifications"
ACCESS_KEY = "AKIAW3MEECM242BBX6NJ"
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")

REGION_NAME = "us-east-2"

# Empirically determined number of records over which the write size will exceed 1 kB
NUM_RECORDS = 30

MAX_REST_API_TRADES = 1000

def publishAndPrintError(mysns, error, subject):
	errorMessage = repr(error) + " encountered at " + str(time.strftime("%H:%M:%S", time.localtime()))
	print(errorMessage)
	try:
		mysns.publish(
			TopicArn = BINANCE_WEBSOCKET_ARN,
			Message = errorMessage,
			Subject = subject + " Exception",
		)
	except Exception as e:
		print(repr(e), "encountered at", str(time.strftime("%H:%M:%S", time.localtime())), "during publishing")

def prepareRecord(response):
	# These two fields are only present in websocket responses
	if 'e' in response:
		response.pop("e")
	if 'E' in response:
		response.pop("E")
	response.pop("f")
	response.pop("l")
	response.pop("M")
	isBuyerMaker = str(response['m']).lower() if isinstance(response['m'], bool) else response['m']
	record = {
		'Time': str(response['T']),
		'MeasureValues': [
			prepareMeasure('aggregatedTradeId', response['a'], 'BIGINT'),
			prepareMeasure('price', response['p'], 'DOUBLE'),
			prepareMeasure('quantity', response['q'], 'DOUBLE'),
			prepareMeasure('isBuyerMaker', isBuyerMaker, 'BOOLEAN')
		]
	}
	return record

def prepareMeasure(name, value, measureType):
	measure = {
		'Name': name,
		'Value': str(value),
		'Type': measureType
	}
	return measure

# Obtain the range of aggregate IDs we are writing
def getAggIds(records):
	aggIds = []
	firstTradeMeasureValues = records[0]['MeasureValues']
	for measure in firstTradeMeasureValues:
		if measure['Name'] == 'aggregatedTradeId':
			aggIds.append(measure['Value'])
			break
	lastTradeMeasureValues = records[-1]['MeasureValues']
	for measure in lastTradeMeasureValues:
		if measure['Name'] == 'aggregatedTradeId':
			aggIds.append(measure['Value'])
			break
	return aggIds

def writeRecords(symbol, writeClient, records, commonAttributes, tableName, mysns):
	try:
		aggIds = getAggIds(records)
		print("Writing %d %s records (%s - %s)" % (len(records), symbol, aggIds[0], aggIds[1]))
		result = writeClient.write_records(DatabaseName=DATABASE_NAME, TableName=tableName, CommonAttributes=commonAttributes, Records=records)
		status = result['ResponseMetadata']['HTTPStatusCode']
		print("Processed %d %s records (%s - %s). WriteRecords HTTPStatusCode: %s" % (len(records), commonAttributes['Dimensions'][0]['Value'], aggIds[0], aggIds[1], status))
	except writeClient.exceptions.RejectedRecordsException as e:
		# print("RejectedRecords at", str(time.strftime("%H:%M:%S", time.localtime())), ":", e)
		publishAndPrintError(mysns, e, "RejectedRecords")
		for rr in e.response["RejectedRecords"]:
			print("Rejected Index " + str(rr["RecordIndex"]) + ": " + rr["Reason"])
			print(json.dumps(records[rr['RecordIndex']], indent=2))
			if "ExistingVersion" in rr:
				print("Rejected record existing version: ", rr["ExistingVersion"])
	except Exception as e:
		publishAndPrintError(mysns, e, "Other WriteClient")

# Timestream does not allow two records with the same timestamp and dimensions to have different measure values
# Therefore, add one ms to the later timestamp
def updateRecordTime(record, lastTrade, recordList, symbol):
	recordTime = record['Time']
	if lastTrade and int(record['Time']) <= int(lastTrade['T']) + int(lastTrade['o']):
		record['Time'] = str(int(lastTrade['T']) + int(lastTrade['o']) + 1)
		print("Time %s for %s conflicts with last trade time (%s with offset %s, aggId %s), updating to %s" % (recordTime, symbol, lastTrade['T'], lastTrade['o'], lastTrade['a'], record['Time']))
		lastTrade['o'] = str(int(lastTrade['o']) + 1)
	else:
		lastTrade['T'] = recordTime
		lastTrade['o'] = str(0)
	lastTrade['a'] = getAggIds([record])[0]
	recordList.append(record)

# Check if we have reached the 1 kB write size and write the records
def checkWriteThreshold(symbol, writeClient, trades, commonAttributes, tableName, mysns):
	if len(trades) == NUM_RECORDS:
		# print(json.dumps(trades, indent=2))
		writeRecords(symbol, writeClient, trades, commonAttributes, tableName, mysns)
		trades.clear()

async def collectData():
	url = "wss://stream.binance.us:9443/ws"
	headers = {}
	headers['X-MBX-APIKEY'] = BINANCE_API_KEY
	btcTrades = []
	ethTrades = []
	lastBtcTrade = {'T': '0', 'o': '0', 'a': '0'}
	lastEthTrade = {'T': '0', 'o': '0', 'a': '0'}
	handleFirstBtcGap = False
	handleFirstEthGap = False

	commonAttributesBtc = {
		'Dimensions': [
			{'Name': 'symbol', 'Value': 'BTCUSDT'}
		],
		'MeasureName': 'price',
		'MeasureValueType': 'MULTI'
	}

	commonAttributesEth = {
		'Dimensions': [
			{'Name': 'symbol', 'Value': 'ETHUSDT'}
		],
		'MeasureName': 'price',
		'MeasureValueType': 'MULTI'
	}

	writeClient = boto3.client('timestream-write', region_name=REGION_NAME, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY)

	mysns = boto3.client("sns", region_name=REGION_NAME, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS_KEY)

	# handleGap({'a': 2710000}, "BTCUSDT", btcTrades, {'T': '0', 'o': '0', 'a': '0'}, writeClient, commonAttributesBtc, BTC_TABLE_NAME, mysns)
	async for websocket in websockets.connect(url, extra_headers=headers):
		request = {"id": 1, "method": "SUBSCRIBE", "params": ["btcusdt@aggTrade", "ethusdt@aggTrade"]}
		try:
			await websocket.send(json.dumps(request))
			while True:
				response = json.loads(await websocket.recv())
				if len(response) > 2:
					record = prepareRecord(response)
					if response["s"] == "BTCUSDT":
						if handleFirstBtcGap or lastBtcTrade['a'] != '0':
							handleGap(response, btcTrades, lastBtcTrade, writeClient, commonAttributesBtc, BTC_TABLE_NAME, mysns)
						updateRecordTime(record, lastBtcTrade, btcTrades, response['s'])
						checkWriteThreshold(response['s'], writeClient, btcTrades, commonAttributesBtc, BTC_TABLE_NAME, mysns)
					elif response["s"] == "ETHUSDT":
						if handleFirstEthGap or lastEthTrade['a'] != '0':
							handleGap(response, ethTrades, lastEthTrade, writeClient, commonAttributesEth, ETH_TABLE_NAME, mysns)
						updateRecordTime(record, lastEthTrade, ethTrades, response['s'])
						checkWriteThreshold(response['s'], writeClient, ethTrades, commonAttributesEth, ETH_TABLE_NAME, mysns)
		except websockets.ConnectionClosedOK as e:
			traceback.print_exc()
			publishAndPrintError(mysns, e, "Websocket ConnectionClosedOK")
		except websockets.ConnectionClosedError as e:
			traceback.print_exc()
			publishAndPrintError(mysns, e, "Websocket ConnectionClosedError")
		except Exception as e:
			traceback.print_exc()
			publishAndPrintError(mysns, e, "Other Websocket")
			break

# If we have to reconnect after a websocket exception, get any trades we might have missed
def handleGap(response, trades, lastTrade, writeClient, commonAttributes, tableName, mysns):
	if int(response['a']) != int(lastTrade['a']) + 1:
		print("Gap found: %s - %s" % (lastTrade['a'], response['a']))
		gap = int(response['a']) - int(lastTrade['a']) - 1
		while gap > 0:
			getGap(response['s'], int(lastTrade['a']) + 1, min(gap, MAX_REST_API_TRADES), trades, lastTrade, writeClient, commonAttributes, tableName, mysns)
			gap -= min(gap, MAX_REST_API_TRADES)

def getGap(symbol, aggId, n, trades, lastTrade, writeClient, commonAttributes, tableName, mysns):
	url = "https://api.binance.us/api/v3/aggTrades"
	params = {"symbol": symbol, "fromId": aggId, "limit": n}
	try:
		print("Sending HTTP request for %s trades from %d to %d" % (symbol, aggId, aggId + n - 1))
		response = requests.get(url, params=params)
		response.raise_for_status()
		for trade in response.json():
			record = prepareRecord(trade)
			updateRecordTime(record, lastTrade, trades, symbol)
			checkWriteThreshold(symbol, writeClient, trades, commonAttributes, tableName, mysns)
	except Exception as e:
		publishAndPrintError(mysns, e, "Requests")

asyncio.run(collectData())
