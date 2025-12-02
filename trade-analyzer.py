import argparse
import boto3
import datetime
import json
import os
import requests
import select
import socket
import time
import traceback

FIFTEEN_MIN_IN_MICROSECONDS = 15 * 60 * 1000000
STOP_LOSS = 1
TARGET = 3.5
WINDOW_IDX = 1
WINDOW = FIFTEEN_MIN_IN_MICROSECONDS * (WINDOW_IDX + 1)
BUY_PERCENTILE_IDX = 4
SELL_PERCENTILE_IDX = 2
ENTRY_THRESHOLD = 0.35

COINBASE_WEBSOCKET_ARN = "arn:aws:sns:us-east-2:471112880949:coinbase-websocket-notifications"
ACCESS_KEY = "AKIAW3MEECM242BBX6NJ"

REGION_NAME = "us-east-2"

NTFY_TOPIC = os.environ.get("NTFY_TOPIC")
EC2_IP = os.environ.get("EC2_IP")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

MICROSECONDS_IN_HOUR = 3600000000
MICROSECONDS_IN_DAY = 86400000000
MICROSECONDS_IN_WEEK = 604800000000
CME_CLOSE = 79200000000
CME_OPEN = 82800000000
CME_CLOSE_FRIDAY = 165600000000
CME_OPEN_SUNDAY = 342000000000

MARCH_1_1972_IN_SECONDS = 68256000

BTC_PORT = 12345
ETH_PORT = 12346

# 30th percentile to 95th percentile in increments of 5 percentiles
buyVolPercentiles = []
sellVolPercentiles = []

with open("buyPercentiles", "r") as f:
	for line in f:
		data = line.split(" ")
		data[-1] = data[-1][:-1]
		buyVolPercentiles.append([float(x) for x in data])

with open("sellPercentiles", "r") as f:
	for line in f:
		data = line.split(" ")
		data[-1] = data[-1][:-1]
		sellVolPercentiles.append([float(x) for x in data])

precomputedTarget = 1 + TARGET * 0.01
precomputedStopLoss = 1 - STOP_LOSS * 0.01
precomputedLongEntryThreshold = 1 + ENTRY_THRESHOLD * 0.01
precomputedShortEntryThreshold = 1 - ENTRY_THRESHOLD * 0.01

buyVolPercentile = buyVolPercentiles[WINDOW_IDX][BUY_PERCENTILE_IDX]
sellVolPercentile = sellVolPercentiles[WINDOW_IDX][SELL_PERCENTILE_IDX]

def publishAndPrintError(error, subject):
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

def isDST(timestamp):
	timestamp //= 1000000
	leapYearCycles = (timestamp - MARCH_1_1972_IN_SECONDS) // ((365 * 4 + 1) * 86400)
	days = (timestamp - MARCH_1_1972_IN_SECONDS) // 86400
	daysInCurrentCycle = days % (365 * 4 + 1)
	yearsInCurrentCycle = daysInCurrentCycle // 365
	daysInCurrentYear = daysInCurrentCycle % 365

	timeInCurrentDay = timestamp % 86400

	marchFirstDayOfWeekInCurrentCycle = leapYearCycles * (365 * 4 + 1) % 7
	marchFirstDayOfWeekInCurrentYear = (marchFirstDayOfWeekInCurrentCycle + yearsInCurrentCycle * 365) % 7

	if marchFirstDayOfWeekInCurrentYear > 4:
		dstStart = 11 - marchFirstDayOfWeekInCurrentYear + 7
	else:
		dstStart = 4 - marchFirstDayOfWeekInCurrentYear + 7
	dstEnd = dstStart + 238
	# dayInCurrentYear = (marchFirstDayOfWeekInCurrentYear + daysInCurrentYear) % 7
	# print(marchFirstDayOfWeekInCurrentCycle)
	# print(marchFirstDayOfWeekInCurrentYear)
	# print(dayInCurrentYear)
	# print(daysInCurrentYear)

	if daysInCurrentYear == dstStart:
		return timeInCurrentDay >= 7200
	elif daysInCurrentYear == dstEnd:
		return timeInCurrentDay < 7200
	else:
		return daysInCurrentYear > dstStart and daysInCurrentYear < dstEnd

def isSocketActive(sock):
	# Use select to check if the socket is readable
	readable, _, _ = select.select([sock], [], [], 0)
	return bool(readable)

timeWindow = 0
buyVol = 0
sellVol = 0
trades = []
entry = []
increasing = False
maxPrice = 1
minPrice = 1000000

def postAndHandleError(subject, message):
	try:
		requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=message)
	except Exception as e:
		publishAndPrintError(e, subject)

def assessTrade(trade):
	global entry

	price = trade['price']
	microseconds = trade['time']
	dayRemainder = microseconds % MICROSECONDS_IN_DAY + isDST(microseconds) * MICROSECONDS_IN_HOUR
	weekRemainder = microseconds % MICROSECONDS_IN_WEEK + isDST(microseconds) * MICROSECONDS_IN_HOUR
	inClose = dayRemainder >= CME_CLOSE and dayRemainder < CME_OPEN
	onWeekend = weekRemainder >= CME_CLOSE_FRIDAY and weekRemainder < CME_OPEN_SUNDAY

	if entry:
		if entry[0]:
			profitMargin = price / entry[1]
			if inClose or profitMargin >= precomputedTarget or profitMargin <= precomputedStopLoss:
				# startingCapitals[j][i] *= (1 + profitMargin)
				entry = []
				if profitMargin >= 1:
					data = '%s: Take profit at %.2f (%s)' % (symbol, price, str(datetime.datetime.now(datetime.timezone.utc)))
				else:
					data = '%s: Take loss at %.2f (%s)' % (symbol, price, str(datetime.datetime.now(datetime.timezone.utc)))
				print(data)
				postAndHandleError("Trade Notifications", data)
				# tradeLogs[j][i].append("Profit: " + str(price) + " " + trade[1] + "T" + trade[2] + " " + trade[0])
				# tradeLogs[j][i].append("Capital: " + str(startingCapitals[j][i]))
				# print("Profit: " + str(price) + " " + trade[1] + "T" + trade[2])
				# print("Capital: " + str(startingCapitals[j]))
				# wins[j][i] += 1
		else:
			profitMargin = 2 - price / entry[1]
			if inClose or profitMargin >= precomputedTarget or profitMargin <= precomputedStopLoss:
				# startingCapitals[j][i] *= (1 + profitMargin)
				entry = []
				if profitMargin >= 1:
					data = '%s: Take profit at %f (%s)' % (symbol, price, str(datetime.datetime.now(datetime.timezone.utc)))
				else:
					data = '%s: Take loss at %f (%s)' % (symbol, price, str(datetime.datetime.now(datetime.timezone.utc)))
				print(data)
				postAndHandleError("Trade Notifications", data)
				# tradeLogs[j][i].append("Profit: " + str(price) + " " + trade[1] + "T" + trade[2] + " " + trade[0])
				# tradeLogs[j][i].append("Capital: " + str(startingCapitals[j][i]))
				# print("Profit: " + str(price) + " " + trade[1] + "T" + trade[2])
				# print("Capital: " + str(startingCapitals[j]))
				# wins[j][i] += 1

	if not entry and not inClose and not onWeekend:
		if increasing and price / minPrice >= precomputedLongEntryThreshold and buyVol >= buyVolPercentile:
			entry = [True, price, microseconds, trade['tradeId']]
			data = '%s: Long entry at %f, target = %f, stop loss = %f (%s)' % (symbol, price, price * precomputedTarget, price * precomputedStopLoss, str(datetime.datetime.now(datetime.timezone.utc)))
			print(data)
			postAndHandleError("Trade Notifications", data)
		elif not increasing and price / maxPrice <= precomputedShortEntryThreshold and sellVol >= sellVolPercentile:
			entry = [False, price, microseconds, trade['tradeId']]
			data = '%s: Short entry at %f, target = %f, stop loss = %f (%s)' % (symbol, price, price * (2 - precomputedTarget), price * (2 - precomputedStopLoss), str(datetime.datetime.now(datetime.timezone.utc)))
			print(data)
			postAndHandleError("Trade Notifications", data)

def processTrade(trade):
	# print(trade)
	global timeWindow, buyVol, sellVol, maxPrice, minPrice, increasing

	# print("before: %f, %f" % (buyVol, sellVol))
	trades.append(trade)
	if timeWindow == 0:
		timeWindow = trade['time']
	elif trade['time'] - timeWindow > WINDOW:
		for i in range(len(trades)):
			if trade['time'] - trades[i]['time'] > WINDOW:
				newVol = trades[i]['size']
				if not trades[i]['isBuyerMaker']:
					buyVol -= newVol
				else:
					sellVol -= newVol
			else:
				timeWindow = trades[i]['time']
				del trades[:i]
				break
	if not trade['isBuyerMaker']:
		buyVol += trade['size']
	else:
		sellVol += trade['size']
	# print("after: %f, %f" % (buyVol, sellVol))

	price = trade['price']
	if increasing:
		maxPrice = max(maxPrice, price)
		if price / maxPrice <= precomputedShortEntryThreshold:
			minPrice = 1000000
			increasing = False
	else:
		minPrice = min(minPrice, price)
		if price / minPrice >= precomputedLongEntryThreshold:
			maxPrice = 1
			increasing = True

	assessTrade(trade)

def processMessageIfPossible(buffer):
	while True:
		if not buffer:
			return buffer
		elif buffer[0] != "{":
			publishAndPrintError(RuntimeError("Invalid buffer: " + buffer), "Trade Analyzer")
			return ""
		else:
			nextBracketIdx = buffer.find("}")
			if nextBracketIdx != -1:
				completeMessage = buffer[0:nextBracketIdx+1]
				# print(json.loads(completeMessage))
				processTrade(json.loads(completeMessage))
				buffer = buffer[nextBracketIdx+1:]
			else:
				return buffer

def analyzeTrades():
	sockValid = False
	buffer = ""
	while True:
		if sockValid:
			try:
				data = sock.recv(1024)
				if not data:
					publishAndPrintError(RuntimeError("No data received"), "Trade Analyzer Socket 1")
					sockValid = False
					sock.close()
				else:
					# print("Received:", data.decode())
					buffer += data.decode()
					buffer = processMessageIfPossible(buffer)
			# except ConnectionRefusedError as e:
				# sockValid = False
				# print(e)
			except Exception as e:
				sockValid = False
				sock.close()
				traceback.print_exc()
				publishAndPrintError(e, "Trade Analyzer Socket 1")
		else:
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect((EC2_IP, BTC_PORT if symbol == "BTC-USD" else ETH_PORT))
				sockValid = True
			# except ConnectionRefusedError as e:
				# print(e)
			except Exception as e:
				sock.close()
				traceback.print_exc()
				publishAndPrintError(e, "Trade Analyzer Socket 2")

parser = argparse.ArgumentParser(description='Analyze trading data from a socket and send trading signals')
parser.add_argument('symbol', help='the trading pair to anaylze', choices=['BTC-USD', 'ETH-USD'])
args = parser.parse_args()
symbol = vars(args)['symbol']

mysns = boto3.client("sns", region_name=REGION_NAME, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

analyzeTrades()
