import argparse
import datetime
import json
import os
import requests
import select
import socket
import traceback

FIFTEEN_MIN_IN_MICROSECONDS = 15 * 60 * 1000000
STOP_LOSS = 2
TARGET = 4
WINDOW_IDX = 0
WINDOW = FIFTEEN_MIN_IN_MICROSECONDS * (WINDOW_IDX + 1)
BUY_PERCENTILE_IDX = 7
SELL_PERCENTILE_IDX = 2
ENTRY_THRESHOLD = 0.3

NTFY_TOPIC = os.environ.get("NTFY_TOPIC")
EC2_IP = os.environ.get("EC2_IP")

MICROSECONDS_IN_DAY = 86400000000
MICROSECONDS_IN_WEEK = 604800000000
CME_CLOSE = 79200000000
CME_OPEN = 82800000000
CME_CLOSE_FRIDAY = 165600000000
CME_OPEN_SUNDAY = 342000000000

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

def assessTrade(trade):
	global entry

	price = trade['price']
	microseconds = trade['time']
	dayRemainder = microseconds % MICROSECONDS_IN_DAY
	weekRemainder = microseconds % MICROSECONDS_IN_WEEK
	inClose = dayRemainder >= CME_CLOSE and dayRemainder < CME_OPEN
	onWeekend = weekRemainder >= CME_CLOSE_FRIDAY and weekRemainder < CME_OPEN_SUNDAY

	if entry:
		if entry[0]:
			profitMargin = price / entry[1]
			if inClose or profitMargin >= precomputedTarget or profitMargin <= precomputedStopLoss:
				# startingCapitals[j][i] *= (1 + profitMargin)
				entry = []
				if profitMargin >= 1:
					data = '%s: Take profit at %f (%s)' % (symbol, price, str(datetime.datetime.now(datetime.timezone.utc)))
				else:
					data = '%s: Take loss at %f (%s)' % (symbol, price, str(datetime.datetime.now(datetime.timezone.utc)))
				print(data)
				requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
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
				requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
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
			requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
		elif not increasing and price / maxPrice <= precomputedShortEntryThreshold and sellVol >= sellVolPercentile:
			entry = [False, price, microseconds, trade['tradeId']]
			data = '%s: Short entry at %f, target = %f, stop loss = %f (%s)' % (symbol, price, price * (2 - precomputedTarget), price * (2 - precomputedStopLoss), str(datetime.datetime.now(datetime.timezone.utc)))
			print(data)
			requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)

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
			print("Invalid buffer: " + buffer)
			return buffer
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

parser = argparse.ArgumentParser(description='Analyze trading data from a socket and send trading signals')
parser.add_argument('symbol', help='the trading pair to anaylze', choices=['BTC-USD', 'ETH-USD'])
args = parser.parse_args()
symbol = vars(args)['symbol']

analyzeTrades()
