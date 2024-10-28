import json
import os
import requests
import select
import socket
import traceback

FIFTEEN_MIN_IN_MICROSECONDS = 15 * 60 * 1000000
STOP_LOSS = 1
TARGET = 1
WINDOW_IDX = 7
WINDOW = FIFTEEN_MIN_IN_MICROSECONDS * (WINDOW_IDX + 1)
BUY_PERCENTILE_IDX = 1
SELL_PERCENTILE_IDX = 0
NTFY_TOPIC = os.environ.get("NTFY_TOPIC")

# 60th percentile to 95th percentile in increments of 5 percentiles
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
maxProfit = 0

def assessTrade(trade):
	global entry, maxProfit

	if not entry:
		if buyVol >= buyVolPercentile:
			maxProfit = trade['price']
			entry = [True, trade['price'], trade['time'], trade['tradeId']]
			data = 'Long entry at %f, target = %f, stop loss = %f' % (trade['price'], trade['price'] * (1 + TARGET / 100), trade['price'] * (1 - STOP_LOSS / 100))
			print(data)
			requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
		elif sellVol >= sellVolPercentile:
			maxProfit = trade['price']
			entry = [False, trade['price'], trade['time'], trade['tradeId']]
			data = 'Short entry at %f, target = %f, stop loss = %f' % (trade['price'], trade['price'] * (1 - TARGET / 100), trade['price'] * (1 + STOP_LOSS / 100))
			print(data)
			requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
	else:
		if entry[0]:
			maxProfit = max(maxProfit, trade['price'])
			profitMargin = (maxProfit - entry[1]) / entry[1]
			if profitMargin >= TARGET / 100:
				# startingCapitals[j][i] *= (1 + profitMargin)
				entry = []
				data = 'Take profit at %f' % (maxProfit)
				print(data)
				requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
				# tradeLogs[j][i].append("Profit: " + str(price) + " " + trade[1] + "T" + trade[2] + " " + trade[0])
				# tradeLogs[j][i].append("Capital: " + str(startingCapitals[j][i]))
				# print("Profit: " + str(price) + " " + trade[1] + "T" + trade[2])
				# print("Capital: " + str(startingCapitals[j]))
				# wins[j][i] += 1
			elif trade['price'] <= (1 - STOP_LOSS / 100) * entry[1]:
				# startingCapitals[j][i] *= 1 - stopLoss / 100
				entry = []
				data = 'Take loss at %f' % (trade['price'])
				print(data)
				requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
				# tradeLogs[j][i].append("Loss: " + str(price) + " " + trade[1] + "T" + trade[2] + " " + trade[0])
				# tradeLogs[j][i].append("Capital: " + str(startingCapitals[j][i]))
				# print("Loss: " + str(price) + " " + trade[1] + "T" + trade[2])
				# print("Capital: " + str(startingCapitals[j]))
				# losses[j][i] += 1
		else:
			maxProfit = min(maxProfit, trade['price'])
			profitMargin = (entry[1] - maxProfit) / entry[1]
			if profitMargin >= TARGET / 100:
				# startingCapitals[j][i] *= (1 + profitMargin)
				entry = []
				data = 'Take profit at %f' % (maxProfit)
				print(data)
				requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
				# tradeLogs[j][i].append("Profit: " + str(price) + " " + trade[1] + "T" + trade[2] + " " + trade[0])
				# tradeLogs[j][i].append("Capital: " + str(startingCapitals[j][i]))
				# print("Profit: " + str(price) + " " + trade[1] + "T" + trade[2])
				# print("Capital: " + str(startingCapitals[j]))
				# wins[j][i] += 1
			elif trade['price'] >= (1 + STOP_LOSS / 100) * entry[1]:
				# startingCapitals[j][i] *= 1 - stopLoss / 100
				entry = []
				data = 'Take loss at %f' % (trade['price'])
				print(data)
				requests.post("https://ntfy.sh/" + NTFY_TOPIC, data=data)
				# tradeLogs[j][i].append("Loss: " + str(price) + " " + trade[1] + "T" + trade[2] + " " + trade[0])
				# tradeLogs[j][i].append("Capital: " + str(startingCapitals[j][i]))
				# print("Loss: " + str(price) + " " + trade[1] + "T" + trade[2])
				# print("Capital: " + str(startingCapitals[j]))
				# losses[j][i] += 1

def processTrade(trade):
	global timeWindow, buyVol, sellVol

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
	trades.append(trade)
	assessTrade(trade)

def processMessageIfPossible(buffer):
	while True:
		if not buffer:
			return buffer
		elif buffer[0] != "{":
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
				sock.connect(('localhost', 12345))
				sockValid = True
			# except ConnectionRefusedError as e:
				# print(e)
			except Exception as e:
				sock.close()
				traceback.print_exc()

analyzeTrades()
