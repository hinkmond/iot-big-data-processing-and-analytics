#!/bin/env python2

from __future__ import print_function
import json
import sys
import datetime
import random
import string

guid = {}
destination = {}
timestamp = {}
messagebody = {}
format = {}
data = {}
payload = {}

# Set number of simulated messages to generate
if len(sys.argv) > 1:
  numMsgs = int(sys.argv[1])
else:
  numMsgs = 1

# Fixed values
guidStr = "0-ZZZ12345678"
destinationStr = "0-AAA12345678"

##### TODO 1: Change the format string to match your project
formatStr = "urn:example:sensor:weatherstation"

# Choice for random letter
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

# Generate JSON output:
print("[ ")
for counter in range(0, numMsgs):

  randInt = random.randrange(0, 9)
  randLetter = random.choice(letters)
  messagebody['guid'] = guidStr+str(randInt)+randLetter
 
  messagebody['destination'] = destinationStr

  today = datetime.datetime.today()
  dateStr = today.isoformat()
  messagebody['timestamp'] = dateStr

  payload['format'] = formatStr 

  ##### TODO 2: Change or add your project random variables
  # Generate random floating point numbers
  randTemp = round(random.uniform(60.0, 100.0), 1)
  randHumidity = round(random.uniform(35.0, 100.0), 1)
  randPressure = round(random.uniform(25.0, 35.0), 1)

  ##### TODO 3: Change or add your project data keys equal to 
  #####           previous variables
  data['temperature'] = randTemp
  data['humidity'] = randHumidity
  data['pressure'] = randPressure

  payload['data'] = (data)
  messagebody['payload'] = (payload)

  if counter != 0:
    print(", ")

  message = json.dumps(messagebody)
  print(message, end = '') 

print()
print(" ]")

