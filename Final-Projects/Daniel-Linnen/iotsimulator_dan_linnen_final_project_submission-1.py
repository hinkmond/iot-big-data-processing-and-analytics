#!/usr/bin/python

import sys
import datetime
import random
import string

# Set number of simulated messages to generate
if len(sys.argv) > 1:
  numMsgs = int(sys.argv[1])
else:
  numMsgs = 1

# Fixed values
guidStr = "0-ZZZ12345678"
destinationStr = "0-AAA12345678"
formatStr = "urn:com:company:sensor:humidity,temperature,longitude,latitude,lowFuel,fuelRange"

# Choice for random letter
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

# Choice for random North or South
northAndSouth = 'NS'

iotmsg_header = """\
{
  "guid": "%s",
  "destination": "%s", """

iotmsg_eventTime = """\
  "eventTime": "%sZ", """

iotmsg_payload ="""\
  "payload": {
     "format": "%s", """

iotmsg_data ="""\
     "data": {
       "humidity": %.1f, 
       "temperature": %.1f,
       "low": %d,
       "range": %.1f,
       "longitude": %.5f,
       "latitude": "%s"
     }
   }
}"""

##### Generate JSON output:

print "["

dataElementDelimiter = ","
for counter in range(0, numMsgs):

  randInt = random.randrange(0, 9)
  randLetter = random.choice(letters)
  print iotmsg_header % (guidStr+str(randInt)+randLetter, destinationStr)

  today = datetime.datetime.today()
  datestr = today.isoformat()
  print iotmsg_eventTime % (datestr)

  print iotmsg_payload % (formatStr)

  # Generate a random floating point number
  randTemperature = random.uniform(0.0, 40.0) + 60.0
  randHum = random.uniform(0.0, 100.0)
  randLongitude = random.uniform(-180.00000, 180.00000)
  randLatitudeDegree = random.uniform(0.00000, 90.00000)
  randLatitudePole = random.choice(northAndSouth)
  randLatitude = str(randLatitudeDegree) + randLatitudePole
  randFuelLow = random.randint(0, 1)
  if randFuelLow:
    randFuelRange = random.uniform(5.0, 50.0)
  else:
    randFuelRange = random.uniform(50.0, 500.0)
  if counter == numMsgs - 1:
    dataElementDelimiter = ""
  print iotmsg_data % (randTemperature, randHum, randFuelLow, randFuelRange, randLongitude, randLatitude) + dataElementDelimiter

print "]"

