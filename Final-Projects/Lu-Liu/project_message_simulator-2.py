#!/usr/bin/python

import sys
import datetime
import random
import string
import math 

# Set number of simulated messages to generate
if len(sys.argv) > 1:
  numMsgs = int(sys.argv[1])
else:
  numMsgs = 1

# Fixed values
guidStr = "0-ZZZ12345678"
destinationStr = "0-AAA12345678"
formatStr = "urn:project:sensor:temp_hr_gps"

# Choice for random letter
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

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
       "longitude": %.7f,
       "latitude": %.7f,
       "bodytemperature": %.1f,
       "heartrate": %d
     }
   }
}"""

#define radius and center location 
radius=10000
radiusInDegrees=radius/111300.0
r=radiusInDegrees
long0=-0.1278990
lat0=51.5032520
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


  #generate random longitude and latitude within the predefined radius
  u = float(random.uniform(0.0,1.0))
  v = float(random.uniform(0.0,1.0))
  
  w = r * math.sqrt(u)
  t = 2 * math.pi * v
  
  x = w * math.cos(t)
  y = w * math.sin(t)
 
  randLong=long0+x
  randLat=lat0+y

  #generate random body temperature data
  randTemp = random.uniform(0.0, 5.0) + 95.0
  #generate random heart rate data 
  randHR=random.uniform(0,130)+30

  if counter == numMsgs - 1:
    dataElementDelimiter = ""
  
  print iotmsg_data % (randLong,randLat,randTemp,randHR) + dataElementDelimiter

print "]"
