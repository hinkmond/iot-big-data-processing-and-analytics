#!/usr/bin/python

import sys
import datetime
import random
import string
import csv

# Set number of simulated messages to generate
# if len(sys.argv) > 1:
#   numMsgs = int(sys.argv[1])
# else:
#   numMsgs = 1

csvfile = file('DF_A02SSH00302.csv','r')
csvreader = csv.reader(csvfile)


# Fixed values
smartmeterIDStr = "SM000543218"
formatStr1 = "urn:com:smartmeter:powerusageinkW"
formatStr2 = "urn:com:tempsensor:temperatureinF"
formatStr3 = "urn:com:humsensor:relativehumidityin%"

iotmsg_header = """\
{
  "smartmeterID": "%s",
  "customerID": "%s", 
"""

iotmsg_eventTime = """\
  "eventTime": "%sZ", 
"""

iotmsg_payload ="""\
  "payload": {
     "format": "%s", 
"""

iotmsg_data1 ="""\
     "data": {
       "Power": %s
     }
   }
}"""

iotmsg_data2 ="""\
     "data": {
       "Temperature": %s
     }
   }
}"""

iotmsg_data3 ="""\
     "data": {
       "Humidity": %s
     }
   }
}"""
##### Generate JSON output:
files_opened = 0


dataElementDelimiter = ","
for row in csvreader:

  # CustId,1
  # Date  ,2
  # Hour  ,3
  # Rhum  ,4
  # Temp  ,5
  # Kw    ,6

  customerIDStr = row[1]

  if (row[2] == "DATE"):  # Header line skipping
    continue

  if ( (row[1] != "DATE") and (files_opened != 1)):
    powr_out = file(row[1] + '_SM.json','w')
    temp_out = file(row[1] + '_TS.json','w')
    rhum_out = file(row[1] + '_HS.json','w')
    powr_out.write("[")
    temp_out.write("[")
    rhum_out.write("[")
    files_opened = 1

  date_s  = row[2].split(" ")
  date_sp = date_s[0].split("/")
  year    = date_sp[2]
  month   = date_sp[0]
  day     = date_sp[1]

  hour    = row[3]

  if (int(month) < 10):
    month = "0" + month

  if (int(day) < 10):
    day = "0" + day

  if (int(hour) < 10):
    hour = "0" + hour

  if (int(hour) == 24):
    hour = "00"

  datestr = year + "-" + month + "-" + day + "T" + hour + ":" + "00:00.000000"

  powr_out.write( iotmsg_header % (smartmeterIDStr, customerIDStr) )
  temp_out.write( iotmsg_header % (smartmeterIDStr, customerIDStr) )
  rhum_out.write( iotmsg_header % (smartmeterIDStr, customerIDStr) )
  powr_out.write( iotmsg_eventTime % (datestr)                     )
  temp_out.write( iotmsg_eventTime % (datestr)                     )
  rhum_out.write( iotmsg_eventTime % (datestr)                     )
  powr_out.write( iotmsg_payload % (formatStr1)                    )
  temp_out.write( iotmsg_payload % (formatStr2)                    )
  rhum_out.write( iotmsg_payload % (formatStr3)                    )
  powr_out.write( iotmsg_data1 % (row[6]) + dataElementDelimiter   )
  temp_out.write( iotmsg_data2 % (row[5]) + dataElementDelimiter   )
  rhum_out.write( iotmsg_data3 % (row[4]) + dataElementDelimiter   )

  # print iotmsg_header % (smartmeterIDStr, customerIDStr)
  # print iotmsg_eventTime % (datestr)
  # print iotmsg_payload % (formatStr)
  # print iotmsg_data % (row[6]) + dataElementDelimiter

# for counter in range(0, numMsgs):
# 
#   print iotmsg_header % (smartmeterIDStr, customerIDStr)
# 
#   today = datetime.datetime.today()
#   datestr = today.isoformat()
#   print iotmsg_eventTime % (datestr)
# 
#   print iotmsg_payload % (formatStr)
# 
#   # Generate a random floating point number
#   randPower = random.uniform(0.0, 0.3) + 0.01
#   if counter == numMsgs - 1:
#     dataElementDelimiter = ""
#   print iotmsg_data % (randPower) + dataElementDelimiter

powr_out.write( "]" )
temp_out.write( "]" )
rhum_out.write( "]" )

