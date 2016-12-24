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
messageid ="f9905dc8-b861-4912-bcf7-"
name ="HealthCheckResponse"
namespace="Alexa.ConnectHome.System"
payloadVersion = 2

# Choice for random letter
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

iotmsg_header = """\
{
  "messageId": "%s",
  "name": "%s",
  "namespace":"%s",
  "payloadVersion":%d, """

iotmsg_eventTime = """\
  "eventTime": "%sZ", """

iotmsg_payload ="""\
  "payload": {
     "description": "%s",
     "isHealthy":"%s"
   }
}"""

##### Generate JSON output:

print "["

dataElementDelimiter = ","
for counter in range(0, numMsgs):

  randInt = random.randrange(0, 9)
  randLetter = random.choice(letters)
  print iotmsg_header % (messageid+str(randInt)+randLetter, name, namespace, payloadVersion)

  today = datetime.datetime.today()
  datestr = today.isoformat()
  print iotmsg_eventTime % (datestr)

  state = "true"
  description = "The system is currently healthy"
  if randInt > 7:
        state = "false"
        description = "The system is currently not healthy"

  if counter == numMsgs - 1:
        dataElementDelimiter = ""
  print iotmsg_payload % (description,  state)  + dataElementDelimiter

  # Generate a random floating point number

print "]"
