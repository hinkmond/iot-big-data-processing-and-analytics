"""
 Processes direct stream from kafka, '\n' delimited text directly received
   every 2 seconds.
 Usage: kafka-direct-iotmsg.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a
   producer first, see:
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iotmsg.py \
      localhost:9092 iotmsgs`
"""

import sys
import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

from operator import add


def print_row(row):
    jsonLine = row.__getitem__('value')
    if re.search(r"temperature.*", jsonLine):
       tempLine = re.sub(r"\"temperature\":", "", jsonLine).split(',')[0]
       print(jsonLine)
       print(tempLine)
    elif re.search(r"humidity.*", jsonLine):
       humidityLine = re.sub(r"\"humidity\":", "", jsonLine).split(',')[0]
       print(humidityLine)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    ###############
    # Globals
    ###############
    tempTotal = 0.0
    tempCount = 0
    tempAvg = 0.0

    brokers, topic = sys.argv[1:]
    spark = SparkSession.builder.master("local[*]") \
                    .appName('kafka-direct-iotmsg') \
                    .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(">>> Starting spark...")
    df = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", brokers) \
     .option("subscribe", topic) \
     .option("startingOffsets", "latest") \
     .load()

    print(">>> Starting DataFrame Processing")
    df_to_strings = df.selectExpr("CAST(value AS STRING)")

    print(">>> Starting DataWriteStream ")
    query2 = df_to_strings.writeStream \
     .foreach(print_row).start()

    spark.streams.awaitAnyTermination()


