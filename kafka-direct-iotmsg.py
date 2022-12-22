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
    json_line = row.__getitem__('value')
    if re.search(r"temperature.*", json_line):
       temp_line = re.sub(r"\"temperature\":", "", json_line).split(',')[0]
       print(json_line)
       print(temp_line)
    elif re.search(r"humidity.*", json_line):
       humidity_line = re.sub(r"\"humidity\":", "", json_line).split(',')[0]
       print(json_line)
       print(humidity_line)

def process_json(batch_df, batch_id):
    json_raw_str = batch_df.rdd \
      .map(lambda row: " ".join(row) + " ") \
      .aggregate("", lambda a,b: a+b, lambda a,b: a+b)
    json_line = re.sub(r"\s+", "", json_raw_str, flags=re.UNICODE)
    print(json_line)
    json_df = spark.read.json(sc.parallelize([json_line]))
    json_df.printSchema()
    json_df.show(truncate=False)

def process_batch_values(batch_df, batch_id):
    # Match local function variables to global variables
    global TEMP_TOTAL
    global TEMP_COUNT
    global TEMP_AVG

    # Search for specific IoT data values (assumes json_rows are split(','))
    json_rows = batch_df.rdd
    temp_values = json_rows.filter(lambda row: re.findall(r"temperature.*", row.__getitem__('value'), 0))

    # Parse out just the value without the JSON key
    parsed_temp_values = temp_values.map(lambda row: re.sub(r"\"temperature\":", "", row.__getitem__('value')) \
      .split(',')[0])

    temps_list = parsed_temp_values.collect()
    for temp_float in temps_list:
      TEMP_TOTAL += float(temp_float)
      TEMP_COUNT += 1
      TEMP_AVG = TEMP_TOTAL / TEMP_COUNT
    print("Temperature Total = " + str(TEMP_TOTAL))
    print("Temperature Count = " + str(TEMP_COUNT))
    print("Avg Temperature = " + str(TEMP_AVG))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    ###############
    # Globals
    ###############
    TEMP_TOTAL = 0.0
    TEMP_COUNT = 0
    TEMP_AVG = 0.0

    brokers, topic = sys.argv[1:]
    spark = SparkSession.builder.master("local[*]") \
                    .appName('kafka-direct-iotmsg') \
                    .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print(">>> Starting spark, bootstrap.servers: " + brokers + ", topic: " + topic + " ...")
    df = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", brokers) \
     .option("subscribe", topic) \
     .option("startingOffsets", "latest") \
     .load()

    print(">>> Starting DataFrame Select value Processing")
    df_to_strings = df.selectExpr("CAST(value AS STRING)")

    print(">>> Starting DataWriteStream of Value column as Strings")
    query1 = df_to_strings.writeStream \
     .foreach(print_row) \
     .start()

    print(">>> Starting DataWriteStream of JSON lines in one batch ")
    query2 = df_to_strings.writeStream \
     .foreachBatch(process_batch_values) \
     .start()

    spark.streams.awaitAnyTermination()
