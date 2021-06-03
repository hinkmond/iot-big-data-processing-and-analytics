"""
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iot-sql.py \
      localhost:9092 test`
"""
from __future__ import print_function

import sys
import re
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from operator import add

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")

    ##############
    # Globals
    ##############
    globals()['maxTemp'] = sc.accumulator(0.0)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    jsonDStream = kvs.map(lambda (key, value): value)

    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def process(time, rdd):
        # Match local function variables to global variables
        maxTemp = globals()['maxTemp']

        print("========= %s =========" % str(time))
        print("rdd = %s" % str(rdd))

        try:
          if not rdd.isEmpty():
            # Parse the multiple JSON lines from the DStream RDD and trim any extra spaces
            jsonLinesRDD = rdd.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE)).reduce(add)
            print("jsonLinesRDD = %s" % str(jsonLinesRDD))

            # Convert RDD of the List of multiple JSON lines to Spark SQL Context by first
            #    joining the list of mulitple JSON lines in a new RDD with a single JSON line
            jsonRDD = sqlContext.read.json(sc.parallelize([jsonLinesRDD]))

            # Register the JSON SQL Context as a temporary SQL Table
            print("JSON Schema\n=====")
            jsonRDD.printSchema()
            jsonRDD.registerTempTable("iotmsgsTable")

            #############
            # Processing and Analytics go here
            #############

            sqlContext.sql("select payload.data.* from iotmsgsTable order by temperature desc").show(n=100)

            # Sort
            sqlContext.sql("select payload.data.temperature from iotmsgsTable order by temperature desc").show(n=100)

            currentTemp = sqlContext.sql("select payload.data.temperature from iotmsgsTable order by temperature desc").collect()[0].temperature
            print("Current temp = " + str(currentTemp))
            if (currentTemp > maxTemp.value):
              maxTemp.value = currentTemp
            print("Max temp = " + str(maxTemp.value))

            # Search
            sqlContext.sql("select payload.data.temperature from iotmsgsTable where payload.data.temperature = 100.0").show(n=100)

            # Filter
            sqlContext.sql("select payload.data.temperature from iotmsgsTable where payload.data.temperature > 85").show(n=100)

            # Clean-up
            sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except:
            pass

    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
