
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
        print("Usage: project_kafka_iot_sql_process.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="project_kafka_iot_sql_process")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    jsonDStream = kvs.map(lambda (key, value): value)

    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def process(time, rdd):
        print("========= %s =========" % str(time))
        print("\nrdd is this (NOTE: If non-empty, Array of individual JSON elements) = ")
        print(rdd.collect())

        try:
            # Parse the one line JSON string from the DStream RDD
	    jsonString = rdd.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE)).reduce(add)
	    print("\nHowever, jsonString looks like this (NOTE: One JSON blob) = \n%s\n" % str(jsonString))

            # Convert the JSON string to an RDD
            jsonRDDString = sc.parallelize([str(jsonString)])

            # Convert the JSON RDD to Spark SQL Context
	    jsonRDD = sqlContext.read.json(jsonRDDString)

            # Register the JSON SQL Context as a temporary SQL Table
	    print("JSON Schema\n=====")
            jsonRDD.printSchema()
            jsonRDD.registerTempTable("iotmsgsTable")

            #############
            # Processing and Analytics go here
            #############
            sqlContext.sql("select payload.data.* from iotmsgsTable").show(n=100)


            # Sort
#            sqlContext.sql("select payload.data.temperature from iotmsgsTable order by temperature desc").show(n=100)

            # Search
#            sqlContext.sql("select payload.data.temperature from iotmsgsTable where payload.data.temperature = 100.0").show(n=100)

            # Filter
            sqlContext.sql("select payload.data.* from iotmsgsTable where \
                payload.data.bodytemperature > 99 or payload.data.bodytemperature<97  \
                or payload.data.heartrate>150  or payload.data.heartrate<50   \
                order by heartrate desc, bodytemperature desc").show(n=100)
            
            # Clean-up
	    sqlContext.dropTempTable("iotmsgsTable")
        # Catch any exceptions
        except:
            pass

    # Process each RDD of the DStream coming in from Kafka
    jsonDStream.foreachRDD(process)
 
    ssc.start()
    ssc.awaitTermination()