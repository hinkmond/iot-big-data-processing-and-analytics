"""
 To run this on your local machine, you need to setup Kafka and create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iot-sql.py \
      localhost:9092 test`
"""
import sys
import re
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

from operator import add


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iot-sql.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    brokers, topic = sys.argv[1:]
    spark = SparkSession.builder.master("local[*]") \
                    .appName('kafka-direct-iotmsg') \
                    .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    ##############
    # Globals
    ##############
    globals()['max_temp'] = sc.accumulator(0.0)

    brokers, topic = sys.argv[1:]
    print(">>> Starting spark, bootstrap.servers: " + brokers + ", topic: " + topic + " ...")
    df = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", brokers) \
     .option("subscribe", topic) \
     .option("startingOffsets", "latest") \
     .load()

    # Define function to process RDDs of the json DStream to convert them
    #   to DataFrame and run SQL queries
    def process(batch_df, batch_id):
        # Match local function variables to global variables
        max_temp = globals()['max_temp']

        rdd = batch_df.rdd
        print("========= %s =========" % str(batch_id))
        print("rdd = %s" % str(rdd))

        try:
          if not rdd.isEmpty():
            # Parse the multiple JSON lines from the DStream RDD and trim any extra spaces
            json_raw_lines = rdd \
              .map(lambda row: " ".join(row) + " ") \
              .aggregate("", lambda a,b: a+b, lambda a,b: a+b)
            json_processed_lines = re.sub(r"\s+", "", json_raw_lines, flags=re.UNICODE)
            print("json_processed_lines = %s" % str(json_processed_lines))

            # Convert RDD of the List of multiple JSON lines to Spark SQL Context by first
            #    joining the list of mulitple JSON lines in a new RDD with a single JSON line
            json_rdd = spark.read.json(sc.parallelize([json_processed_lines]))

            # Register the JSON SQL Context as a temporary SQL Table
            print("JSON Schema\n=====")
            json_rdd.printSchema()
            json_rdd.createOrReplaceTempView("iotmsgs_table")

            #############
            # Processing and Analytics go here
            #############

            spark.sql("select payload.data.* from iotmsgs_table order by temperature desc").show(n=100)

            # Sort
            spark.sql("select payload.data.temperature from iotmsgs_table order by temperature desc").show(n=100)

            current_temp = spark.sql("select payload.data.temperature from iotmsgs_table order by temperature desc").collect()[0].temperature
            print("Current temp = " + str(current_temp))
            if (current_temp > max_temp.value):
              max_temp.value = current_temp
            print("Max temp = " + str(max_temp.value))

            # Search
            spark.sql("select payload.data.temperature from iotmsgs_table where payload.data.temperature = 100.0").show(n=100)

            # Filter
            spark.sql("select payload.data.temperature from iotmsgs_table where payload.data.temperature > 85").show(n=100)

            # Clean-up
            spark.dropTempTable("iotmsgs_table")
        # Catch any exceptions
        except:
            pass

    print(">>> Starting DataFrame Select value Processing")
    df_to_strings = df.selectExpr("CAST(value AS STRING)")

    print(">>> Starting DataWriteStream of Value column as Strings")
    query1 = df_to_strings.writeStream \
     .foreachBatch(process) \
     .start()
   
    spark.streams.awaitAnyTermination()

