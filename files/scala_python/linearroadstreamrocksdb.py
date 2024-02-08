from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
from pyspark.sql.functions import col, window, expr
# from pyspark.sql.streaming import StreamingQueryListener
# from pyspark.sql.streaming import Trigger
import sys
import time

import tensorflow as tf

class LinearRoadStreamRocksDB:
    def main(self, args):
        spark = (SparkSession.builder
                 .appName("linear-road-streaming")
                 .config("spark.master", "local[*]")
                 .config("spark.sql.streaming.metricsEnabled", "true")
                 .config("spark.eventLog.enabled", "true")
                 .config("spark.eventLog.dir", "file:///data/history")
                 .config("spark.logConf", "true")
                 .config("spark.sql.adaptive.enabled", "false")
                 .config("spark.sql.codegen.wholeStage", "true")
                 .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 .config("spark.kryoserializer.buffer.max", "2047m")
                 .getOrCreate())

        spark.sparkContext.setLogLevel("ERROR")

        executor_num = int(args[0])
        row_num = int(args[1])
        schedule_mode = args[2]
        query_type = int(args[3])
        amount_num = args[4]
        total_latency_per_batch = 0.0

        eventSchema = (StructType()
                       .add("type", IntegerType())
                       .add("time", IntegerType())
                       .add("vehicleId", IntegerType())
                       .add("speed", IntegerType())
                       .add("highway", IntegerType())
                       .add("lane", IntegerType())
                       .add("direction", IntegerType())
                       .add("segment", IntegerType())
                       .add("position", IntegerType())
                       .add("qid", IntegerType())
                       .add("init", IntegerType())
                       .add("end", IntegerType())
                       .add("dow", IntegerType())
                       .add("tod", IntegerType())
                       .add("day", IntegerType())
                       .add("timestamp", TimestampType()))


        linearRoad_input_path = "/home/sej/workspace/csv_data/"
        fileInputDF = (spark.readStream
                       .option("failOnDataLoss", False)
                       .schema(eventSchema)
                       .csv(linearRoad_input_path))

        inputDF = fileInputDF

        segSpeedDF = (inputDF
                      .select("vehicleId", "speed", "highway", "lane", "direction", "segment", "timestamp"))

        aDF = (segSpeedDF
               .select(window(col("timestamp"), "30 seconds", "5 seconds").alias("aDF_window"),
                       col("timestamp").alias("aDF_timestamp"),
                       col("vehicleId").alias("aDF_vehicleId"),
                       col("speed").alias("aDF_speed"),
                       col("highway").alias("aDF_highway"),
                       col("lane").alias("aDF_lane"),
                       col("direction").alias("aDF_direction"),
                       col("segment").alias("aDF_segment"))
               .withWatermark("aDF_timestamp", "30 seconds"))

        lDF = (segSpeedDF
               .select(col("timestamp").alias("lDF_timestamp"),
                       col("vehicleId").alias("lDF_vehicleId"),
                       col("speed").alias("lDF_speed"),
                       col("highway").alias("lDF_highway"),
                       col("lane").alias("lDF_lane"),
                       col("direction").alias("lDF_direction"),
                       col("segment").alias("lDF_segment")))

        vehicleSegEntryDF = (aDF
                             .join(lDF, expr("aDF_vehicleId = lDF_vehicleId"))
                             .select(col("aDF_window").alias("Q2_window"),
                                     col("lDF_timestamp").alias("Q2_timestamp"),
                                     col("lDF_vehicleId").alias("Q2_vehicleId"),
                                     col("lDF_speed").alias("Q2_speed"),
                                     col("lDF_highway").alias("Q2_highway"),
                                     col("lDF_lane").alias("Q2_lane"),
                                     col("lDF_direction").alias("Q2_direction"),
                                     col("lDF_segment").alias("Q2_segment"))
                             .writeStream
                             .queryName("linear-road-Q2")
                             .format("console")
                             .option("truncate", False)
                             .option("numRows", 10)
                             .option("checkpointLocation", "/data/checkpoint/q2")
                             .trigger(processingTime='5 seconds')
                             .outputMode("append")
                             .start())


# Listener setup
        """
        class QueryListener(StreamingQueryListener):
            def __init__(self, fileOutResult):
                self.fileOutResult = fileOutResult

            def onQueryStarted(self, event):
                print(f"******************Query started: {event.id}******************************************")

            def onQueryProgress(self, event):
                print("ssss")
            # ... [your logging logic here, similar to the Scala code]

            def onQueryTerminated(self, event):
                print(f"******************Query terminated: {event.id}******************************************")

        resultFileName = f"/home/sej/workspace/kafka_logs/result-Exec_{executor_num}-Rows_{row_num}-Schedule_{schedule_mode}-Querytype_{query_type}-TaskResource_{amount_num}.txt"
        with open(resultFileName, 'w') as fileOutResult:
            spark.streams.addListener(QueryListener(fileOutResult))
        """
        #print(tf.__version__)
        mnist = tf.keras.datasets.mnist

        (x_train, y_train), (x_test, y_test) = mnist.load_data()
        x_train, x_test = x_train / 255.0, x_test / 255.0
        
        model = tf.keras.models.Sequential([
            tf.keras.layers.Flatten(input_shape=(28, 28)),
            tf.keras.layers.Dense(128, activation='relu'),
            tf.keras.layers.Dropout(0.2),
            tf.keras.layers.Dense(10)
        ])
        
        loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
        
        model.compile(optimizer='adam',
              loss=loss_fn,
              metrics=['accuracy'])
        
        model.fit(x_train, y_train, epochs=5)
        
        model.evaluate(x_test,  y_test, verbose=2)
        
        while(1):True
        vehicleSegEntryDF.awaitTermination()
        spark.sqlContext.clearCache()
        # for maintaining web UI
        time.sleep(86400)
        spark.stop()

if __name__ == "__main__":
    app = LinearRoadStreamRocksDB()
    app.main(sys.argv[1:])
