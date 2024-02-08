name := "LinearRoadStreamRocksDB"
version := "0.1"
scalaVersion := "2.12.15"

// Spark 3.2.3
val sparkVersion = "3.2.3"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-sql-kafka-0-10",
  "org.apache.spark" %% "spark-streaming-kafka-0-10"
).map(_ % sparkVersion)

// kafka
val kafkaVersion = "3.2.3"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion

// spark-cassandra-connector
//val sparkCassandraConnectorVersion = "3.0.0-beta"
//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion

// https://mvnrepository.com/artifact/com.nvidia/rapids-4-spark
libraryDependencies += "com.nvidia" %% "rapids-4-spark" % "22.10.0"

// https://mvnrepository.com/artifact/ai.rapids/cudf
libraryDependencies += "ai.rapids" % "cudf" % "22.10.0"
