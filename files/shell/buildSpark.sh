#!/bin/bash

#spark-submit execution file path check
sudo ./build/mvn -pl :spark-core_2.12,:spark-sql_2.12 -DskipTests -X clean install -rf :spark-sql_2.12

cd spark
sudo ./build/mvn -pl :spark-core_2.12,:spark-sql_2.12 -DskipTests -X clean install
sudo cp ./sql/core/target/spark-sql_2.12-3.2.3.jar ./assembly/target/scala-2.12/jars/
sudo cp ./core/target/spark-core_2.12-3.2.3.jar ./assembly/target/scala-2.12/jars/
cd ..