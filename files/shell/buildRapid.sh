#!/bin/bash

cd rapids_nnStream
sudo mvn clean install -Dbuildver=323 -DskipTests -Drat.skip=true -DallowConventionalDistJar=true -X
sudo cp ./dist/target/rapids-4-spark_2.12-23.06.0-cuda11.jar /opt/rapid/ 
