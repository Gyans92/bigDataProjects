#!/bin/bash
set -e
mvn package
spark-submit --class edu.ucr.cs.cs226.gprak001.SparkRDD --master local[8] ./target/SparkRDD-1.0-SNAPSHOT.jar "$1"