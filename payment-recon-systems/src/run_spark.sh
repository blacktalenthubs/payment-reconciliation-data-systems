#!/usr/bin/env bash
#
# run_spark.sh
#
# Usage:
#   ./run_spark.sh <your_spark_script.py>
#
# This script sets a specific JAVA_HOME (if you want to enforce a certain Java version),
# then runs spark-submit with the Kafka connector and some Parquet config flags,
# finally appending your script as the entry point.
#
# Example:
#   ./run_spark.sh batch.py
#

# 1) Check for an argument (the Python script)
if [ -z "$1" ]; then
  echo "ERROR: No Spark script specified."
  echo "Usage: ./run_spark.sh <script.py>"
  exit 1
fi

SPARK_SCRIPT="$1"

# 2) Optionally set JAVA_HOME to a known valid JDK (e.g., OpenJDK 17).
#    Adjust the path if your system's JDK is in another location.
export JAVA_HOME="/opt/homebrew/Cellar/openjdk@17/17.0.13/libexec/openjdk.jdk/Contents/Home"

# Confirm the java binary is present
if [ ! -f "$JAVA_HOME/bin/java" ]; then
  echo "ERROR: Java not found at $JAVA_HOME"
  echo "Please update JAVA_HOME in this script or install Java 17."
  exit 1
fi

echo "Using JAVA_HOME=$JAVA_HOME"
"$JAVA_HOME/bin/java" -version

# 3) Now run spark-submit with the Kafka connector + Parquet options
#    Adjust the Spark version and package if needed.

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.sql.parquet.filterPushdown=false \
  "$SPARK_SCRIPT"
