FROM apache/spark-py:v3.2.4

COPY target/flight-spark-source-1.0-SNAPSHOT-shaded.jar .
