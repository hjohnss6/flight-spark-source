FROM maven:3.8.2-jdk-11 as builder

COPY . .

RUN mvn clean package

# ----

FROM jupyter/all-spark-notebook:spark-3.2.1

COPY --from=builder /target/flight-spark-source-1.0-SNAPSHOT-shaded.jar .
