# Arrow flight (server)

This is a small arrow flight server written in Python.

It is just meant for exploration.


## Query with Spark (include Jar!)

Assuming that server is running on network.

```python
df = spark.read.format('cdap.org.apache.arrow.flight.spark').option("uri", "grpc://127.0.0.1:8815").load("iris_small.parquet")
```



## Refs

- [Arrow flight cookbook](https://arrow.apache.org/cookbook/py/flight.html)
