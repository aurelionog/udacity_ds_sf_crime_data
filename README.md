Udactity Data Stream Nanodegree project

Questions:

1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
We can see increase or decrease by changing processedRowsPerSecond and/or inputRowsPerSecond


2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

To find the right balance for processedRowsPerSecond by knowing the data and running the job we can modify following settings get fine tuning:

spark.default.parallelism: total number of cores on all executor nodes.

spark.sql.shuffle.partitions: number of partitions to use when shuffling data for joins or aggregations.

spark.streaming.kafka.maxRatePerPartition: maximum rate at which data will be read from each Kafka partition.