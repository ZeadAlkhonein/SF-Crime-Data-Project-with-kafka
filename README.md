# SF-Crime-Data-Project-with-kafka

### Q1 : How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

when i changed spark.default.parallelism to 100 i have seen the throuput increased to tenfold. 
and also spark.sql.shuffle.partitions when i put it to 10 it increased to triple

### Q2 : What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

like i have written above the spark.default.parallelism is the most effected one. 
followed by spark.sql.shuffle.partitions 
