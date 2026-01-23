docker run -it --rm \
  --user root \
  -v "$(pwd)":/app \
  apache/spark-py /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /app/job_spark.py