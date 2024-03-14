#Pyspark submit command to execute the job by assigning job argument
spark-submit \
  --jars /opt/spark/jars/postgresql-connector.jar \
  --packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 \
  /app/project/main.py 
  --job remove_special_characters
