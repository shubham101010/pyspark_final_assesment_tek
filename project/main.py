from pyspark.sql import SparkSession, SQLContext
import pyspark.sql.functions as sql_func
from jobs.non_duplicate.non_duplicate import remove_duplicates
from jobs.clean_special_characters import clean_special_characters
from jobs.clean_special_characters_with_udf import clean_special_characters_with_udf
from jobs.write_s3_csv_to_pg.write_s3_csv_to_pg import write_s3_csv_to_pg
from jobs.write_pg_to_s3_parquet.write_pg_to_s3_parquet import write_pg_to_s3_parquet
from jobs.write_pg_to_custom_file_format import write_pg_to_custom_file_format
from shared.read_yaml import read_yaml
import argparse

class PySparkJobRunner:
    def __init__(self):
        # Initialize SparkSession and SQLContext
        self.spark = SparkSession.builder.appName("pyspark-jobs").getOrCreate()
        self.sql_context = SQLContext(self.spark)
        
        # Read secrets from YAML file
        self.secrets_data = read_yaml("/app/project/configs/secrets.yaml")
        self.access_key = self.secrets_data["aws"]["aws_access_key_id"]
        self.secret_key = self.secrets_data["aws"]["aws_secret_access_key"]
        
        # Set AWS access key and secret key for S3 access
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", self.access_key)
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", self.secret_key)

    def run_job(self, job_name):
        # Execute different jobs based on the job_name argument

        #job for remove duplicate records
        if job_name == "remove_duplicate_records":
            print(f"Executing {job_name} job.............. ")
            remove_duplicates(self.spark)
        
        #job for remove special characters
        elif job_name == "remove_special_characters":
            print(f"Executing {job_name} job.............. ")
            clean_special_characters(self.spark, sql_func)
        
        #job for remove special characters with udf 
        elif job_name == "remove_special_characters_with_udf":
            print(f"Executing {job_name} job.............. ")
            clean_special_characters_with_udf(self.spark,sql_func)
        
        #job for writing data from AWS s3 bucket csv file to Postgres DB
        elif job_name == "write_aws_s3_csv_to_pg":
            print(f"Executing {job_name} job.............. ")
            write_s3_csv_to_pg(self.spark, self.sql_context)
        
        #job for writing postgres data as source to S3 bucket in parquet format
        elif job_name == "write_pg_to_aws_s3_parquet":
            print(f"Executing {job_name} job.............. ")
            write_pg_to_s3_parquet(self.spark)

        elif job_name == "write_pg_to_aws_s3_custom_file_format":
            print(f"Executing {job_name} job............. ")
            write_pg_to_custom_file_format(self.spark,"parquet")
            

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("job", type=str)
    args = parser.parse_args()
    job_args = args.job

    # Create PySparkJobRunner instance and execute the specified job
    job_runner = PySparkJobRunner()
    job_runner.run_job(job_args)
