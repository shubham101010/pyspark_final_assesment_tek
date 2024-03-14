from shared.read_yaml import read_yaml
from shared.read_postgress_tbls import read_from_postgres
import glob
import subprocess
import os
import datetime
import sys

def write_pg_to_s3_parquet(spark):
    try:
        #Reading configurations as a yaml file
        yaml_data = read_yaml("/app/project/configs/pg_to_s3_parquet.yaml")
        source_tbl = yaml_data["source_table"]
        bucket_name = yaml_data["target_bucket"]
        num_of_partitions = yaml_data["num_of_partions"]

        df = read_from_postgres(spark,source_tbl)
        df.repartition(num_of_partitions).write.mode("overwrite").parquet("/app/project/parquet")
        parquet_files = glob.glob(os.path.join("/app/project/parquet","*.parquet"))
        for file_path in parquet_files:
            current_datetime = datetime.datetime.now().strftime("%Y_%d-%m-%Y_%H:%M:%S")
            print(current_datetime)
            new_file = f"2022_csv_data_{current_datetime}.parquet"
            s3_url = f"s3://{bucket_name}/parquet/{new_file}"
            aws_cli = f"aws s3 mv {file_path} {s3_url}"
            subprocess.run(aws_cli,shell=True,check=True)
            print(f"{file_path} to s3 completed!!! ")
    except Exception as e:
        print(f"Error occured while writing from pg to s3 parquet: {e}")
        sys.exit(1)