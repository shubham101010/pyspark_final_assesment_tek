import sys
from shared.read_yaml import read_yaml
from shared.read_postgress_tbls import read_from_postgres
import glob
import subprocess
import os
import datetime


#Function by default recieves file format as csv when no format is passed as 
def write_pg_to_custom_file_format(spark,file_format="csv"):
    try:
        #Reading configurations as a yaml file
        yaml_data = read_yaml("/app/project/configs/pg_to_s3_parquet.yaml")
        source_tbl = yaml_data["source_table"]
        bucket_name = yaml_data["target_bucket"]
        num_of_partitions = yaml_data["num_of_partions"]
        
        df = read_from_postgres(spark,source_tbl)

        start_time = datetime.datetime.now()
        if file_format=="csv":
            df.repartition(num_of_partitions).write.mode("overwrite").csv(f"/app/project/{file_format}")
        
        if file_format=="parquet":
            df.repartition(num_of_partitions).write.mode("overwrite").parquet(f"/app/project/{file_format}")

        files = glob.glob(os.path.join(f"/app/project/{file_format}","*.{file_format}"))
        for file_path in files:
            current_datetime = datetime.datetime.now().strftime("%Y_%d-%m-%Y_%H:%M:%S")
            print(current_datetime)
            new_file = f"2022_csv_data_{current_datetime}.{file_format}"
            s3_url = f"s3://{bucket_name}/{file_format}/{new_file}"
            aws_cli = f"aws s3 mv {file_path} {s3_url}"
            subprocess.run(aws_cli,shell=True,check=True)
            print(f"{file_path} to s3 completed!!! ")
        total_time = start_time - datetime.datetime.now()
        print(f"Executed code in {total_time}---------------------------------------------")
    except Exception as e:
        print(f"Error occured while writing from pg to s3 parquet: {e}")
        sys.exit(1)