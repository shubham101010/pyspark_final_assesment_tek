import sys
from shared.read_yaml import read_yaml
from shared.column_transformation import column_transformation
from shared.upload_to_s3 import uplaod_to_s3
from shared.write_to_postgress_tbls import write_to_postgres

def write_s3_csv_to_pg(spark,sql_context):
  
  try:
        yaml_data = read_yaml("/app/project/configs/s3_to_pg.yaml")
        bucket_name = yaml_data["source_bucket"]
        source_key = yaml_data["source_file"]
        source_sql = yaml_data["source_sql"]
        target_table = yaml_data["target_table"]

        #Uploading dummy csv data from datasets to s3 folder
        uplaod_to_s3("/app/project/datasets/2022_data_v3.csv",bucket_name,source_key)
        
        s3_url = f"s3a://{bucket_name}/{source_key}"
        print(s3_url)
        df = spark.read.option("header", "true").csv(s3_url)
        new__df = column_transformation(df)
        sql_context.registerDataFrameAsTable(new__df,"csv_tbl")
        res = spark.sql(source_sql)
        write_to_postgres(res,target_table)
        print(f"Data succesfully written to postgress db {target_table}!!!!!!")
  except Exception as e:
        print(f"Error while writing from s3 csv to Postgress: {e}")
        sys.exit(1)
