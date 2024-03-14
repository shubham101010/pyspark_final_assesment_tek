from .read_yaml import read_yaml
import sys

def read_from_postgres(spark,source_table):
    try:
        postgres_creds = read_yaml("/app/project/configs/secrets.yaml")
        database_url = postgres_creds["postgres"]["database_url"]
        database_properties = {
                "user":postgres_creds["postgres"]["user"],
                "password": postgres_creds["postgres"]["password"],
                "driver": postgres_creds["postgres"]["driver"]
            }
        tbl_df = spark.read.jdbc(url=database_url,table=source_table,properties=database_properties)
        return tbl_df

    except Exception as e:
        print(f"error occured while reading data from postgress db: {e}")
        sys.exit(1)