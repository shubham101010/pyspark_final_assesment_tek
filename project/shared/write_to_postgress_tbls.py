from .read_yaml import read_yaml
import sys

#Function which writes to a Postgress DB
def write_to_postgres(res,target_table,mode="append"):
    try:
        postgres_creds = read_yaml("/app/project/configs/secrets.yaml")
        database_url = postgres_creds["postgres"]["database_url"]
        database_properties = {
                "user":postgres_creds["postgres"]["user"],
                "password": postgres_creds["postgres"]["password"],
                "driver": postgres_creds["postgres"]["driver"]
            }
        res.write.mode(mode).jdbc(url=database_url,table=target_table,properties=database_properties)
        print(f"Data is succesfully written Postgress table {target_table}!!!!!!!!!")
    except Exception as e:
        print(f"error occured while writing data to postgress db: {e}")
        sys.exit(1)
