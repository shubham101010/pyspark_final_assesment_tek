import sys
from shared.write_to_postgress_tbls import write_to_postgres
from shared.clean_special_characters import remove_special_characters
from pyspark.sql.types import StringType

def clean_special_characters_with_udf(spark,sql_func):
    try:
        # Register the UDF
        remove_special_characters_udf = sql_func.udf(remove_special_characters, StringType())
        data = [["ab//","bc####","cdl::"],["ab","n''m","dd"],["a???b","bc","cd"],["ab","nm","d;;d"],["eff","d././dw","dd]]w"]]
        cols = ["colA","colB","colC"]
        df = spark.createDataFrame(data,cols)
        columns = df.columns
        for col in columns:
            df = df.withColumn(col,remove_special_characters_udf(sql_func.col(col)))
        write_to_postgres(df,"non_special_characters_tbl")
    except Exception as e:
        print("Error occured while cleaning special character with udf")
        sys.exit(1)
