import sys
from shared.write_to_postgress_tbls import write_to_postgres

def clean_special_characters(spark,sql_func):
    try:
        print("Remove special characters using regular expression and write it to Postgress DB")
        data = [["ab//","bc####","cdl::"],["ab","n''m","dd"],["a???b","bc","cd"],["ab","nm","d;;d"],["eff","d././dw","dd]]w"]]
        cols = ["colA","colB","colC"]
        df = spark.createDataFrame(data,cols)
        df_columns = df.columns
        for col in df_columns:
            df = df.withColumn(col,sql_func.regexp_replace(sql_func.col(col),"[^a-zA-Z0-9]", ""))
        write_to_postgres(df,"non_special_characters_tbl")
    except Exception as e:
        print(f"Error occured while cleaning special characters: {e}")
        sys.exit(1)