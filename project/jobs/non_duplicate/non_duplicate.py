import sys
from shared.write_to_postgress_tbls import write_to_postgres

def remove_duplicates(spark):
    try:
        print("Remove Duplicates and write to Postgress db")
        data = [["ab","bc","cd"],["ab","nm","dd"],["ab","bc","cd"],["ab","nm","dd"],["eff","ddw","ddw"]]
        cols = ["colA","colB","colC"]
        df = spark.createDataFrame(data,cols)
        df.registerDataFrameAsTable(df,"tbl1")
        res = spark.sql("""
                        select colA,colB,colC
                        from tbl1 
                        group by colA,colB,colC 
                        having count(*)==1
                    """)
        write_to_postgres(res,"non_duplicate_tbl")
    except Exception as e:
        print(f"Error occured while remove duplicates: {e}")        
        sys.exit(1)