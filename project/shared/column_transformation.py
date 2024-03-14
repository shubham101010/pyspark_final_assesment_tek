# function for Transforming column names by replacing spaces with underscores.
import sys

def column_transformation(dataframe):
    try:
        columns = dataframe.columns
        columns_with_underscores = [col.replace(" ","_").lower() for col in columns]
        for new_col,old_col in zip(columns_with_underscores,dataframe.columns):
            dataframe = dataframe.withColumnRenamed(old_col,new_col)
        return dataframe
    except Exception as e:
        print(f"Error occured while column transformations: {e}")
        sys.exit(1)