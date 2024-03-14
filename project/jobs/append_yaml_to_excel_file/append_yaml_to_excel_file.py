from shared.read_yaml import read_yaml
import openpyxl
import glob
import os
import sys

def append_yaml_to_excel_file(spark,output_path,filedir):
    try:
        yaml_files = glob.glob(os.path.join(filedir, "*.yaml"))
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        header_written = False

        for file_path in yaml_files:
            data = read_yaml(file_path)
            temp_df = spark.createDataFrame([data])
            if(not header_written):
                sheet.append(temp_df.columns)
                header_written = True

            for row_data in temp_df.collect():
                sheet.append(row_data)

        workbook.save(output_path)
    except Exception as e:
        print(f"Error occured while appending yaml to excel file: {e}")
