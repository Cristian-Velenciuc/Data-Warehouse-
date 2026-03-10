from db_connection import connect
import pandas as pd
import os

export_folder = "C:/UM/Yr3/Data Engineering/Classwork/datasets/test_open"
os.makedirs(export_folder, exist_ok=True)

with connect("test_database") as conn:

    tables = pd.read_sql("""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'ingestion'
    """, conn)

    for table_name in tables["TABLE_NAME"]:

        df = pd.read_sql(f"SELECT * FROM ingestion.{table_name}", conn)

        file_path = os.path.join(export_folder, f"{table_name}.xlsx")

        df.to_excel(file_path, index=False)

        os.startfile(file_path)

        print(f"Opened {table_name}.xlsx")