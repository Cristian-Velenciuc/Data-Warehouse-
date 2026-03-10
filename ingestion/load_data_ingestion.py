from db_connection import connect
import os
import pandas as pd

contents_folder = "C:/UM/Yr3/Data Engineering/Classwork/datasets"

with connect("test_database") as conn:
    cur = conn.cursor()
    cur.fast_executemany = True

    for root, dirs, files in os.walk(contents_folder):
        for file in files:
            if file.endswith(".csv"):
                print(f"Loading file: {file}")

                full_path = os.path.join(root, file)

                table_variables = pd.read_csv(full_path)
                table_variables = table_variables.astype(object).where(pd.notnull(table_variables), None)

                rows = table_variables.values.tolist()

                num_cols = len(table_variables.columns)
                placeholders = ", ".join(["?"] * num_cols)

                table_name = file.replace(".csv", "")
                cols = ", ".join(table_variables.columns)

                #Clear ingestion table so we don't have duplicates
                truncate_query = f"TRUNCATE TABLE ingestion.{table_name}"
                cur.execute(truncate_query)

                query = f"INSERT INTO ingestion.{table_name} ({cols}) VALUES ({placeholders})"

                cur.executemany(query, rows)
                conn.commit()

                print(f"{len(rows)} rows inserted into ingestion.{table_name}")