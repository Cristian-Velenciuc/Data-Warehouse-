from db_connection import connect
import pandas as pd
import os

contents_folder = r"C:\UM\Yr3\Data Engineering\Classwork\datasets"

with connect("test_database") as conn:

    for root, dirs, files in os.walk(contents_folder):
        for file in files:
            if file.endswith(".csv"):

                table_name = file.replace(".csv", "")
                csv_path = os.path.join(root, file)

                print(f"\nChecking table: {table_name}")

                csv_df = pd.read_csv(csv_path)
                sql_df = pd.read_sql(f"SELECT * FROM ingestion.{table_name}", conn)

            

                csv_df = csv_df.sort_values(by=list(csv_df.columns)).reset_index(drop=True)
                sql_df = sql_df.sort_values(by=list(sql_df.columns)).reset_index(drop=True)

                if csv_df.equals(sql_df):
                    print("✔ Data matches perfectly")
                else:
                    print("✖ Data mismatch detected")

                    # Find differences
                    diff = csv_df.compare(sql_df)

                    if diff.empty:
                        print("Only formatting/type differences (values equal)")
                    else:
                        print("Differences found:")
                        print(diff)