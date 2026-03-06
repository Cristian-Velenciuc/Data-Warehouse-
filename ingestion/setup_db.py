from db_connection import connect

#CREATE SCHEMA IF IT DOESNT EXIST
with connect("test_database") as conn:
    cur = conn.cursor()

    cur.execute("""
    IF NOT EXISTS (
        SELECT * FROM sys.schemas WHERE name = 'ingestion'
    )
    EXEC('CREATE SCHEMA ingestion');
    """)

    conn.commit()

#CREATE TABLES IF THEY DONT EXIST YET
