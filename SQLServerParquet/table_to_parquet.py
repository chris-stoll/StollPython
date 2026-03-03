import sys
import os
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq

def sanitize_filename(name: str) -> str:
    return name.replace("[", "").replace("]", "").replace(".", "_")

def main():
    if len(sys.argv) < 2:
        print("Usage: python sqlserver_table_to_parquet.py <schema.table or table>")
        sys.exit(1)

    table_input = sys.argv[1]

    # ---- CONFIG (from env) ----
    SERVER = os.getenv("SQLSERVER_HOST")
    DATABASE = os.getenv("SQLSERVER_DB")
    USER = os.getenv("SQLSERVER_USER")
    PASSWORD = os.getenv("SQLSERVER_PASS")
    BATCH_SIZE = 50000

    if not all([SERVER, DATABASE, USER, PASSWORD]):
        print("Missing env vars: SQLSERVER_HOST, SQLSERVER_DB, SQLSERVER_USER, SQLSERVER_PASS")
        sys.exit(1)

    CONN_STR = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USER};"
        f"PWD={PASSWORD};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    # ----------------------------

    # Parse schema + table
    if "." in table_input:
        schema, table = table_input.split(".", 1)
    else:
        schema = "dbo"
        table = table_input

    full_table = f"[{schema}].[{table}]"
    output_file = sanitize_filename(f"{schema}.{table}") + ".parquet"

    print(f"📤 Exporting {full_table} → {output_file}")

    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {full_table}")
    columns = [col[0] for col in cursor.description]

    writer = None
    total_rows = 0

    while True:
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        # Convert batch to Arrow table
        batch_dict = {col: [] for col in columns}
        for row in rows:
            for i, value in enumerate(row):
                batch_dict[columns[i]].append(value)

        table_arrow = pa.Table.from_pydict(batch_dict)

        if writer is None:
            writer = pq.ParquetWriter(output_file, table_arrow.schema)

        writer.write_table(table_arrow)
        total_rows += len(rows)
        print(f"   ➜ {total_rows} rows exported")

    if writer:
        writer.close()

    cursor.close()
    conn.close()

    print(f"✅ Export complete: {output_file}")

if __name__ == "__main__":
    main()