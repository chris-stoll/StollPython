import sys
import os
import re
import pyarrow.parquet as pq
import pyarrow as pa
import pyodbc

def sanitize_table_name(filename: str) -> str:
    name = os.path.splitext(os.path.basename(filename))[0].lower()
    name = re.sub(r'[^a-z0-9_]+', '_', name)
    name = re.sub(r'_+', '_', name).strip('_')
    if name and name[0].isdigit():
        name = f"t_{name}"
    return name[:128]

def arrow_to_sql_type(field: pa.Field):
    t = field.type

    if pa.types.is_int8(t) or pa.types.is_uint8(t):
        return "TINYINT"
    if pa.types.is_int16(t) or pa.types.is_uint16(t):
        return "SMALLINT"
    if pa.types.is_int32(t) or pa.types.is_uint32(t):
        return "INT"
    if pa.types.is_int64(t) or pa.types.is_uint64(t):
        return "BIGINT"

    if pa.types.is_float16(t) or pa.types.is_float32(t) or pa.types.is_float64(t):
        return "FLOAT"

    if pa.types.is_decimal(t):
        return f"DECIMAL({t.precision}, {t.scale})"

    if pa.types.is_boolean(t):
        return "BIT"

    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "NVARCHAR(MAX)"

    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "VARBINARY(MAX)"

    if pa.types.is_timestamp(t):
        return "DATETIME2"

    if pa.types.is_date32(t) or pa.types.is_date64(t):
        return "DATE"

    return "NVARCHAR(MAX)"

def main():
    if len(sys.argv) < 2:
        print("Usage: python parquet_to_sqlserver_schema_aware_autotable_sqlauth_drop.py <parquet_file>")
        sys.exit(1)

    parquet_path = sys.argv[1]
    if not os.path.exists(parquet_path):
        print(f"File not found: {parquet_path}")
        sys.exit(1)

    # ---- CONFIG (from env) ----
    SERVER = os.getenv("SQLSERVER_HOST")
    DATABASE = os.getenv("SQLSERVER_DB")
    USER = os.getenv("SQLSERVER_USER")
    PASSWORD = os.getenv("SQLSERVER_PASS")
    SCHEMA = "dbo"
    BATCH_SIZE = 5000

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

    table_name = sanitize_table_name(parquet_path)
    print(f"📛 Auto-mapped table name: {SCHEMA}.{table_name}")

    parquet_file = pq.ParquetFile(parquet_path)
    schema = parquet_file.schema_arrow

    columns = []
    for field in schema:
        sql_type = arrow_to_sql_type(field)
        nullable = "NULL" if field.nullable else "NOT NULL"
        columns.append(f"[{field.name}] {sql_type} {nullable}")

    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    drop_sql = f"""
    IF OBJECT_ID('{SCHEMA}.{table_name}', 'U') IS NOT NULL
        DROP TABLE {SCHEMA}.{table_name};
    """

    print("💣 Dropping existing table if it exists...")
    cursor.execute(drop_sql)
    conn.commit()

    create_sql = f"""
    CREATE TABLE {SCHEMA}.{table_name} (
        {", ".join(columns)}
    );
    """

    print("🛠️  Creating table...")
    cursor.execute(create_sql)
    conn.commit()

    column_names = [f"[{f.name}]" for f in schema]
    placeholders = ", ".join(["?"] * len(schema))
    insert_sql = f"""
        INSERT INTO {SCHEMA}.{table_name} ({", ".join(column_names)})
        VALUES ({placeholders})
    """

    print("🚚 Inserting data in batches...")
    cursor.fast_executemany = True

    total = 0
    for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
        rows = batch.to_pylist()
        values = [tuple(row.get(col) for col in schema.names) for row in rows]
        cursor.executemany(insert_sql, values)
        conn.commit()
        total += len(values)
        print(f"   ➜ {total} rows inserted")

    print(f"✅ Import complete: {SCHEMA}.{table_name}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()