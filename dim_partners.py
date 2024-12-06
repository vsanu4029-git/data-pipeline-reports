import psycopg2
try:
    conn = psycopg2.connect(
        host="10.150.136.36",
        database="businessIntelligenceDB",
        user="postgres",
        password="@Welcome2Post"

    )
    cur = conn.cursor()
    schema_name=' "deviceSchema" '
    table_name="dim_partners"

    csv_file_path = r"C:\Users\BIRSTSA\Desktop\src_partner.csv"

    cur.execute(f"TRUNCATE TABLE {schema_name}.{table_name};")
    conn.commit()

    with open(csv_file_path, 'r') as file:
        cur.copy_expert(f"COPY {schema_name}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", file)
        conn.commit()

except psycopg2.Error as e:
    print(f"An error occured with DB: {e}")

except FileNotFoundError:
    print(f"Error: The file {csv_file_path} not found.")

except Exception as e:
    print(f"An unexpected error: {e}")

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()

print("SUCCESSFUL")
