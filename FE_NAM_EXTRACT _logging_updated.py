import mysql.connector
import csv
import os
import datetime as dt
import logging
import psycopg2
import glob
# Set up dates
todayDate = dt.date.today()
yesterdayDate = todayDate - dt.timedelta(days=1)
cleanupDate = todayDate - dt.timedelta(days=1)
cleanupDate = cleanupDate.strftime("%Y%m%d")
todayDate = todayDate.strftime("%Y%m%d")
yesterdayDate = yesterdayDate.strftime("%Y%m%d")
# Set up logging
log_path = os.path.abspath(__file__)
log_file_name = (os.path.basename(log_path))[:-3] + '_' + todayDate
logging.basicConfig(
    level=logging.INFO,
    filename='E:\\BirstSourceData\\TiVo_Mart_Sources\\PRD\\PostgresPipeline\\Logs\\' + log_file_name + '.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# MySQL connection details
mysql_host = 'feprovods-tpa1-prod.cluster-caay9ochkaph.us-west-2.rds.amazonaws.com'
mysql_user = 'birst_production_us' 
mysql_password = 'birstproductionus1999$' 
mysql_database = 'feprovods_production' 
try:
    # MySQL Connection
    logging.info("Connecting to MySQL database.")
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database)
    mysql_cursor = mysql_conn.cursor()
    logging.info("Connected to MySQL database successfully.")
    # MySQL queries
    mysql_queries = {
        'stg_transientFeDeviceHistory': "SELECT bodyId, hardwareSerialNumber, caDeviceId, deviceType, manufacturedDate, serviceState, updateDate, activationDate, partnerId, accountAnonExternalId FROM transientFeDeviceHistory",
        'stg_transientFeDeviceResolvedFav': "SELECT bodyId, globalExternalName, value, updateDate FROM transientFeDeviceResolvedFAV",
        'stg_transientFeMfgDeviceHistory': "SELECT tsn, hsn, manufacturedDate, partnerId, hostId1, hostId2, skuNumber, skuDescription, updateDate FROM transientFeMfgDeviceHistory",
        'stg_transientFeAccountHistory': "SELECT accountAnonExternalId, partnerCustomerId, tiVoCustomerId, partnerId, mak, optStatus, updateDate FROM transientFeAccountHistory"
    }
    # Execute MySQL Queries
    for mysql_query_key, mysql_query_value in mysql_queries.items():
        logging.info(f"Executing query for {mysql_query_key}.")
        mysql_cursor.execute(mysql_query_value)
        csv_file_path = f'E:\\BirstSourceData\\TiVo_Mart_Sources\\PRD\\PostgresPipeline\\FEProvODS\\{mysql_query_key}_{todayDate}.csv'
        logging.info(f"Writing data to CSV file: {csv_file_path}.")
        # Write MySQL Data to CSV
        with open(csv_file_path, 'w', newline='', encoding='utf8') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow([i[0] for i in mysql_cursor.description])  # Write column headers
            csvwriter.writerows(mysql_cursor)
        logging.info(f"Data for {mysql_query_key} written to {csv_file_path} successfully.")
    # Fetch list of CSV files
    files = glob.glob(f'E:\\BirstSourceData\\TiVo_Mart_Sources\\PRD\\PostgresPipeline\\FEProvODS\\*{todayDate}.csv')
    logging.info(f"Found {len(files)} CSV files for processing: {files}")
    # PostgreSQL connection details
    logging.info("Connecting to PostgreSQL database.")
    conn = psycopg2.connect(
        host="10.150.136.36",
        database="businessIntelligenceDB",
        user="postgres",
        password="@Welcome2Post"
    )
    logging.info("Connected to PostgreSQL database successfully.")
    SchemaName = 'deviceSchema'
    # Create a cursor object
    cur = conn.cursor()
    # Load each file into PostgreSQL
    for file in files:
        table_name = file[68:68 + file[68:].index('_' + todayDate)]
        abspath = "\\\\morphed.corporate.local" + file[2:]
        logging.info(f"Processing file {file}. Loading data into table {SchemaName}.{table_name}.")
        try:
            # Truncate the table before loading new data
            cur.execute(f"""TRUNCATE TABLE "{SchemaName}"."{table_name}";""")
            logging.info(f"Truncated table {SchemaName}.{table_name}.")
            # Copy data from the CSV file to the PostgreSQL table
            cur.execute(f"""COPY "{SchemaName}"."{table_name}" FROM '{abspath}' DELIMITER ',' CSV HEADER;""")
            logging.info(f"Data copied from {file} to {SchemaName}.{table_name} successfully.")
            conn.commit()
        except Exception as e:
            logging.error(f"Error processing file {file}: {e}")
            conn.rollback()
    # Close the cursor and connection
    cur.close()
    conn.close()
    logging.info("Closed PostgreSQL connection.")
    mysql_cursor.close()
    mysql_conn.close()
    logging.info("Closed MySQL connection.")
except Exception as ex:
    logging.error(f"An error occurred: {ex}")