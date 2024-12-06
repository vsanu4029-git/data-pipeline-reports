import mysql.connector 
import csv 
import os
import datetime as dt
import logging
import psycopg2
import datetime as dt
import glob


todayDate=dt.date.today()
yesterdayDate=todayDate - dt.timedelta(days=1)
cleanupDate=todayDate - dt.timedelta(days=1)
cleanupDate=cleanupDate.strftime("%Y%m%d")
todayDate=todayDate.strftime("%Y%m%d")
yesterdayDate=yesterdayDate.strftime("%Y%m%d")

log_path = os.path.abspath(__file__)
log_file_name = (os.path.basename(log_path))[:-3]+'_'+todayDate
#print(log_file_name)

logging.basicConfig(

    level = logging.INFO,
    filename = 'E:\\BirstSourceData\\TiVo_Mart_Sources\\PRD\\PostgresPipeline\\Logs\\'+log_file_name+'.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)


mysql_host = 'feprovods-tpa1-prod.cluster-caay9ochkaph.us-west-2.rds.amazonaws.com'
mysql_user = 'birst_production_us' 
mysql_password = 'birstproductionus1999$' 
mysql_database = 'feprovods_production' 


try:
#MySQL Connection 
    mysql_conn = mysql.connector.connect( 
        host=mysql_host, 
        user=mysql_user, 
        password=mysql_password, 
        database=mysql_database )
        
    mysql_cursor = mysql_conn.cursor()
    mysql_queries={}
    mysql_queries['stg_trasientFeDeviceHistory'] = "select transientFeDeviceHistory.bodyId,transientFeDeviceHistory.hardwareSerialNumber,transientFeDeviceHistory.caDeviceId,transientFeDeviceHistory.deviceType,transientFeDeviceHistory.manufacturedDate,transientFeDeviceHistory.serviceState,transientFeDeviceHistory.updateDate,transientFeDeviceHistory.activationDate,transientFeDeviceHistory.partnerId,transientFeDeviceHistory.accountAnonExternalId from transientFeDeviceHistory"
    mysql_queries['stg_transientFeDeviceResolvedFav'] = "select transientFeDeviceResolvedFAV.bodyId, transientFeDeviceResolvedFAV.globalExternalName, transientFeDeviceResolvedFAV.value, transientFeDeviceResolvedFAV.updateDate from transientFeDeviceResolvedFAV"
    mysql_queries['stg_transientFeMfgDeviceHistory'] = "select transientFeMfgDeviceHistory.tsn, transientFeMfgDeviceHistory.hsn, transientFeMfgDeviceHistory.manufacturedDate, transientFeMfgDeviceHistory.partnerId, transientFeMfgDeviceHistory.hostId1, transientFeMfgDeviceHistory.hostId2, transientFeMfgDeviceHistory.skuNumber, transientFeMfgDeviceHistory.skuDescription, transientFeMfgDeviceHistory.updateDate from transientFeMfgDeviceHistory"
    mysql_queries['stg_transientFeAccountHistory']= "select transientFeAccountHistory.accountAnonExternalId, transientFeAccountHistory.partnerCustomerId, transientFeAccountHistory.tiVoCustomerId, transientFeAccountHistory.partnerId, transientFeAccountHistory.mak, transientFeAccountHistory.optStatus, transientFeAccountHistory.updateDate from transientFeAccountHistory"
    
    # Execute MySQL Query
    for mysql_query_key, mysql_query_value in mysql_queries.items():
        mysql_cursor.execute(mysql_query_value)
        snowflake_table = mysql_query_key
        csv_file_path = 'E:\\BirstSourceData\\TiVo_Mart_Sources\\PRD\\PostgresPipeline\\FEProvODS\\'+mysql_query_key+'_'+todayDate+'.csv'
    # Write MySQL Data to CSV 
        with open(csv_file_path, 'w', newline='') as csvfile: 
            csvwriter = csv.writer(csvfile) 
            csvwriter.writerow([i[0] for i in mysql_cursor.description]) # Write column headers 
            csvwriter.writerows(mysql_cursor)

    files = glob.glob('E:\BirstSourceData\TiVo_Mart_Sources\PRD\PostgresPipeline\FEProvODS\*'+todayDate+'.csv')

    # Establish a connection
    conn = psycopg2.connect(
        host="10.150.136.36",
        database="businessIntelligenceDB",
        user="postgres",
        password="@Welcome2Post"
    )
    SchemaName = 'deviceSchema'
    TableName = 'ConnectionTestTable'
    # Create a cursor object
    cur = conn.cursor()

    # Execute a SQL command
    for file in files:
        abspath = "\\\\SJHRBIR01.corporate.local"+file[2:]
        cur.execute(f"""TRUNCATE TABLE "{SchemaName}".{file[68:68+file[68:].index('_'+todayDate)]}""")
        cur.execute(f"""COPY "{SchemaName}".{file[68:68+file[68:].index('_'+todayDate)]} FROM '{abspath}' Delimiter ',' CSV HEADER;""")
        print(cur.statusmessage)
        # Fetch the results
        results = cur.rowcount
        conn.commit()



    # Close the cursor and connection
    cur.close()
    conn.close()

except Exception as ex:
    logging.error(f"{ex}")

#mysql_cursor.close() 
#snowflake_cursor.close()

#mysql_conn.close() 
#snowflake_conn.close()





