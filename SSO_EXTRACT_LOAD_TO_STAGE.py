import snowflake.connector as sf
import pandas as pd
import settings
import os
from pandasql import sqldf
import etl_job_control as etl
import snowflake_load as sl
import errno
import send_email
import sys

pysqldf = lambda q: sqldf(q, globals())
conn = sf.connect(user=settings.username, password=settings.password, account=settings.snowflake_account)
file_names = ""


# Function to create connection and run passed query in snowflake.
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


try:
    sql = 'use role {}'.format(settings.role)
    execute_query(conn, sql)

    sql = 'use database {}'.format(settings.database)
    execute_query(conn, sql)

    sql = 'use warehouse {}'.format(settings.warehouse)
    execute_query(conn, sql)

    sql = 'use schema {}'.format(settings.schema)
    execute_query(conn, sql)

    # Create a new Batch
    etl.create_new_batch(conn)

    latest_batch_id = etl.get_latest_batch(conn)
    data_fetch_strt_date = etl.get_fetch_start_date(conn)

    # Create new Job
    etl.create_new_job(conn, latest_batch_id, data_fetch_strt_date)
    latest_job_id = etl.get_latest_job(conn)



    df_extracts = pd.DataFrame({'EXTRACT_FILE_NAMES': []})
    for file in os.listdir(settings.extract_file_path):
        if file.endswith(".csv"):
            df_extracts = df_extracts.append({'EXTRACT_FILE_NAMES': file}, ignore_index=True)


    # Getting all the files, already staged in snowflake stage.
    snowflake_stage_files = pd.read_sql_query("list " + settings.snowflake_sso_stage, conn)

    # Compare to find new files.

    stg_file_name = pysqldf(u"select substr(name, instr(name,'/')+1,18)  as file  from snowflake_stage_files;")
    # substr(name,15,18)



    unmatched_files = pysqldf(
        u"select a.EXTRACT_FILE_NAMES as source_file from df_extracts a left join stg_file_name b on a.EXTRACT_FILE_NAMES = b.file where b.file is null;")
    if unmatched_files.empty:
        raise FileNotFoundError("No new file arrived")
    else:
        for index, row in unmatched_files.iterrows():
            csv_file = settings.load_from_file_path + row[0]
            file_names = file_names + row[0] + ", "



        sl._upload_files(csv_file, conn)
        sl._create_stage_table(conn)
        sl._copy_stage_files(conn, row[0])

        sl._merge_tgt_table(conn)
        temp_table_count = sl._get_temp_table_count(conn)
        merge_table_count = sl._get_merge_count(conn)


except Exception as e:
    # update job table with as success
    etl.update_job_control_table(latest_job_id, 'FAILED', 0, str(sys.exc_info()[1]), 'Job failed', conn)

    # update batch table with as success
    etl.update_batch_table(latest_batch_id, 'FAILED', conn)

    receivers = [settings.email_reciver]

    # message to be sent
    SUBJECT = "SSO extract Load to snowflake was Unsuccessful"
    TEXT = """ Exception Occurred \n    
    Below exception occured \n """ + str(sys.exc_info()[0])
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    send_email._send_email(settings.email_sender, receivers, message)

else:
    temp_table_count = sl._get_temp_table_count(conn)
    merge_table_count = sl._get_merge_count(conn)

    # update job table with as success
    etl.update_job_control_table(latest_job_id, 'SUCCESS', temp_table_count, 'NA',
                                 'SOURCE COUNT = ' + temp_table_count + " MERGE COUNT = " + merge_table_count, conn)

    # update batch table with as success
    etl.update_batch_table(latest_batch_id, 'SUCCESS', conn)

    receivers = [settings.email_reciver]

    # message to be sent
    SUBJECT = "SSO extract Loaded Successfully."

    TEXT = """ Below files are loaded successfully.
    """ + file_names[0:len(file_names) - 2] + """ \n

    Total source  count = """ + temp_table_count + """ \n
    Total merge to target table count = """ + merge_table_count
    message = 'Subject: {}\n\n{}'.format(SUBJECT, TEXT)
    send_email._send_email(settings.email_sender, receivers, message)