import settings
import pandas as pd


def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


def create_new_batch(conn):
    sql = "insert into " + settings.elt_batch_table + " (BATCH_ID, BATCH_NAME, BATCH_START_DATE, BATCH_STATUS) VALUES (ETL_BATCH_TABLE_SEQ.NEXTVAL, 'SSO_LOAD_BATCH', CURRENT_TIMESTAMP(), 'RUNNING')"
    execute_query(conn, sql)


def create_new_job(conn, latest_batch_id, data_fetch_strt_date):
    sql = "insert into " + settings.job_control_table + " (JOB_CONTROL_ID, BATCH_ID,JOB_NAME,JOB_START_DATE,LWM_Date,HWM_DATE,JOB_STATUS,JOB_FAILED_REASON,RECORD_COUNT, COMMENTS) VALUES (JOB_CONTROL_TABLE_SEQ.NEXTVAL, " + str(
        latest_batch_id) + ",'SSO_LOAD_JOB',CURRENT_TIMESTAMP(),'" + str(
        data_fetch_strt_date) + "',CURRENT_TIMESTAMP(),'RUNNING', 'NA',0,'NA')"
    execute_query(conn, sql)


# Function to get the current batch

def get_latest_batch(conn):
    max_batch_id = pd.read_sql_query(
        "select max(batch_id) from " + settings.elt_batch_table + " where BATCH_NAME = 'SSO_LOAD_BATCH' and BATCH_STATUS = 'RUNNING'",
        conn)
    return max_batch_id.iloc[0][0]


# Function to get current job
def get_latest_job(conn):
    max_job_control_id = pd.read_sql_query(
        "select max(job_control_id) from " + settings.job_control_table + " where JOB_NAME = 'SSO_LOAD_JOB' and JOB_STATUS = 'RUNNING'",
        conn)
    return max_job_control_id.iloc[0][0]


def update_job_control_table(JOB_CONTROL_ID, job_status, rec_cnt, job_failed_reason, comments, conn):
    sql = "update " + settings.job_control_table + " set JOB_END_DATE = CURRENT_TIMESTAMP() , JOB_STATUS = '" + job_status + "' , RECORD_COUNT = " + str(
        rec_cnt) + " , JOB_FAILED_REASON = '" + job_failed_reason + "' , COMMENTS = '" + comments + "' where JOB_CONTROL_ID = " + str(
        JOB_CONTROL_ID)
    execute_query(conn, sql)


def update_batch_table(batch_id, batch_status, conn):
    sql = "update " + settings.elt_batch_table + " set BATCH_END_DATE = CURRENT_TIMESTAMP()   , BATCH_STATUS = '" + batch_status + "' where BATCH_ID = " + str(
        batch_id)
    execute_query(conn, sql)

# Function to get source fetch start date
def get_fetch_start_date(conn):
    max_HWM_DATE = pd.read_sql_query(
        "select TO_DATE(max(HWM_DATE)) from " + settings.job_control_table + " where JOB_STATUS = 'SUCCESS' and JOB_NAME = 'SSO_LOAD_JOB'",
        conn)
    return max_HWM_DATE.iloc[0][0]