import settings
import pandas as pd


# Function to create connection and run passed query in snowflake.
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()


def _upload_files(csv_file, conn):
    sql = 'PUT file://' + csv_file + ' ' + settings.snowflake_sso_stage + ' auto_compress=true;'
    execute_query(conn, sql)


def _create_stage_table(conn):
    sql = 'CREATE OR REPLACE TEMPORARY TABLE ' + settings.snowflake_load_temp_table + ' (FILENAME varchar,FILE_ROW_NUMBER INTEGER ,USAGE_START_DATE TIMESTAMP NOT NULL, USAGE_USER VARCHAR (1000), URL STRING,USAGE_IP_ADDRESS VARCHAR (100))'
    execute_query(conn, sql)


def _copy_stage_files(conn, filename):
    sql = 'copy into ' + settings.snowflake_load_temp_table + ' (FILENAME, FILE_ROW_NUMBER,  USAGE_START_DATE,USAGE_USER,URL,USAGE_IP_ADDRESS) from (select metadata$filename, metadata$file_row_number, t.$1, t.$2, t.$3, t.$4 from ' + settings.snowflake_sso_stage + '/' + filename + ' (file_format => IDP_SSO_FILE_FORMAT) t) ON_ERROR = SKIP_FILE'
    execute_query(conn, sql)


def _merge_tgt_table(conn):
    sql = "merge into " + settings.snowflake_load_tgt_table + " tgt using (select filename, file_row_number, to_timestamp(left(USAGE_START_DATE,19),'yyyy-mm-dd hh:mi:ss') USAGE_START_DATE ,USAGE_USER,  APP_link,USAGE_IP_ADDRESS, url from (select a.filename, a.file_row_number,trim(A.USAGE_START_DATE) USAGE_START_DATE,trim(SPLIT_PART(A.url,'/',T.IND)) APP_link,case when length(trim(USAGE_USER)) > 7 then upper(substr(REGEXP_SUBSTR(USAGE_USER,'\\([[:alnum:]\-]+\\)'),2, length(REGEXP_SUBSTR(USAGE_USER,'\\([[:alnum:]\-]+\\)'))-2))  else upper(trim(A.USAGE_USER)) end USAGE_USER  , trim(A.USAGE_IP_ADDRESS) USAGE_IP_ADDRESS , A.URL FROM " + settings.snowflake_load_temp_table + " A inner join ( select url, max(index) IND from (   select url, c.value, c.seq, c.index from " + settings.snowflake_load_temp_table + " , lateral split_to_table(url, '/') c order by seq, index  ) where value not in ('sp.xml','metadata.php', '1.0', 'module.php','VSPHERE.LOCAL')  and (value like '%.%') group by url) T on A.url = t.url where trim(a.USAGE_USER) <> 'ping999')) as src on tgt.filename = src.filename and tgt.file_row_number = src.file_row_number when not matched then insert (filename, file_row_number,usage_start_date, usage_user, app_link, usage_ip_address, url,LOAD_DATE ) values (src.filename, src.file_row_number, src.USAGE_START_DATE,src.USAGE_USER,  src.APP_link,src.USAGE_IP_ADDRESS, src.url,to_timestamp_ntz(current_timestamp()) )"
    execute_query(conn, sql)


def _get_temp_table_count(conn):
    temp_table_count = pd.read_sql_query("select count(1) from " + settings.snowflake_load_temp_table, conn)
    return str(temp_table_count.iloc[0][0])


def _get_merge_count(conn):
    tgt_table_count = pd.read_sql_query(
        "select count(1) from " + settings.snowflake_load_tgt_table + " a join " + settings.snowflake_load_temp_table + " b on a.filename = b.filename and a.file_row_number = b.file_row_number",
        conn)
    return str(tgt_table_count.iloc[0][0])
