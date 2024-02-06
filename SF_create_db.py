import snowflake.connector as sf
import pandas as pd

sf_creds = pd.read_csv("../ankur_accessKeys_SF.csv")
user = sf_creds["user"][0]
password = sf_creds["password"][0]
account = sf_creds["account"][0]


conn = sf.connect(user=user, password=password, account=account)

def run_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

statement = "CREATE OR REPLACE DATABASE CLOTHING;"
run_query(conn, statement)

statement = "USE DATABASE CLOTHING;"
run_query(conn, statement)

statement = ''' CREATE SCHEMA IF NOT EXISTS SALES;'''
run_query(conn, statement)

statement = ''' CREATE TABLE IF NOT EXISTS CLOTHING.SALES.SALES_RECORDS (
                ID INT,
                Region VARCHAR(50),
                Country VARCHAR(50),
                Item_Type VARCHAR(50),
                Sales_Channel VARCHAR (50),
                Order_Priority VARCHAR(50),
                Order_Date DATE,
                Order_ID VARCHAR(50),
                Ship_Date DATE,
                Units_Sold INT,
                Unit_Price NUMERIC(10, 2),
                Unit_Cost NUMERIC(10, 2),
                Total_Revenue NUMERIC(10, 2),
                Total_Cost NUMERIC(10, 2),
                Total_Profit NUMERIC(10, 2)
            );'''
run_query(conn, statement)

statement = ''' CREATE OR REPLACE FILE FORMAT csv_format
                type = CSV
                field_delimiter = ','
                skip_header = 1
                empty_field_as_null = FALSE;'''
run_query(conn, statement)

statement = ''' CREATE OR REPLACE STAGE CLOTHING_STAGE
                URL = 's3://forever21etl/sales-records.csv'
                credentials = (aws_key_id = ''
                               aws_secret_key = '')
                file_format = csv_format;'''
run_query(conn, statement)

statement = "copy into CLOTHING.SALES.SALES_RECORDS \
             from @CLOTHING.SALES.CLOTHING_STAGE \
             file_format = (format_name=CLOTHING.SALES.CSV_FORMAT) \
             on_error=continue;"
run_query(conn, statement)