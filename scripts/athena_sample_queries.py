import boto3
import csv
import pandas as pd
import logging
import time

athena_client = boto3.client('athena')
athena_output_path = 's3://aws-athena-query-results-377855164311-us-west-2/'

def run_athena_query(athena_client, db_name, query, output_path):
    start_query = athena_client.start_query_execution(
                  QueryString = query,
                  QueryExecutionContext = {'Database': db_name},
                  ResultConfiguration = {'OutputLocation': output_path}    
    )
    query_exe_id = start_query['QueryExecutionId']
    query_response = 'NULL'
    while query_response not in ('SUCCEEDED','FAILED'):
        get_response = athena_client.get_query_execution(QueryExecutionId = query_exe_id)
        query_response = get_response['QueryExecution']['Status']['State']
        logging.info("Athena query response: " + query_response)
        time.sleep(1)
    return(query_response)


db_name = 'db_on_s3'
table_name = 'ss_research_corpus'
partition_no = '999'

query="""
SELECT count(1) FROM %s.%s
WHERE corpus='%s'
  AND journalname != '';
""" % (db_name, table_name, partition_no)

res=run_athena_query(athena_client, db_name, query, athena_output_path)


