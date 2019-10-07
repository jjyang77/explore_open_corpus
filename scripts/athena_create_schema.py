import boto3
import csv
import pandas as pd
import logging
import time

athena_client = boto3.client('athena')
athena_output_path = 's3://aws-athena-query-results-377855164311-us-west-2/'

db_name = 'db_on_s3'
table_name = 'ss_research_corpus'
query_create_table = """
CREATE EXTERNAL TABLE %s.%s (
  `entities` array<string>, 
  `journalvolume` string, 
  `journalpages` string, 
  `pmid` string, 
  `year` int, 
  `outcitations` array<string>, 
  `s2url` string, 
  `s2pdfurl` string, 
  `id` string, 
  `authors` array<struct<name:string,ids:array<string>>>, 
  `journalname` string, 
  `paperabstract` string, 
  `incitations` array<string>, 
  `pdfurls` array<string>, 
  `title` string, 
  `doi` string, 
  `sources` array<string>, 
  `doiurl` string, 
  `venue` string)
PARTITIONED BY (corpus string)
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES (   'paths'='authors,doi,doiUrl,entities,id,inCitations,journalName,journalPages,journalVolume,outCitations,paperAbstract,pdfUrls,pmid,s2PdfUrl,s2Url,sources,title,venue,year') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://jjyang-dev/ss_homework/research_corpus/'
TBLPROPERTIES (
  'classification'='json', 
  'compressionType'='gzip', 
  'typeOfData'='file');
""" % (db_name, table_name)

partition_no='138'
query_add_partition = """
ALTER TABLE %s.%s ADD PARTITION (corpus='%s') LOCATION 's3://jjyang-dev/ss_homework/research_corpus/corpus=%s';
    """ % (db_name, table_name, partition_no, partition_no)


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


#res=run_athena_query(athena_client, db_name, query_create_table, athena_output_path)

res=run_athena_query(athena_client, db_name, query_add_partition, athena_output_path)


