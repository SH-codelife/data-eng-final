import json
import apache_beam as beam
import apache_beam.io.gcp.bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromBigQuery
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import argparse
#new comment to push to repo
#new comment to push to repo

# Data Studio Report #Name: SH Report
# Url:  https://datastudio.google.com/u/0/reporting/abfaa824-4bce-4e7f-9865-aec6fec10c2d/page/wf7iC

options = PipelineOptions(
    project="york-cdf-start", #Jenkins will need
    region="us-central1", #Jenkins will need
    temp_location="gs://york_temp_files/tmp",
    job_name='sonja-hayden-final-job',
    runner="DataflowRunner", #Jenkins will need
    staging_locations="gs://york_temp_files/staging",
    save_main_session=True
)
if __name__ == '__main__':
    # schema for table to write data back into
    TABLE_SCHEMA = {  ##sku: input type string/output type integer
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_no_of_product_views', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        ]
    }
    ORDERS_SCHEMA = {
        'fields': [
            {'name': 'cust_tier_code', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sku', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'total_sales_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'},
        ]
    }
    # table specs always needed to create tables, tableID can be named here if does not exist already
    table_spec = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_sonja_hayden",
        tableId="cust_tier_code-sku-total_no_of_product_views"
    )
    orders_spec = bigquery.TableReference(
        projectId="york-cdf-start",
        datasetId="final_sonja_hayden",
        tableId="cust_tier_code-sku-total_sales_amount"
    )
    #make sure order of output matches schema being created and inserted into
    with beam.Pipeline(options=options) as pipeline:
        product_views = pipeline | 'Read/join tables from BQ to count product views' >> beam.io.ReadFromBigQuery(
            query='SELECT c.CUST_TIER_CODE, p.SKU, count(p.SKU) AS total_no_of_product_views FROM york-cdf-start.final_input_data.product_views AS p '
            'JOIN york-cdf-start.final_input_data.customers AS c ON p.customer_id=c.CUSTOMER_ID GROUP BY c.CUST_TIER_CODE, p.SKU',
            project='york-cdf-start',
            use_standard_sql=True
        ) #| "print" >> beam.Map(print) #can't write to BQ and print at the same time, pcollection is different...datatype/

        #tableReference can be a PROJECT:DATASET.TABLE or DATASET.TABLE string.
        orders = pipeline | 'Read/join tables fro BQ for sum order amt' >> beam.io.ReadFromBigQuery(
            query='SELECT c.CUST_TIER_CODE, o.SKU, round(sum(o.order_amt),2) AS total_sales_amount '
            'FROM york-cdf-start.final_input_data.customers AS c '
            'JOIN york-cdf-start.final_input_data.orders AS o ON o.customer_id=c.CUSTOMER_ID '
            'GROUP BY c.CUST_TIER_CODE, o.SKU',
            project='york-cdf-start',
            use_standard_sql=True
        )
       # table_data write data into BigQuery table
        product_views | 'write product_views fields into a BigQuery table' >> beam.io.WriteToBigQuery(
            table_spec,  # table specs needed to create tables, use variable already assigned
            schema=TABLE_SCHEMA,  # schema variable already assigned
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  # must have to create table
            #with batch need to remove values if there is an error, so do NOT .WRITE_append
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
        orders | 'write required fields into a BQ table' >> beam.io.WriteToBigQuery(
            orders_spec,  # table specs needed to create tables, use variable already assigned
            schema=ORDERS_SCHEMA,  # schema variable was already assigned
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,  # must have to create table
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )