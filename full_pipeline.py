import boto3
import pandas as pd
import time

def connect_to_redshift_data():
    # Connect to Redshift Data API
    redshift_data = boto3.client('redshift-data')
    return redshift_data

def execute_redshift_query(sql, redshift_data):
    # Execute SQL query using Redshift Data API
    response = redshift_data.execute_statement(
        ClusterIdentifier='your_cluster_name',
        Database='your_db',
        Sql=sql,
        SecretArn='your_SecretArn',
    )
    execution_id = response['Id']
    return execution_id
def create_tables(redshift_data):
    # SQL queries to create tables
    sql_create_fact_table = '''
    CREATE TABLE IF NOT EXISTS public.fact_avgs_tbl (
        fscldt_id VARCHAR,
        sku_id VARCHAR,
        average_unit_standardcost VARCHAR,
        average_unit_landedcost VARCHAR
    )
    '''

    sql_create_dimension_table = '''
    CREATE TABLE IF NOT EXISTS public.hier_prod_tbl (
        sku_id VARCHAR,
        sku_label VARCHAR,
        stylclr_id VARCHAR,
        stylclr_label VARCHAR,
        styl_id VARCHAR,
        styl_label VARCHAR,
        subcat_id VARCHAR,
        subcat_label VARCHAR,
        cat_id VARCHAR,
        cat_label VARCHAR,
        dept_id VARCHAR,
        dept_label VARCHAR,
        issvc INTEGER,
        isasmbly INTEGER,
        isnfs INTEGER
    )
    '''

    # Execute SQL queries to create tables
    execute_redshift_query(sql_create_fact_table, redshift_data)
    execute_redshift_query(sql_create_dimension_table, redshift_data)
    
def check_table_has_data(table_name, redshift_data):
    # Query to check if the table has any rows
    sql_query = f"SELECT COUNT(*) FROM {table_name} LIMIT 10"
    try:
        response = redshift_data.execute_statement(
            ClusterIdentifier='your_cluster_name',
            Database='your_db',
            Sql=sql_query,
            SecretArn='your_SecretArn',
        )
        execution_id = response['Id']
        max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                result_response = redshift_data.get_statement_result(Id=execution_id)
                row_count = int(result_response['Records'][0][0]['longValue'])
                print("Row count:", row_count)
                return row_count > 0
            except redshift_data.exceptions.ResourceNotFoundException:
                print("Query does not have result yet. Waiting for results...")
                retries += 1
                time.sleep(30)  # Wait for 30 seconds before retrying
        
        print("Failed to fetch data after maximum retries.")
        return False
    except Exception as e:
        print(f"Error checking if table {table_name} has data:", e)
        return False

def load_data_from_s3(redshift_data):
    # Connect to S3
    s3 = boto3.client('s3')

    # Specify S3 bucket and keys for data files
    bucket = 'raw-synergy'
    fact_key = 'fact_averagecosts.dlm'
    dimension_key = 'hier.prod.dlm'

    # Load data from S3 to Redshift
    sql_fact_load = f'''
    COPY public.fact_avgs_tbl 
    FROM 's3://{bucket}/{fact_key}'
    IAM_ROLE 'arn:aws:iam::211125729106:role/glue'
    DELIMITER '|' 
    IGNOREHEADER 1
    '''

    sql_dimension_load = f'''
    COPY public.hier_prod_tbl 
    FROM 's3://{bucket}/{dimension_key}'
    IAM_ROLE 'arn:aws:iam::211125729106:role/glue'
    DELIMITER '|' 
    IGNOREHEADER 1
    '''
    
    # Execute SQL queries to load data from S3 to Redshift
    execution_id_fact=execute_redshift_query(sql_fact_load, redshift_data)
    execution_id_dimension=execute_redshift_query(sql_dimension_load, redshift_data)
    while True:
        fact_status = redshift_data.describe_statement(Id=execution_id_fact)['Status']
        dimension_status = redshift_data.describe_statement(Id=execution_id_dimension)['Status']
        
        if fact_status == 'FINISHED' and dimension_status == 'FINISHED':
            print("Data loading queries have finished execution. Proceeding further.")
            return True
        elif fact_status == 'FAILED' or dimension_status == 'FAILED':
            print("Data loading queries have failed. Please check Redshift Data API.")
            return False
        else:
            print("Data loading queries are still in progress. Waiting...")
            #time.sleep(30)  
    # time.sleep(60)

# 
def fetch_data_from_redshift(execution_id, redshift_data):
    # Fetch data from Redshift Data 
    response = None
    while True:
        try:
            response = redshift_data.get_statement_result(Id=execution_id)
            break
        except redshift_data.exceptions.ResourceNotFoundException:
            print("Query does not have result yet. Waiting for results...")
            continue

    if response:
        columns = [col['label'] for col in response['ColumnMetadata']]
        rows = response['Records']
        data = []
        for row in rows:
            values = []
            for value_info in row:
                if 'stringValue' in value_info:
                    values.append(value_info['stringValue'])
                elif 'longValue' in value_info:
                    values.append(value_info['longValue'])
                elif 'doubleValue' in value_info:
                    values.append(value_info['doubleValue'])
                else:
                    values.append(None)
            data.append(values)

        df = pd.DataFrame(data, columns=columns)
        return df
    else:
        print("Failed to fetch data from Redshift.")
        return None

def check_null_values(df):
    # Replace empty strings with NaN
    df.replace('', pd.NA, inplace=True)
    # Check for missing values in the DataFrame
    return df.isna().any().any()

def check_primary_key_uniqueness(df, key_columns):
    # Check if the primary key columns have unique values
    duplicate_rows = df[df.duplicated(subset=key_columns, keep=False)]
    return duplicate_rows if not duplicate_rows.empty else False

def check_foreign_key_constraints(df_fact, df_dimension):
    # Check for foreign key constraints between fact and dimension tables
    foreign_key_violations = df_fact[~df_fact['sku_id'].isin(df_dimension['sku_id'])]
    return foreign_key_violations if not foreign_key_violations.empty else False



def schema_exists(schema_name, redshift_data):
    # Check if schema exists in Redshift
    sql = f"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema_name}'"
    try:
        response = redshift_data.execute_statement(
            ClusterIdentifier='your_cluster_name',
            Database='your_db',
            Sql=sql,
            SecretArn='your_SecretArn',
        )
        return len(response.get('Records', [])) > 0
    except Exception as e:
        print(f"Error checking if schema {schema_name} exists:", e)
        return False

def create_schema(schema_name, redshift_data):
    # Create schema in Redshift if it does not exist
    if not schema_exists(schema_name, redshift_data):
        sql = f"CREATE SCHEMA {schema_name}"
        redshift_data.execute_statement(
            ClusterIdentifier='your_cluster_name',
            Database='your_db',
            Sql=sql,
            SecretArn='your_SecretArn',
        )


def main():
    # Connect to Redshift Data API
    redshift_data = connect_to_redshift_data()
    
    # Step 1: Create tables
    create_tables(redshift_data)
    #Step 2: Check if tables has data loaded
    fact_table_has_data = check_table_has_data('public.fact_avgs_tbl', redshift_data)
    print("checked")
    dimension_table_has_data = check_table_has_data('public.hier_prod_tbl', redshift_data)

    # If tables have data, proceed with further steps
    if fact_table_has_data and dimension_table_has_data:
        print("Tables already have data. Proceeding with further steps...")
    else:
        # Step 2: Load data from S3 to Redshift
        load_data_from_s3(redshift_data)

    # Step a: Load raw data into staging tables and perform data quality checks
    sql_fact = """
    SELECT * FROM public.fact_avgs_tbl
    """
    sql_dimension = """
    SELECT * FROM public.hier_prod_tbl
    """
    execution_id_fact = execute_redshift_query(sql_fact, redshift_data)
    execution_id_dimension = execute_redshift_query(sql_dimension, redshift_data)

    if execution_id_fact:
        print("Fetching data for the fact table...")
        df_fact = fetch_data_from_redshift(execution_id_fact, redshift_data)
        if df_fact is not None:
            print("Data fetched for the fact table:")
            print(df_fact.head()) 
            if check_null_values(df_fact):
                print("Null values found in fact table.")
           
            duplicate_rows_fact = check_primary_key_uniqueness(df_fact, ['sku_id'])
            if duplicate_rows_fact is not None:
                print("Primary key uniqueness violation found in fact table:")
                print(duplicate_rows_fact)

    if execution_id_dimension:
        print("Fetching data for the dimension table...")
        df_dimension = fetch_data_from_redshift(execution_id_dimension, redshift_data)
        if df_dimension is not None:
            print("Data fetched for the dimension table:")
            print(df_dimension.head())  # Print first few rows for verification
            if check_null_values(df_dimension):
                print("Null values found in dimension table.")
            
            duplicate_rows_dimension = check_primary_key_uniqueness(df_dimension, ['sku_id'])
            if duplicate_rows_dimension is not None:
                print("Primary key uniqueness violation found in fact table:")
                print(duplicate_rows_dimension)
            foreign_key_violations = check_foreign_key_constraints(df_fact, df_dimension)
            if foreign_key_violations is not None:
                print("Foreign key constraints violation found between fact and dimension tables:")
                print(foreign_key_violations)
           


    # Step b: Create staging schema with normalized hierarchy tables
    schema_name = 'stages'
    create_schema(schema_name, redshift_data)

    staging_queries = [
        """
        CREATE TABLE IF NOT EXISTS stages.product_tbl (
            sku_id VARCHAR,
            sku_label VARCHAR,
            stylclr_id VARCHAR,
            styl_id VARCHAR,
            subcat_id VARCHAR,
            issvc INTEGER,
            isasmbly INTEGER,
            isnfs INTEGER
        )
        """,
        """
        TRUNCATE TABLE stages.product_tbl;
        INSERT INTO stages.product_tbl
        (sku_id, sku_label, stylclr_id, styl_id, subcat_id, issvc, isasmbly, isnfs)
        SELECT sku_id, sku_label, stylclr_id, styl_id, subcat_id, issvc, isasmbly, isnfs
        FROM public.hier_prod_tbl
        """,

        """
        CREATE TABLE IF NOT EXISTS stages.style_tbl (
            styl_id VARCHAR,
            styl_label VARCHAR
        )
        """,
        """
        TRUNCATE TABLE stages.style_tbl;
        INSERT INTO stages.style_tbl
        (styl_id, styl_label)
        SELECT styl_id, styl_label
        FROM public.hier_prod_tbl
        """,
        """
        CREATE TABLE IF NOT EXISTS stages.styleclr_tbl (
            stylclr_id VARCHAR,
            stylclr_label VARCHAR
        )
        """,
        """
        TRUNCATE TABLE stages.styleclr_tbl;
        INSERT INTO stages.styleclr_tbl
        (stylclr_id, stylclr_label)
        SELECT stylclr_id, stylclr_label
        FROM public.hier_prod_tbl
        """,
        """
        CREATE TABLE IF NOT EXISTS stages.subcategory_tbl (
            subcat_id VARCHAR,
            subcat_label VARCHAR,
            cat_id VARCHAR
        )
        """,
        """
        TRUNCATE TABLE stages.subcategory_tbl;
        INSERT INTO stages.subcategory_tbl
        (subcat_id, subcat_label, cat_id)
        SELECT subcat_id, subcat_label, cat_id
        FROM public.hier_prod_tbl
        """,
        """
        CREATE TABLE IF NOT EXISTS stages.category_tbl (
            cat_id VARCHAR,
            cat_label VARCHAR,
            dept_id VARCHAR
        )
        """,
        """
        TRUNCATE TABLE stages.category_tbl;
        INSERT INTO stages.category_tbl
        (cat_id, cat_label, dept_id)
        SELECT cat_id, cat_label, dept_id
        FROM public.hier_prod_tbl
        """,
        """
        CREATE TABLE IF NOT EXISTS stages.dept_tbl (
            dept_id VARCHAR,
            dept_label VARCHAR
        )
        """,
        """
        TRUNCATE TABLE stages.dept_tbl;
        INSERT INTO stages.dept_tbl
        (dept_id, dept_label)
        SELECT dept_id, dept_label
        FROM public.hier_prod_tbl
        """
    ]


    for query in staging_queries:
        execution_id = execute_redshift_query(query, redshift_data)
        if execution_id:
            print("Executed staging query:", query)
        else:
            print("Failed to execute staging query:", query)

    # Step c: Create materialized view for weekly sales aggregation
    
    mview_query = """
    CREATE TABLE IF NOT EXISTS public.mview_weekly_sales AS
    SELECT
        pos_site_id,
        sku_id,
        fscldt_id,
        price_substate_id,
        type,
        SUM(sales_units) AS total_sales_units,
        SUM(sales_dollars) AS total_sales_dollars,
        SUM(discount_dollars) AS total_discount_dollars
    FROM
        public.fact_trans_tbl 
    GROUP BY
        pos_site_id,
        sku_id,
        fscldt_id,
        price_substate_id,
        type
    """
    execution_id_mview = execute_redshift_query(mview_query, redshift_data)
    if execution_id_mview:
        print("Materialized view created successfully.")
    else:
        print("Failed to create materialized view.")
    

if __name__ == "__main__":
    main()
