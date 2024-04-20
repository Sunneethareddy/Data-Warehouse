<<<<<<< HEAD
         ___        ______     ____ _                 _  ___  
        / \ \      / / ___|   / ___| | ___  _   _  __| |/ _ \ 
       / _ \ \ /\ / /\___ \  | |   | |/ _ \| | | |/ _` | (_) |
      / ___ \ V  V /  ___) | | |___| | (_) | |_| | (_| |\__, |
     /_/   \_\_/\_/  |____/   \____|_|\___/ \__,_|\__,_|  /_/ 
 ----------------------------------------------------------------- 


Hi there! Welcome to AWS Cloud9!

To get started, create some files, play with the terminal,
or visit https://docs.aws.amazon.com/console/cloud9/ for our documentation.

Happy coding!

1. Project's Title

DATA WAREHOUSE PIPELINE WITH AMAZON REDSHIFT

2. Project Description
   
This project aims to build a data pipeline that facilitates the loading of raw data into a data warehouse from external storage such as AWS S3. The pipeline is constructed using Amazon Redshift, and it consists of the following components:
Data Loading: Raw data is loaded into the data warehouse from AWS S3. Due to limitations with AWS Glue and AWS Lambda, the loading process was executed manually through AWS Redshift Spectrum.
Data Quality Checks: Basic data quality checks are performed on the loaded data, including checks for non-null values, primary key uniqueness, data types, and foreign key constraints between fact and dimension tables.
Staging Schema Creation: The project includes the creation of a staging schema where a hierarchy table is normalized into separate tables for each level. The staged fact table establishes foreign key relationships with these normalized tables.
Refined Table Creation: A refined table named mview_weekly_sales is created to aggregate sales data by various dimensions such as pos_site_id, sku_id, fscldt_id, price_substate_id, and type.

3. How to Install and Run the Project
   
Create a Cloud9 environment on AWS, which runs on an EC2 instance.
Install the Pandas library in the Cloud9 environment using pip.
Ensure that the IAM roles used have appropriate permissions to fetch data from Redshift tables and to create schema/tables in Redshift.
Establish a connection from Cloud9 to Redshift by providing the cluster name, database, user, and respective ARN.

4.Additional Considerations:

Amazon Redshift: Ensure you have provisioned a Redshift cluster to work with Redshift. Make sure to configure the necessary security groups, VPC settings, and IAM roles for accessing the cluster.
Amazon S3: Prepare your S3 bucket and ensure that the required data files are stored in the bucket. Set up appropriate permissions and access policies for accessing the S3 objects.
AWS Cloud9: Created an environment in AWS Cloud9 to develop and execute the project. Ensure that the Cloud9 environment has the necessary permissions to interact with AWS services such as Redshift and S3. Configure the environment with the required dependencies and libraries, such as the Pandas library for data processing.
we can run the pipeline using the command python filename.py


