from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource

# ===================================
# RAW DATA ASSETS (SOURCE DATABASE)
# =================================== 
# source_database(customers)   --> staging_area(customers.csv)
#   id                                     id
#   email                                  full_name
#   full_name                              gender
#   gender                                 etl_timestamp
#
# source_database(goals)       --> staging_area(goals.csv)
#   Goal Type                              Goal Type
#   User ID                                User ID
#   Portfolio ID                           Portfolio ID
#   Region                                 Region
#   ID                                     ID
#                                          etl_timestamp
#
# source_database(performance) --> staging_area(performance.csv)
#  portfolioid                            portfolioid
#  calcdate                               calcdate
#  officialnav                            officialnav
#  currency                               currency
#                                         etl_timestamp
#
# pull data from source database table
# write data to staging area in csv format
#
#
@asset()
def customers_staging(context: AssetExecutionContext, source_database: DuckDBResource):
    with source_database.get_connection() as conn:
        # change the right query for task 1
        # below is just an example only
        select_query = f"""
            SELECT
                id 
            FROM customers;
        """
        data_df = conn.sql(select_query).to_df()
        staging_file_path = './staging_area/customers.csv'
        context.log.info(f"Data: {data_df}")
        data_df.to_csv(staging_file_path, index=False)
        return staging_file_path


# =======================================
# STAGING DATA ASSETS (DATA WAREHOUSE)
# =======================================
# staging_area(customers.csv)   --> data_warehouse(raw.customers)
#   id                                id
#   full_name                         full_name
#   gender                            gender
#                                     etl_timestamp
#
# staging_area(goals.csv)       --> data_warehouse(raw.goals)
#   Goal Type                         Goal Type
#   User ID                           User ID
#   Portfolio ID                      Portfolio ID
#   Region                            Region
#   ID                                ID
#                                     etl_timestamp
#
# staging_area(performance.csv) --> data_warehouse(raw.performance)
#  portfolioid                       portfolioid
#  calcdate                          calcdate
#  officalnav                        officialnav
#  currency                          currency
#                                    etl_timestamp
#
# create `raw` schema if not exists and create table if not exists
# copy data from csv to table
#
#
@asset()
def customers_raw(context: AssetExecutionContext, data_warehouse: DuckDBResource, customers_staging):
    with data_warehouse.get_connection() as conn:
        create_schema_query = f"""

        """
        conn.sql(create_schema_query)
        create_table_query = f"""
            
        """
        conn.sql(create_table_query)
        copy_query = f"COPY raw.customers FROM '{customers_staging}' WITH (HEADER true);"
        conn.sql(copy_query)
        return ''


# ===================================
# BASE DATA ASSETS (DATA WAREHOUSE)
# =================================== 
# data_warehouse(raw.customers)   --> data_warehouse(base.customers)
#  id                                   id
#  full_name                            first_name
#  gender                               last_name
#  etl_timestamp                        gender
#                                       etl_timestamp
#
# data_warehouse(raw.goals)       --> data_warehouse(base.goals)
#   Goal Type                           goal_type
#   User ID                             user_id
#   Portfolio ID                        portfolio_id
#   Region                              region
#   ID                                  id
#   etl_timestamp                       stragety_id
#                                       etl_timestamp
#
# data_warehouse(raw.performance) --> data_warehouse(base.performance)
#   portfolioid                         portfolio_id
#   calcdate                            calc_date
#   officalnav                          official_nav
#   currency                            currency
#   etl_timestamp                       above_1000
#                                       etl_timestamp
#
# create `base` schema is not exist
# create table with select query
#
#
@asset()
def customers_base(context: AssetExecutionContext, data_warehouse: DuckDBResource, customers_raw):
    with data_warehouse.get_connection() as conn:
        create_schema_query = f"""
            
        """
        conn.sql(create_schema_query)
        select_query = f"""
            
        """
        create_table_query = f"""
            CREATE TABLE base.customers AS {select_query};
        """
        conn.sql(create_table_query)
        return ''
