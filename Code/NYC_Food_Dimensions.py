#!/usr/bin/python

import os
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import current_date, current_timestamp
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, collect_list
from pyspark.sql.functions import collect_set, first                   # To handle cases where the first item retrieved by getItem(0) might be null
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import DecimalType

from sodapy import Socrata

spark = SparkSession.builder.appName("NycFoodInspection").config("spark.sql.debug.maxToStringFields", "50000").getOrCreate()

####################################################################
# Function to read credentials from the Socrata API
####################################################################

def read_credentials(cred_path):
    cred = spark.read.text(cred_path)

    credentials = cred.head()
    text = credentials[0]
    words = text.split(",")

    cred1, cred2, cred3 = words

    return cred1, cred2, cred3


####################################################################
# Getting the Data from Sodapy API
####################################################################

def fetch_data_from_api(cred_path, df_path):
    ################################################################
    # Store the API Token and Credentials in a file name cred.txt
    # In the below format
    #
    # MyAppToken:"Your API Token"
    # USERNAME:"abc.example.com"
    # PASSWORD:"yourpassword"
    ################################################################
    
    print("Fetching data from the API")

    # Call the function and get the credentials as strings
    MyAppToken, USERNAME, PASSWORD = read_credentials(cred_path)

    client = Socrata("data.cityofnewyork.us", 
                       MyAppToken,
                       username=USERNAME,
                       password=PASSWORD
                       )

    results = client.get_all("43nn-pn8j")
    
    df = spark.createDataFrame(results)
           
    ####################################################################
    # Write raw data to parquet file. This will be the staging data
    ####################################################################
    num_partitions = 2
    df.repartition(num_partitions).write.mode("overwrite").parquet(df_path)
    
    print("Staging data stored as Parquet file")
    

################################################################
# Creating Dimension Tables FOOD_PLACES_SK
################################################################
def Dim_NYC_Food_Places(parquet_df, output_path):
    # Joining with the original DataFrame to get corresponding 'DBA' values
    distinct_CAMIS = parquet_df.select('camis').distinct()


    # Retrieve all 'DBA' values associated with each unique 'CAMIS'
    unique_CAMIS_with_DBA = distinct_CAMIS.join(parquet_df, on='camis', how='inner') \
                                          .groupBy('camis') \
                                          .agg(first('dba', ignorenulls=True).alias('dba'))

    # Renaming the column to UpperCase for uniformity
    Dim_NYC_Food_Places = unique_CAMIS_with_DBA.withColumnRenamed('camis', 'CAMIS') \
                                               .withColumnRenamed('dba', 'DBA')


    # Adding a new column 'date_created' with the current date
    Dim_NYC_Food_Places = Dim_NYC_Food_Places.withColumn("FOOD_PLACES_SK", (monotonically_increasing_id() + 1).cast("integer")) \
                                            .withColumn('INGESTION_DATE', current_date()) \
                                            .withColumn('INGESTION_TIME', current_timestamp()) 

    # Selecting the specified columns for the Parquet table
    column_position = ["FOOD_PLACES_SK", "CAMIS", "DBA", "INGESTION_DATE", "INGESTION_TIME"]
    Dim_NYC_Food_Places = Dim_NYC_Food_Places.select(column_position)
    
    dim_table_name = "Dim_NYC_Food_Places"
    complete_path = f"{output_path}/{dim_table_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Food_Places.write.mode('overwrite').parquet(complete_path)
    
    print("Dim_NYC_Food_Places created")
    
    
################################################################
# Creating Dimension Table Dim_NYC_Cuisine
################################################################    
def Dim_NYC_Cuisine(parquet_df, output_path):
    # Creating a new DataFrame
    unique_Cuisine = parquet_df.select('cuisine_description').distinct()

    Dim_NYC_Cuisine= unique_Cuisine.withColumnRenamed('cuisine_description', 'CUISINE_DESCRIPTION')

    Dim_NYC_Cuisine= Dim_NYC_Cuisine.orderBy('CUISINE_DESCRIPTION')

    # Adding a new column 'date_created' with the current date
    Dim_NYC_Cuisine = Dim_NYC_Cuisine.withColumn("CUISINE_DESCRIPTION_SK", (monotonically_increasing_id() + 1).cast("integer")) \
                                    .withColumn('INGESTION_DATE', current_date()) \
                                    .withColumn('INGESTION_TIME', current_timestamp())


    # Selecting the specified columns for the Parquet table
    column_position = ["CUISINE_DESCRIPTION_SK", "CUISINE_DESCRIPTION", "INGESTION_DATE", "INGESTION_TIME"]
    Dim_NYC_Cuisine = Dim_NYC_Cuisine.select(column_position)
    
    file_name = "Dim_NYC_Cuisine"
    complete_path = f"{output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Cuisine.write.mode('overwrite').parquet(complete_path)
    
    print("Dim_NYC_Cuisine created")

    
################################################################
# Creating Dimension Table Dim_NYC_Borough
################################################################
def Dim_NYC_Borough(parquet_df, output_path):
    # Creating a new DataFrame
    unique_Borough = parquet_df.select('boro').distinct()

    Dim_NYC_Borough= unique_Borough.withColumnRenamed('boro', 'BOROUGH')

    Dim_NYC_Borough= Dim_NYC_Borough.orderBy('BOROUGH')

    # Adding a new column 'date_created' with the current date
    Dim_NYC_Borough = Dim_NYC_Borough.withColumn("BOROUGH_SK", (monotonically_increasing_id() + 1).cast("integer")) \
                                    .withColumn('INGESTION_DATE', current_date()) \
                                    .withColumn('INGESTION_TIME', current_timestamp()) 


    # Selecting the specified columns for the Parquet table
    column_position = ["BOROUGH_SK", "BOROUGH", "INGESTION_DATE", "INGESTION_TIME"]
    Dim_NYC_Borough = Dim_NYC_Borough.select(column_position)
    
    file_name = "Dim_NYC_Borough"
    complete_path = f"{output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Borough.write.mode('overwrite').parquet(complete_path)
    
    print("Dim_NYC_Borough created")


################################################################
# Creating Dimension Table Dim_NYC_Addresses
################################################################
def Dim_NYC_Addresses(parquet_df, output_path):
    # Creating a new DataFrame
    selected_columns = ['building', 'bbl', 'bin', 'street', 'zipcode',
                        'boro', 'nta', 'latitude', 'longitude', 'census_tract',
                        'community_board', 'council_district']

    group_by_columns = ['boro', 'building', 'street', 'zipcode', 'bin', 'bbl', 'nta']

    selected_df = parquet_df.select(*selected_columns)

    Dim_NYC_Addresses = selected_df.groupBy(*group_by_columns).agg(
                            first('latitude').alias('latitude'),
                            first('longitude').alias('longitude'),
                            first('census_tract').alias('census_tract'),
                            first('community_board').alias('community_board'),
                            first('council_district').alias('council_district')
                            )

    Dim_NYC_Addresses= Dim_NYC_Addresses.withColumnRenamed('building', 'BUILDING') \
                                    .withColumnRenamed('bbl', 'BBL') \
                                    .withColumnRenamed('bin', 'BIN') \
                                    .withColumnRenamed('street', 'STREET') \
                                    .withColumnRenamed('zipcode', 'ZIPCODE') \
                                    .withColumnRenamed('boro', 'BOROUGH') \
                                    .withColumnRenamed('nta', 'NTA') \
                                    .withColumnRenamed('latitude', 'LATITUTDE') \
                                    .withColumnRenamed('longitude', 'LONGITUDE') \
                                    .withColumnRenamed('census_tract', 'CENSUS_TRACT') \
                                    .withColumnRenamed('community_board', 'COMMUNITY_BOARD') \
                                    .withColumnRenamed('council_district', 'COUNCIL_DISTRICT') 


    Dim_NYC_Addresses = Dim_NYC_Addresses.withColumn('LATITUTDE', col('LATITUTDE').cast(DecimalType(38, 12))) \
                                        .withColumn('LONGITUDE', col('LONGITUDE').cast(DecimalType(38, 12))) \
                                        .withColumn('CENSUS_TRACT', col('CENSUS_TRACT').cast("integer")) \
                                        .withColumn('COMMUNITY_BOARD', col('COMMUNITY_BOARD').cast("integer")) \
                                        .withColumn('COUNCIL_DISTRICT', col('COUNCIL_DISTRICT').cast("integer")) 

    # Define a window specification for row_number
    window_spec = Window.orderBy("BOROUGH", "BUILDING")

    # Add a new column 'row_num' using row_number
    Dim_NYC_Addresses = Dim_NYC_Addresses.withColumn("row_num", F.row_number().over(window_spec))

    # Use row_num as the unique identifier for ADDRESS_SK
    Dim_NYC_Addresses = Dim_NYC_Addresses.withColumn("ADDRESS_SK", F.col("row_num").cast("integer")) \
                                        .drop("row_num")  # Drop the temporary column

    # Add the remaining columns
    Dim_NYC_Addresses = Dim_NYC_Addresses.withColumn('INGESTION_DATE', current_date()) \
                                        .withColumn('INGESTION_TIME', current_timestamp())



    # Selecting the specified columns for the Parquet table
    column_position = ["ADDRESS_SK", "BUILDING",'BBL', 'BIN', 'STREET', 'ZIPCODE', 
                       'BOROUGH', 'NTA', 'LATITUTDE', 'LONGITUDE', 'CENSUS_TRACT', 
                       'COMMUNITY_BOARD', 'COUNCIL_DISTRICT', "INGESTION_DATE", "INGESTION_TIME"]

    Dim_NYC_Addresses = Dim_NYC_Addresses.select(column_position)
    
    file_name = "Dim_NYC_Addresses"
    complete_path = f"{output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Addresses.write.mode('overwrite').partitionBy("BOROUGH").parquet(complete_path)
    
    print("Dim_NYC_Addresses created")

################################################################
# Creating Dimension Table Dim_NYC_Critical_Flag
################################################################
def Dim_NYC_Critical_Flag(parquet_df, output_path):
    # Creating a new DataFrame
    unique_critical_flag = parquet_df.select('critical_flag').distinct()

    Dim_NYC_Critical_Flag= unique_critical_flag.withColumnRenamed('critical_flag', 'CRITICAL_FLAG')

    Dim_NYC_Critical_Flag= Dim_NYC_Critical_Flag.orderBy('CRITICAL_FLAG')

    # Adding a new column 'date_created' with the current date
    Dim_NYC_Critical_Flag = Dim_NYC_Critical_Flag.withColumn("CRITICAL_FLAG_SK", (monotonically_increasing_id() + 1).cast("integer")) \
                                    .withColumn('INGESTION_DATE', current_date()) \
                                    .withColumn('INGESTION_TIME', current_timestamp()) 


    # Selecting the specified columns for the Parquet table
    column_position = ["CRITICAL_FLAG_SK", "CRITICAL_FLAG", "INGESTION_DATE", "INGESTION_TIME"]
    Dim_NYC_Critical_Flag = Dim_NYC_Critical_Flag.select(column_position)
    
    file_name = "Dim_NYC_Critical_Flag"
    complete_path = f"{output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Critical_Flag.write.mode('overwrite').parquet(complete_path)
    
    print("Dim_NYC_Critical_Flag created")
    

################################################################
# Creating Dimension Table Dim_NYC_Inspection_Grades
################################################################
def Dim_NYC_Inspection_Grades(parquet_df, output_path):
    # Creating a new DataFrame
    unique_grade = parquet_df.select('grade').distinct()

    Dim_NYC_Inspection_Grades= unique_grade.withColumnRenamed('grade', 'GRADE')

    Dim_NYC_Inspection_Grades = Dim_NYC_Inspection_Grades.orderBy('GRADE')

    # Adding a new column 'date_created' with the current date
    Dim_NYC_Inspection_Grades = Dim_NYC_Inspection_Grades.withColumn("GRADE_SK", (monotonically_increasing_id() + 1).cast("integer")) \
                                    .withColumn('INGESTION_DATE', current_date()) \
                                    .withColumn('INGESTION_TIME', current_timestamp()) 


    # Selecting the specified columns for the Parquet table
    column_position = ["GRADE_SK", "GRADE", "INGESTION_DATE", "INGESTION_TIME"]
    Dim_NYC_Inspection_Grades = Dim_NYC_Inspection_Grades.select(column_position)
    
    file_name = "Dim_NYC_Inspection_Grades"
    complete_path = f"{output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Inspection_Grades.write.mode('overwrite').parquet(complete_path)
    
    print("Dim_NYC_Inspection_Grades created")

    
################################################################
# Creating Dimension Table Dim_NYC_Inspection_Type
################################################################
def Dim_NYC_Inspection_Type(parquet_df, output_path):
    # Creating a new DataFrame
    unique_inspection_type = parquet_df.select('inspection_type').distinct()

    Dim_NYC_Inspection_Type = unique_inspection_type.withColumnRenamed('inspection_type', 'INSPECTION_TYPE')

    Dim_NYC_Inspection_Type = Dim_NYC_Inspection_Type.orderBy('INSPECTION_TYPE')

    # Adding a new column 'date_created' with the current date
    Dim_NYC_Inspection_Type = Dim_NYC_Inspection_Type.withColumn("INSPECTION_TYPE_SK", (monotonically_increasing_id() + 1).cast("integer")) \
                                    .withColumn('INGESTION_DATE', current_date()) \
                                    .withColumn('INGESTION_TIME', current_timestamp()) 


    # Selecting the specified columns for the Parquet table
    column_position = ["INSPECTION_TYPE_SK", "INSPECTION_TYPE", "INGESTION_DATE", "INGESTION_TIME"]
    Dim_NYC_Inspection_Type = Dim_NYC_Inspection_Type.select(column_position)
    
    file_name = "Dim_NYC_Inspection_Type"
    complete_path = f"{output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Inspection_Type.write.mode('overwrite').parquet(complete_path)
    
    print("Dim_NYC_Inspection_Type created")
    

################################################################
# Creating Dimension Table Dim_NYC_Inspection_Actions
################################################################
def Dim_NYC_Inspection_Actions(parquet_df, output_path):
    # Creating a new DataFrame
    unique_Actions = parquet_df.select('action').distinct()

    Dim_NYC_Inspection_Actions = unique_Actions.withColumnRenamed('action', 'ACTION')

    Dim_NYC_Inspection_Actions = Dim_NYC_Inspection_Actions.orderBy('ACTION')

    # Adding a new column 'date_created' with the current date
    Dim_NYC_Inspection_Actions = Dim_NYC_Inspection_Actions.withColumn("ACTION_SK", (monotonically_increasing_id() + 1).cast("integer")) \
                                    .withColumn('INGESTION_DATE', current_date()) \
                                    .withColumn('INGESTION_TIME', current_timestamp()) 


    # Selecting the specified columns for the Parquet table
    column_position = ["ACTION_SK", "ACTION", "INGESTION_DATE", "INGESTION_TIME"]
    Dim_NYC_Inspection_Actions = Dim_NYC_Inspection_Actions.select(column_position)
    
    file_name = "Dim_NYC_Inspection_Actions"
    complete_path = f"{output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Inspection_Actions.write.mode('overwrite').parquet(complete_path)
    
    print("Dim_NYC_Inspection_Actions created")
    

################################################################
# Creating Dimension Table Dim_NYC_Violation_Codes
################################################################
def Dim_NYC_Violation_Codes(parquet_df, output_path):
    # Creating a new DataFrame
    unique_violation_codes = parquet_df.select('violation_code', 'violation_description').distinct()

    Dim_NYC_Violation_Codes = unique_violation_codes.withColumnRenamed('violation_code', 'VIOLATION_CODE') \
                                                    .withColumnRenamed('violation_description', 'VIOLATION_DESCRIPTION')

    Dim_NYC_Violation_Codes = Dim_NYC_Violation_Codes.orderBy('VIOLATION_CODE')

    # Adding a new column 'date_created' with the current date
    Dim_NYC_Violation_Codes = Dim_NYC_Violation_Codes.withColumn("VIOLATION_CODE_SK", (monotonically_increasing_id() + 1).cast("integer")) \
                                    .withColumn('INGESTION_DATE', current_date()) \
                                    .withColumn('INGESTION_TIME', current_timestamp()) 


    # Selecting the specified columns for the Parquet table
    column_position = ["VIOLATION_CODE_SK", "VIOLATION_CODE", "VIOLATION_DESCRIPTION", "INGESTION_DATE", "INGESTION_TIME"]
    Dim_NYC_Violation_Codes = Dim_NYC_Violation_Codes.select(column_position)
    
    file_name = "Dim_NYC_Violation_Codes"
    complete_path = f"{output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    Dim_NYC_Violation_Codes.write.mode('overwrite').parquet(complete_path)
    
    print("Dim_NYC_Violation_Codes created")

def main():
    dim_table_output_path = "gs://food-inspection-data-modeling/Output_Data/Dimension_Table/"
    cred_path = "gs://food-inspection-data-modeling/Dependency/cred.txt"
    parquet_df_path = "gs://food-inspection-data-modeling/Raw_Data/NYC_Food_Inspection_raw_data.parquet"
    # Getting the Data from Sodapy API
    fetch_data_from_api(cred_path, parquet_df_path)

    # Read staging data for creating Dimensions
    parquet_df = spark.read.parquet("gs://food-inspection-data-modeling/Raw_Data/NYC_Food_Inspection_raw_data.parquet")
    
    Dim_NYC_Food_Places(parquet_df, dim_table_output_path)
    Dim_NYC_Cuisine(parquet_df, dim_table_output_path)
    Dim_NYC_Borough(parquet_df, dim_table_output_path)
    Dim_NYC_Addresses(parquet_df, dim_table_output_path)
    Dim_NYC_Critical_Flag(parquet_df, dim_table_output_path)
    Dim_NYC_Inspection_Grades(parquet_df, dim_table_output_path)
    Dim_NYC_Inspection_Type(parquet_df, dim_table_output_path)
    Dim_NYC_Inspection_Actions(parquet_df, dim_table_output_path)
    Dim_NYC_Violation_Codes(parquet_df, dim_table_output_path)
    
    spark.stop()
    print("Spark Session Stopped")
    print("----------------------------------------------------")
    
    print("Script Execution Complete, All Dimension Tables created")

if __name__ == "__main__":
    main()