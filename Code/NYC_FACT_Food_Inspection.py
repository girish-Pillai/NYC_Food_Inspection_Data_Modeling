import os
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import current_date, current_timestamp
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("NycFoodInspection").config("spark.sql.debug.maxToStringFields", "50000").getOrCreate()

################################################################
# Staging table joined to Dim_NYC_Food_Places
################################################################
def df_1(parquet_df, input_dim_table_path):
    dim_food_places = "Dim_NYC_Food_Places"
    # Construct the full path to the parquet files
    dim_food_places = input_dim_table_path + dim_food_places

    # Read the parquet files into a DataFrame
    dim_food_places = spark.read.parquet(dim_food_places)

    joined_df_1 = parquet_df.alias("rdf").join(
                        dim_food_places.alias("dim_food_places"),
                        (col("rdf.camis") == col("dim_food_places.CAMIS")) | (col("rdf.camis").isNull() & col("dim_food_places.CAMIS").isNull()),
                        'inner'
                    )

    column_names = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis", "rdf.census_tract",
        "rdf.community_board", "rdf.council_district", "rdf.critical_flag", "rdf.dba",
        "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta", "rdf.phone",
        "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action", "rdf.cuisine_description",
        "rdf.inspection_type", "rdf.score", "rdf.grade", "rdf.grade_date", "rdf.violation_code",
        "rdf.violation_description", "dim_food_places.FOOD_PLACES_SK"
    ]

    return joined_df_1.select(*column_names)


################################################################
# Result of df_1 joined to Dim_NYC_Borough
################################################################
def df_2(result_df_1, input_dim_table_path):
    dim_borough = "Dim_NYC_Borough"
    # Construct the full path to the parquet files
    dim_borough = input_dim_table_path + dim_borough

    # Read the parquet files into a DataFrame
    dim_borough = spark.read.parquet(dim_borough)

    joined_df_2 = result_df_1.alias("rdf").join(
                        dim_borough.alias("dim_borough"),
                        (col("rdf.boro") == col("dim_borough.BOROUGH")) | (col("rdf.boro").isNull() & col("dim_borough.BOROUGH").isNull()),
                        'inner'
                    )

    column_names = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis", "rdf.census_tract",
        "rdf.community_board", "rdf.council_district", "rdf.critical_flag", "rdf.dba",
        "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta", "rdf.phone",
        "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action", "rdf.cuisine_description",
        "rdf.inspection_type", "rdf.score", "rdf.grade", "rdf.grade_date", "rdf.violation_code",
        "rdf.violation_description", "rdf.FOOD_PLACES_SK", "dim_borough.BOROUGH_SK"
    ]

    return joined_df_2.select(*column_names)


################################################################
# Result of df_2 joined to Dim_NYC_Addresses
################################################################
def df_3(result_df_2, input_dim_table_path):
    dim_addresses = "Dim_NYC_Addresses"
    # Construct the full path to the parquet files
    dim_addresses = input_dim_table_path + dim_addresses

    # Read the parquet files into a DataFrame
    dim_addresses = spark.read.parquet(dim_addresses)


    joined_df_3 = result_df_2.alias("rdf").join(
        dim_addresses.alias("dim_addresses"),
        on=[
            (col("rdf.bin") == col("dim_addresses.BIN")) | (col("rdf.bin").isNull() & col("dim_addresses.BIN").isNull()),
            (col("rdf.bbl") == col("dim_addresses.BBL")) | (col("rdf.bbl").isNull() & col("dim_addresses.BBL").isNull()),
            (col("rdf.zipcode") == col("dim_addresses.ZIPCODE")) | (col("rdf.zipcode").isNull() & col("dim_addresses.ZIPCODE").isNull())
        ],
        how="inner"
    )


    # Select columns up to ADDRESS_SK
    selected_columns = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis",
        "rdf.census_tract", "rdf.community_board", "rdf.council_district", "rdf.critical_flag",
        "rdf.dba", "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta",
        "rdf.phone", "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action",
        "rdf.cuisine_description", "rdf.inspection_type", "rdf.score", "rdf.grade",
        "rdf.grade_date", "rdf.violation_code", "rdf.violation_description", "rdf.FOOD_PLACES_SK",
        "rdf.BOROUGH_SK", "dim_addresses.ADDRESS_SK"
    ]

    return joined_df_3.select(*selected_columns)


################################################################
# Result of df_3 joined to Dim_NYC_Cuisine
################################################################
def df_4(result_df_3, input_dim_table_path):
    dim_cuisine = "Dim_NYC_Cuisine"
    # Construct the full path to the parquet files
    dim_cuisine = input_dim_table_path + dim_cuisine

    # Read the parquet files into a DataFrame
    dim_cuisine = spark.read.parquet(dim_cuisine)

    # Explicitly specify DataFrame aliases for the join condition
    joined_df_4 = result_df_3.alias("rdf").join(
                        dim_cuisine.alias("dim_cuisine"),
                        (col("rdf.cuisine_description") == col("dim_cuisine.CUISINE_DESCRIPTION")) | (col("rdf.cuisine_description").isNull() & col("dim_cuisine.CUISINE_DESCRIPTION").isNull()),
                        'inner'
                    )

    column_names = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis", "rdf.census_tract",
        "rdf.community_board", "rdf.council_district", "rdf.critical_flag", "rdf.dba",
        "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta", "rdf.phone",
        "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action", "rdf.cuisine_description",
        "rdf.inspection_type", "rdf.score", "rdf.grade", "rdf.grade_date", "rdf.violation_code",
        "rdf.violation_description", "rdf.FOOD_PLACES_SK", "rdf.BOROUGH_SK", "rdf.ADDRESS_SK",
        "dim_cuisine.CUISINE_DESCRIPTION_SK"
    ]

    return joined_df_4.select(*column_names)
    
    
################################################################
# Creating 1st Fact using df_4
################################################################
def FCT_NYC_Food_Inspections(result_df_4, fact_output_path):
    FCT_NYC_Food_Inspections = result_df_4.groupBy(
                                            "inspection_date", "camis", "boro", "zipcode", "phone", "cuisine_description", "bin", "bbl"
                                ).agg(
                                    {"FOOD_PLACES_SK": "first",  # You can use any aggregation function based on your requirement
                                     "BOROUGH_SK": "first",
                                     "ADDRESS_SK": "first",
                                     "CUISINE_DESCRIPTION_SK": "first"}
                                ).select(
                                    "inspection_date", "first(FOOD_PLACES_SK)", "first(BOROUGH_SK)",
                                    "first(ADDRESS_SK)", "first(CUISINE_DESCRIPTION_SK)", "phone"
                                ).toDF(
                                    "inspection_date", "FOOD_PLACES_SK", "BOROUGH_SK",
                                    "ADDRESS_SK", "CUISINE_DESCRIPTION_SK", "phone"
                                )
    
    # Define a window specification for row_number
    window_spec = Window.orderBy("FOOD_PLACES_SK")

    # Add a new column 'row_num' using row_number
    FCT_NYC_Food_Inspections = FCT_NYC_Food_Inspections.withColumn("row_num", F.row_number().over(window_spec))

    # Use row_num as the unique identifier for ADDRESS_SK
    FCT_NYC_Food_Inspections = FCT_NYC_Food_Inspections.withColumn("INSPECTION_SK", F.col("row_num").cast("integer")) \
                                        .drop("row_num")  # Drop the temporary column

    # Adding a new column 'date_created' with the current date
    FCT_NYC_Food_Inspections = FCT_NYC_Food_Inspections.withColumn('INGESTION_DATE', current_date()) \
                                                        .withColumn('INGESTION_TIME', current_timestamp()) 

    FCT_NYC_Food_Inspections= FCT_NYC_Food_Inspections.withColumnRenamed("inspection_date", "INSCPECTION_DATE") \
                                                    .withColumnRenamed('phone', 'PHONE') \

    column_position = ["INSPECTION_SK", "INSCPECTION_DATE", "FOOD_PLACES_SK", "BOROUGH_SK", "ADDRESS_SK", "PHONE", "CUISINE_DESCRIPTION_SK", 'INGESTION_DATE', 'INGESTION_TIME']

    FCT_NYC_Food_Inspections = FCT_NYC_Food_Inspections.select(column_position)
    
    file_name = "FCT_NYC_Food_Inspections"
    complete_path = f"{fact_output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    FCT_NYC_Food_Inspections.write.mode('overwrite').partitionBy("BOROUGH_SK").parquet(complete_path)
    
    
    
def main():
    # Read the DataFrame from a Parquet file
    print("Main starts")
    parquet_df = spark.read.parquet("gs://food-inspection-data-modeling/Raw_Data/NYC_Food_Inspection_raw_data.parquet")
    input_dim_table_path = "gs://food-inspection-data-modeling/Output_Data/Dimension_Table/"
    fact_output_path = "gs://food-inspection-data-modeling/Output_Data/Fact_Table/"
    
    result_df_1 = df_1(parquet_df, input_dim_table_path)
    result_df_2 = df_2(result_df_1, input_dim_table_path)
    result_df_3 = df_3(result_df_2, input_dim_table_path)
    result_df_4 = df_4(result_df_3, input_dim_table_path)
    FCT_NYC_Food_Inspections(result_df_4, fact_output_path)
    
    spark.stop()
    print("Spark Session Stopped")
    print("----------------------------------------------------")
    
    print("FCT_NYC_Food_Inspections created")
    
if __name__ == "__main__":
    main()