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


spark = SparkSession.builder.appName("NycFoodInspection").config("spark.sql.debug.maxToStringFields", "50000").getOrCreate()



def df_1(parquet_df, input_dim_table_path):
    dim_violation_code = "Dim_NYC_Violation_Codes"
    # Construct the full path to the parquet files
    dim_violation_code = input_dim_table_path + dim_violation_code

    # Read the parquet files into a DataFrame
    dim_violation_code = spark.read.parquet(dim_violation_code)

    # Explicitly specify DataFrame aliases for the join condition
    #joined_df_1 = parquet_df.alias("rdf").join(dim_food_places.alias("dim_food_places"), col("rdf.camis") == col("dim_food_places.CAMIS"), 'inner')

    joined_df_1 = parquet_df.alias("rdf").join(
            dim_violation_code.alias("dim_violation_code"),
            on=[
                (col("rdf.violation_code") == col("dim_violation_code.VIOLATION_CODE")) | (col("rdf.violation_code").isNull() & col("dim_violation_code.VIOLATION_CODE").isNull()),
                (col("rdf.violation_description") == col("dim_violation_code.VIOLATION_DESCRIPTION")) | (col("rdf.violation_description").isNull() & col("dim_violation_code.VIOLATION_DESCRIPTION").isNull())
            ],
            how="inner"
        )

    column_names = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis", "rdf.census_tract",
        "rdf.community_board", "rdf.council_district", "rdf.critical_flag", "rdf.dba",
        "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta", "rdf.phone",
        "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action", "rdf.cuisine_description",
        "rdf.inspection_type", "rdf.score", "rdf.grade", "rdf.grade_date", "rdf.violation_code",
        "rdf.violation_description", "dim_violation_code.VIOLATION_CODE_SK"
    ]

    return joined_df_1.select(*column_names)


def df_2(result_df_1, input_dim_table_path):
    dim_inscpection_action = "Dim_NYC_Inspection_Actions"
    # Construct the full path to the parquet files
    dim_inscpection_action = input_dim_table_path + dim_inscpection_action

    # Read the parquet files into a DataFrame
    dim_inscpection_action = spark.read.parquet(dim_inscpection_action)

    joined_df_2 = result_df_1.alias("rdf").join(
                        dim_inscpection_action.alias("dim_inscpection_action"),
                        (col("rdf.action") == col("dim_inscpection_action.ACTION")) | (col("rdf.action").isNull() & col("dim_inscpection_action.ACTION").isNull()),
                        'inner'
                    )

    column_names = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis", "rdf.census_tract",
        "rdf.community_board", "rdf.council_district", "rdf.critical_flag", "rdf.dba",
        "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta", "rdf.phone",
        "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action", "rdf.cuisine_description",
        "rdf.inspection_type", "rdf.score", "rdf.grade", "rdf.grade_date", "rdf.violation_code",
        "rdf.violation_description", "rdf.VIOLATION_CODE_SK", "dim_inscpection_action.ACTION_SK"
    ]

    return joined_df_2.select(*column_names)

def df_3(result_df_2, input_dim_table_path):
    dim_inspection_type = "Dim_NYC_Inspection_Type"
    # Construct the full path to the parquet files
    dim_inspection_type = input_dim_table_path + dim_inspection_type

    # Read the parquet files into a DataFrame
    dim_inspection_type = spark.read.parquet(dim_inspection_type)


    joined_df_3 = result_df_2.alias("rdf").join(
        dim_inspection_type.alias("dim_inspection_type"),
        on=[
            (col("rdf.inspection_type") == col("dim_inspection_type.INSPECTION_TYPE")) | (col("rdf.inspection_type").isNull() & col("dim_inspection_type.INSPECTION_TYPE").isNull())
        ],
        how="inner"
    )


    column_names = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis", "rdf.census_tract",
        "rdf.community_board", "rdf.council_district", "rdf.critical_flag", "rdf.dba",
        "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta", "rdf.phone",
        "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action", "rdf.cuisine_description",
        "rdf.inspection_type", "rdf.score", "rdf.grade", "rdf.grade_date", "rdf.violation_code",
        "rdf.violation_description", "rdf.VIOLATION_CODE_SK", "rdf.ACTION_SK", "dim_inspection_type.INSPECTION_TYPE_SK"
    ]


    return joined_df_3.select(*column_names)
         

def df_4(result_df_3, input_dim_table_path):
    dim_inspection_grades = "Dim_NYC_Inspection_Grades"
    # Construct the full path to the parquet files
    dim_inspection_grades = input_dim_table_path + dim_inspection_grades

    # Read the parquet files into a DataFrame
    dim_inspection_grades = spark.read.parquet(dim_inspection_grades)


    joined_df_4 = result_df_3.alias("rdf").join(
        dim_inspection_grades.alias("dim_inspection_grades"),
        on=[
            (col("rdf.grade") == col("dim_inspection_grades.GRADE")) | (col("rdf.grade").isNull() & col("dim_inspection_grades.GRADE").isNull())
        ],
        how="inner"
    )


    column_names = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis", "rdf.census_tract",
        "rdf.community_board", "rdf.council_district", "rdf.critical_flag", "rdf.dba",
        "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta", "rdf.phone",
        "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action", "rdf.cuisine_description",
        "rdf.inspection_type", "rdf.score", "rdf.grade", "rdf.grade_date", "rdf.violation_code",
        "rdf.violation_description", "rdf.VIOLATION_CODE_SK", "rdf.ACTION_SK", "rdf.INSPECTION_TYPE_SK", 
        "dim_inspection_grades.GRADE_SK"
    ]


    return joined_df_4.select(*column_names)


def df_5(result_df_4, input_dim_table_path):
    dim_critical_flag = "Dim_NYC_Critical_Flag"
    # Construct the full path to the parquet files
    dim_critical_flag = input_dim_table_path + dim_critical_flag

    # Read the parquet files into a DataFrame
    dim_critical_flag = spark.read.parquet(dim_critical_flag)


    joined_df_5 = result_df_4.alias("rdf").join(
        dim_critical_flag.alias("dim_critical_flag"),
        on=[
            (col("rdf.critical_flag") == col("dim_critical_flag.CRITICAL_FLAG")) | (col("rdf.critical_flag").isNull() & col("dim_critical_flag.CRITICAL_FLAG").isNull())
        ],
        how="inner"
    )


    column_names = [
        "rdf.bbl", "rdf.bin", "rdf.boro", "rdf.building", "rdf.camis", "rdf.census_tract",
        "rdf.community_board", "rdf.council_district", "rdf.critical_flag", "rdf.dba",
        "rdf.inspection_date", "rdf.latitude", "rdf.longitude", "rdf.nta", "rdf.phone",
        "rdf.record_date", "rdf.street", "rdf.zipcode", "rdf.action", "rdf.cuisine_description",
        "rdf.inspection_type", "rdf.score", "rdf.grade", "rdf.grade_date", "rdf.violation_code",
        "rdf.violation_description", "rdf.VIOLATION_CODE_SK", "rdf.ACTION_SK", "rdf.INSPECTION_TYPE_SK", 
        "rdf.GRADE_SK", "dim_critical_flag.CRITICAL_FLAG_SK"
    ]


    return joined_df_5.select(*column_names)

def df_6(result_df_5, FCT_NYC_Food_Inspections):
    joined_df_6 = result_df_5.alias("rdf").join(
    FCT_NYC_Food_Inspections.alias("FCT_I"),
    on=[
        (col("rdf.inspection_date") == col("FCT_I.INSCPECTION_DATE")) | (col("rdf.inspection_date").isNull() & col("FCT_I.INSCPECTION_DATE").isNull()),
        (col("rdf.phone") == col("FCT_I.PHONE")) | (col("rdf.phone").isNull() & col("FCT_I.PHONE").isNull())
    ],
    how="inner"
    )

    column_names = [
        "FCT_I.INSPECTION_SK", "FCT_I.INSCPECTION_DATE", "FCT_I.FOOD_PLACES_SK",
        "FCT_I.BOROUGH_SK", "FCT_I.ADDRESS_SK", "FCT_I.PHONE", "FCT_I.CUISINE_DESCRIPTION_SK",
        'FCT_I.INGESTION_DATE', 'FCT_I.INGESTION_TIME', "rdf.VIOLATION_CODE_SK", "rdf.ACTION_SK", 
        "rdf.INSPECTION_TYPE_SK", "rdf.GRADE_SK", "rdf.CRITICAL_FLAG_SK", "rdf.score", "rdf.grade_date", "rdf.action"
    ]

    return joined_df_6.select(*column_names)


def fact_food_inspection_violation(result_df_6, fact_output_path):
    group_by_columns = [
    "BOROUGH_SK", "INSPECTION_SK", "PHONE", "ACTION_SK",
    "CRITICAL_FLAG_SK", "GRADE_SK", "INSPECTION_TYPE_SK", "VIOLATION_CODE_SK"
    ]

    # Group by the specified columns and select only the desired columns
    FCT_NYC_FoodInspection_Vio = result_df_6.groupBy(group_by_columns).agg(
        first(col("FCT_I.INSCPECTION_DATE")).alias("INSCPECTION_DATE"),
        first(col("FCT_I.FOOD_PLACES_SK")).alias("FOOD_PLACES_SK"),
        first(col("FCT_I.ADDRESS_SK")).alias("ADDRESS_SK"),
        first(col("FCT_I.CUISINE_DESCRIPTION_SK")).alias("CUISINE_DESCRIPTION_SK"),
        first(col("FCT_I.INGESTION_DATE")).alias("INGESTION_DATE"),
        first(col("FCT_I.INGESTION_TIME")).alias("INGESTION_TIME"),
        first(col("rdf.score")).alias("score"),
        first(col("rdf.grade_date")).alias("grade_date"),
        first(col("rdf.action")).alias("action")
    )
    
    # Define a window specification for row_number
    window_spec = Window.orderBy("BOROUGH_SK")

    # Add a new column 'row_num' using row_number
    FCT_NYC_FoodInspection_Violations = FCT_NYC_FoodInspection_Vio.withColumn("row_num", F.row_number().over(window_spec))

    # Use row_num as the unique identifier for ADDRESS_SK
    FCT_NYC_FoodInspection_Violations = FCT_NYC_FoodInspection_Violations.withColumn("INSPECTION_VIOLATION_SK", F.col("row_num").cast("integer")) \
                                        .drop("row_num")  # Drop the temporary column

    # Adding a new column 'date_created' with the current date
    FCT_NYC_FoodInspection_Violations = FCT_NYC_FoodInspection_Violations.withColumn('INGESTION_DATE', current_date()) \
                                                        .withColumn('INGESTION_TIME', current_timestamp()) 

    FCT_NYC_FoodInspection_Violations= FCT_NYC_FoodInspection_Violations.withColumnRenamed("score", "SCORE") \
                                                    .withColumnRenamed('grade_date', 'GRADE_DATE') \
                                                    .withColumnRenamed('action', 'ACTION') \

    column_position = ["INSPECTION_VIOLATION_SK", "INSPECTION_SK", "INSPECTION_TYPE_SK", "ACTION_SK", 
                       "CRITICAL_FLAG_SK", "SCORE", "GRADE_SK", "VIOLATION_CODE_SK", "ADDRESS_SK", 
                       "BOROUGH_SK", "CUISINE_DESCRIPTION_SK", "FOOD_PLACES_SK",  "INSCPECTION_DATE", 
                       "PHONE", "ACTION", 'GRADE_DATE','INGESTION_DATE','INGESTION_TIME'
                      ]


    FCT_NYC_FoodInspection_Violations = FCT_NYC_FoodInspection_Violations.select(column_position)
    
    file_name = "FCT_NYC_FoodInspection_Violations"
    complete_path = f"{fact_output_path}/{file_name}"

    # Save the DataFrame as a Parquet table
    FCT_NYC_FoodInspection_Violations.write.mode('overwrite').partitionBy("BOROUGH_SK").parquet(complete_path)
    

def main():
    # Read the DataFrame from a Parquet file
    print("Main starts")
    parquet_df = spark.read.parquet("gs://food-inspection-data-modeling/Raw_Data/NYC_Food_Inspection_raw_data.parquet")
    input_dim_table_path = "gs://food-inspection-data-modeling/Output_Data/Dimension_Table/"
    fact_output_path = "gs://food-inspection-data-modeling/Output_Data/Fact_Table/"
    
    #Read fact_food_inspection
    foodInspections = "FCT_NYC_Food_Inspections"
    # Construct the full path to the parquet files
    FCT_NYC_Food_Inspections = fact_output_path + foodInspections
    # Read the parquet fact file into a DataFrame
    FCT_NYC_Food_Inspections = spark.read.parquet(FCT_NYC_Food_Inspections)
    
    result_df_1 = df_1(parquet_df, input_dim_table_path)
    result_df_2 = df_2(result_df_1, input_dim_table_path)
    result_df_3 = df_3(result_df_2, input_dim_table_path)
    result_df_4 = df_4(result_df_3, input_dim_table_path)
    result_df_5 = df_5(result_df_4, input_dim_table_path)
    result_df_6 = df_6(result_df_5, FCT_NYC_Food_Inspections)
    
    fact_food_inspection_violation(result_df_6, fact_output_path)
    
    spark.stop()
    print("Spark Session Stopped")
    print("----------------------------------------------------")
    
    print("fact_food_inspection_violation created")
    
if __name__ == "__main__":
    main()