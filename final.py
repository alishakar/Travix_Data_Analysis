import os
import zipfile
from pyspark.sql.functions import lit
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType
import re
import shutil
import argparse

daily_clicks = "daily_clicks"
daily_transaction = "daily_transaction"

# List of zipped folder paths on the local system
zipped_folder_paths = "/Users/alishakar/Downloads/business_case_travix"

# Define the function to extract files
def extract_files(zip_file_path, extract_to_dir):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_dir)

def filter_and_remove_files(folder):
    pattern = re.compile(r'\d{4}-\d{2}-\d{2}\.(csv|xlsx)')
    for file in os.listdir(folder):
        if not pattern.match(file):
            os.remove(os.path.join(folder, file))

# Function to process each zip file
def process_zip_file(input_folder, file_name_prefix):
    file_name = file_name_prefix + ".zip"
    zip_path = os.path.join(input_folder, file_name)
    # Extract files to the directory where the zip file is located
    extract_files(zip_path, input_folder)

    extracted_folder = os.path.join(input_folder, file_name_prefix)
    filter_and_remove_files(extracted_folder)
    return extracted_folder

def process_daily_clicks_data(spark, input_folder):
    # Directory containing the CSV files
    csv_data = os.path.join(input_folder, daily_clicks)

    output_dir = os.path.join(input_folder, "output")
    output_csv = os.path.join(output_dir, daily_clicks + ".csv")

    # Read all CSV files from the directory
    df = spark.read.option("header", "true").csv(f"{csv_data}/*.csv")

    # Extract the date from the file path
    df = df.withColumn("file_path", input_file_name())
    df = df.withColumn("date", regexp_extract("file_path", r'(\d{4}-\d{2}-\d{2})', 1))

    # Drop the file_path column as it is no longer needed
    df = df.drop("file_path")

    # Order the DataFrame by date
    df = df.orderBy("date")

    # Combine all partitions into a single partition
    df = df.coalesce(1)

    # Temporary output directory
    temp_output_dir = os.path.join(output_dir, 'temp_output')

    # Save the combined DataFrame to a new CSV file
    df.write.option("header", "true").mode('overwrite').csv(temp_output_dir)

    # Find the generated part file and rename it to hello.csv
    for file_name in os.listdir(temp_output_dir):
        if file_name.startswith('part-') and file_name.endswith('.csv'):
            os.rename(os.path.join(temp_output_dir, file_name), output_csv)
            break

    # Clean up temporary directory
    for file_name in os.listdir(temp_output_dir):
        os.remove(os.path.join(temp_output_dir, file_name))

    os.rmdir(temp_output_dir)

    print(f"Combined CSV file saved as {output_csv}")

 # Function to read an Excel file and add a "date" column
def process_excel(file_path):
    # Extract the date from the filename (assuming filename format is "YYYY-MM-DD.xlsx")
    date = os.path.splitext(os.path.basename(file_path))[0]
    # Read the Excel file
    df = pd.read_excel(file_path)
    # Add the "date" column
    df['date'] = date
    return df
    
def process_daily_transaction_data(spark, input_folder):
    # Directory containing the Excel files
    excel_data = os.path.join(input_folder, daily_transaction)

    output_dir = os.path.join(input_folder, "output")
    output_excel = os.path.join(output_dir, daily_transaction + ".csv")
   
    # List all Excel files in the directory
    excel_files = [os.path.join(excel_data, f) for f in os.listdir(excel_data) if f.endswith('.xlsx')]

    # Read and process all Excel files in parallel
    # Create an RDD of the file paths
    rdd = spark.sparkContext.parallelize(excel_files)

    # Map each file path to the processed DataFrame, then collect the results
    processed_dataframes = rdd.map(lambda file: process_excel(file).astype(str)).collect()

    # Combine all the Pandas DataFrames into a single DataFrame
    combined_df = pd.concat(processed_dataframes, ignore_index=True)

    combined_df.sort_values(by='date', inplace=True)

    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    combined_df.to_csv(output_excel, index=False)

    print(f"Combined excel file saved as {output_excel}")
    return


def cleanup_extracted_data(input_folder):
    shutil.rmtree(os.path.join(input_folder, daily_clicks))
    shutil.rmtree(os.path.join(input_folder, daily_transaction))
    return


def main(input_folder):
    process_zip_file(input_folder, "daily_clicks")
    process_zip_file(zipped_folder_paths, "daily_transaction")

    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("process daily files") \
        .getOrCreate()
    
    process_daily_clicks_data(spark, zipped_folder_paths)

    process_daily_transaction_data(spark, zipped_folder_paths)

    # Stop the Spark session
    spark.stop()
    
    cleanup_extracted_data(zipped_folder_paths)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Merge daily data')
    parser.add_argument('-i', '--input', type=str, help='Input folder containing zip files')
    
    args = parser.parse_args()
    main(args.input)
    