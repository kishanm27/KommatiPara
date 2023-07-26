import sys
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("KommatiPara").getOrCreate()

# Set up rotating logging
log_file = 'data_cleaning_rot.log'
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler = RotatingFileHandler(log_file, maxBytes= 4000000,backupCount=4)
log_handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)


# Paths used for the data
df1_path = '../KommatiPara/DATA/dataset_one.csv'
df2_path = '../KommatiPara/DATA/dataset_two.csv'
df_path = '../KommatiPara/DATA/'

def clean_data(path_x, path_y, countries):
    """ This function cleans the data
    
    This function performs data cleaning and transformation tasks:

    Import data from the specified paths.
    Filter the data based on the provided country list using the filter_countries() function.
    Rename the columns using the rename_column() function.
    Remove personal credentials from the two datasets.
    Finally, it joins the transformed datasets and saves the result in a CSV file.
    """
  # Df creation log
    logging.info("Creating DataFrame from {} and {}".format(path_x, path_y))

    #Import the data from given path
    df1 = spark.read.csv(path_x, header=True, inferSchema=True)
    df2 = spark.read.csv(path_y, header=True, inferSchema=True)
    
    #Filter based on given countries
    df1 = filter_countries(df1, 'country',countries)
    logging.info("Filtered Df1 based on these given countries: {}".format(countries))
 
    # remove identifiable info from df1
    columns_to_remove = ['first_name', 'last_name']
    df1 = df1.drop(*columns_to_remove)
    logging.info("Removed identifiable info from df1")

    
    # remove credit card number from df2
    df2= df2.drop('cc_n')
    logging.info("Dropped credit number column from df2")
    
    # Join Df
    df = df1.join(df2, on='id', how='inner')
    logging.info("Joined DataFrames")

    # rename columns
    df=rename_column(df,'id', 'client_identifier')
    df=rename_column(df,'btc_a', 'bitcoin_address')
    df=rename_column(df,'cc_t', 'credit_card_type')
    logging.info("Renamed columns")
    
    df.show()
    
    # Extract the directory from the df1_path
    output_folder = os.path.dirname(os.path.abspath(df_path))

    # Write the DataFrame to a CSV file within the output folder
    output_path = os.path.join(output_folder, 'dataset_cleaned')
    df.write.csv(output_path, header=True, mode='overwrite')
    logging.info("DataFrame saved to CSV: {}".format(output_path))


    
    return df

def filter_countries(df,column: str, countries):
    """Return filtered data
    
    This function takes tree arguments: the data, column name and a list of countries.
    The function filters out every other country from the data except the ones in the list.
    """
    df = df.filter(df[column].isin(countries))
    return df


def rename_column(df, old_name, new_name):
    """Returns data with the new given column names
    
    This function uses the Pyspark withColumnRenamed to rename a column
    """
    return df.withColumnRenamed(old_name, new_name)

# Call the data cleaning functionn
clean_data(df1_path,df2_path, ['United Kingdom', 'Netherlands'])
spark.stop()
