# KommatiPara

This Python script contains a data cleaning and transformation utility, specifically designed for the **KommatiPara** project. The script utilizes both the **PySpark** libraries to process and clean the input datasets.

### Requirements

Make sure to have the following Python packages installed in your environment before running the script:

- **pyspark**

### Functionality

The script performs the following tasks:

1. **Initializing SparkSession**: The script initializes a SparkSession using the `SparkSession.builder` with the application name "KommatiPara".

2. **Rotating Logging Setup**: The script sets up a rotating log file to record the data cleaning process. The log file name is `data_cleaning_rot.log`, and it will have a maximum size of 4 MB. Up to 4 backup log files will be created.

3. **Data Paths**: The script defines the paths for two input datasets (`dataset_one.csv` and `dataset_two.csv`) and a common path for the output directory.

4. **Data Cleaning Function**: The `clean_data` function is defined to perform the data cleaning and transformation tasks on the input datasets. The steps involved in the cleaning process are as follows:
   - Import the data from the specified paths into Spark DataFrames.
   - Filter the data based on the given list of countries using the `filter_countries` function.
   - Remove personal credentials from `dataset_one.csv` (columns `first_name` and `last_name`).
   - Remove the `cc_n` (credit card number) column from `dataset_two.csv`.
   - Join the filtered datasets on the `id` column using an inner join.
   - Rename columns `id`, `btc_a`, and `cc_t` to `client_identifier`, `bitcoin_address`, and `credit_card_type`, respectively, using the `rename_column` function.
   - Show the resulting DataFrame with the cleaned and transformed data.
   - Save the cleaned DataFrame as a CSV file named `dataset_cleaned` in the output directory.

5. **Filtering and Renaming Functions**: The script also defines two utility functions: `filter_countries` and `rename_column`. These functions are used within the `clean_data` function to perform specific tasks.

6. **Data Cleaning Invocation**: The script calls the `clean_data` function with the provided paths for `dataset_one.csv` and `dataset_two.csv` and a list of countries to filter the data (in this case, 'United Kingdom' and 'Netherlands').

7. **Stopping SparkSession**: Finally, the script stops the SparkSession to release the resources used by Spark.

### Usage

To use this data cleaning utility, ensure that the required packages are installed. You can run the script, and it will process the input datasets, perform the data cleaning, and save the cleaned data as a CSV file in the output directory. The log file will record the data cleaning process and any important messages or errors encountered during execution.

Please ensure you have the necessary data files in the specified paths (`df1_path` and `df2_path`) and adjust the paths as needed before running the script.

