# -*- coding: utf-8 -*-
"""
Created on Sat Dec  2 19:20:24 2023

@author: riyad
"""
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import mean
from time import time
from time import sleep

def perform_throughput_analysis(spark, partitions_list, record_sizes, filepath):
    
    # Loading the dataframe from HDFS filepath provided
    
    
    input_data = spark.read.option("header", "true").csv(filepath)
    
    #Declaring a variable to print the number of cores declared while creating spark object
    num_cores = 2
    
    # Loop through number of partitions and number of records 
    for partitions in partitions_list:
        
        # Setting the number of partitions
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        
        for record_size in record_sizes:
            
            # Limiting the data to a specific record size to check throughput
            data = input_data.limit(record_size)

            # Record the start time
            start_time = time()
            
            #Analysing data for willingness to conceive v/s mean age across all states
            
            data = data.dropna(subset=['willing_to_get_pregnant', 'age'])
                
            # Convert 'willing_to_get_pregnant' to numeric for filtering
            data = data.withColumn('willing_to_get_pregnant', col('willing_to_get_pregnant').cast('float'))
                
            # Filter non-zero ages and convert 'age' to numeric
            data = data.filter(col('age') != 0).withColumn('age', col('age').cast('float'))
                
            # Get unique non-zero 'willing_to_get_pregnant' values
            unique_values = data.select('willing_to_get_pregnant').distinct().\
            filter(col('willing_to_get_pregnant') != 0.0).\
            collect()

            # Extract unique values from the collected DataFrame
            unique_values = [row['willing_to_get_pregnant'] for row in unique_values]
                
            # Calculate mean age for each 'willing_to_get_pregnant' category
            mean_agearr = []
            for value in unique_values:
                mean_age = data.filter(col('willing_to_get_pregnant') == value).agg({"age": "mean"}).collect()[0][0]
                mean_agearr.append(mean_age)
                
            # Plotting the bar chart
            ticks = ['Now', 'Later-Within 2 Years', 'Later-2 years or more', 'Do not want more children']
            plt.figure()
            plt.bar(ticks, mean_agearr, color='blue')
            plt.xticks(rotation=50)
            plt.subplots_adjust(bottom=0.4)
            plt.xlabel('Willing to get Pregnant')
            plt.ylabel('Mean Age')
            plt.title('Mean Age and Willingness to get Pregnant - All States')
            plt.savefig('plots/plotbarAge.png')
    
    
            # Record the end time
            end_time = time()

            # Calculate the runtime
            runtime = end_time - start_time

            # Calculate Throughput
            throughput = record_size / runtime

            print(f"Configuration: Cores={num_cores}, Partitions={partitions}, Record Size={record_size}, Throughput={throughput} records/second")

if __name__ == "__main__":
    
    
    
    # Create a Spark session with the specific number of cores defined using local[]
    spark = SparkSession.builder \
    .appName("CorePartitionAnalysis") \
    .master("local[2]") \
    .getOrCreate()
    
    
    #Setting the partitions to loop through
    partitions_list = [32, 64]  
    
    #Setting the number of records
    record_sizes = [100000, 5000000, 7000000] 
    
    filepath = 'hdfs://localhost:9200/CS532_Proj_Dataset/CombinedData_AllStates.csv'
    
    # Call the function for analysing throughput
    perform_throughput_analysis(spark, partitions_list, record_sizes, filepath)

    # Stop the Spark session
    spark.stop()
