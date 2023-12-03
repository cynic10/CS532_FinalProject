# -*- coding: utf-8 -*-
"""
Created on Sat Dec  2 13:20:28 2023

@author: riyad
"""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from time import time
from time import sleep
from DataAnalyser import DataAnalyser

def perform_core_partition_analysis(spark, da, partitions_list):
    
    num_cores =4
    for partitions in partitions_list:
       
            # Set the number of partitions
            spark.conf.set("spark.sql.shuffle.partitions", partitions)

            # Record the start time
            start_time = time()
            
            #Calling the data analyser method to run the various analytical functions
            da.process_data(spark)
            
            
            # Record the end time
            end_time = time()

            # Calculate and print the runtime
            runtime = end_time - start_time
            

            print(f"Configuration: Cores={num_cores}, Partitions={partitions}, Runtime = {runtime}")

if __name__ == "__main__":
    
    
    
    # Create a Spark session with the specific number of cores defined using local[]
    spark = SparkSession.builder \
    .appName("CorePartitionAnalysis") \
    .master("local[4]") \
    .config("spark.executor.memory", "4g")\
    .getOrCreate()
    
    #Create a data analyser object
    da = DataAnalyser()
    
    #Defining the list of partitions to vary
    partitions_list = [16, 32, 64]
    
    # Calling the function for core and partition analysis
    perform_core_partition_analysis(spark, da, partitions_list)

    # Stop the Spark session
    spark.stop()
