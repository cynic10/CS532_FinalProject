# -*- coding: utf-8 -*-
"""
Created on Sat Dec  2 13:20:28 2023

@author: riyad

Description: Class to analyse the data based on various parameters and columns
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import mean
from time import time
from time import sleep

class DataAnalyser:
    
    
    def __init__(self):
        pass
    
    def process_data(self,spark):
        
        
        #Declaring the basepath for HDFS location and the individual file paths.
        basepath = 'hdfs://localhost:9200/CS532_Proj_Dataset/'
        
        files = ['AHS_Woman_08_Rajasthan_modified.csv',
                 'AHS_Woman_10_Bihar_modified.csv',
                    'AHS_Woman_18_Assam_modified.csv',
                    'AHS_Woman_20_Jharkhand_modified.csv',
                    'AHS_Woman_21_Odisha_modified.csv',
                    'AHS_Woman_22_Chhattisgarh_modified.csv',
                    'AHS_Woman_23_Madhya_Pradesh_modified.csv']
        
        
        #Naming the various states and colors to create various charts and their formatting
        states = ['Rajasthan', 'Bihar', 'Assam', 'Jharkhand', 'Odisha', 'Chattisgarh', 'Madhya Pradesh']
        
        colors = ['blue', 'green', 'red', 'magenta', 'cyan', 'black', 'orange']
        
        #Calling each function once for each file listed above to get state-wise analysis
        for i in range(len(files)):
            self.plot_conceived(spark, basepath+files[i], states[i])
            self.illness_type(spark, basepath+files[i], states[i])
            self.occupation_type(spark, basepath+files[i], states[i], colors[i])
            self.qualification_type(spark, basepath+files[i], states[i], colors[i])
            self.fpmethods(spark, basepath+files[i], states[i])
            
            
        
        
    #This function plots pie chart to verify the number of women who conceived v/s who have not conceived for each state.    
    def plot_conceived(self,spark, filepath, state):
        
        data = spark.read.option("header", "true").csv(filepath)
        
        

        # Convert 'ever_conceived' column to numeric type
        data = data.withColumn('ever_conceived', col('ever_conceived').cast('double'))

        # Select only the relevant column
        data = data.select('ever_conceived')

        count0 = data.filter(col('ever_conceived') == 1).count()
        count1 = data.filter(col('ever_conceived') == 2).count()

        # Create a pie chart using Matplotlib
        labels = ['Conceived', 'Never conceived']
        counts = [count0, count1]

        plt.figure()
        plt.pie(counts, labels=labels)
        plt.axis('equal')
        plt.title('Subjects who have Conceived: '+state)
        plt.savefig('plots/plotpie'+state+'.png')
    
    # Through this state we see the various illness types which women suffer typically for each state
    def illness_type(self, spark, filepath, state):
        
        data = spark.read.option("header", "true").csv(filepath)
        
        
        # Drop rows with NaN values in 'illness_type' column
        data = data.dropna(subset=['illness_type'])
        
        # Count occurrences of each illness type using DataFrame operations
        counts = [data.filter(col('illness_type') == i).count() for i in range(10)]
        
        labels = ['No Illness', 'Diarrhoea', 'Dysentery', 'Acute Respiratory Infection', 'Jaundice with Fever', 'Fever with chill/rigors (Malaria etc.)', 'Fever of Short Duration with Rashes', 'Other Types of Fever', 'Reproductive Tract Infection (RTI)', 'Others']
        
        # Convert NaN or non-integer values to zero
        counts = [int(c) if (isinstance(c, float) and np.isnan(c)) or not isinstance(c, int) else c for c in counts]
        
        plt.figure(figsize=(14, 10))
        plt.pie(counts)
        plt.legend(labels, loc='upper right', bbox_to_anchor=(1.4, 1))
        plt.title('Illness Types - '+state)
        plt.savefig('plots/plotpieIllness'+state+'.png')
        
    #The below function compares the occupation of people with the age at which the first conception took place to analyse the socio-economic impact of occupation on pregnancy     
    def occupation_type(self, spark, filepath, state, color):
        
        data = spark.read.option("header", "true").csv(filepath)

        # Select relevant columns
        data = data.select('age_at_first_conception', 'occupation_status')

        # Filter out rows where 'age_at_first_conception' is not equal to 0
        data = data.filter(data.age_at_first_conception != 0)

        # Group by 'occupation_status' and calculate the mean age
        mean_age_by_occupation = (
            data.groupBy('occupation_status')
                .agg(mean('age_at_first_conception').alias('mean_age'))
                .orderBy('occupation_status')
                .collect()
        )

        # Extract values for plotting
        occupations = [row['occupation_status'] for row in mean_age_by_occupation]
        mean_ages = [row['mean_age'] for row in mean_age_by_occupation]

        # Plot the bar chart
        plt.figure()
        plt.bar(occupations, mean_ages, color=color)
        plt.xticks(range(0,17,1))
        plt.xlabel('Occupation Status')
        plt.ylabel('Mean Age at First Conception')
        plt.title('Mean Age at First Conception for Each Occupation Status - '+state)
        plt.savefig('plots/plotbar'+state+'Occupation.png')
    
    #The below function compares the qualification of people with the age at which the first conception took place to analyse the socio-economic impact of it on pregnancy     
    def qualification_type(self, spark, filepath, state, color):
        # Read CSV files into PySpark DataFrames
        data = spark.read.option("header", "true").csv(filepath)

        # Select relevant columns
        data = data.select('age_at_first_conception', 'highest_qualification')

        # Filter out rows where 'age_at_first_conception' is not equal to 0
        data = data.filter(data.age_at_first_conception != 0)

        # Group by 'highest_qualification' and calculate the mean age
        mean_age_by_occupation = (
            data.groupBy('highest_qualification')
                .agg(mean('age_at_first_conception').alias('mean_age'))
                .orderBy('highest_qualification')
                .collect()
        )

        # Extract values for plotting
        occupations = [row['highest_qualification'] for row in mean_age_by_occupation]
        mean_ages = [row['mean_age'] for row in mean_age_by_occupation]

        # Plot the bar chart
        plt.figure()
        plt.bar(occupations, mean_ages, color=color)
        plt.xlabel('Highest Qualification')
        plt.ylabel('Mean Age at First Conception')
        plt.title('Mean Age at First Conception for Each Highest Qualification - '+state)
        plt.savefig('plots/plotbar'+state+'Qualification.png')
    
    #This function plots a pie chart of various contraceptive methods adopted by people across different states    
    def fpmethods(self, spark, filepath, state):
        
        # Read CSV files into PySpark DataFrames
        data = spark.read.option("header", "true").csv(filepath)
        
        # Drop NA values
        data = data.dropna(subset=["fp_method_used"])
        
        counts = data.groupBy('fp_method_used').count().orderBy('fp_method_used').collect()
        
        labels = ['Modern Method-Tubectomy', 'Modern Method- Vasectomy', 'Modern Method-Copper T/IUD', 'Modern Method-Pills, Daily', 'Modern Method- Pills, Weekly', 'Modern Method-Emergency Contraceptive Pill', 'Modern Method- Condom/Nirodh', 'Modern Method- Other', 'Traditional Method-Contraceptive Herbs', 'Traditional Method- Rhythm/periodic abstinence', 'Traditional Method- Withdrawal', 'Traditional Method- Lactational Amenorrhoea Method', 'Traditional Method- Other']
        
        counts_values = [row['count'] for row in counts]
        
        plt.figure(figsize=(14, 8))
        plt.pie(counts_values)
        plt.legend(labels, loc='upper right', bbox_to_anchor=(1.6, 1))
        plt.title('Contraceptive Methods - '+state)
        plt.savefig('plots/plotpieFP'+state+'.png')