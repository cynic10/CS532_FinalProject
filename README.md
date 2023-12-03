# CS 532 Final Project: Big Data Analytics for Pregnancy Data

## Overview:

Pregnancy is a challenging phase in a woman’s life. Predicting the outcome of a pregnancy (succesful/unsuccesful/abnormalities/healthy) may also be aided by a number of social and economic factors. The outcome of the pregnancy affects the mother’s health in addition to the life of the newborn. Due to inadequate care, many women, particularly in developing or underdeveloped nations, experience several problems during pregnancy, which can have long-term effects on their physical and mental health. In order to reduce risks, prevent unintended pregnancies, and maintain the health of both the mother and the newborn child, it is crucial to analyze these diverse aspects, their degree of correlation, and their significance.

The dataset includes information on pregnancy outcomes, maternal health during pregnancy and the prenatal period, and
different indices of household economic conditions from surveys conducted in seven Indian states. Analyzing these many variables will help identify the causes of pregnancy problems as well as the effects of related physical, psychological, and socioeconomic variables on the course of the pregnancy. 

Through this project and the associated analysis, we also aim to measure variances in various system performance factors and process with the
changing system resources or other parameters. It includes gathering, cleaning, storing, analyzing data. We plan to use Hadoop with Spark for this performance analysis.

## Design and Analysis:

The dataset was preprocessed initially, to remove erroneous records, drop unnecessary columns, etc using Python. The dataset has been divided as per the column 'state'. For value of each state in this column a seperate file was created. The combined dataset as well as these individual state-wise files can be accessed through the drive link provided in the 'Dataset_Drive_Link' file in the 'Datasets' folder.

### File Structure:

HDFS was used to store the datasets. HDFS, designed for distributed storage, offers scalability and fault tolerance. An essential feature which was observed by us is the division of data into blocks, typically around 128 MB in size after loading into HDFS. This block-oriented approach allows for parallel processing, optimizing data distribution and retrieval. The HDFS UI upon observed, confirmed this block division, highlighting the system's adherence to this design. The significance lies in improved fault tolerance and increased parallel processing efficiency, minimizing data movement and enhancing overall performance. 

### Data Analysis

The dataset consists of the data of 7 Indian states. Through this data we have succesfully analysed the various relations:

1. For each state a distribution of the number of women who conceived v/s the ones who have never conceived.
2. The various illnesses which women suffer from and their distribution for each state which can impair the pregnancy outocme.
3. Primary occupation of women/their families compared with the age at which they first conceived.
4. Qualification of women compared with the age at which they first conceived.
5. Family planning methods practiced by couples to avoid unwanted pregnancy and their distribution across each state.
6. Willingness to conceive v/s mean age of conception.

From the analysis above, interesting trends were observed which ocnfirmed th initial prediction of impact of socio-economic conditions of women and their families to their pregnancy outcome and overall health. Many women report to have no major illneses but that it not reflected in their pregnancy outcomes. As the age increases (>30) more number of women wish to avoid pregnancy whereas 30 to 25 is the duration in which women aim to get pregnant ideally if desired. Also, many women especially from rural areas still rely on tradiitonal methods of contraception, efficacy of which is unknown.

### System Performance Analysis and Results:

#### Throughput Analysis:


The performance analysis of our Spark job revealed intriguing insights into the impact of varying configurations on throughput. When employing 2 cores, we observed a consistent trend across different partition sizes – an increase in throughput with larger record sizes. This behavior suggests that our job benefits from enhanced parallelism. Similarly, with 4 cores, the trend persists, showcasing the positive influence of increased core count on performance. Notably, throughput demonstrated a continuous rise with the number of records, without reaching a performance plateau. This phenomenon could be attributed to the fact that the data processing overhead is not fully harnessed and more number of data would probably cause a plateau. Upon hitting this observation initially, we used the combined dataset for all set instead of the partitioned state-specific dataset. However, that too did not reach a saturation point as per our observations.
The choice of configurations, such as using master("local[4]") for local testing and setting spark.sql.shuffle.partitions for influencing parallelism during shuffling operations, played a crucial role in achieving these results. These findings underscore the importance of tailoring configuration parameters to match the characteristics of the dataset and computing resources, with a continuous need for experimentation and optimization.

#### Runtime Analysis:

Our performance analysis further delves into the impact of varying configurations on runtime, shedding light on the efficiency of our Spark job. With 2 cores, the runtime exhibited a consistent decrease as the number of partitions increased – from 16 to 64. This trend suggests that increased parallelism achieved through more partitions contributes to a reduction in overall execution time. Similarly, the scenario with 4 cores revealed a notable decrease in runtime with higher partition counts. The runtime improvements observed across different core and partition configurations align with expectations, highlighting the efficiency gains from enhanced parallel processing. These results emphasize the crucial role of core and partition configuration in optimizing runtime performance, underscoring the importance of selecting configurations tailored to the specific characteristics of the dataset and computational resources.

## Running the codes:

If the codes are required to be run locally certain parameter changes will have to be performed.
First of all, the datasets can be accessed by the drive link provided. The file path provided in the codes for each files, will be replaced by either an HDFS location or a local file system path. 
By varying the partitions_list and record_sizes parameter, analysis for various record count and partitions can be done. Cores can be varied by putting the number of cores desired in the spark config "master(local[2])" variable. Here 2 indicates, 2 cores. Similarly this number can be varied as per requirements and system features.

Core_Partition_Analysis_Combined: By running this code, the runtime analysis can be obtained for values of a particular core and partitions. It call the DataAnalyser class methods to perform the data analysis described above.

Throughput_Analysis_Combined: This method, performs the throughput analysis of the entire combined dataset by varying the number of records and printing the associated throughput from it. It analyses the data to give mean age of conception v/s the willingness to get pregnant. It does not use the DataAnalyser class.

plots: This folder stores all the plots which get generated upon running the above codes.

Results: The results folder stores the results of the system performance which we got after we ran the above codes on our machine.

## Conclusion:
In conclusion, our project has delved into the intricacies of large-scale data processing using Apache Spark and HDFS, revealing valuable insights into performance optimization. Through a meticulous exploration of core counts, partition configurations, and runtime analyses, we identified the crucial impact of these parameters on the efficiency of our Spark job. The adoption of HDFS played a pivotal role in our project, providing a robust and scalable storage solution. The observation of data division into blocks, as confirmed by the HDFS UI, underscored the system's ability to manage data distribution efficiently. Our findings highlight the significance of tailoring configurations to dataset characteristics, showcasing the importance of parallelism in achieving optimal performance. As we move forward, these insights will guide our approach to further refine our data processing workflows, emphasizing the continued exploration of configuration optimizations and the seamless integration of distributed storage solutions to meet the evolving demands of large-scale data analytics.

