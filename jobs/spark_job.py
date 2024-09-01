from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import col, median, count, max, min, avg
  
import os
import csv


spark = SparkSession.builder \
        .appName('Word count App') \
        .getOrCreate()
    
# EXTRACT
csv_content = []
filepath = SparkFiles.get('test.csv')
if os.path.exists(filepath):
    with open(filepath, 'r', newline='', encoding = "ISO-8859-1") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Пропускаем заголовок файла
        csv_content = [[x for x in row] for row in reader]

    df = spark.createDataFrame(csv_content)
    
    ### TRANSFORM
    # Rename columns
    old_names = [f'_{i}' for i in range(1, 13)]
    new_names = ['id', 'latitude', 'longitude', 'maintenance_year', 'square', 'population', 
                'region', 'locality_name', 'address', 'full_address', 'communical_service_id', 
                'description']
    for o, n in zip(old_names, new_names): df = df.withColumnRenamed(o, n)
    
    # Change type of columns
    change_types = [['id', 'int'], ['latitude', 'float'], ['longitude', 'float'], 
                    ['maintenance_year', 'int'], ['square', 'float'], ['population', 'int'], 
                    ['communical_service_id', 'int']]
    for n, t in change_types: df = df.withColumn(n, df[n].cast(t))
    
    # Average year of construction
    average_year = df.groupBy().agg(avg("maintenance_year").alias('maintenance_year'))['maintenance_year']
    print(f'Average year: {average_year}')

    # Median year of construction
    median_year = df.groupBy().agg(median("maintenance_year").alias('maintenance_year'))['maintenance_year']
    print(f'Median year: {median_year}')
    
    # Top 10 regions and cities with the largest number of objects
    top_highest_square = df \
        .groupBy('region') \
        .agg(max('square').alias('square')) \
        .orderBy('square') \
        .head(10)
    
    print(f'Top 10 regions and cities with the largest number of objects: \n'
        f'{top_highest_square}')
    
    # Buildings with maximum and minimum area within each area
    extreme_areas = df \
        .groupBy('region') \
        .agg(max('square').alias("max"), 
            min('square').alias("min")) \
        .show(10)
    
    print(f'Buildings with maximum and minimum area within each area: \n'
        f'{extreme_areas}')
    
    # Count construction group by decades
    # decades_buildings = df \
    #     .groupBy('decade') \
    #     .agg(count('*').alias('count'))
    
    # print(f'Count construction group by decades: \n'
    #     f'{decades_buildings.show(10)}')
    
    ### LOAD
    
    
    
else:
    pass

    spark.stop()