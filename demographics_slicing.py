from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import os

# Build session 
os.environ["JAVA_HOME"] = 'C:\Program Files\Java\jre-1.8'
spark = SparkSession.builder.appName('demographics').getOrCreate()

# Read the data
demo_data = spark.read.text(r'./data_slicing/us_1990_2020_19ages_adjusted.txt')

# Extract columns from value column
demo_data = demo_data.withColumn(
    "year",
    F.substring(F.col("value"), 1, 4)
    )

# Obtain unique values of the column 'year'
unique_years = demo_data.select("year").distinct()

# Iterate over unique_years
for row in unique_years.collect():
    year = row["year"]
    # Filter demo_data for the current year
    demo_data_year = demo_data.filter(demo_data.year == year)
    # Save the selection
    demo_data_year = demo_data_year.select("value")
    demo_data_year.toPandas().to_csv(
        f'C:/Users/guill/Documents/GitHub/lmos_opioids/data/source/county_demographics/demo_data_{int(year)}.csv',
        sep=',',
        index=False
    )