from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import setuptools

'''
https://arcos.nd.edu/query
'''

# Ensure JAVA_HOME points to a JDK (not JRE) and matches the version required by PySpark
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-22"
spark = SparkSession.builder.appName("ARCOS_DataSlicing").getOrCreate()

spark

prescriptions_df = spark \
    .read \
    .format("csv") \
    .option("compression", "gzip") \
    .option("header", True) \
    .load("arcos-fulldataset-2006-2019.csv.gz")

prescriptions_df.show()

prescriptions_df = prescriptions_df \
    .select(
        "TRANSACTION_DATE",
        "TRANSACTION_CODE",
        "CALC_BASE_WT_IN_GM",
        "DOSAGE_UNIT",
        "BUYER_COUNTY",
        "BUYER_STATE",
        "BUYER_BUS_ACT",
        "DRUG_CODE",
        "DRUG_NAME",
        "MME_CONVERSION_FACTOR",
    )

prescriptions_df = prescriptions_df \
    .filter(
        prescriptions_df["TRANSACTION_CODE"] == "S"
    )

prescriptions_df = prescriptions_df \
    .filter(
        (prescriptions_df["BUYER_BUS_ACT"] == "RETAIL PHARMACY") | 
        (prescriptions_df["BUYER_BUS_ACT"] == "TEACHING INSTITUTION") |
        # Hospitals and clinics
        (prescriptions_df["BUYER_BUS_ACT"] == "HOSP/CLINIC FED") |
        (prescriptions_df["BUYER_BUS_ACT"] == "HOSP/CLINIC NG") |
        (prescriptions_df["BUYER_BUS_ACT"] == "HOSP/CLINIC- MIL") |
        (prescriptions_df["BUYER_BUS_ACT"] == "HOSP/CLINIC-VA") |
        (prescriptions_df["BUYER_BUS_ACT"] == "HOSPITAL/CLINIC") | 
        # Mid-level practitioners
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-AMBULANCE SERVICE") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-ANIMAL SHELTER") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-DEPT OF STATE") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-EUTHANASIA TECHNICIAN") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-MILITARY") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-NATUROPATHIC PHYSICIAN") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-NURSE PRACTITIONER") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-NURSE PRACTITIONER-DW/100") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-NURSE PRACTITIONER-DW/30") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-NURSE PRACTITIONER-DW/30SW") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-NURSING HOME") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-OPTOMETRIST") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-PHYSICIAN ASSISTANT") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-PHYSICIAN ASSISTANT FED") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-PHYSICIAN ASSISTANT-DW/100") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-PHYSICIAN ASSISTANT-DW/30") |
        (prescriptions_df["BUYER_BUS_ACT"] == "MLP-REGISTERED PHARMACIST") |
        # Practitioners
        (prescriptions_df["BUYER_BUS_ACT"] == "PRACTITIONER") |
        (prescriptions_df["BUYER_BUS_ACT"] == "PRACTITIONER FED") |
        (prescriptions_df["BUYER_BUS_ACT"] == "PRACTITIONER-DW/100") |
        (prescriptions_df["BUYER_BUS_ACT"] == "PRACTITIONER-DW/275") |
        (prescriptions_df["BUYER_BUS_ACT"] == "PRACTITIONER-DW/30") |
        (prescriptions_df["BUYER_BUS_ACT"] == "PRACTITIONER-MILITARY")
    )

prescriptions_df = prescriptions_df \
    .select(
        "TRANSACTION_DATE",
        "CALC_BASE_WT_IN_GM",
        "DOSAGE_UNIT",
        "BUYER_COUNTY",
        "BUYER_STATE",
        "DRUG_CODE",
        "DRUG_NAME",
        "MME_CONVERSION_FACTOR",
    )

prescriptions_df = prescriptions_df \
    .withColumn(
        "year",
        F.substring(
            F.col("TRANSACTION_DATE"),
            7,
            4
        )
    )

prescriptions_df = prescriptions_df \
    .withColumn(
        "month",
        F.substring(
            F.col("TRANSACTION_DATE"),
            1,
            2
        )
    )

prescriptions_df = prescriptions_df  \
    .withColumn(
        "CALC_BASE_WT_IN_GM",
        prescriptions_df["CALC_BASE_WT_IN_GM"].cast("float")
    )

prescriptions_df = prescriptions_df  \
    .withColumn(
        "DOSAGE_UNIT",
        prescriptions_df["DOSAGE_UNIT"].cast("float")
    )

prescriptions_df = prescriptions_df  \
    .withColumn(
        "MME_CONVERSION_FACTOR",
        prescriptions_df["MME_CONVERSION_FACTOR"].cast("float")
    )

prescriptions_df = prescriptions_df \
    .groupBy(
        "year",
        "month",
        "BUYER_COUNTY",
        "BUYER_STATE",
        "DRUG_CODE",
        "DRUG_NAME",
    ).sum(
        "CALC_BASE_WT_IN_GM",
        "DOSAGE_UNIT",
        "MME_CONVERSION_FACTOR"
    )

prescriptions_df = prescriptions_df \
    .withColumnRenamed(
        "sum(CALC_BASE_WT_IN_GM)",
        "CALC_BASE_WT_IN_GM"
    )

prescriptions_df = prescriptions_df \
    .withColumnRenamed(
        "sum(DOSAGE_UNIT)",
        "DOSAGE_UNIT"
    )

prescriptions_df = prescriptions_df \
    .withColumnRenamed(
        "sum(MME_CONVERSION_FACTOR)",
        "MME_CONVERSION_FACTOR"
    )

#prescriptions_df.show()

years = prescriptions_df.select("year").distinct().collect()

for year in years:
    year = year["year"]
    year_df = prescriptions_df.filter(prescriptions_df["year"] == year)
    year_df.toPandas().to_csv(f"C:/Users/g-mart36/Documents/GitHub/lmos_opioids/data/source/prescriptions/prescriptions_{year}.csv", index=False)
