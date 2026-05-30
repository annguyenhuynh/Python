import sys
from datetime import datetime, timedelta
from pyspark.context import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession 
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from dateutil import relativedelta
import boto3

#-----Spark and AWS Init
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client('s3')

# --- Config ---
data_bucket = "credence-cloudwatch-metrics-raw"
report_bucket = "credence-cloudwatch-metrics-reports"
prefix = "ec2-metrics/"
tenants = ["eda", "wawf"]
environment = "prod"

#----- Previous month range
now = datetime.now()
first_day_of_current_month = datetime(now.year, now.month, 1)
last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
month = last_day_of_previous_month.strftime('%b')
year_num = now.year
month_num = now.month - 1

# Accounts for January getting last month of previous year
if now.month == 1:
    month_num = 12
    year_num = now.year -1 

# Accounts for the required two digits on one digit months
elif now.month < 11:
    month_num = f"0{str(month_num)}"

year_num = str(year_num)
month_num = str(month_num)

rnd = 0 # Number of digits to round to

start_date = f"{year_num}-{month_num}-01"
end_date = f"{year_num}-{month_num}-31"

#----- Load and clean raw data
def read_data(tenant):
    path = f"s3://{data_bucket}/{prefix}{tenant}/{environment}/"
    print(f"Reading, {path}")

    df =(spark.read
        .option("header", True)
        .csv(path)
        .filter(col("instance_type") != "terminanted_instance")
        .filter(col("DATE").between(start_date, end_date))
        .withColumn("timestamp", to_timestamp(concat_ws(col("DATE"), col("TIME"))))
        .withColumn("host_name", regexp_replace("host_name", r"\['|'\]", ""))
    )
    return df

#-----Compute Uptime Metrocs
def compute_uptime(df, tenant):
    return (
        df.groupBy("instance_id", "instance_name", "environment", "instance_type", "host_name")
        .agg(
            count("*").alias("Recorded Uptime"),
            min("timestamp").alias("First Recorded Time"),
            max("timestamp").alias("Last Recorded Time"),
            round(max("timestamp").cast("long") - min("timestamp").cast("long") + 300 / 3600, 0).alias("Total Availability (Hours)")
        )
        .withColumn("Program", lit(tenant))
    )

#-----Process tenant
all_metrics = []
for tenant in tenants:
    df = read_data(tenant)
    uptime_df = compute_uptime(df, tenant)
    all_metrics.append(uptime_df)

#-----Merge results
final_df = all_metrics[0].unionByName(*all_metrics[1:]) if len(all_metrics) > 1 else all_metrics[0]

#-----Format output
final_df = (
    final_df
    .withColumn("Recorded Uptime", round(col("Recorded Uptime") / 12, 0))  # 12 records per hour
    .withColumnRenamed("instance_id", "Instance ID")
    .withColumnRenamed("instance_name", "Instance Name")
    .withColumnRenamed("environment", "Environment")
    .withColumnRenamed("instance_type", "Instance Type")
    .withColumn(
                "Percent Uptime",
                when(col("Total Availability (Hours)") == 'N/A', lit('N/A'))
                .otherwise(round(col("Recorded Uptime") / col("Total Availability (Hours)"), 2))
            )
    .withColumn("IP Address", when(
        col("host_name").rlike(r"ip-\d{1,3}-\d{1,3}-\d{1,3}-\d{1,3}"),
        regexp_replace(regexp_extract(col("host_name"), r"ip-(\d{1,3}-\d{1,3}-\d{1,3}-\d{1,3})", 1), "-", ".")
    ).otherwise(None))
)
print("Final DF count", final_df.count())
final_df.show()

output_path = f"s3://{report_bucket}/Uptime_Reports/EDA_WAWF_Prod/{year_num}/{month_num}"

final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print("EDA & WAWF Prod Uptime Report Complete")