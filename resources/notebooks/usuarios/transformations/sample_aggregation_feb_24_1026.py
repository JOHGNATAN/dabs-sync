from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, count_if
from utilities import utils

catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

@dp.table(
    name= f"{catalog}.{schema}.sample_aggregation_feb_24_1026"
)
def sample_aggregation_feb_24_1026():
    return (
        spark.read.table(f"{catalog}.{schema}.sample_users_feb_24_1026")
        .withColumn("valid_email", utils.is_valid_email(col("email")))
        .groupBy(col("user_type"))
        .agg(
            count("user_id").alias("total_count"),
            count_if("valid_email").alias("count_valid_emails")
        )
    )
