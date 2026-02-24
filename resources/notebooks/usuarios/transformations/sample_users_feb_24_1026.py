from pyspark import pipelines as dp
from pyspark.sql.functions import col

@dp.table()

def sample_users_feb_24_1026():
    return (
        spark.read.table("samples.wanderbricks.users")
        .select("user_id", "email", "name", "user_type")
    )
