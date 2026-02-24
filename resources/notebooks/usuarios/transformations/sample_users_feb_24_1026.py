from pyspark import pipelines as dp
from pyspark.sql.functions import col

# Configura widgets de entrada
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

@dp.table(name=f"{catalog}.{schema}.sample_users_feb_24_1026")

def sample_users_feb_24_1026():
    return (
        spark.read.table("samples.wanderbricks.users")
        .select("user_id", "email", "name", "user_type")
    )
