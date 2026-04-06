from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = (
    SparkSession.builder
    .appName("Megaline_Rentabilidad_2025")
    .config("spark.sql.shuffle.partitions", "2000") 
    .getOrCreate()
)

df_fact = (
    app.read.format("jdbc")
    .option("url", db_url)
    .option("dbtable", "fact_monthly_revenue")
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", jdbc_driver)
    .load()
)

df_dim_plans = (
    app.read.format("jdbc")
    .option("url", db_url)
    .option("dbtable", "dim_plans")
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", jdbc_driver)
    .load()
)

avg_revenue_2025_df = (
    df_fact
    .filter(F.year(F.col("revenue_date")) == 2025)
    .join(
        df_dim_plans, 
        df_fact['plan_sk'] == df_dim_plans['plan_sk'], 
        'inner'
    )
    .groupby('plan_name')
    .agg(
        F.avg('total_revenue').alias('avg_revenue_2025')
    )
    .orderBy(F.desc('avg_revenue_2025'))
)

avg_revenue_2025_df.show()