from app.dependencies.spark import spark, log

def save_df(df, table_name):
    try:
        mode = "append"
        url = "jdbc:postgresql://postgres/postgres"
        properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified",
        }
        df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)


    except Exception as error:
        log.error(f"Failed to insert record into ${table_name} table, reason: {error}")

def read_df(table_name: str):
    try:

        df = spark.read.format('jdbc') .option("url", "jdbc:postgresql://postgres:5432/postgres") \
            .option("dbtable", table_name) \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        return df
    except Exception as err:
        log.error(f"Failed to read record from ${table_name} table, reason: {err}")
        return None
