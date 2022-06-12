def save_df(df, table_name):
    try:
        mode = "append"
        url = "jdbc:postgresql://postgres/postgres"
        properties = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver",
                      "stringtype": "unspecified"}
        df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

    except Exception as error:
        print(f"Failed to insert record into ${table_name} table")
