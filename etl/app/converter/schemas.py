from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DateType,
    TimestampType,
    BooleanType,
    ArrayType,
    FloatType,
    DoubleType
)

award_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("units", FloatType(), True),
        StructField("cost", FloatType(), True),
    ]
)

allowance_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("value", FloatType(), True),
        StructField("cost", FloatType(), True),
    ]
)

break_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("start", LongType(), True),
        StructField("finish", LongType(), True),
        StructField("paid", BooleanType(), True),
    ]
)

shift_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("date", StringType(), True),
        StructField("start", LongType(), True),
        StructField("finish", LongType(), True),
        StructField("breaks", ArrayType(break_schema)),
        StructField("allowances", ArrayType(allowance_schema)),
        StructField("award_interpretations", ArrayType(award_schema)),
    ]
)


kpi_schema = StructType(
    [
        StructField("kpi_name", StringType(), True),
        StructField("kpi_date", StringType(), True),
        StructField("kpi_value", DoubleType(), True)
    ]
)
