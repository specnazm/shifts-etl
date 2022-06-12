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


shift_schema = StructType(
    [
        StructField("shift_id", StringType(), True),
        StructField("shift_date", StringType(), True),
        StructField("shift_start", StringType(), True),
        StructField("shift_finish", StringType(), True),
        StructField("shift_cost", DoubleType(), True)
    ]
)


break_schema = StructType(
    [
        StructField("break_id", StringType(), True),
        StructField("shift_id", StringType(), True),
        StructField("break_start", LongType(), True),
        StructField("break_finish", LongType(), True),
        StructField("is_paid", BooleanType(), True),
    ]
)