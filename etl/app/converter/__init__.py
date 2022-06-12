from pyspark.sql.functions import col, to_date, to_timestamp, from_unixtime, explode, expr
from abc import ABC, abstractmethod
from app.converter.schemas import *


class Converter(ABC):
    def __init__(self, spark, name, columns):
        self.spark = spark
        self.__name = name
        self.__columns = columns
        super().__init__()

    @abstractmethod
    def create_df(self, data):
        pass

    def transform_df(self, df):
        df = df.select(*self.__columns)

        return df

    @property
    def columns(self):
        return self.__columns

    @property
    def name(self):
        return self.__name

    @property
    def columns(self):
        return self.__columns


class ShiftConverter(Converter):
    def __init__(self, spark):
        Converter.__init__(
            self,
            spark,
            name="shifts",
            columns=["shift_id", "shift_date", "shift_start", "shift_finish"],
        )
        self.schema = shift_schema

    def create_df(self, data):
        df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize(data), schema=self.schema
        ).withColumnRenamed("id", "shift_id")

        return df

    def transform_df(self, df):
        df = self.__rename_attributes(df)
        df = self.__convert_date_attr(df)
        # df = self.__calc_total_cost(df)
        df = df.select(*self.columns)

        return df

    def __rename_attributes(self, df):
        renamed_df = (
            df.withColumnRenamed("date", "shift_date")
            .withColumnRenamed("start", "shift_start")
            .withColumnRenamed("finish", "shift_finish")
        )

        return renamed_df

    def __convert_date_attr(self, df):
        df = (
            df.withColumn("shift_date", to_date(col("shift_date"), "yyyy-MM-dd"))
            .withColumn("shift_start", to_timestamp(col("shift_start") / 1e3))
            .withColumn("shift_finish", to_timestamp(col("shift_finish") / 1e3))
        )

        return df
    def __calc_total_cost(self, df):
        df.select(
            'shift_id',
            expr('AGGREGATE(allowances, 0, (acc, x) -> acc + x)').alias('Total_allowance_cost')
        ).show()

        return df


class BreakConverter(Converter):
    def __init__(self, spark):
        Converter.__init__(
            self,
            spark,
            name="breaks",
            columns=["break_id", "break_start", "break_finish", "is_paid", "shift_id"],
        )
        self.schema = break_schema

    def create_df(self, data):
        breaks_df = data.withColumn("break_obj", explode("breaks"))
        breaks_df = (
            breaks_df.withColumn("break_id", breaks_df["break_obj"].id)
            .withColumn("break_start", breaks_df["break_obj"].start)
            .withColumn("break_finish", breaks_df["break_obj"].finish)
            .withColumn("is_paid", breaks_df["break_obj"].paid)
        )

        return breaks_df

    def transform_df(self, df):
        df = self.__convert_date_attr(df)
        df = df.select(*self.columns)

        return df

    def __convert_date_attr(self, df):
        df = df.withColumn(
            "break_start", to_timestamp(col("break_start") / 1e3)
        ).withColumn("break_finish", to_timestamp(col("break_finish") / 1e3))

        return df


class AllowanceConverter(Converter):
    def __init__(self, spark):
        Converter.__init__(
            self,
            spark,
            name="allowances",
            columns=["allowance_id", "allowance_value", "allowance_cost", "shift_id"],
        )
        self.schema = allowance_schema

    def create_df(self, data):
        df = data.withColumn("allow_obj", explode("allowances"))
        df = (
            df.withColumn("allowance_id", df["allow_obj"].id)
            .withColumn("allowance_value", df["allow_obj"].value)
            .withColumn("allowance_cost", df["allow_obj"].cost)
        )

        return df


class AwardConverter(Converter):
    def __init__(self, spark):
        Converter.__init__(
            self,
            spark,
            name="award_interpretations",
            columns=["award_id", "award_date", "award_units", "award_cost", "shift_id"],
        )
        self.schema = award_schema

    def create_df(self, data):
        df = data.withColumn("award_obj", explode("award_interpretations"))
        df = (
            df.withColumn("award_id", df["award_obj"].id)
            .withColumn("award_date", df["award_obj"].date)
            .withColumn("award_units", df["award_obj"].units)
            .withColumn("award_cost", df["award_obj"].cost)
        )

        return df
