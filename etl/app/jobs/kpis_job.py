from app.dependencies.spark import spark, log
from app.database import read_df
from app.jobs.utils import log_status
from pyspark.sql import Row
from pyspark.sql.functions import col, round, mean, min, datediff, current_date, max, lit, asc, lead
from pyspark.sql.window import Window
from app.converter.schemas import kpi_schema
from app.database import save_df
from datetime import datetime

@log_status
def calculate_kpis_job():
    results = []
    today = datetime.today().strftime('%Y-%m-%d')

    break_df = read_df(table_name="breaks")
    shifts_df = read_df(table_name="shifts")

    results.append(['mean_break_length_in_minutes', today, mean_break_len(break_df)])
    results.append(['total_number_of_paid_breaks', today, total_paid_breaks(break_df)])
    results.append(['mean_shift_cost', today, mean_shift_cost(shifts_df)])
    results.append(['max_break_free_shift_period_in_days', today, max_break_free_period(break_df, shifts_df)])

    break_df.unpersist()

    results.append(['min_shift_length_in_hours	', today, min_shift_len(shifts_df)])
    results.append(['max_allowance_cost_14d	', today, max_allowance_cost_14d(shifts_df)])

    shifts_df.unpersist()
    kpi_df = create_kpi_df(results)
    save_df(kpi_df, table_name='kpis')



def create_kpi_df(results):
    for result in results:
        result[2] = float(result[2])
    rowData = map(lambda x: Row(*x), results)
    kpi_df = spark.createDataFrame(rowData, kpi_schema)

    return kpi_df


def mean_break_len(break_df):
    log.info("Started task : MEAN BREAK TIME")

    if (not break_df):
        return
    break_df = break_df.withColumn('duration_min', round((col("break_finish").cast("long") - col('break_start').cast("long"))/60))
    stats_df = break_df.select(mean("duration_min").alias('mean')).collect()
    mean_duration_min = stats_df[0]['mean']

    return mean_duration_min

def total_paid_breaks(break_df):
    log.info("Started task : PAID BREAKS COUNT")
    paid_number = break_df.filter(break_df.is_paid == True).count()

    return paid_number

def min_shift_len(shifts_df):
    log.info("Started task : MIN SHIFT LEN")
    if (not shifts_df):
        return
    shifts_df = shifts_df.withColumn('len_hours', round((col("shift_finish").cast("long") - col('shift_start').cast("long"))/3600))
    stats_df = shifts_df.select(min("len_hours").alias('min_len_hours')).collect()
    min_length = stats_df[0]['min_len_hours']
    print(min_length)

    return min_length


def max_allowance_cost_14d(shifts_df):
    log.info("Started task : MAX ALLOWANCE COST 14d")
    allowance_df = read_df(table_name="allowances")
    if (not allowance_df or not shifts_df):
        return

    stats_df = allowance_df.join(shifts_df, ['shift_id']).\
        select(allowance_df["allowance_cost"],shifts_df["shift_date"])
    allowance_df.unpersist()
    stats_df = stats_df.withColumn("current_date", current_date())\
                .withColumn("date_diff", datediff(col('current_date'), col('shift_date')))
    stats_df.printSchema()
    stats_df = stats_df.filter(stats_df.date_diff <= 14) \
                .select(max("allowance_cost").alias('max_cost')).collect()
    max_cost = stats_df[0]['max_cost']


    return max_cost

def max_break_free_period(break_df, shifts_df):
    log.info("Started task : MAX BREAK FREE PERIOD")
    if (not shifts_df):
        return

    has_break_df = shifts_df.join(break_df, ['shift_id']) \
                    .select(shifts_df["shift_id"], shifts_df["shift_date"])\
                    .withColumn('has_break', lit(True))

    window = Window.partitionBy(["has_break"]).orderBy("shift_date")
    has_break_df = has_break_df.withColumn("next_date", lead('shift_date').over(window))
    has_break_df = has_break_df.withColumn("date_diff", datediff(col('next_date'), col('shift_date')))

    res_df = has_break_df.select(max('date_diff').alias('max_days_no_break')).collect()
    max_cost = res_df[0]['max_days_no_break']

    return max_cost

def mean_shift_cost(shifts_df):
    log.info("Started task : MEAN SHIFT COST")
    if (not shifts_df):
        return 0
    stats_df = shifts_df.select(mean('shift_cost').alias('mean_cost')).collect()
    mean_cost = stats_df[0]['mean_cost']

    return mean_cost






