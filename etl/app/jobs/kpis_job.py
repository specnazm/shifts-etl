from app.dependencies.spark import spark, log
from app.database import read_df
from app.jobs.utils import log_status
from pyspark.sql.functions import col, round, mean, min, datediff, current_date


@log_status
def calculate_kpis_job():
    # breaks_queries()
    shifts_df = read_df(table_name="shifts")
    # shift_queries(shifts_df)

    allowance_query(shifts_df)
    shifts_df.unpersist()
    #ucitaj iz baze
    #izvrsi upite
    #vrati u bazu



def breaks_queries():
    log.info("Started task : MEAN BREAK TIME, PAID BREAKS COUNT")
    break_df = read_df(table_name="breaks")
    if (not break_df):
        return
    break_df = break_df.withColumn('duration_min', round((col("break_finish").cast("long") - col('break_start').cast("long"))/60))
    stats_df = break_df.select(mean("duration_min").alias('mean')).collect()
    mean_duration_min = stats_df[0]['mean']
    paid_number = break_df.filter(break_df.is_paid == True).count()
    break_df.unpersist()

    return mean_duration_min, paid_number

def shift_queries(shifts_df):
    log.info("Started task : MIN SHIFT LEN")

    if (not shifts_df):
        return
    shifts_df = shifts_df.withColumn('len_hours', round((col("shift_finish").cast("long") - col('shift_start').cast("long"))/3600))
    shifts_df.show()
    stats_df = shifts_df.select(min("len_hours").alias('min_len_hours')).collect()
    min_length = stats_df[0]['min_len_hours']
    print(min_length)

    # paid_number = break_df.filter(break_df.is_paid == True).count()

def allowance_query(shifts_df):
    log.info("Started task : MAX ALLOWANCE COST 14d")
    allowance_df = read_df(table_name="allowances")

    #dodaj date iz shifts
    stats_df = allowance_df.join(shifts_df, ['shift_id']).\
        select(allowance_df["allowance_cost"],shifts_df["shift_date"])
    allowance_df.unpersist()

    # stats_df.select(
    #     col("shift_date"),
    #     current_date().as("current_date"),
    #     datediff(current_date(), col("shift_date")).as("datediff")
    # ).show()
    #
    #

    #filtriraj po datumu da je u posl 14 dana
    # uzmi max cost

    if (not allowance_df):
        return
    #join sa shift



