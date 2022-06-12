from pyspark.sql.functions import sum as _sum, col
from app.converter import *
from app.database import save_df
from app.jobs.utils import *
from app.dependencies.spark import spark, log
from app.jobs.kpis_job import calculate_kpis_job

def main():
    """Main ETL script definition.

    :return: None
    """

    extract_models_job()
    calculate_kpis_job()

    return None


@log_status
def extract_models_job():

    called: int = 0
    next_url: str = BASE_URL + INITIAL_URL
    while next_url and called < API_CALLS_LIMIT:
        response = send_request(next_url)
        if not response:
            log.error("extract_models_job stopped")
            break

        df_dict = extract_data(response["results"])
        transformed_dict = transform_data(df_dict)

        load_data(transformed_dict)

        next_url = get_next_url(response)
        called += 1


def extract_data(json_results):
    """Load data from api file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    log.info("Extracting data")
    shift_df = ShiftConverter(spark).create_df(json_results)
    breaks_df = BreakConverter(spark).create_df(shift_df)
    allowance_df = AllowanceConverter(spark).create_df(shift_df)
    award_df = AwardConverter(spark).create_df(shift_df)

    return {
        "shifts": shift_df,
        "breaks": breaks_df,
        "allowances": allowance_df,
        "award_interpretations": award_df,
    }


def transform_data(df_dict):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """
    log.info("Transforming")
    converters = [
        ShiftConverter(spark),
        BreakConverter(spark),
        AllowanceConverter(spark),
        AwardConverter(spark),
    ]

    transformed_dict = {}
    for conv in converters:
        df = df_dict[conv.name]
        transformed_dict[conv.name] = conv.transform_df(df)

    shifts = calc_total_cost(transformed_dict['allowances'], transformed_dict['award_interpretations'], transformed_dict['shifts'])
    transformed_dict['shifts'] = shifts

    return transformed_dict

def calc_total_cost(allowance_df, award_df, shift_df):
    allowance_df_with_cost = allowance_df.groupBy("shift_id").agg(
        _sum("allowance_cost").alias("total_allowance_cost")
    )
    award_df_with_cost = award_df.groupBy("shift_id").agg(
        _sum("award_cost").alias("total_award_cost")
    )
    joined_df = allowance_df_with_cost.join(award_df_with_cost, ['shift_id'])\
                .withColumn("shift_cost", col("total_allowance_cost") + col("total_award_cost"))
    shift_df = shift_df.join(joined_df, ['shift_id'], 'left_outer').select(shift_df["*"],joined_df["shift_cost"])


    return shift_df

def load_data(df_dict):
    """Collect data locally and write to PostgreSQL.

    :param df: DataFrame to print.
    :return: None
    """
    log.info("saving data")
    for key, df in df_dict.items():
        save_df(df, table_name=key)


if __name__ == "__main__":
    main()
    spark.stop()
