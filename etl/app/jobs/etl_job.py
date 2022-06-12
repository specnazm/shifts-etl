from app.dependencies.spark import start_spark
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    to_date,
    to_timestamp,
    from_unixtime,
    explode,
)
import requests
from app.converter import *
from app.database import save_df
from app.jobs.utils import *

spark, log = start_spark(app_name="shift-app")


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
        # for df in df_dict.values():
        #     df.show()
        transformed_dict = transform_data(df_dict)
        # for df in transformed_dict.values():
        #     df.show()
        load_data(transformed_dict)

        next_url = get_next_url(response)
        called += 1


def extract_data(json_results):
    """Load data from api file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
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

    return transformed_dict


def load_data(df_dict):
    """Collect data locally and write to PostgreSQL.

    :param df: DataFrame to print.
    :return: None
    """
    for key, df in df_dict.items():
        save_df(df, table_name=key)


@log_status
def calculate_kpis_job():
    pass


if __name__ == "__main__":
    main()
    spark.stop()
