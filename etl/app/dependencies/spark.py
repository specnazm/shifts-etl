from pyspark.sql import SparkSession
from app.dependencies.logger import Log4j

def start_spark(app_name):
    """Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details
    :return: A tuple of references to the Spark session and logger
    """
    spark_builder = SparkSession.builder.appName(app_name)

    spark_sess = spark_builder.getOrCreate()
    spark_logger = Log4j(spark_sess)

    return spark_sess, spark_logger