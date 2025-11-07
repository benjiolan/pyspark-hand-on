import os


def spark_session():
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    # os.environ["SPARK_HOME"] = "/home/ben/spark-3.4.3-bin-hadoop3"
    os.environ["SPARK_HOME"] = (
        "/home/ben/github/pyspark-hand-on/.venv/lib64/python3.12/site-packages/pyspark"
    )
    spark_home = (
        "/home/ben/github/pyspark-hand-on/.venv/lib64/python3.12/site-packages/pyspark"
    )
    import findspark

    findspark.init(spark_home=spark_home)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    return spark
