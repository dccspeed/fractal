from pyspark.sql import SparkSession


def fractal_jar_path():
    import os
    script_path = os.path.dirname(os.path.realpath(__file__))
    os.environ["PYFRACTAL_LIB"] = script_path
    return f"{script_path}/fractal-core-SPARK-3.5.0.jar"

def DefaultSparkBuilder():
    return SparkSession.builder \
        .config("spark.jars.packages",
                "com.koloboke:koloboke-impl-jdk8:1.0.0,com.typesafe.akka:akka-remote_2.13:2.5.23") \
        .config("spark.jars", fractal_jar_path())

