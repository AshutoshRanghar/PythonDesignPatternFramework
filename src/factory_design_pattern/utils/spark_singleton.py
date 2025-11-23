from pyspark.sql import SparkSession

class SparkSingleton:
    """Singleton Pattern → ensures only one Spark session exists"""
    _instance = None


    @staticmethod
    def get():
        if SparkSingleton._instance is None:
            SparkSingleton._instance = (
                SparkSession.builder.appName("FactoryETL").getOrCreate()
            )
        return SparkSingleton._instance