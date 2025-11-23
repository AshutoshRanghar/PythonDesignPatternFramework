from base_processor import BaseProcessor
from factory_design_pattern.utils.spark_singleton import SparkSingleton
class FactProcessor(BaseProcessor):
    """FACT table ETL"""


def __init__(self, table_name, source_path, target_path):
    self.table_name = table_name
    self.source_path = source_path
    self.target_path = target_path
    self.spark = SparkSingleton.get()


def extract(self):
    return self.spark.read.parquet(self.source_path)


def transform(self, df):
    return df.dropDuplicates()


def load(self, df):
    df.write.mode("overwrite").parquet(self.target_path)