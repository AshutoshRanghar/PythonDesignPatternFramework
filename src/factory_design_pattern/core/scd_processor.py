from pyspark.sql.functions import current_timestamp
from factory_design_pattern.core.base_processor import BaseProcessor
from factory_design_pattern.utils.spark_singleton import SparkSingleton
class SCD2Processor(BaseProcessor):
    """SCD2 Implementation class (Polymorphism → overrides BaseProcessor methods)"""


    def __init__(self, table_name, source_path, target_path):
        self.table_name = table_name
        self.source_path = source_path
        self.target_path = target_path
        self.spark = SparkSingleton.get()


    def extract(self):
        print("Inside Extract of SCD Processor")
        return self.spark.read.csv(self.source_path)


    def transform(self, df):
        return df.withColumn("updated_at", current_timestamp())


    def load(self, df):
        df.write.format("delta").mode("append").save(self.target_path)