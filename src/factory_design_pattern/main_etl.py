# main_etl.py

from factory_design_pattern.core.factory import ProcessorFactory
from factory_design_pattern.utils.logger import Logger
from factory_design_pattern.utils.validations import SchemaValidator

class ETLRunner:
    """Main orchestration class called by Databricks job."""

    def __init__(self):
        self.validator = SchemaValidator()
        self.logging=Logger()
    def run_table(self, config: dict):
        """Run a single table ETL based on config dictionary."""
        self.logging.log(f"Starting ETL for: {config.get('table_name')}")

        # Validate essential config keys
        required_keys = ["table_name", "table_type", "source_path", "target_path"]
        # self.validator.validate_config(config, required_keys)

        # Get processor (Factory Pattern)
        processor = ProcessorFactory.get_processor(config)

        # Run ETL (Template Method Pattern)
        result = processor.run()

        Logger.log(f"ETL Completed for: {config.get('table_name')}")
        return result

    def run_all(self, config_list: list):
        """Run multiple tables ETL in a loop."""
        results = {}
        for config in config_list:
            table = config["table_name"]
            try:
                results[table] = self.run_table(config)
            except Exception as e:
                results[table] = f"FAILED: {str(e)}"
        return results


# Databricks Entry Point
def main(config: dict):
    runner = ETLRunner()
    return runner.run_table(config)


# If running as script (optional)
if __name__ == "__main__":
    sample = {
        "table_name": "customer_dim",
        "table_type": "SCD2",
        "source_path": "/FileStore/capstoneprojash/datasets/raw/gym_logins_bz",
        "target_path": "/FileStore/capstoneprojash/datasets/raw_new/"
    }
    main(sample)
