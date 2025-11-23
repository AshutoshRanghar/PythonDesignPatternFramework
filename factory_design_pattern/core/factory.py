from factory_design_pattern.core.fct_processor import FactProcessor
from factory_design_pattern.core.scd_processor import SCD2Processor
class ProcessorFactory:
    """FACTORY PATTERN → Creates correct processor instance"""
    @staticmethod
    def get_processor(config: dict):
        t = config["table_type"].upper()
        table_name = config.get("table_name")
        source = config.get("source_path")
        target = config.get("target_path")
        if t == "SCD2":
            return SCD2Processor(table_name, source, target)
        elif t == "FACT":
            return FactProcessor(table_name, source, target)
        else:
            return "Wrong Processor Selected"