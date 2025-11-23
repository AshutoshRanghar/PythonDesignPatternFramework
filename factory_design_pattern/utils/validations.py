class SchemaValidator:
    """Example of Strategy Pattern for validations"""

    def validate(self, df, expected_cols):
        df_cols = set(df.columns)
        missing = set(expected_cols) - df_cols
        if missing:
            raise Exception(f"Missing columns: {missing}")
        return True

