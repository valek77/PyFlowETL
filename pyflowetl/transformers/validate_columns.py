from pyflowetl.log import get_logger, log_memory_usage

logger = get_logger()

class ValidateColumnsTransformer:
    def __init__(self, rules: dict, reject_output_path=None):
        """
        :param rules: dict del tipo {"colonna": [Validator1(), Validator2()]}
        :param reject_output_path: path CSV in cui salvare i record non validi (facoltativo)
        """
        self.rules = rules
        self.reject_output_path = reject_output_path
        self.invalid_rows = []

    def transform(self, data):
        logger = get_logger()
        logger.info("[ValidateColumnsTransformer] Inizio validazione")

        valid_rows = []
        rejected_rows = []

        for index, row in data.iterrows():
            row_valid = True
            error_messages = []

            for column, validators in self.rules.items():
                value = row.get(column)
                for validator in validators:
                    if not validator.validate(value):
                        msg = f"{column}: {validator.error_message()}"
                        error_messages.append(msg)
                        row_valid = False
                        break  # Ferma sul primo errore per quella colonna

            if row_valid:
                valid_rows.append(row)
            else:
                row_with_error = row.copy()
                row_with_error["error"] = " | ".join(error_messages)
                rejected_rows.append(row_with_error)

        import pandas as pd
        df_valid = pd.DataFrame(valid_rows)
        df_rejected = pd.DataFrame(rejected_rows)

        if self.reject_output_path and not df_rejected.empty:
            logger.warning(f"[ValidateColumnsTransformer] Scrittura righe non valide: {len(df_rejected)}")
            df_rejected.to_csv(self.reject_output_path, index=False,   encoding="utf-8-sig",  sep=";")

        logger.info(f"[ValidateColumnsTransformer] Validi: {len(df_valid)} / Invalidi: {len(df_rejected)}")
        log_memory_usage("Dopo ValidateColumnsTransformer")

        return df_valid


# ESEMPIO D'USO
if __name__ == "__main__":
    import pandas as pd
    from pyflowetl.pipeline import EtlPipeline
    from pyflowetl.validators.not_empty import NotEmptyValidator
    from pyflowetl.validators.is_email import IsEmailValidator
    from pyflowetl.transformers.validate_columns import ValidateColumnsTransformer
    from pyflowetl.loaders.csv_loader import CsvLoader
    from pyflowetl.log import set_log_file

    # Configura log file
    set_log_file("logs/validate_example.log")

    # Simula DataFrame
    df = pd.DataFrame([
        {"nome": "Mario", "email": "mario@example.com"},
        {"nome": "", "email": "nope"},
        {"nome": "Luca", "email": "luca@domain.it"},
    ])

    # Costruzione pipeline con input manuale
    pipeline = EtlPipeline()
    pipeline.data = df
    pipeline.transform(ValidateColumnsTransformer(
        rules={
            "nome": [NotEmptyValidator()],
            "email": [IsEmailValidator()]
        },
        reject_output_path="output/scartati.csv"
    )).load(CsvLoader("output/validi.csv"))

