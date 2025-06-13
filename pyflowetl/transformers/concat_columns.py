from pyflowetl.log import get_logger, log_memory_usage



class ConcatColumnsTransformer:
    def __init__(self, columns, output_column, separator="_", drop_originals=False):
        self.columns = columns
        self.output_column = output_column
        self.separator = separator
        self.drop_originals = drop_originals

    def transform(self, data):
        logger = get_logger()
        logger.info(f"[ConcatColumnsTransformer] Creo colonna '{self.output_column}' da: {self.columns}")
        try:
            data[self.output_column] = data[self.columns].astype(str).agg(self.separator.join, axis=1)
            if self.drop_originals:
                data.drop(columns=self.columns, inplace=True)
            logger.info(f"[ConcatColumnsTransformer] Colonna '{self.output_column}' creata")
            log_memory_usage((f"Dopo Creazione colonna '{self.output_column}' da: {self.columns}"))
            return data
        except Exception as e:
            logger.exception(f"[ConcatColumnsTransformer] Errore durante la trasformazione: {e}")
            raise
