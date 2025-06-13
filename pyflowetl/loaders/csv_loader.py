import os
from pyflowetl.log import get_logger



class CsvLoader:
    def __init__(self, output_path, encoding="utf-8", delimiter=","):
        self.output_path = output_path
        self.encoding = encoding
        self.delimiter = delimiter

    def load(self, data):
        logger = get_logger()
        logger.info(f"[CsvLoader] Scrittura su file: {self.output_path}")

        try:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            data.to_csv(self.output_path, index=False, encoding=self.encoding, sep=self.delimiter)
            logger.info(f"[CsvLoader] Scrittura completata: {len(data)} record")
        except Exception as e:
            logger.exception(f"[CsvLoader] Errore durante la scrittura del file: {e}")
            raise
