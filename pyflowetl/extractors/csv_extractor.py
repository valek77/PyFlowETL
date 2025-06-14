import pandas as pd
import os
from pyflowetl.log import get_logger




class CsvExtractor:
    def __init__(self, filepath, encoding="utf-8", delimiter=",", low_memory=True):
        self.filepath = filepath
        self.encoding = encoding
        self.delimiter = delimiter
        self.low_memory = low_memory

    def extract(self):
        logger = get_logger()
        logger.info(f"[CsvExtractor] Leggo file: {self.filepath}")

        if not os.path.exists(self.filepath):
            logger.error(f"[CsvExtractor] File non trovato: {self.filepath}")
            raise FileNotFoundError(f"File non trovato: {self.filepath}")

        try:
            df = pd.read_csv(self.filepath, encoding=self.encoding, delimiter=self.delimiter, low_memory=self.low_memory)
            logger.info(f"[CsvExtractor] Letti {len(df)} record")
            return df
        except Exception as e:
            logger.exception(f"[CsvExtractor] Errore durante la lettura del file: {e}")
            raise
