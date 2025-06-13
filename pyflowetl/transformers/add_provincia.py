import pandas as pd
import os
from pyflowetl.log import get_logger, log_memory_usage
from pyflowetl.utils.string_cleaner import clean_string
from metaphone import doublemetaphone

class AddProvinciaTransformer:
    def __init__(self, comune_column: str, output_column: str = "PROVINCIA"):
        self.comune_column = comune_column
        self.output_column = output_column
        logger = get_logger()

        # Percorso file
        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),  'data', 'gi_comuni_cap.csv')
        self.comuni_df = pd.read_csv(csv_path, sep=';', encoding='utf-8-sig')

        # Pulizia e codifica Metaphone
        self.comuni_df["COMUNE_CLEAN"] = self.comuni_df["comune"].astype(str).apply(clean_string)
        self.comuni_df["METAPHONE"] = self.comuni_df["COMUNE_CLEAN"].apply(lambda x: doublemetaphone(x)[0])

        self.comuni_map = self.comuni_df.set_index("METAPHONE")["denominazione_provincia"].to_dict()
        logger.info(f"[AddProvinciaTransformer] Mapping '{self.comune_column}' to '{self.output_column}'")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger = get_logger()

        df["COMUNE_CLEAN"] = df[self.comune_column].astype(str).apply(clean_string)
        df["METAPHONE"] = df["COMUNE_CLEAN"].apply(lambda x: doublemetaphone(x)[0])
        df[self.output_column] = df["METAPHONE"].map(self.comuni_map)

        df.drop(columns=["COMUNE_CLEAN", "METAPHONE"], inplace=True)
        log_memory_usage("[AddProvinciaTransformer] post-transform")
        return df
