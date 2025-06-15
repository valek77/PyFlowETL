from copy import deepcopy
from pyflowetl.log import get_logger, log_memory_usage
import pandas as pd

class EtlPipeline:
    def __init__(self):
        self.data = None

    def extract(self, extractor):
        self.data = extractor.extract()
        log_memory_usage("[EtlPipeline] dopo extract")
        return self

    def preprocess(self, preprocessor):
        self.data = preprocessor.process(self.data)
        log_memory_usage("[EtlPipeline] dopo preprocess")
        return self

    def transform(self, transformer):
        self.data = transformer.transform(self.data)
        log_memory_usage("[EtlPipeline] dopo transform")
        return self

    def load(self, loader):
        loader.load(self.data)
        log_memory_usage("[EtlPipeline] dopo load")
        return self

    def clone(self):
        new_pipeline = EtlPipeline()
        if self.data is not None:
            new_pipeline.data = self.data.copy(deep=True)
        return new_pipeline

    def split(self, flow_names: tuple[str], row_selector_fn) -> dict:
        """
        Divide i dati della pipeline in più sottopipeline in base a una funzione di routing per riga.
        :param flow_names: tuple con i nomi delle nuove pipeline (es. ("a", "b", "c"))
        :param row_selector_fn: funzione che prende una riga e restituisce uno dei nomi della tupla
        def instradamento_personalizzato(row):
            if row["PROVINCIA"] == "Napoli":
                return "napoli"
            elif row["PROVINCIA"] == "Roma":
                return "roma"
            else:
                return "altre"

        :return: dict {flow_name: EtlPipeline con subset dei dati}
        """
        logger = get_logger()
        if self.data is None:
            raise RuntimeError("Nessun dato disponibile nella pipeline. Hai dimenticato extract()?")

        logger.info(f"[EtlPipeline] Split su flussi: {flow_names}")
        data_by_key = {name: [] for name in flow_names}

        for _, row in self.data.iterrows():
            key = row_selector_fn(row)
            if key in data_by_key:
                data_by_key[key].append(row)
            else:
                logger.warning(f"[EtlPipeline] Chiave '{key}' non prevista in flow_names. Riga ignorata.")

        result = {}
        for key in flow_names:
            df = pd.DataFrame(data_by_key[key])
            new_pipeline = EtlPipeline()
            new_pipeline.data = df
            result[key] = new_pipeline
            logger.info(f"[EtlPipeline] Creato sottopipeline '{key}' con {len(df)} righe.")

        return result
