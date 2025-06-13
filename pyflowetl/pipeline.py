from pyflowetl.log import get_logger

logger = get_logger()

class EtlPipeline:
    def __init__(self):
        self.data = None
        logger.info("Pipeline inizializzata")

    def extract(self, extractor):
        logger.info(f"Inizio fase EXTRACT con {extractor.__class__.__name__}")
        self.data = extractor.extract()
        logger.info(f"Estrazione completata: {len(self.data)} record")
        return self

    def transform(self, transformer):
        logger.info(f"Inizio fase TRANSFORM con {transformer.__class__.__name__}")
        self.data = transformer.transform(self.data)
        logger.info(f"Trasformazione completata")
        return self

    def load(self, loader):
        logger.info(f"Inizio fase LOAD con {loader.__class__.__name__}")
        loader.load(self.data)
        logger.info("Caricamento completato")
        return self
