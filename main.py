from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.pipeline import EtlPipeline
from pyflowetl.transformers.concat_columns import ConcatColumnsTransformer
from pyflowetl.transformers.set_output_columns import SetOutputColumnsTransformer
from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.loaders.csv_loader import CsvLoader

#Log init
set_log_file("logs/etl_run.log")
logger = get_logger()

#Main
logger.info("Avvio esecuzione ETL personalizzata")
#TOPONIMO	INDIRIZZO	CIVICO	LOCALITA'	CAP

pipeline = EtlPipeline()
pipeline.extract(CsvExtractor("c:\\tmp\\ml_06.csv",  delimiter=";", low_memory=False))
pipeline.transform(ConcatColumnsTransformer(columns=["COGNOME", "NOME"], output_column="COGNOME_NOME" , separator=" ",drop_originals=True))
pipeline.transform(ConcatColumnsTransformer(columns=["TOPONIMO", "INDIRIZZO", "CIVICO", "LOCALITA'", "CAP"], output_column="INDIRIZZO_COMPLETO" , separator=" ",drop_originals=False))
pipeline.transform(SetOutputColumnsTransformer(columns={"COGNOME_NOME": "COGNOME_NOME", "POD": "POD", "INDIRIZZO_COMPLETO":"INDIRIZZO", "CAP":"CAP"}, rename=True))
pipeline.load(CsvLoader("c:\\tmp\\ml_06_out.csv", delimiter=";"))

