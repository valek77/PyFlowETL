from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.pipeline import EtlPipeline
from pyflowetl.transformers.concat_columns import ConcatColumnsTransformer
from pyflowetl.transformers.validate_columns import ValidateColumnsTransformer
from pyflowetl.transformers.set_output_columns import SetOutputColumnsTransformer
from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.loaders.csv_loader import CsvLoader
from pyflowetl.validators.is_email import IsEmailValidator
from pyflowetl.validators.telefono_italiano import TelefonoItalianoValidator
from pyflowetl.validators.not_empty import NotEmptyValidator

#Log init
set_log_file("logs/etl_run.log")
logger = get_logger()

#Main
logger.info("Avvio esecuzione ETL personalizzata")

#Regole di validazione
validation_rules ={
                    "NOME": [NotEmptyValidator()],
                    "COGNOME": [NotEmptyValidator()],
                    "CELL":[TelefonoItalianoValidator()]
                  }




pipeline = EtlPipeline()

pipeline.extract(CsvExtractor("c:\\tmp\\ml_06.csv",  delimiter=";", low_memory=False))

pipeline.transform(ValidateColumnsTransformer(rules=validation_rules, reject_output_path="c:\\tmp\\scarti_ml_06_out.csv"))

pipeline.transform(ConcatColumnsTransformer(columns=["COGNOME", "NOME"], output_column="COGNOME_NOME" , separator=" ",drop_originals=True))

pipeline.transform(ConcatColumnsTransformer(columns=["TOPONIMO", "INDIRIZZO", "CIVICO", "LOCALITA'", "CAP"], output_column="INDIRIZZO_COMPLETO" , separator=" ",drop_originals=False))

pipeline.transform(SetOutputColumnsTransformer(columns={"COGNOME_NOME": "COGNOME_NOME", "POD": "POD", "INDIRIZZO_COMPLETO":"INDIRIZZO", "CAP":"CAP"}, rename=True))

pipeline.load(CsvLoader("c:\\tmp\\ml_06_out.csv", delimiter=";"))

