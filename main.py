from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.pipeline import EtlPipeline

from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.loaders.csv_loader import CsvLoader

from pyflowetl.transformers.concat_columns import ConcatColumnsTransformer
from pyflowetl.transformers.validate_columns import ValidateColumnsTransformer
from pyflowetl.transformers.set_output_columns import SetOutputColumnsTransformer
from pyflowetl.transformers.apply_preprocessing_rules import ApplyPreprocessingRulesTransformer

from pyflowetl.validators.not_empty import NotEmptyValidator
from pyflowetl.validators.telefono_italiano import TelefonoItalianoValidator

from pyflowetl.preprocessors.normalize_phone import NormalizePhoneNumberPreProcessor
from pyflowetl.preprocessors.to_upper import ToUpperPreProcessor
from pyflowetl.preprocessors.to_lower import ToLowerPreProcessor

# Inizializzazione log
set_log_file("logs/etl_run.log")
logger = get_logger()
logger.info("Avvio esecuzione ETL personalizzata")

# Regole di validazione
validation_rules = {
    "NOME": [NotEmptyValidator()],
    "COGNOME": [NotEmptyValidator()],
    "CELL": [TelefonoItalianoValidator()]
}

# Regole di preprocessing
preprocessing_rules = {
    "CELL"    : [NormalizePhoneNumberPreProcessor()],
    "NOME"    : [ToUpperPreProcessor()],
    "COGNOME" : [ToLowerPreProcessor()]
}

# Esecuzione pipeline
pipeline = EtlPipeline()

pipeline.extract(CsvExtractor("c:\\tmp\\ml_06.csv", delimiter=";", low_memory=False))

pipeline.transform(ApplyPreprocessingRulesTransformer(preprocessing_rules))

pipeline.transform(ValidateColumnsTransformer(
    rules=validation_rules,
    reject_output_path="c:\\tmp\\scarti_ml_06_out.csv"
))

pipeline.transform(ConcatColumnsTransformer(
    columns=["COGNOME", "NOME"],
    output_column="COGNOME_NOME",
    separator=" ",
    drop_originals=True
))

pipeline.transform(ConcatColumnsTransformer(
    columns=["TOPONIMO", "INDIRIZZO", "CIVICO", "LOCALITA'", "CAP"],
    output_column="INDIRIZZO_COMPLETO",
    separator=" ",
    drop_originals=False
))

pipeline.transform(SetOutputColumnsTransformer(columns={
    "COGNOME_NOME": "COGNOME_NOME",
    "POD": "POD",
    "INDIRIZZO_COMPLETO": "INDIRIZZO",
    "CAP": "CAP"
}, rename=True))

pipeline.load(CsvLoader("c:\\tmp\\ml_06_out.csv", delimiter=";"))

log_memory_usage("Fine ETL")
