from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.pipeline import EtlPipeline

from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.extractors.postgres_extractor import PostgresExtractor
from pyflowetl.loaders.csv_loader import CsvLoader

from pyflowetl.transformers.concat_columns import ConcatColumnsTransformer
from pyflowetl.transformers.validate_columns import ValidateColumnsTransformer
from pyflowetl.transformers.set_output_columns import SetOutputColumnsTransformer
from pyflowetl.transformers.apply_preprocessing_rules import ApplyPreprocessingRulesTransformer
from pyflowetl.transformers.convert_date_format import ConvertDateFormatTransformer
from pyflowetl.transformers.add_provincia import AddProvinciaTransformer
from pyflowetl.transformers.add_regione import AddRegioneTransformer
from pyflowetl.transformers.add_constant_column import AddConstantColumnTransformer
from pyflowetl.transformers.log_head import LogHeadTransformer
from pyflowetl.transformers.distinct import DistinctTransformer


from pyflowetl.validators.not_empty import NotEmptyValidator
from pyflowetl.validators.telefono_italiano import TelefonoItalianoValidator
from pyflowetl.validators.codice_fiscale import CodiceFiscaleValidator
from pyflowetl.validators.column_comparison import ColumnComparisonValidator

from pyflowetl.preprocessors.normalize_phone import NormalizePhoneNumberPreProcessor
from pyflowetl.preprocessors.to_upper import ToUpperPreProcessor
from pyflowetl.preprocessors.to_lower import ToLowerPreProcessor



# Inizializzazione log
set_log_file("logs/etl_run.log")
logger = get_logger()
logger.info("Avvio esecuzione ETL personalizzata")

preprocessing_rules = {
    "CELL1"    : [NormalizePhoneNumberPreProcessor()],
}

validation_rules = {
    "EMAIL": [NotEmptyValidator(), ColumnComparisonValidator("!=", "all@gmail.com")],
    "CELL1": [TelefonoItalianoValidator()],
}

# Esecuzione pipeline
pipeline = EtlPipeline()

pipeline.extract(CsvExtractor(f"C:\\Users\\marco\\Downloads\\MAIL\\5_MILIONI_DB_EMAIL.txt", delimiter="\t", low_memory=False))

pipeline.transform(LogHeadTransformer())

pipeline.transform(ApplyPreprocessingRulesTransformer(preprocessing_rules))

pipeline.transform(ValidateColumnsTransformer(
    rules=validation_rules,
    reject_output_path=f"C:\\Users\\marco\\Downloads\\MAIL\\SCARTI_5_MILIONI_DB_EMAIL.txt"
))
