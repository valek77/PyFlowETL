import sys
import argparse

from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.pipeline import EtlPipeline

from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.loaders.csv_loader import CsvLoader
from pyflowetl.transformers.add_regione_from_sigla import AddRegioneFromSiglaProvinciaTransformer
from pyflowetl.transformers.apply_preprocessing_rules import ApplyPreprocessingRulesTransformer
from pyflowetl.transformers.validate_columns import ValidateColumnsTransformer
from pyflowetl.transformers.add_constant_column import AddConstantColumnTransformer
from pyflowetl.transformers.set_output_columns import SetOutputColumnsTransformer
from pyflowetl.transformers.log_head import LogHeadTransformer

from pyflowetl.validators.not_empty import NotEmptyValidator
from pyflowetl.validators.regex import RegexValidator
from pyflowetl.validators.telefono_italiano import TelefonoItalianoValidator
from pyflowetl.validators.column_comparison import ColumnComparisonValidator
from pyflowetl.preprocessors.normalize_phone import NormalizePhoneNumberPreProcessor

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument("--input_csv", required=True)
parser.add_argument("--output_csv", required=True)
parser.add_argument("--reject_csv", required=True)
args = parser.parse_args()

# Logging
set_log_file("logs/etl_run.log")
logger = get_logger()
logger.info("Avvio esecuzione ETL parametrico")

# Regole
preprocessing_rules = {
    "MOBILE": [NormalizePhoneNumberPreProcessor()],
}

validation_rules = {
    "MAIL": [
        NotEmptyValidator(),
        ColumnComparisonValidator("!=", "all@gmail.com"),
        RegexValidator(pattern=r"^(?!\d+@gmail\.com$).+")
    ],
    "MOBILE": [TelefonoItalianoValidator()],
}

# Pipeline
pipeline = EtlPipeline()

pipeline.extract(CsvExtractor(args.input_csv, delimiter=";", low_memory=False))

pipeline.transform(ApplyPreprocessingRulesTransformer(preprocessing_rules))

pipeline.transform(ValidateColumnsTransformer(
    rules=validation_rules,
    reject_output_path=args.reject_csv
))

pipeline.transform(AddConstantColumnTransformer("COMUNE", ""))
pipeline.transform(AddConstantColumnTransformer("CAP", ""))

pipeline.transform(AddRegioneFromSiglaProvinciaTransformer("PROV"))

pipeline.transform(LogHeadTransformer())

pipeline.transform(SetOutputColumnsTransformer(columns={
    "MOBILE": "CELL",
    "ANAGRAFICA": "COGNOME_NOME",
    "MAIL": "EMAIL",
    "CF": "CF",
    "COMUNE": "COMUNE",
    "CAP": "CAP",
    "PROV": "PROVINCIA",
    "Regione": "REGIONE"
}, rename=True))

pipeline.load(CsvLoader(args.output_csv, delimiter=";"))

log_memory_usage("Fine ETL")
