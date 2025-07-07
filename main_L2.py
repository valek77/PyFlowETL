from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.pipeline import EtlPipeline

from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.extractors.postgres_extractor import PostgresExtractor
from pyflowetl.loaders.csv_loader import CsvLoader
from pyflowetl.transformers.add_regione_from_sigla import AddRegioneFromSiglaProvinciaTransformer

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
from pyflowetl.transformers.drop_columns import DropColumnsTransformer


from pyflowetl.validators.not_empty import NotEmptyValidator
from pyflowetl.validators.regex import RegexValidator
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
    "MOBILE"    : [NormalizePhoneNumberPreProcessor()],
}





validation_rules = {
    "E_MAIL": [NotEmptyValidator(), ColumnComparisonValidator("!=", "all@gmail.com"), RegexValidator(pattern=r"^(?!\d+@gmail\.com$).+")],
    "MOBILE": [TelefonoItalianoValidator()],
}

# Esecuzione pipeline
pipeline = EtlPipeline()

pipeline.extract(CsvExtractor(f"/Users/marco/tmp/L2.csv", delimiter=";", low_memory=False))

pipeline.transform(DropColumnsTransformer(["AreaNilsen","ANNO_NASCITA","LOTTO"]))



pipeline.transform(ApplyPreprocessingRulesTransformer(preprocessing_rules))

pipeline.transform(ValidateColumnsTransformer(
    rules=validation_rules,
    reject_output_path=f"/Users/marco/tmp/SCARTI_l2.csv"
))

pipeline.transform(ConcatColumnsTransformer(["COGNOME","NOME"],"COGNOME_NOME"," ", True))

pipeline.transform(LogHeadTransformer())




#pipeline.transform(AddRegioneFromSiglaProvinciaTransformer("PROV"))



pipeline.transform(SetOutputColumnsTransformer(columns={
    "MOBILE":"CELL",
    "COGNOME_NOME": "COGNOME_NOME",
    "E_MAIL":"EMAIL",
    "CF":"CF",
    "COMUNE": "COMUNE",
    "CAP": "CAP",
    "PROV":"PROVINCIA",
    "Regione":"REGIONE",

}, rename=True))

pipeline.load(CsvLoader("/Users/marco/tmp/L2_OUT.txt", delimiter=";"))

log_memory_usage("Fine ETL")
