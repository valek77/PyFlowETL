from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.pipeline import EtlPipeline

from pyflowetl.extractors.csv_extractor import CsvExtractor
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

from pyflowetl.validators.not_empty import NotEmptyValidator
from pyflowetl.validators.telefono_italiano import TelefonoItalianoValidator
from pyflowetl.validators.codice_fiscale import CodiceFiscaleValidator

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
    "CELL": [TelefonoItalianoValidator()],
    "COD FISCALE":[CodiceFiscaleValidator()]
}

# Regole di preprocessing
preprocessing_rules = {
    "CELL"    : [NormalizePhoneNumberPreProcessor()],
    "NOME"    : [ToUpperPreProcessor()],
    "COGNOME" : [ToUpperPreProcessor()]
}

# Esecuzione pipeline
pipeline = EtlPipeline()

pipeline.extract(CsvExtractor("/Users/marco/tmp/ml_06.csv", delimiter=";", low_memory=False))

pipeline.transform(LogHeadTransformer())

pipeline.transform(ApplyPreprocessingRulesTransformer(preprocessing_rules))

pipeline.transform(ValidateColumnsTransformer(
    rules=validation_rules,
    reject_output_path="/Users/marco/tmp/scarti_ml_06_out.csv"
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

pipeline.transform(AddProvinciaTransformer("LOCALITA'"))
pipeline.transform(AddRegioneTransformer("LOCALITA'"))


pipeline.transform(AddConstantColumnTransformer("NOME_FILE", "ML GIUGNO 2025"))
pipeline.transform(AddConstantColumnTransformer("DATA_CESSAZIONE", None))

pipeline.transform(ConvertDateFormatTransformer(
    columns="data attivazione",
    input_format="%d-%b-%y",  # formato tipo Oracle
    output_format="%Y-%m-%d"
))

pipeline.transform(SetOutputColumnsTransformer(columns={
    "COGNOME_NOME": "COGNOME_NOME",
    "POD": "POD",
    "INDIRIZZO_COMPLETO": "INDIRIZZO_COMPLETO",
    "CAP": "CAP",
    "LOCALITA'":"LOCALITA",
    "PROVINCIA":"PROVINCIA",
    "REGIONE":"REGIONE",
    "NOME_FILE":"NOME_FILE",
    "data attivazione": "DATA_ATTIVAZIONE",
    "DATA_CESSAZIONE":"DATA_CESSAZIONE"

}, rename=True))


def instradamento_personalizzato(row):
    if row["PROVINCIA"] == "Napoli":
        return "napoli"
    elif row["PROVINCIA"] == "Roma":
        return "roma"
    else:
        return "altre"

sottopipeline = pipeline.split(("napoli", "roma", "altre"), instradamento_personalizzato)

sottopipeline["napoli"].transform(LogHeadTransformer())

sottopipeline["roma"].transform(LogHeadTransformer())

sottopipeline["altre"].transform(LogHeadTransformer())

pipeline.load(CsvLoader("/Users/marco/tmp/ml_06_out.csv", delimiter=";"))

log_memory_usage("Fine ETL")
