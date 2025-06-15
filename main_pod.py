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
from pyflowetl.transformers.drop_columns import DropColumnsTransformer
from pyflowetl.transformers.extract_cap_from_address import ExtractCapFromAddressTransformer

from pyflowetl.validators.not_empty import NotEmptyValidator
from pyflowetl.validators.telefono_italiano import TelefonoItalianoValidator
from pyflowetl.validators.codice_fiscale import CodiceFiscaleValidator

from pyflowetl.preprocessors.normalize_phone import NormalizePhoneNumberPreProcessor
from pyflowetl.preprocessors.to_upper import ToUpperPreProcessor
from pyflowetl.preprocessors.to_lower import ToLowerPreProcessor

# Inizializzazione log
set_log_file("logs/etl_run.log")
logger = get_logger()
logger.info("Avvio esecuzione ETL POD")

#VECCHIO_TRADER
#NUOVO_TRADER
#PDR
#DATA_ATTIVAZIONE
#DATA_CESSAZIONE
#NOME_COGNOME
#TELEFONO
#CF
#COMUNE
#PROV
#REGIONE
#INDIRIZZO


# Regole di validazione
validation_rules = {
    "NOME_COGNOME": [NotEmptyValidator()],
    "COD FISCALE":[CodiceFiscaleValidator()]
}

# Regole di preprocessing
preprocessing_rules = {
    "CELL"    : [NormalizePhoneNumberPreProcessor()],
    "NOME_COGNOME"    : [ToUpperPreProcessor()],
    "COMUNE" : [ToUpperPreProcessor()],
    "INDIRIZZO" : [ToUpperPreProcessor()]

}

# Esecuzione pipeline
pipeline = EtlPipeline()

pipeline.extract(CsvExtractor("/Users/marco/tmp/pod_luglio_2025.csv", delimiter=";", low_memory=False))

pipeline.transform(LogHeadTransformer())

pipeline.transform(ApplyPreprocessingRulesTransformer(preprocessing_rules))

pipeline.transform(ValidateColumnsTransformer(
    rules=validation_rules,
    reject_output_path="/Users/marco/tmp/scarti_pod_luglio_2025_out.csv"
))

pipeline.transform(ExtractCapFromAddressTransformer("INDIRIZZO", "CAP"))



#pipeline.transform(AddConstantColumnTransformer("NOME_FILE", "ML GIUGNO 2025"))
#pipeline.transform(AddConstantColumnTransformer("DATA_CESSAZIONE", None))

cloned  = pipeline.clone()

pipeline.transform(AddConstantColumnTransformer("NOME_FILE", "POD ATTIVI LUGLIO 2025"))
cloned.transform(AddConstantColumnTransformer("NOME_FILE", "POD CESSATI GIUGNO 2025"))



#VECCHIO_TRADER
#NUOVO_TRADER
#PDR
#DATA_ATTIVAZIONE
#DATA_CESSAZIONE
#NOME_COGNOME
#TELEFONO
#CF
#COMUNE
#PROV
#REGIONE
#INDIRIZZO
pipeline.transform(SetOutputColumnsTransformer(columns={
    "CF":"CF",
    "NOME_COGNOME": "NOME_COGNOME",
    "TELEFONO": "TELEFONO",
    "PDR": "PDR",
    "INDIRIZZO": "INDIRIZZO",
    "CAP": "CAP",
    "LOCALITA'":"COMUNE",
    "PROV": "PROVINCIA",
    "REGIONE": "REGIONE",
    "VECCHIO_TRADER":"TRADER",
    "NOME_FILE":"NOME_FILE",
    "DATA_ATTIVAZIONE": "DATA_ATTIVAZIONE",
    "DATA_CESSAZIONE":"DATA_CESSAZIONE"

}, rename=True))







pipeline.load(CsvLoader("/Users/marco/tmp/pod_attivi_luglio_2025_out.csv", delimiter=";"))

cloned.transform(DropColumnsTransformer(["DATA_ATTIVAZIONE"]))
cloned.transform(AddConstantColumnTransformer("DATA_ATTIVAZIONE",None))

cloned.transform(SetOutputColumnsTransformer(columns={
    "CF":"CF",
    "NOME_COGNOME": "NOME_COGNOME",
    "TELEFONO":"TELEFONO",
    "PDR": "PDR",
    "INDIRIZZO": "INDIRIZZO",

    "CAP": "CAP",
    "LOCALITA'":"COMUNE",
    "PROV":"PROVINCIA",
    "REGIONE":"REGIONE",
    "NUOVO_TRADER":"TRADER",
    "NOME_FILE":"NOME_FILE",
    "DATA_ATTIVAZIONE": "DATA_ATTIVAZIONE",
    "DATA_CESSAZIONE":"DATA_CESSAZIONE",




}, rename=True))


cloned.load(CsvLoader("/Users/marco/tmp/pod_cessati_giugno_2025_out.csv", delimiter=";"))

log_memory_usage("Fine ETL")
