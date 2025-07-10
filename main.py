from pyflowetl.extractors.xlsx_extractor import XlsxExtractor
from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.pipeline import EtlPipeline

from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.loaders.csv_loader import CsvLoader
from pyflowetl.preprocessors.nan_to_empty_string import NanToEmptyStringPreprocessor

from pyflowetl.transformers.concat_columns import ConcatColumnsTransformer
from pyflowetl.transformers.validate_columns import ValidateColumnsTransformer
from pyflowetl.transformers.set_output_columns import SetOutputColumnsTransformer
from pyflowetl.transformers.apply_preprocessing_rules import ApplyPreprocessingRulesTransformer
from pyflowetl.transformers.convert_date_format import ConvertDateFormatTransformer
from pyflowetl.transformers.add_provincia import AddProvinciaTransformer
from pyflowetl.transformers.add_regione import AddRegioneTransformer
from pyflowetl.transformers.add_constant_column import AddConstantColumnTransformer
from pyflowetl.transformers.log_head import LogHeadTransformer
from pyflowetl.validators.column_comparison import ColumnComparisonValidator
from pyflowetl.validators.date_format import DateFormatValidator

from pyflowetl.validators.not_empty import NotEmptyValidator
from pyflowetl.validators.telefono_italiano import TelefonoItalianoValidator
from pyflowetl.validators.codice_fiscale import CodiceFiscaleValidator

from pyflowetl.preprocessors.normalize_phone import NormalizePhoneNumberPreProcessor
from pyflowetl.preprocessors.to_upper import ToUpperPreProcessor
from pyflowetl.preprocessors.to_lower import ToLowerPreProcessor




path= "c:\\tmp\\AttiviPod\\"
fileInput = "attivi_05_01.xlsx"
fileOut   = "attivi_05_01_out.csv"
fileScarti ="scarti_attivi_05_01.csv"
nomeFile= "ML_ATTIVI_MAGGIO_2025"
input_data_format="%Y-%m-%d %H:%M:%S" #01/05/2025

#input_data_format="%d-%b-%y" #01-JAN-25





# Inizializzazione log
set_log_file("logs/etl_run.log")
logger = get_logger()
logger.info("Avvio esecuzione ETL personalizzata")

# Regole di validazione
validation_rules = {
    "NOME": [NotEmptyValidator()],
    "COGNOME": [NotEmptyValidator()],
    "CELL": [TelefonoItalianoValidator()],
    "COD FISCALE":[CodiceFiscaleValidator()],
    "trader":[ColumnComparisonValidator("!=", "0"), ColumnComparisonValidator("!=", "")],
    "data attivazione" :[DateFormatValidator(input_data_format)]
}

# Regole di preprocessing
preprocessing_rules = {
    "CELL"    : [NormalizePhoneNumberPreProcessor()],
    "NOME"    : [ToUpperPreProcessor()],
    "COGNOME" : [ToUpperPreProcessor()],
    "trader"  : [NanToEmptyStringPreprocessor()]

}

# Esecuzione pipeline
pipeline = EtlPipeline()



pipeline.extract(XlsxExtractor(path+fileInput))



pipeline.transform(ApplyPreprocessingRulesTransformer(preprocessing_rules))

pipeline.transform(ValidateColumnsTransformer(
    rules=validation_rules,
    reject_output_path=path+fileScarti
))

#pipeline.transform(ConcatColumnsTransformer(
#    columns=["COGNOME", "NOME"],
#    output_column="COGNOME_NOME",
#    separator=" ",
#    drop_originals=True
#))

pipeline.transform(LogHeadTransformer(100))

pipeline.transform(ConcatColumnsTransformer(
    columns=["TOPONIMO", "INDIRIZZO", "CIVICO", "LOCALITA'", "CAP"],
    output_column="INDIRIZZO_COMPLETO",
    separator=" ",
    drop_originals=True
))

pipeline.transform(AddProvinciaTransformer("LOCALITA'"))
pipeline.transform(AddRegioneTransformer("LOCALITA'"))


pipeline.transform(AddConstantColumnTransformer("NOME_FILE", nomeFile))
pipeline.transform(AddConstantColumnTransformer("DATA_CESSAZIONE", None))

pipeline.transform(ConvertDateFormatTransformer(
    columns="data attivazione",
    input_format=input_data_format,
    output_format="%Y-%m-%d"
))

pipeline.transform(SetOutputColumnsTransformer(columns={
    "COD FISCALE":"CF",
    "NOME": "NOME",
    "COGNOME": "COGNOME",
    "CELL":"CELL",
    "POD": "POD",
    "INDIRIZZO_COMPLETO": "INDIRIZZO_COMPLETO",
    "CAP": "CAP",
    "LOCALITA'":"COMUNE",
    "PROVINCIA":"PROVINCIA",
    "REGIONE":"REGIONE",
    "trader":"TRADER",
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

#sottopipeline = pipeline.split(("napoli", "roma", "altre"), instradamento_personalizzato)

#sottopipeline["napoli"].transform(LogHeadTransformer())

#sottopipeline["roma"].transform(LogHeadTransformer())

#sottopipeline["altre"].transform(LogHeadTransformer())

pipeline.load(CsvLoader(path+fileOut, delimiter=";"))

log_memory_usage("Fine ETL")
