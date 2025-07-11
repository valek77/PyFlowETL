from pyflowetl.extractors.xlsx_extractor import XlsxExtractor
from pyflowetl.log import set_log_file, get_logger, log_memory_usage
from pyflowetl.pipeline import EtlPipeline

from pyflowetl.extractors.csv_extractor import CsvExtractor
from pyflowetl.loaders.csv_loader import CsvLoader
from pyflowetl.preprocessors.nan_to_empty_string import NanToEmptyStringPreprocessor
from pyflowetl.preprocessors.text_replace import TextReplacePreProcessor
from pyflowetl.transformers.add_random_ip import AddRandomIpTransformer
from pyflowetl.transformers.add_ranom_date import AddRandomDatetimeTransformer

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




path= "c:\\tmp\\"
fileInput = "lista_190.csv"
fileOut   = "attivi_07_02_out.csv"
dataInzio="2025-05-01 00:00:00"

dataFine="2025-05-30 23:59:59"




# Inizializzazione log
set_log_file("logs/etl_run.log")
logger = get_logger()
logger.info("Avvio esecuzione ETL personalizzata")

# Regole di validazione

# Regole di preprocessing
preprocessing_rules = {
    "Pod Pdr"    : [TextReplacePreProcessor("=", ""), TextReplacePreProcessor("\"", "")],
    "Telefono"    : [TextReplacePreProcessor("=", ""), TextReplacePreProcessor("\"", "")],
}

# Esecuzione pipeline
pipeline = EtlPipeline()



pipeline.extract(CsvExtractor(path+fileInput,delimiter=";",low_memory=False))




pipeline.transform(ApplyPreprocessingRulesTransformer(preprocessing_rules))




pipeline.transform(AddRandomIpTransformer("Regione"))

pipeline.transform(AddConstantColumnTransformer("CONSENSO_0","S"))
pipeline.transform(AddConstantColumnTransformer("CONSENSO_1","S"))
pipeline.transform(AddConstantColumnTransformer("CONSENSO_2","S"))
pipeline.transform(AddRandomDatetimeTransformer("DATA_CONSENSO",dataInzio,dataFine))


pipeline.transform(LogHeadTransformer(10))
exit(10)

#pipeline.transform(ConcatColumnsTransformer(
#    columns=["COGNOME", "NOME"],
#    output_column="COGNOME_NOME",
#    separator=" ",
#    drop_originals=True
#))


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


pipeline.load(CsvLoader(path+fileOut, delimiter=";"))

log_memory_usage("Fine ETL")
