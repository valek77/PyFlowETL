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

from pyflowetl.preprocessors.normalize_phone import NormalizePhoneNumberPreProcessor
from pyflowetl.preprocessors.to_upper import ToUpperPreProcessor
from pyflowetl.preprocessors.to_lower import ToLowerPreProcessor



# Inizializzazione log
set_log_file("logs/etl_run.log")
logger = get_logger()
logger.info("Avvio esecuzione ETL personalizzata")



# Esecuzione pipeline
pipeline = EtlPipeline()

pipeline.extract(PostgresExtractor("postgresql://postgres:Temp2009!@192.168.3.231:5432/fub",query="select phone_number from fubbed_numbers fn where fn.is_present_in_rop = true"))


pipelineCSV = EtlPipeline()

pipelineCSV.extract(CsvExtractor("c:/tmp/fub_stefania.csv"))

pipelineCSV.transform(DistinctTransformer())


pipeline = pipeline.anti_join_with(pipelineCSV, on="phone_number")

pipeline.load(CsvLoader("c:/tmp/solo_nostri.csv"))