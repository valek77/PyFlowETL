import re
from pyflowetl.validators.base import BaseValidator
import pandas as pd

class CodiceFiscaleValidator(BaseValidator):
    CF_REGEX = r"^[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z]$"

    def validate(self, value) -> bool:
        if pd.isna(value):
            return False
        value = str(value).strip().upper()
        return re.match(self.CF_REGEX, value) is not None

    def error_message(self):
        return "Codice fiscale non valido"

# ESEMPIO D'USO
if __name__ == "__main__":
    v = CodiceFiscaleValidator()
    print(v.validate("RSSMRA85T10A562S"))  # True
    print(v.validate("abc"))               # False
    print(v.validate(None))                # False
