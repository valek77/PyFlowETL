import re
from pyflowetl.validators.base import BaseValidator

EMAIL_REGEX = r"^[\w\.-]+@[\w\.-]+\.\w+$"

class IsEmailValidator(BaseValidator):
    def validate(self, value) -> bool:
        if value is None:
            return False
        return re.match(EMAIL_REGEX, str(value).strip()) is not None

    def error_message(self):
        return "Email non valida"


# ESEMPIO
if __name__ == "__main__":
    v = IsEmailValidator()
    print(v.validate("test@example.com"))  # True
    print(v.validate("nope"))              # False
