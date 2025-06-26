import os
from pathlib import Path

IS_DEBUG = int(os.environ.get("DEBUG_STATUS", ""))
PROJECT_ID = os.environ.get("PROJECT_ID", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
LOG_FILE = Path(os.environ.get("LOG_FILE", ""))

## OAUTH ##
SERVICE_ACCOUNT_CONFIG_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "")
OAUTH_CLIENT_CONFIG_JSON_FILE = os.environ.get("AUTH_FILE", "")
ANIMALS_NAME_FILE = os.environ.get("ANIMALS_NAME_FILE", "all_names.txt")
SECRET_NAME = os.environ.get("SECRET_NAME", "")
TEST_TOKEN = Path(os.environ.get("TEST_TOKEN", ""))
PROD_TOKEN = Path(os.environ.get("PROD_TOKEN", ""))
REDIRECT_URI = os.environ.get("REDIRECT_URL", "")

PROD_EMAIL = os.environ.get("PROD_EMAIL", "")
TEST_EMAIL = os.environ.get("TEST_EMAIL", "")


# GMAIL CONSTANTS
GMAIL_INVOICE_LABEL = "Invoices/Vet invoice"
GMAIL_FROM_LABEL = "Label_5838368921937526589"
GMAIL_TO_LABEL = "Label_342337121491929089"

# Drive Constants #
DRIVE_INVOICES_FOLDER = "VET_INVOICES"

GMAIL_TEST_LABEL = "Label_8306108300123845242"
GMAIL_TEST_LABEL_COMPLETE = "Label_7884775180973112661"

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]
