import json
import os
from pathlib import Path

from dotenv import load_dotenv

from parsers import Cost

load_dotenv()

IS_DEBUG = int(os.environ.get("DEBUG_STATUS", ""))

GLOBAL_CREDS = ""
PROJECT_ID = os.environ.get("PROJECT_ID", "")
SVC_ACCOUNT = os.environ.get("SERVICE_ACCOUNT_FILE", "")
SECRET_NAME = os.environ.get("SECRET_NAME", "")
DB_NAME = os.environ.get("DB_NAME", "")
DB_USERNAME = os.environ.get("DB_USER", "")
DB_PASS = os.environ.get("DB_PASS", "")
REDIRECT_URI = os.environ.get("REDIRECT_URL", "")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# EMAILS
PROD_EMAIL = os.environ.get("PROD_EMAIL", "")
TEST_EMAIL = os.environ.get("TEST_EMAIL", "")

# FILES
LOG_FILE = Path(os.environ.get("LOG_FILE", ""))
TEST_TOKEN = Path(os.environ.get("TEST_TOKEN", ""))
PROD_TOKEN = Path(os.environ.get("PROD_TOKEN", ""))
OAUTH_FILE = os.environ.get("AUTH_FILE", "")

# GMAIL LABELS
TEST_LABEL = "Label_8306108300123845242"
TEST_LABEL_COMPLETE = "Label_7884775180973112661"
FROM_LABEL = "Label_5838368921937526589"
TO_LABEL = "Label_342337121491929089"

# DATE CONSTANTS
DATE_MDY = "%m-%d-%y"
DATE_M_D_Y = "%m/%d/%Y"
DATE_MDYYYY = "%m-%d-%Y"

# DIRECTORIES
DATA_DIR = Path("../data/")
INVOICE_DIR = Path("../data/invoices/")
NON_INVOICES_DIR = Path("../data/non_invoices/")
UNPROCESSED_DIR = Path("../data/invoices/unprocessed_invoices")

# SCOPES
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
]

if Path(SVC_ACCOUNT).exists():
    with open(SVC_ACCOUNT) as f:
        SVC_ACCOUNT = json.load(f)
else:
    SVC_ACCOUNT = json.loads(SVC_ACCOUNT)

if Path(OAUTH_FILE).exists():
    with open(OAUTH_FILE) as f:
        OAUTH_FILE = json.load(f)
else:
    OAUTH_FILE = json.loads(OAUTH_FILE)


def get_login_data() -> dict:
    return {
        "database": DB_NAME,
        "username": DB_USERNAME,
        "password": DB_PASS,
    }


class Regex:
    surgery = r"surgery|extract|ectomy|mass rem|ablation|rooted|\w+tomy"
    test = r"(test|blood|ide?x|wood's|fecal|echocardiogram|hw|cbc|screen|ometry|ology|x.?ray|parasite|(?:ua |urin[ea])|glucose|freestyle)"
    med_dose = r"(\d+\.?\d*?\s?(?:mg|ml|meq|ug|mcg|g|\%\/g|\%\/ml))"
    med_range_lb = r"((?:\d+\.?\d+- ?\d+)lb|(?:\d+- ?\d+?\.?\d+?)lb)"
    med_other = r"(\d{1,2}\.\d-\d{2})"
    med_other2 = r"(\d{1,2}\.?\d? ?- ?\d{1,3})"
    food = r"k9|treat|ckn|chicken"
    microchip = r"microchip"
    grooming = r"prophy|tartar|pedicure|polish|nail trim"
    supplies = r"shampoo|oz|collar|syr|mousse|\d+? ?ct\b"
    exam = r"(office|ofc e| ofc|exam|anal gland)"
    bandage = r"bandage"
    vaccine = r"vacc|bordetella"
    spay_neuter = r"spay|neuter"
    euthanasia = r"euthanasia"


PROCEDURE_MAP = {
    Regex.supplies: (Cost.SUPPLIES, None),
    Regex.surgery: (Cost.SURGERY, None),
    Regex.test: (
        Cost.TEST,
        [
            "TESTTYPE",
            "TESTPERFORMEDDATE",
            "TESTDUEDATE",
            "TESTCOMMENTS",
        ],
    ),
    Regex.vaccine: (
        Cost.VACCINATION,
        [
            "VACCINATIONTYPE",
            "VACCINATIONGIVENDATE",
            "VACCINATIONCOMMENTS",
            "VACCINATIONDUEDATE",
        ],
    ),
    Regex.med_dose: (
        Cost.MEDICATION,
        [
            "MEDICALGIVENDATE",
            "MEDICALNAME",
            "MEDICALDOSAGE",
            "MEDICALCOMMENTS",
        ],
    ),
    Regex.med_range_lb: (
        Cost.MEDICATION,
        [
            "MEDICALGIVENDATE",
            "MEDICALNAME",
            "MEDICALDOSAGE",
            "MEDICALCOMMENTS",
        ],
    ),
    Regex.food: (Cost.FOOD, None),
    Regex.med_other: (
        Cost.MEDICATION,
        [
            "MEDICALGIVENDATE",
            "MEDICALNAME",
            "MEDICALDOSAGE",
            "MEDICALCOMMENTS",
        ],
    ),
    Regex.microchip: (Cost.MICROCHIP, None),
    Regex.grooming: (Cost.GROOMING, None),
    Regex.exam: (Cost.EXAMINATION, None),
    Regex.bandage: (Cost.BANDAGE, None),
    Regex.spay_neuter: (Cost.SPAY_NEUTER, None),
    Regex.euthanasia: (Cost.EUTHANASIA, None),
}
