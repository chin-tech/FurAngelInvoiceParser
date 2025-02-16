import os
import logging
from dotenv import load_dotenv


log = logging.log

DB_NAME = "ASM_NAME"
DB_USERNAME = "ASM_USER"
DB_PASS = "ASM_PASS"


def get_login_data() -> dict:
    load_dotenv(verbose=True)
    try:
        db_name = os.getenv(DB_NAME)
        db_user = os.getenv(DB_USERNAME)
        db_pass = os.getenv(DB_PASS)
        if not db_name or not db_user or not db_pass:
            raise Exception("Environment variables not set")
    except Exception as e:
        log.exception(f"Environment Variables not set!\n{e}")
    return {
        'database': db_name,
        'username': db_user,
        'password': db_pass,
    }
