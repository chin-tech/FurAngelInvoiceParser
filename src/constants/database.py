import os
## DATABASE ENV ##
DB_NAME = os.environ.get("DB_NAME", "")
DB_USERNAME = os.environ.get("DB_USER", "")
DB_PASS = os.environ.get("DB_PASS", "")

DATABASE_URL = "https://us06d.sheltermanager.com"
LOGIN_URL = DATABASE_URL + "/login?smaccount="
CSV_URL = DATABASE_URL + "/report_export_csv?id=216"
CSV_UPLOAD_URL = DATABASE_URL + "/csvimport"

DB_LOGIN_DATA = {
        "database": DB_NAME,
        "username": DB_USERNAME,
        "password": DB_PASS,

}
