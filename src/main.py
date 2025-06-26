#!/usr/bin/env python3
import logging
import os
import json

import google.auth.transport.requests
from flask import Flask, Response, jsonify, redirect, request, session, url_for
from google.oauth2 import id_token
from werkzeug.middleware.proxy_fix import ProxyFix
from googleauth import CredentialManager, SecretManager
from google_services import DriveService,  Processor
from blueprints.oauth_routes import auth_bp
from blueprints.name_route import name_bp
from utils import process_invoices

from animal_db_handler import get_all_animals, prepare_animals_for_failure_matching
#
from constants.project import (
    PROJECT_ID,
    SECRET_NAME,
    SERVICE_ACCOUNT_CONFIG_FILE,
    OAUTH_CLIENT_CONFIG_JSON_FILE,
    REDIRECT_URI,
    SCOPES,
    DRIVE_INVOICES_FOLDER,



)
from constants.database import (
    DB_LOGIN_DATA
)
from web_process import process_invoice_corrections, show_failed_invoices

log = logging.getLogger(__name__)
log_formatter = logging.Formatter("[%(asctime)s] %(message)s")

ROUTINE_DAYS = 14

REDIRECT_CALLBACK="/oauth_callback"
FULL_REDIRECT_URL = REDIRECT_URI + REDIRECT_CALLBACK


app = Flask(__name__)
app.secret_key = os.urandom(24)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

app.web_creds_manager = CredentialManager(
    scopes=SCOPES,
    oauth_client_config_path=OAUTH_CLIENT_CONFIG_JSON_FILE
)

# app.local_creds_manager = CredentialManager(
#     scopes=SCOPES,
#     oauth_client_config_path=OAUTH_CLIENT_CONFIG_JSON_FILE
# )

app.secret_manager = SecretManager(
    project_id= PROJECT_ID,
    secret_names = [SECRET_NAME]
)



app.register_blueprint(auth_bp, url_prefix="/auth")
app.register_blueprint(name_bp, url_prefix="/names")

# Test routes #
# from blueprints.debug_routes import test_routes
# app.register_blueprint(test_routes, url_prefix="/test")



def verify_request():
    """Verify the OIDC token from Cloud Scheduler."""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 403

    token = auth_header.split("Bearer ")[1]
    try:
        request_adapter = google.auth.transport.requests.Request()
        decoded_token = id_token.verify_oauth2_token(token, request_adapter)
        with open(SERVICE_ACCOUNT_CONFIG_FILE, 'rb') as f:
            cfg_json = json.load(f)

        if decoded_token["email"] != cfg_json["client_email"]:
            return jsonify({"error": "Unauthorized requester"}), 403
    except Exception as e:
        return jsonify({"error": f"Invalid token: {e!s}"}), 403

    return None  


@app.route("/", methods=["GET"])
def routine_invoice_processor():
    return Response(
        f"Hello!, You likely want this! <a href={request.url_root}retry_failed> Retry the failed invoices </a>",
        200,
    )

@app.route("/process_routine", methods=["GET"])
def routine_processor():
    auth_error = verify_request()
    if auth_error:
        return auth_error
    creds = app.secret_manager.retrieve_secret_from_file(SECRET_NAME, SERVICE_ACCOUNT_CONFIG_FILE)
    processor = Processor(creds)
    success = process_invoices(processor, ROUTINE_DAYS )
    if not success:
        return Response("Something went wrong!", 304)
    return Response("Success", 200)



@app.route("/retry_failed", methods=["GET", "POST"])
def process_failed_invoices():
    creds = app.web_creds_manager.load_from_session_data(session)
    if not creds:
        return redirect(url_for("auth.start_oauth_process"))
    drive = DriveService(creds)
    drive_folder_id = drive.get_or_create_folder(DRIVE_INVOICES_FOLDER)
    failed, pdfs = drive.get_all_failed_invoice_data(drive_folder_id)
    animals = prepare_animals_for_failure_matching()

    if request.method == "GET":
        return show_failed_invoices(failed, pdfs, animals)
    if request.method == "POST":
        return process_invoice_corrections(
            drive, request, drive_folder_id, failed, pdfs, animals,
        )
    return Response("Unknown Method", 405)



@app.route("/get_animals", methods=["GET"])
def list_animals():
    animals = get_all_animals(login_data=DB_LOGIN_DATA)
    return Response(animals.to_html())



if __name__ == "__main__":
    app.secret_key = os.urandom(24)
    app.run(
        debug=True,
        host="0.0.0.0",
        port=8000,
        load_dotenv=True,
        ssl_context=("../secrets/cert.pem", "../secrets/key.pem"),
    )
