#!/usr/bin/env python3
import base64
import logging
import os
import pickle

import google.auth.transport.requests
from flask import Flask, Response, jsonify, redirect, request, session, url_for
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
from werkzeug.middleware.proxy_fix import ProxyFix

from animal_getter import prepare_animals_for_failure_matching
from constants import (
    FROM_LABEL,
    IS_DEBUG,
    OAUTH_FILE,
    PROD_EMAIL,
    PROJECT_ID,
    REDIRECT_URI,
    SCOPES,
    SECRET_NAME,
    SVC_ACCOUNT,
    TO_LABEL,
)
from google_api_functions import GoogleClient, prune_by_threadId
from web_process import process_invoice_corrections, show_failed_invoices

log = logging.getLogger(__name__)
log_formatter = logging.Formatter("[%(asctime)s] %(message)s")

# GMAIL CONSTANTS
GMAIL_INVOICE_LABEL = "Invoices/Vet invoice"

# Drive Constants #
DRIVE_INVOICES_FOLDER = "VET_INVOICES"

# GLOBAL_CREDS = ""
GLOBAL_CREDS = None


app = Flask(__name__)
app.secret_key = os.urandom(24)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)


def verify_request():
    """Verify the OIDC token from Cloud Scheduler."""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 403

    token = auth_header.split("Bearer ")[1]
    try:
        request_adapter = google.auth.transport.requests.Request()
        decoded_token = id_token.verify_oauth2_token(token, request_adapter)

        if decoded_token["email"] != SVC_ACCOUNT["client_email"]:
            return jsonify({"error": "Unauthorized requester"}), 403
    except Exception as e:
        return jsonify({"error": f"Invalid token: {e!s}"}), 403

    return None  # If everything is fine, return None


@app.route("/", methods=["GET"])
def routine_invoice_processor():
    return Response(
        f"Hello!, You likely want this! <a href={request.url_root}retry_failed> Retry the failed invoices </a>",
        200,
    )


@app.route("/process_all", methods=["GET"])
def process_all_emailed_invoices():
    assert IS_DEBUG == 1
    google = GoogleClient()
    google.init_from_secret(PROJECT_ID, SECRET_NAME)
    google.set_services()
    if not google.email_matches(PROD_EMAIL):
        return Response("AUTHORIZATION ERRROR", "403")

    messages = google.get_messages_from(GMAIL_INVOICE_LABEL)
    if not messages:
        log.error(f"No messages in folder! {GMAIL_INVOICE_LABEL} ")
        return "No messages in specified folder!", 404
    messages = prune_by_threadId(messages)
    log.info(f"Starting processing of {len(messages)}")

    if google.process_invoices(messages, DRIVE_INVOICES_FOLDER, FROM_LABEL, TO_LABEL):
        return Response("Success!", 200)
    return Response("Something Failed", 404)


@app.route("/process_routine", methods=["GET"])
def routine_processor():
    auth_error = verify_request()
    if auth_error:
        return auth_error
    days = 14
    google = GoogleClient()
    google.init_from_secret(PROJECT_ID, SECRET_NAME)
    google.set_services()
    if not google.email_matches(PROD_EMAIL):
        return Response("Authorization Error", 403)
    messages = google.get_messages_from(GMAIL_INVOICE_LABEL, days)
    messages = prune_by_threadId(messages)

    if google.process_invoices(messages, DRIVE_INVOICES_FOLDER, FROM_LABEL, TO_LABEL):
        return Response("Success!", 200)
    return Response("Something Failed", 404)


@app.route("/start_auth")
def start_oauth():
    flow = Flow.from_client_config(OAUTH_FILE, scopes=SCOPES)
    flow.redirect_uri = REDIRECT_URI + "/oauth_callback"
    auth_url, state = flow.authorization_url(prompt="consent")
    session["state"] = state
    return redirect(auth_url)


@app.route("/oauth_callback")
def oauth_callback():
    state = session.get("state")
    if not state or request.args.get("state") != state:
        return "Authorization state mismatch! Try again", 400

    flow = Flow.from_client_config(OAUTH_FILE, scopes=SCOPES, state=state)
    # url_for('oauth_callback', _external=True)
    flow.redirect_uri = REDIRECT_URI + "/oauth_callback"

    auth_response = request.url
    flow.fetch_token(authorization_response=auth_response)

    session["user_creds"] = base64.b64encode(pickle.dumps(flow.credentials))
    session["state"] = state

    return redirect(url_for("process_failed_invoices"))


@app.route("/retry_failed", methods=["GET", "POST"])
def process_failed_invoices():
    google = GoogleClient()
    creds_serialized = session.get("user_creds")
    if not creds_serialized:
        return redirect(url_for("start_oauth"))
    google.creds = pickle.loads(base64.b64decode(creds_serialized))
    google.set_services()
    if not google.email_matches(PROD_EMAIL):
        return Response("Authorization Error", 404)
    parent_folder = google.get_drive_folder(DRIVE_INVOICES_FOLDER)
    failed, pdfs = google.get_failed_invoice_data(parent_folder)
    animals = prepare_animals_for_failure_matching()

    if request.method == "GET":
        return show_failed_invoices(failed, pdfs, animals)
    if request.method == "POST":
        return process_invoice_corrections(
            google, request, parent_folder, failed, pdfs, animals,
        )
    return Response("Unknown Method", 405)


@app.route("/test_basic_api", methods=["GET"])
def test_apis():
    assert IS_DEBUG == 1
    google = GoogleClient()
    google.init_from_secret(PROJECT_ID, SECRET_NAME)
    google.set_services()
    match = google.email_matches(PROD_EMAIL)

    assert match
    assert google.drive is not None
    assert google.gmail is not None
    return Response("Success! We can access data!", 200)


@app.route("/test_auth", methods=["GET"])
def test_auth_with_apis():
    auth_error = verify_request()
    if auth_error:
        return auth_error
    google = GoogleClient()
    google.init_from_secret(PROJECT_ID, SECRET_NAME)
    google.set_services()
    match = google.email_matches(PROD_EMAIL)

    assert match
    assert google.drive is not None
    assert google.gmail is not None
    return Response("Success! We can access data!", 200)


if __name__ == "__main__":
    app.secret_key = os.urandom(24)
    app.run(
        debug=True,
        host="0.0.0.0",
        port=8000,
        load_dotenv=True,
        ssl_context=("../secrets/cert.pem", "../secrets/key.pem"),
    )
