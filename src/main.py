#!/usr/bin/env python3
import re
import pandas as pd
import os
import logging
import shutil
import json
from pathlib import Path
import google.auth.transport.requests
# import google.ouath2.id_token
from google_auth_oauthlib.flow import Flow
from rich import print
from flask import Flask, request, redirect, url_for, session, render_template, jsonify
from constants import LOG_FILE
from constants import TEST_TOKEN, PROD_TOKEN, OAUTH_FILE
from constants import INVOICE_DIR, NON_INVOICES_DIR
from constants import TEST_LABEL, TEST_LABEL_COMPLETE, TEST_EMAIL
from constants import PROD_EMAIL, SVC_ACCOUNT, PROJECT_ID, SECRET_NAME
from constants import IS_DEBUG
from constants import get_login_data
from gfuncs import get_creds, get_creds_secret
from gfuncs import get_drive_service, get_gmail_service
from gfuncs import get_invoices_gmail  # , get_drive_folder
from gfuncs import process_msg_invoices, prune_by_threadId  # , get_failed_pdfs
# from gfuncs import parse_failed_pdfs_from_drive

from animal_getter import get_all_animals, match_animals, get_probable_matches
from invoices import get_parser


# Redirect URI (must match the one in your Google Cloud Console)
REDIRECT_URI = 'http://localhost:8080/callback'  # Adjust if needed


log = logging.getLogger(__name__)
log_formatter = logging.Formatter('[%(asctime)s] %(message)s')

# GMAIL CONSTANTS
GMAIL_INVOICE_LABEL = "Invoices/Vet invoice"

# Drive Constants #
DRIVE_INVOICES_FOLDER = "VET_INVOICES"


app = Flask(__name__)


def verify_request():
    """Verify the OIDC token from Cloud Scheduler"""
    if Path(SVC_ACCOUNT).exists():
        with open(SVC_ACCOUNT, 'r') as f:
            info = json.load(f)
    else:
        info = json.loads(SVC_ACCOUNT)
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({"error": "Unauthorized"}), 403

    token = auth_header.split("Bearer ")[1]
    try:
        request_adapter = google.auth.transport.requests.Request()
        decoded_token = google.oauth2.id_token.verify_oauth2_token(
            token, request_adapter
        )

        if decoded_token["email"] != info['client_email']:
            return jsonify({"error": "Unauthorized requester"}), 403
    except Exception as e:
        return jsonify({"error": f"Invalid token: {str(e)}"}), 403

    return None  # If everything is fine, return None


@app.route('/', methods=['GET'])
def routine_invoice_processor():
    ...


@app.route('/process_all', methods=['GET'])
def process_all_emailed_invoices():
    assert IS_DEBUG == 1
    creds = get_creds_secret(PROJECT_ID, SECRET_NAME)
    gmail = get_gmail_service(creds)
    email = gmail.users().getProfile(userId='me').execute()['emailAddress']
    if email != PROD_EMAIL:
        return 'Authorization Error', 403
    drive = get_drive_service(creds)
    messages = get_invoices_gmail(gmail, GMAIL_INVOICE_LABEL)
    if not messages:
        raise Exception(f"No messages in folder! {GMAIL_INVOICE_LABEL} ")
    messages = prune_by_threadId(messages)
    log.info(f"Starting processing of {len(messages)}")
    good = process_msg_invoices(gmail, drive, messages, DRIVE_INVOICES_FOLDER)
    if good:
        return 'Success!', 200
    else:
        return 'Something Failed', 404


@app.route('/process_routine', methods=['GET'])
def routine_processor():
    auth_error = verify_request()
    if auth_error:
        return auth_error
    days = 14
    creds = get_creds_secret(PROJECT_ID, SECRET_NAME)
    gmail = get_gmail_service(creds)
    email = gmail.users().getProfile(userId='me').execute()['emailAddress']
    if email != PROD_EMAIL:
        return 'Authorization Error', 200
    drive = get_drive_service(creds)
    messages = get_invoices_gmail(gmail, GMAIL_INVOICE_LABEL, days)
    messages = prune_by_threadId(messages)
    good = process_msg_invoices(gmail, drive, messages, DRIVE_INVOICES_FOLDER)
    if good:
        return 'Success!', 200
    else:
        return 'Something Failed', 404


@app.route('/failed_invoices', methods=['GET', 'POST'])
def process_failed_invoices():
    return "Currently Unimplemented....", 200
    # TODO: Implement user derived failure fixing....

    # creds = get_creds(OAUTH_FILE, TEST_TOKEN)
    # gmail = get_gmail_service(creds)
    # drive = get_drive_service(creds)
    # parent_folder = get_drive_folder(drive, INVOICE_DIR)
    # animals_db = get_all_animals(get_login_data())
    # if request.method == 'GET':
    #     pdfs = get_failed_pdfs(drive, parent_folder)
    #     failed_items = parse_failed_pdfs_from_drive(drive, pdfs, animals_db)
    #
    #     fail_display_cols = ['COSTDESCRIPTION', 'name']
    #     success_display_cols = ['ANIMALNAME', 'DATEBROUGHTIN', 'TIMEONSHELTER']


@app.route('/debug_failed', methods=['GET', 'POST'])
def process_fail_debug():
    assert IS_DEBUG == 1
    animal_df = get_all_animals(get_login_data())
    animal_df.sort_values(by='DATEBROUGHTIN')
    animal_df['date_in'] = animal_df['DATEBROUGHTIN'].dt.date
    animal_df['last_day_on_shelter'] = animal_df['end_date'].dt.date
    if request.method == 'GET':
        bads = pd.read_csv('data/completed_csvs/errata.csv')
        bads['date'] = pd.to_datetime(bads['COSTDATE'])
        bads['name'] = bads['ANIMALNAME']
        bads['invoice'] = bads['COSTDESCRIPTION'].str.extract(
            r'- (\d+) -')
        bads['date'] = pd.to_datetime(bads['COSTDATE'])
        bads.sort_values(by='date', inplace=True)
        fails = bads[['name', 'invoice', 'COSTDATE', 'date']
                     ].drop_duplicates(['name', 'invoice'])
        data_to_show = list()
        for row in fails.itertuples():
            possible_animals = get_probable_matches(
                row.name, animal_df, row.date)
            data_to_show.append(
                (row, possible_animals.to_dict(orient='records')))
        return render_template('tmpl.html', data_to_show=data_to_show)


@app.route('/debug_routine', methods=['GET'])
def test_process():
    assert IS_DEBUG == 1
    creds = get_creds(OAUTH_FILE, TEST_TOKEN)
    gmail = get_gmail_service(creds)
    drive = get_drive_service(creds)
    messages = get_invoices_gmail(gmail, 'invoices')
    good = process_msg_invoices(gmail, drive, messages, 'test_invoices123',
                                from_label=TEST_LABEL, to_label=TEST_LABEL_COMPLETE, is_debug=True)
    if good:
        return 'Success!', 200
    else:
        return 'Something Failed', 404


@app.route('/debug', methods=['GET'])
def run_local():
    assert IS_DEBUG == 1
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO)
    invoice_items = pd.DataFrame()
    errors = list()
    print("---------")
    invoices = list(INVOICE_DIR.rglob("*.*"))
    # ipath = INVOICE_DIR / Path("ezyvet_invoices")
    # invoices = list(ipath.rglob("*.*"))
    log.info(" -- BEGIN INVOICE PARSING --")
    log.info(f"Invoice Count: {len(invoices)}")
    db_animals = get_all_animals(get_login_data())

    print(f"Invoices: {len(invoices)}")
    invoice_count = 0
    for invoice in invoices:
        try:
            if re.search(r'statement|treatment|estimate|record', invoice.name.lower()):
                try:
                    shutil.move(invoice, NON_INVOICES_DIR)
                except Exception as e:
                    print(f"{invoice.name} had error moving: {e}")
                continue
            # text = extract_text(invoice)
            parser = get_parser(invoice)
            parser.parse_invoice()
            invoice_items = pd.concat([invoice_items, parser.items])
            invoice_count += 1
        except Exception as e:
            errors.append(invoice)
            print(f"\t[ERROR]: {e}")
    print("--------------")
    print(f"------[ERRORS: {len(errors)}]-------")
    for e in errors:
        print(e.name)
    print("____________________")
    print(f"Successfully Processed: {invoice_count} invoices")
    output_path = Path("../data/completed_csvs")
    if invoice_items.empty:
        raise ValueError("WE MADE A MISTAKE!")
    invoice_items = match_animals(invoice_items, db_animals)
    good = invoice_items['ANIMALCODE'] != 'ERROR_CODE'
    invoice_items[good].to_csv(output_path / 'good_parse.csv', index=False)
    invoice_items[~good].to_csv(output_path / 'errata.csv', index=False)
    log.info('-- END INVOICE PARSING --')
    return "-- Finished -- ", 200


@app.route('/test_basic_api', methods=['GET'])
def test_apis():
    creds = get_creds_secret(PROJECT_ID, SECRET_NAME)
    gmail = get_gmail_service(creds)
    drive = get_drive_service(creds)
    email = gmail.users().getProfile(userId='me').execute()['emailAddress']

    assert email == PROD_EMAIL
    assert drive != None
    return "Success! We can access data!", 200


@app.route('/test_auth', methods=['GET'])
def test_auth_with_apis():
    auth_error = verify_request()
    if auth_error:
        return auth_error
    creds = get_creds_secret(PROJECT_ID, SECRET_NAME)
    gmail = get_gmail_service(creds)
    drive = get_drive_service(creds)
    email = gmail.users().getProfile(userId='me').execute()['emailAddress']

    assert email == PROD_EMAIL
    assert drive != None
    return "Success! We can access data!", 200


if __name__ == '__main__':
    app.secret_key = os.urandom(24)
    app.run(debug=True, host='0.0.0.0', port=8000)


# @pytest.fixture
# def vp():
#     return Vaccine()
#
#
# def test_vaccine_matches_exact(vp):
#     assert vp.parse("DHLPP") == Vaccine.DHLPP
#     assert vp.parse("DHPP") == Vaccine.DHPP
#     assert vp.parse("Bordetella") == Vaccine.BORDETELLA
#     assert vp.parse("Leptospirosis") == Vaccine.LEPTOSPIROSIS
#     assert vp.parse("Parainfluenza") == Vaccine.PARAINFLUENZA
#
#
# def test_vaccine_matches(vp):
#     assert vp.parse("DA2LPP - Puppy vaccine") == Vaccine.DHLPP
#     assert vp.parse("DA2PP (No Lepto) - Litter, 1st vacc") == Vaccine.DHPP
#     assert vp.parse("Bordetella Oral - Adult Vaccine") == Vaccine.BORDETELLA
#     assert vp.parse("Leptospirosis 4 vaccine") == Vaccine.LEPTOSPIROSIS
# from gfuncs import get_creds, process_msg_invoices, get_drive_service, get_gmail_service, get_invoices_gmail, get_drive_folder, parse_failed_pdfs_from_drive, get_failed_pdfs
# from constants import LOG_FILE, TEST_TOKEN, PROD_TOKEN, OAUTH_FILE, INVOICE_DIR, NON_INVOICES_DIR, TEST_LABEL, TEST_LABEL_COMPLETE, TEST_EMAIL, PROD_EMAIL, SVC_ACCOUNT, get_login_data
