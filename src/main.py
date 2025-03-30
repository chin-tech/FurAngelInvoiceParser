#!/usr/bin/env python3
import re
import pandas as pd
import os
import logging
import shutil
import pickle
import base64
from pathlib import Path
import google.auth.transport.requests
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
from flask import Flask, request, redirect, url_for
from flask import session, jsonify, Response
from constants import OAUTH_FILE, SCOPES, LOG_FILE
from constants import PROD_TOKEN, PROD_EMAIL, FROM_LABEL, TO_LABEL
from constants import SVC_ACCOUNT, PROJECT_ID, SECRET_NAME
from constants import INVOICE_DIR, NON_INVOICES_DIR, UNPROCESSED_DIR
from constants import TEST_LABEL, TEST_LABEL_COMPLETE, TEST_EMAIL
from constants import REDIRECT_URI
from constants import IS_DEBUG
from constants import get_login_data
from web_process import show_failed_invoices, process_invoice_corrections
from gfuncs import prune_by_threadId
from gfuncs import Google
from werkzeug.middleware.proxy_fix import ProxyFix
from animal_getter import prepare_animals_for_failure_matching, get_all_animals
from animal_getter import match_animals
from invoices import get_parser


log = logging.getLogger(__name__)
log_formatter = logging.Formatter('[%(asctime)s] %(message)s')

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
    """Verify the OIDC token from Cloud Scheduler"""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        print(f"Token not authorized: \n\tAuth Headers: {auth_header}")
        return jsonify({"error": "Unauthorized"}), 403

    token = auth_header.split("Bearer ")[1]
    print(f"{auth_header}")
    try:
        request_adapter = google.auth.transport.requests.Request()
        decoded_token = id_token.verify_oauth2_token(
            token, request_adapter
        )

        if decoded_token["email"] != SVC_ACCOUNT['client_email']:
            print(f"Token not authorized: {
                  decoded_token['email']} vs. {SVC_ACCOUNT['client_email']}")
            return jsonify({"error": "Unauthorized requester"}), 403
    except Exception as e:
        print(f"Token not authorized: {e} \n\tAuth Headers: {auth_header}")
        return jsonify({"error": f"Invalid token: {str(e)}"}), 403

    return None  # If everything is fine, return None


@app.route('/', methods=['GET'])
def routine_invoice_processor():
    ...


@app.route('/process_all', methods=['GET'])
def process_all_emailed_invoices():
    assert IS_DEBUG == 1
    google = Google()
    google.init_from_secret(PROJECT_ID, SECRET_NAME)
    google.set_services()
    if not google.email_matches(PROD_EMAIL):
        return 'AUTHORIZATION ERRROR', '403'

    messages = google.get_messages_from(GMAIL_INVOICE_LABEL)
    if not messages:
        raise Exception(f"No messages in folder! {GMAIL_INVOICE_LABEL} ")
    messages = prune_by_threadId(messages)
    log.info(f"Starting processing of {len(messages)}")

    if google.process_invoices(messages, DRIVE_INVOICES_FOLDER, FROM_LABEL, TO_LABEL):
        return 'Success!', 200
    else:
        return 'Something Failed', 404


@app.route('/process_routine', methods=['GET'])
def routine_processor():
    auth_error = verify_request()
    if auth_error:
        return auth_error
    days = 14
    google = Google()
    google.init_from_secret(PROJECT_ID, SECRET_NAME)
    google.set_services()
    if not google.email_matches(PROD_EMAIL):
        return 'Authorization Error', 403
    messages = google.get_messages_from(GMAIL_INVOICE_LABEL, days)
    messages = prune_by_threadId(messages)

    if google.process_invoices(messages, DRIVE_INVOICES_FOLDER, FROM_LABEL, TO_LABEL):
        return 'Success!', 200
    else:
        return 'Something Failed', 404


@app.route('/start_auth')
def start_oauth():
    flow = Flow.from_client_config(OAUTH_FILE, scopes=SCOPES)
    flow.redirect_uri = REDIRECT_URI + '/oauth_callback'
    auth_url, state = flow.authorization_url(prompt='consent')
    session['state'] = state
    return redirect(auth_url)


@app.route('/oauth_callback')
def oauth_callback():
    state = session.get('state')
    print(f'[DEBUG] - {state} vs. {request.args.get("state")}')
    if not state or request.args.get('state') != state:
        return 'Authorization state mismatch! Try again', 400

    flow = Flow.from_client_config(OAUTH_FILE, scopes=SCOPES, state=state)
    # url_for('oauth_callback', _external=True)
    flow.redirect_uri = REDIRECT_URI + '/oauth_callback'

    auth_response = request.url
    flow.fetch_token(authorization_response=auth_response)

    session['user_creds'] = base64.b64encode(pickle.dumps(flow.credentials))
    session['state'] = state

    return redirect(url_for('process_failed_invoices'))


@app.route('/retry_failed', methods=['GET', 'POST'])
def process_failed_invoices():
    google = Google()
    creds_serialized = session.get('user_creds')
    if not creds_serialized:
        return redirect(url_for('start_oauth'))
    google.creds = pickle.loads(base64.b64decode(creds_serialized))
    google.set_services()
    if not google.email_matches(PROD_EMAIL):
        return 'Authorization Error', 404
    parent_folder = google.get_drive_folder(DRIVE_INVOICES_FOLDER)
    failed, pdfs = google.get_failed_invoice_data(parent_folder)
    animals = prepare_animals_for_failure_matching()

    if request.method == 'GET':
        return show_failed_invoices(failed, pdfs, animals)
    if request.method == 'POST':
        return process_invoice_corrections(google, request, parent_folder, failed, pdfs, animals)


@app.route('/dbg_failed', methods=['GET', 'POST'])
def fail_debug_processor():
    assert IS_DEBUG == 1
    google = Google()
    creds_serialized = session.get('user_creds')
    if not creds_serialized:
        return redirect(url_for('start_oauth'))
    google.creds = pickle.loads(base64.b64decode(creds_serialized))
    google.set_services()
    if not google.email_matches(PROD_EMAIL):
        return 'Authorization Error', 404
    google.set_services()
    parent_folder = google.get_drive_folder(DRIVE_INVOICES_FOLDER)
    failed, pdfs = google.get_failed_invoice_data(parent_folder)
    animals = prepare_animals_for_failure_matching()

    if request.method == 'GET':
        return show_failed_invoices(failed, pdfs, animals)

    return process_invoice_corrections(google, request, parent_folder, failed, pdfs, animals)


@app.route('/dbg_routine', methods=['GET'])
def test_process():
    assert IS_DEBUG == 1
    google = Google()
    google.init_from_token(OAUTH_FILE, PROD_TOKEN, False)
    google.set_services()
    messages = google.get_messages_from(GMAIL_INVOICE_LABEL)
    if not messages or len(messages) == 0:
        return 'No messages found', 200
    messages = prune_by_threadId(messages)
    good = google.process_invoices(
        messages, DRIVE_INVOICES_FOLDER,
        from_label=FROM_LABEL, to_label=TO_LABEL, debugging=True)
    if good:
        return 'Success!', 200
    else:
        return 'Something Failed', 404


@app.route('/dbg', methods=['GET'])
def run_local():
    assert IS_DEBUG == 1
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO)
    invoice_items = pd.DataFrame()
    good, bad = pd.DataFrame(), pd.DataFrame()
    errors = list()
    print("---------")
    NON_INVOICES_DIR.mkdir(exist_ok=True)
    UNPROCESSED_DIR.mkdir(exist_ok=True)

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
            matched = match_animals(parser.items, db_animals)
            condition = matched['ANIMALCODE'] != 'ERROR_CODE'
            if matched[condition].shape[0] == parser.items.shape[0]:
                shutil.move(invoice, parser.success_dir / Path(parser.name))
            else:
                shutil.move(invoice, parser.fail_dir / Path(parser.name))
            good = pd.concat([good, matched[condition]])
            bad = pd.concat([bad, matched[~condition]])
            invoice_count += 1
        except Exception as e:
            errors.append(invoice)
            try:
                shutil.move(invoice, UNPROCESSED_DIR)
            except Exception as e:
                print(f"File {invoice} exists there already: {e}")
                continue
            print(f"\t[ERROR]: {e}")
    print("--------------")
    print(f"------[ERRORS: {len(errors)}]-------")
    for e in errors:
        print(e.name)
    print("____________________")
    print(f"Successfully Processed: {invoice_count} invoices")
    output_path = Path("../data/completed_csvs")
    good.to_csv(output_path / 'good_parse.csv', index=False)
    bad.to_csv(output_path / 'errata.csv', index=False)
    log.info('-- END INVOICE PARSING --')
    return "-- Finished -- ", 200


@app.route('/test_basic_api', methods=['GET'])
def test_apis():
    assert IS_DEBUG == 1
    google = Google()
    google.init_from_secret(PROJECT_ID, SECRET_NAME)
    google.set_services()
    match = google.email_matches(PROD_EMAIL)

    assert True == match
    assert google.drive != None
    assert google.gmail != None
    return "Success! We can access data!", 200


@app.route('/test_auth', methods=['GET'])
def test_auth_with_apis():
    auth_error = verify_request()
    if auth_error:
        return auth_error
    google = Google()
    google.init_from_secret(PROJECT_ID, SECRET_NAME)
    google.set_services()
    match = google.email_matches(PROD_EMAIL)

    assert True == match
    assert google.drive != None
    assert google.gmail != None
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


# @app.route('/debug_failed', methods=['GET', 'POST'])
# def process_fail_debug():
#     assert IS_DEBUG == 1
#     google = Google()
#     google.init_from_token(OAUTH_FILE, PROD_TOKEN, False)
#     google.set_services()
#     print(google.creds)
#     print(google.drive)
#     print(google.gmail)
#     parent_folder = google.get_drive_folder(DRIVE_INVOICES_FOLDER)
#     pdfs = pd.DataFrame(google.get_failed_pdfs(parent_folder))
#     assert pdfs.empty == False
#     animals = prepare_animals_for_failure_matching()
#     bads = pd.read_csv('../data/completed_csvs/errata.csv')
#     bads, pdfs = add_invoices_col(bads, pdfs)
#
#     if request.method == 'GET':
#         return show_failed_invoices(bads, pdfs, animals)
#
#     if request.method == 'POST':
#         post_df = get_post_data(request, animals)
#         post_df.to_csv('../data/post_data.csv', index=False)
#         updated = update_invoice_data(bads, post_df)
#         updated.to_csv("../data/completed_csvs/updated_errata.csv")
#
#         return render_template('post.html', invoices=post_df.shape[0], rows=updated[updated['ANIMALCODE'] != 'ERROR_CODE'].shape[0])
#


# @app.route('/failed_invoices', methods=['GET', 'POST'])
# def process_failed_invoices():
#     global GLOBAL_CREDS
#     # creds = get_creds_secret(PROJECT_ID, SECRET_NAME)
#     google = Google()
#     if not GLOBAL_CREDS:
#         # url_for('oauth_callback', _external=True)
#         redirect_uri = REDIRECT_URI + '/oauth_callback'
#         google.creds = google.init_from_web(OAUTH_FILE, redirect_uri)
#         # creds = get_creds(OAUTH_FILE, "", True, redirect_uri)
#         if isinstance(google.creds, Response):
#             return google.creds
#     google.creds = session['creds']
#     creds = GLOBAL_CREDS
#     gmail = get_gmail_service(creds)
#     email = gmail.users().getProfile(userId='me').execute()['emailAddress']
#     if email != PROD_EMAIL:
#         return 'Authorization Error', 403
#     drive = get_drive_service(creds)
#     parent_folder = get_drive_folder(drive, DRIVE_INVOICES_FOLDER)
#     pdfs = pd.DataFrame(get_failed_pdfs(drive, parent_folder))
#     animal_df = get_all_animals(get_login_data())
#     animal_df.sort_values(by='DATEBROUGHTIN')
#     animal_df['date_in'] = animal_df['DATEBROUGHTIN'].dt.date
#     animal_df['last_day_on_shelter'] = animal_df['end_date'].dt.date
#     failed_invoice = get_failed_csv(drive, parent_folder)
#     assert failed_invoice != None
#     failed_bytes = drive_file_to_bytes(drive, failed_invoice.get('id'))
#     assert failed_bytes != None
#     f_frame = pd.read_csv(failed_bytes)
#     f_frame, pdfs = add_invoices_col(f_frame, pdfs)
#
#     if request.method == 'GET':
#         return show_failed_invoices(f_frame, pdfs, animal_df)
#
#     if request.method == 'POST':
#         post_df = get_post_data(request, animal_df)
#         updated = update_invoice_data(f_frame, post_df)
#         good_data_condition = updated['ANIMALCODE'] != 'ERROR_CODE'
#         to_upload = updated[good_data_condition]
#         if to_upload.empty:
#             return render_template('post.html', invoices=post_df.shape[0], rows=updated[updated['ANIMALCODE'] != 'ERROR_CODE'].shape[0])
#
#         to_fails = updated[~good_data_condition]
#         timestamp = dt.now().date()
#         error_name = f"{timestamp}-failures.csv"
#         success_name = f"{timestamp}-corrections.csv"
#         new_id = upload_drive(
#             drive, to_fails.drop(['invoice', 'invoice_date', 'cmp'], axis=1), error_name, [
#                 parent_folder], 'text/csv'
#         )
#         corrected_folder = get_drive_folder(
#             drive, 'corrections', parent_folder)
#         corrected_id = upload_drive(
#             drive, to_upload, f'{success_name}', [corrected_folder], 'text/csv'
#         )
#         success = upload_dataframe_to_database(
#             to_upload.drop(
#                 ['invoice', 'invoice_date', 'cmp'], axis=1
#             ), False)
#         if success:
#             if new_id:
#                 old_id = failed_invoice.get('id')
#                 drive.files().delete(fileId=old_id).execute()
#                 print(f"Deleted {failed_invoice.get('name')}")
#             batch = drive.new_batch_http_request()
#             folders = pd.DataFrame(get_invoice_folders(drive, parent_folder))
#             completed_pdfs = to_upload['cmp'].unique()
#             incomplete_pdfs = to_fails['cmp'].unique()
#             completed_invoices = pdfs[
#                 (pdfs['cmp'].isin(completed_pdfs)) &
#                 (~pdfs['cmp'].isin(incomplete_pdfs))
#             ]
#             for pdf in completed_invoices.itertuples():
#                 invoice_type = pdf.name.split('_')[0]
#                 incomplete_folder = folders[folders['name'] == f"{
#                     invoice_type}_incomplete"]['id'].values[0]
#                 complete_folder = folders[folders['name'] == f"{
#                     invoice_type}_completed"]['id'].values[0]
#                 batch.add(drive.files().update(
#                     fileId=pdf.id,
#                     addParents=complete_folder,
#                     removeParents=incomplete_folder,
#                 ))
#             batch.execute()
#             GLOBAL_CREDS = None
#
#         return render_template('post.html', invoices=post_df.shape[0], rows=updated[updated['ANIMALCODE'] != 'ERROR_CODE'].shape[0])
#
