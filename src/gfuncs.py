import pandas as pd
import re
import io
import logging
import base64
import pickle
import os
import json
from flask import session
from pathlib import Path
from collections import defaultdict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.cloud import secretmanager
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from invoices import get_parser
from animal_getter import upload_dataframe_to_database, get_all_animals, match_animals
from constants import SVC_ACCOUNT
from constants import get_login_data
from datetime import datetime as dt
from datetime import timedelta as td
from typing import Union

log = logging.getLogger(__name__)

# GOOGLE SCOPES
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify'
]

# GMAIL DATE FORMAT
GMAIL_DATE = "%a, %d %b %Y %H:%M:%S %z"
GMAIL_DATE_ZONE = "%a, %d %b %Y %H:%M:%S %z (%Z)"

# INVOICE LABELS
INCOMPLETE_INVOICE = 'Label_5838368921937526589'
COMPLETE_INVOICE = 'Label_342337121491929089'

NON_INVOICE_REGEXES = r"statement|treatment|estimate|record|payment"


def get_creds(client_id_file: str, token_file: str) -> Credentials:
    """Returns google auth credentials with given id_file or stored token file"""
    creds = None
    if os.path.exists(token_file):
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                client_id_file, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
    return creds


def get_secret(name, project) -> Credentials:
    client = secretmanager.SecretManagerServiceClient.from_service_account_info(
        SVC_ACCOUNT)
    path = f"projects/{project}/secrets/{name}/versions/latest"
    response = client.access_secret_version(name=path)
    return pickle.loads(response.payload.data)


def update_secret(name, project, value):
    client = secretmanager.SecretManagerServiceClient.from_service_account_info(
        SVC_ACCOUNT)

    parent = f"projects/{project}/secrets/{name}"
    client.add_secret_version(
        parent=parent,
        payload={"data": pickle.dumps(value)},
    )


def get_creds_secret(project, token_name):
    creds = get_secret(token_name, project)

    if not creds.valid or creds.expired:
        creds.refresh(Request())
        if creds.refresh_token:
            update_secret(token_name, project, creds)
    return creds


def get_drive_service(creds=None):
    """Returns Google Drive service with given credentials"""
    if not creds:
        creds = Credentials.from_authorized_user_info(session['credentials'])
    service = build('drive', 'v3', credentials=creds)
    return service


def get_gmail_service(creds=None):
    """Returns Google's GMail service with given credentials"""
    if not creds:
        creds = Credentials.from_authorized_user_info(session['credentials'])
    service = build('gmail', 'v1', credentials=creds)
    return service


def get_drive_folder(service, folder_name, parent_folder_id=None) -> str:
    """Returns drive folder ID; creates if it doesn't exist, adds to parent if provided."""

    mime_type = 'application/vnd.google-apps.folder'
    query = f"name='{folder_name}' and mimeType='{mime_type}'"

    if parent_folder_id:
        # Add parent folder to query
        query += f" and '{parent_folder_id}' in parents"

    res = service.files().list(q=query, spaces='drive').execute()
    files = res.get('files', [])

    if not files:
        file_metadata = {
            'name': folder_name,
            'mimeType': mime_type,
        }

        if parent_folder_id:
            # Add parent folder to metadata
            file_metadata['parents'] = [parent_folder_id]

        file = service.files().create(body=file_metadata, fields='id').execute()
        log.info(f"{folder_name}: Created with ID: {file.get('id')}")
        return file.get('id')

    # Ensure parent is set if it was provided
    files_parents = files[0].get('parents', [])
    if parent_folder_id and not any(parent_folder_id in parents for parents in files_parents):
        file_metadata = {
            'addParents': [parent_folder_id]
        }
        file = service.files().update(fileId=files[0].get(
            'id'), body=file_metadata, fields='id').execute()
        log.info(f"{folder_name}: Updated to add parent ID: {
                 parent_folder_id}")

    return files[0].get('id')


def get_drive_folder_ids(drive):
    "Debug Function: Returns all folders and ID's in google drive service"
    query = "mimeType = 'application/vnd.google-apps.folder'"
    res = drive.files().list(
        q=query,
        spaces='drive',
        fields='nextPageToken, files(id,name)'
    ).execute()
    files = res.get('files', [])
    if not files:
        print("No Folders Found!")
    print("-- Folders -- ")
    for folder in files:
        print(f"{folder.get('name')}: {folder.get('id')}")
    while res.get('nextPageToken'):
        res = drive.files().list(
            q=query,
            spaces='drive',
            fields='nextPageToken, files(id,name)',
            pageToken=res.get('nextPageToken')
        ).execute()
        folders = res.get('files', [])
        for folder in folders:
            print(f"{folder.get('name')}: {folder.get('id')}")
    return None


def get_invoices_gmail(gmail, folder_name, days_ago: int = None):
    """Returns all messages in given folder for further processing"""
    try:
        # Get folder ID
        labels = gmail.users().labels().list(userId='me').execute().get('labels', [])
        folder_id = next(
            (label['id'] for label in labels if label['name'] == folder_name), None)

        if not folder_id:
            log.error(f"Folder '{folder_name}' not found.")
            return

        if days_ago:
            cutoff = dt.now() - td(days=days_ago)
            cutoff_date = cutoff.strftime("%Y/%m/%d")

        # Fetch all messages in the folder (handling pagination)
        messages = []
        page_token = None

        while True:
            query = f"after:{cutoff_date}" if days_ago else ""
            response = gmail.users().messages().list(
                userId='me', q=query, labelIds=[folder_id], pageToken=page_token
            ).execute()

            messages.extend(response.get('messages', []))
            page_token = response.get('nextPageToken')

            if not page_token:
                break

        if not messages:
            log.info(f"No messages found in folder '{folder_name}'.")
            return
        return messages
    except Exception as e:
        log.error(f"{e}")
        return None


def prune_by_threadId(messages: list) -> list:
    """Prunes messages belonging to the same conversation"""
    assert len(messages) != 0
    msgs = defaultdict(list)
    for m in messages:
        msgs[m['threadId']].append(m['id'])
    messages = [{'id': v[0], 'threadId': k} for k, v in msgs.items()]
    return messages


def get_email_dates_sender(headers) -> (str, str):
    sender = "unknown_sender"
    date = "1999-01-01"
    formats = [GMAIL_DATE, GMAIL_DATE_ZONE]
    for header in headers:
        if header['name'] == "Date":
            for format in formats:
                try:
                    email_date = dt.strptime(header['value'], format)
                    date = email_date.strftime("%Y-%m-%d")
                except Exception as e:
                    continue
                if date == '1999-01-01':
                    log.warn(f"TIME FORMAT IS UNKNOWN! : {header['value']}")
        if header["name"] == "From":
            sender_email = header["value"].split(
                "<")[-1].replace(">", "").strip()
            sender = sender_email.replace(
                "@", "_at_")  # Avoid issues in filenames
    return sender, date


def process_msg_invoices(gmail, drive, messages: list, folder: str, local=False, from_label=INCOMPLETE_INVOICE, to_label=COMPLETE_INVOICE, is_debug: bool = False) -> bool:
    """Checks given messages for invoices, parses them, moves successfully found emails from FROM_LABEL to TO_LABEL, copies them and the parsed content to a drive folder, and uploads the data to a database"""
    animal_db = get_all_animals(get_login_data())
    stats = Statistics()
    stats.emails_count = len(messages)
    invoice_folder = get_drive_folder(drive, folder)
    unprocessed_folder = get_drive_folder(
        drive, 'unprocessed_invoices', parent_folder_id=invoice_folder)
    # Process each message
    success, fail = pd.DataFrame(), pd.DataFrame()
    for i, msg in enumerate(messages):
        # print(f"Message: {i:04d} / {len(messages):04d}", end='\r')
        msg_id = msg['id']
        message = gmail.users().messages().get(userId='me', id=msg_id).execute()
        payload = message.get("payload", {})
        headers = payload.get("headers", [])
        parts = payload.get("parts", [])

        # Extract sender email
        sender_email, date_str = get_email_dates_sender(headers)

        # Check for attachments
        for part in parts:
            condition = part.get(
                'filename') and 'attachmentId' in part.get('body', {})
            # Only operate on attachements
            if not (condition):
                continue
            attachment_id = part["body"]["attachmentId"]
            attachment = gmail.users().messages().attachments().get(
                userId='me', messageId=msg_id, id=attachment_id
            ).execute()

            # Construct filename with sender's email
            normalized_name = part['filename'].replace(' ', '_')
            if re.search(NON_INVOICE_REGEXES, normalized_name.lower()):
                stats.non_invoices.append(normalized_name)
                continue
            filename = f"{date_str}_{sender_email}_{
                normalized_name}"

            file_data = base64.urlsafe_b64decode(
                attachment["data"].encode("UTF-8"))
            invoice = io.BytesIO(file_data)

            output_path = unprocessed_folder
            try:
                parser = get_parser(
                    invoice, filename=filename, is_drive=True)
                parser.parse_invoice()
                filename = parser.name
                output_path = get_drive_folder(
                    drive, parser.drive_dir, invoice_folder)
                parsed_items = match_animals(parser.items, animal_db)
                success_condition = parsed_items['ANIMALCODE'] != 'ERROR_CODE'

                matched_data = parsed_items[success_condition]
                unmatched_data = parsed_items[~success_condition]
                if not matched_data.empty:
                    success = pd.concat([success, matched_data])
                if unmatched_data.empty:
                    stats.successes.append(filename)
                else:
                    fail = pd.concat([fail, unmatched_data])
                    stats.fails.append(filename)
                try:
                    gmail.users().messages().modify(
                        id=msg_id,
                        userId='me',
                        body={
                            'removeLabelIds': [from_label],
                            'addLabelIds': [to_label],
                        }
                    ).execute()
                except Exception as e:
                    log.error(f"Failed modifying the email: {e}")
            except Exception as e:
                log.error(f"{filename} - Could not be processed | {e}")
                stats.fails.append(filename)
            upload_drive(drive, invoice, filename, [
                         output_path], part['mimeType'])
    stats.upload_success = upload_dataframe_to_database(success, is_debug)
    success_file = f"{dt.now().date()}-successes.csv"
    fail_file = f"{dt.now().date()}-failures.csv"
    upload_drive(drive, success, success_file, [invoice_folder], 'text/csv')
    upload_drive(drive, fail, fail_file, [invoice_folder], 'text/csv')

    sent = stats.send_summary(gmail)
    return sent


def upload_drive(drive, file_data: Union[pd.DataFrame, io.BytesIO], file_name: str, parents: list[str], mimetype: str):
    if isinstance(file_data, pd.DataFrame):
        csv = file_data.to_csv(index=False)
        file_data = io.BytesIO(csv.encode())
    initial_check = f'name = "{file_name}" and "{
        parents[0]}" in parents and trashed = false'

    r = drive.files().list(q=initial_check, spaces='drive', fields='files(id)').execute()
    if r.get('files'):
        log.warn(f"File: '{file_name}' exits. Skipping")
        return None
    metadata = {
        'name': file_name,
        'parents': parents,
    }
    media = MediaIoBaseUpload(file_data, mimetype)
    try:
        file = drive.files().create(body=metadata, media_body=media, fields='id').execute()
    except HttpError as e:
        log.error(f"Connection Failed: {e}")
    except Exception as e:
        log.error(f"Unexpected RunTimeError: {e}")
    id = file.get('id')
    if id:
        log.info(f'{file_name} successfully added to Drive')
        return id
    else:
        log.error(f"Failed to upload {file_name}")
        return None


def get_pdfs_in_drive(drive, parent_id):
    try:
        query = f"'{parent_id}' in parents and name contains 'completed'"
        res = drive.files().list(q=query, spaces='drive', fields='files(id,name)').execute()
        incomplete_folders = res.get('files', [])

        pdf_files = []
        for folder in incomplete_folders:
            folder_id = folder['id']

            pdf_query = f"'{
                folder_id}' in parents and mimeType='application/pdf'"
            r = drive.files().list(q=pdf_query, spaces='drive',
                                   fields='files(id,name,webViewLink)').execute()
            pdfs = r.get('files', [])
            pdf_files.extend(pdfs)
        return pdf_files
    except Exception as e:
        log.error(f"Error: {e}")


def get_failed_pdfs(drive, parent_id):
    """Retrieves all .PDFs in folder that in _incomplete folders"""
    try:
        query = f"'{parent_id}' in parents and name contains '_incomplete'"
        res = drive.files().list(q=query, spaces='drive', fields='files(id,name)').execute()
        incomplete_folders = res.get('files', [])

        pdf_files = []
        for folder in incomplete_folders:
            folder_id = folder['id']

            pdf_query = f"'{
                folder_id}' in parents and mimeType='application/pdf'"
            r = drive.files().list(q=pdf_query, spaces='drive',
                                   fields='files(id,name,webViewLink)').execute()
            pdfs = r.get('files', [])
            pdf_files.extend(pdfs)
        return pdf_files
    except Exception as e:
        log.error(f"Error: {e}")


def get_failed_csv(drive, parent_id):
    try:
        query = f"'{
            parent_id}' in parents and name contains '_failures' and mimeType='text/csv'"
        res = drive.files().list(q=query, spaces='drive', fields='files(id,name)').execute()
        return res.get('files')[0]
    except Exception as e:
        print(f"Failed to grab failures, ironic huh? {e}")


def drive_file_to_bytes(drive, file_id) -> io.BytesIO:
    """Reads a PDF from drive and returns a BytesIO object for manipulation"""
    try:
        req = drive.files().get_media(fileId=file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        file.seek(0)
        return file
    except Exception as e:
        log.error(f"Error reading file: {e}")
        return None


def get_invoice_folders(drive, parent_id):
    try:
        q = f"'{
            parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
        r = drive.files().list(q=q, spaces='drive', fields='files(id,name)').execute()
        return r.get('files')
    except Exception as e:
        log.error(f"Could not retrieve drive folders: {e}")


def parse_failed_pdfs_from_drive(drive, pdf_files, animals_db: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    items = pd.DataFrame()
    if not pdf_files:
        return items
    for pdf_file in pdf_files:
        pdf = drive_file_to_bytes(drive, pdf_file['id'])
        invoice_parser = get_parser(pdf, pdf_file['name'], True)
        invoice_parser.parse()
        items = pd.concat([items, invoice_parser.items])
    items = match_animals(items, animals_db)
    items = items[items['SHELTERCODE'] == 'ERROR_CODE'].copy()

    return items


def batch_move_drive(drive, ids, old_parent_id, new_parent_id):
    batch = drive.new_batch_http_request()
    for file_id in ids:
        batch.add(drive.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=old_parent_id,
        ))
        batch.execute()


def batch_delete_drive(drive, ids):
    batch = drive.new_batch_http_request()
    for id in ids:
        batch.add(drive.files().delete(fileId=id))
    batch.execute()


def retry_failed(gmail, drive,):
    pass


class Statistics:
    upload_success = False

    def __init__(self):
        self.emails_count = 0
        self.entries = 0
        self.successes = list()
        self.fails = list()
        self.non_invoices = list()

    def summary(self) -> str:
        s = len(self.successes)
        f = len(self.fails)
        n = len(self.non_invoices)
        s_table, f_table = "", ""
        non_table = ""
        if self.successes:
            sframe = pd.DataFrame(self.successes, columns=['Successes'])
            sframe.sort_values(by='Successes', inplace=True)
            s_table = sframe.to_html(index=False)
        if self.fails:
            fframe = pd.DataFrame(self.fails, columns=[
                                  'Name Conflicts or Database Errors'])
            fframe.sort_values(
                by='Name Conflicts or Database Errors', inplace=True)
            f_table = fframe.to_html(index=False)
        if self.non_invoices:
            nonframe = pd.DataFrame(
                self.non_invoices, columns=['Non-Invoices'])
            nonframe.sort_values(by='Non-Invoices', inplace=True)
            non_table = nonframe.to_html(index=False)

        return f"""
        <h1>[Invoice Processor]</h1><br>
        <strong>Total Email Messages</strong>: {self.emails_count}<br>
        <strong>Total PDFs</strong>: {s + f} PDFs.<br>
        <strong>Successes</strong>: {s}<br>
        <strong>Failures</strong>: {f}<br>
        <strong>Non-Invoices</strong>: {n}<br>
        <br>
        <strong>Data Successfully Uploaded to ASM?<strong> {self.upload_success}<br>
        ---
        <h2> Successes </h2><br>
                {s_table}

        <h2> Failures </h2><br>
                {f_table}

        <h2> Non-Invoices </h2><br>
                {non_table}
        """

    def send_summary(self, gmail):
        msg = MIMEMultipart()
        msg['to'] = gmail.users().getProfile(
            userId='me').execute()['emailAddress']
        msg['from'] = 'me'
        success = "Success" if self.upload_success else "Failed"
        msg['subject'] = f'Invoice Processor: {
            dt.now().strftime("%Y-%m-%d")} | {success}'
        msg.attach(MIMEText(self.summary(), 'html'))
        raw = {'raw': base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')}
        try:
            gmail.users().messages().send(userId='me', body=raw).execute()
        except Exception as e:
            log.error(f"Failed to send message: {e}")
            return None
        return True
