import pandas as pd
import re
import io
import logging
import base64
import pickle
import os
from flask import session, redirect
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
from animal_getter import upload_dataframe_to_database
from animal_getter import get_all_animals, match_animals, add_invoices_col
from constants import SVC_ACCOUNT, SCOPES
from constants import get_login_data
from datetime import datetime as dt
from datetime import timedelta as td
from typing import Union

log = logging.getLogger(__name__)

# GMAIL DATE FORMAT
GMAIL_DATE = "%a, %d %b %Y %H:%M:%S %z"
GMAIL_DATE_ZONE = "%a, %d %b %Y %H:%M:%S %z (%Z)"

NON_INVOICE_REGEXES = r"statement|treatment|estimate|record|payment"


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


class Google:

    def __init__(self):
        self.creds = None
        self.gmail = None
        self.drive = None

    def init_from_web(self, config: dict, redirect_url: str = None) -> Credentials:
        """ Initializes credentials via a web request to authenticate the user"""
        flow = InstalledAppFlow.from_client_config(
            config, SCOPES
        )
        if redirect:
            flow.redirect_uri = redirect_url

        auth_url, state = flow.authorization_url(
            access_type="offline",
            include_granted_scopes="true",
        )
        session['state'] = state
        return redirect(auth_url)

    def init_from_token(self, config: str, token_file: str, secure: bool = True) -> Credentials:
        """ Initialize credentials from token file. Used for debugging purposes."""
        creds = None
        if token_file:
            if os.path.exists(token_file):
                with open(token_file, 'rb') as tk:
                    creds = pickle.load(tk)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_config(config, scopes=SCOPES)
            creds = flow.run_local_server(port=0)

        if not secure:
            with open(token_file, 'wb') as tk:
                pickle.dump(creds, tk)
        assert creds != None
        self.creds = creds

    def init_from_secret(self, project_id: str, secret_name: str) -> Credentials:
        creds = get_secret(secret_name, project_id)

        if not creds.valid or creds.expired:
            creds.refresh(Request())
            if creds.refresh_token:
                update_secret(secret_name, project_id, creds)
        self.creds = creds

    def set_services(self):
        assert self.creds != None
        self.gmail = build('gmail', 'v1', credentials=self.creds)
        self.drive = build('drive', 'v3', credentials=self.creds)

    def email_matches(self, email: str) -> bool:
        current = self.gmail.users().getProfile(
            userId='me').execute()['emailAddress']
        return current == email

    def get_drive_folder(self, folder_name, parent_folder_id=None) -> str:
        """Returns drive folder ID; creates if it doesn't exist, adds to parent if provided."""
        mime_type = 'application/vnd.google-apps.folder'
        query = f"name='{folder_name}' and mimeType='{mime_type}'"

        if parent_folder_id:
            # Add parent folder to query
            query += f" and '{parent_folder_id}' in parents"

        res = self.drive.files().list(q=query, spaces='drive').execute()
        files = res.get('files', [])

        if not files:
            file_metadata = {
                'name': folder_name,
                'mimeType': mime_type,
            }

            if parent_folder_id:
                # Add parent folder to metadata
                file_metadata['parents'] = [parent_folder_id]

            file = self.drive.files().create(body=file_metadata, fields='id').execute()
            log.info(f"{folder_name}: Created with ID: {file.get('id')}")
            return file.get('id')

        # Ensure parent is set if it was provided
        files_parents = files[0].get('parents', [])
        if parent_folder_id and not any(parent_folder_id in parents for parents in files_parents):
            file_metadata = {
                'addParents': [parent_folder_id]
            }
            file = self.drive.files().update(fileId=files[0].get(
                'id'), body=file_metadata, fields='id').execute()
            log.info(f"{folder_name}: Updated to add parent ID: {
                     parent_folder_id}")

        return files[0].get('id')

    def update_csv_in_drive(self, file_id: str, file_data: pd.DataFrame, file_name: str, parents: list[str],  mimetype: str) -> str:
        """Updates drive file with specified ID with the provided arguments """
        old_data = self.drive_file_to_bytes(file_id)
        old_data.seek(0)
        old_bytes = old_data.getvalue().decode('utf-8')
        df = pd.read_csv(io.StringIO(old_bytes))
        df = pd.concat([df, file_data], ignore_index=True)
        csv = df.to_csv(index=False)
        file_data = io.BytesIO(csv.encode())

        metadata = {
            'name': file_name,
            'parents': parents,
        }
        media = MediaIoBaseUpload(file_data, mimetype)
        try:
            file = self.drive.files().update(
                body=metadata, media_body=media, fields='id').execute()
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

    def upload_drive(self, file_data: Union[pd.DataFrame, io.BytesIO], file_name: str, parents: list[str], mimetype: str) -> str:
        """Uploads file to google drive and returns file id"""
        drive = self.drive
        if isinstance(file_data, pd.DataFrame):
            csv = file_data.to_csv(index=False)
            file_data = io.BytesIO(csv.encode())
        initial_check = f'name = "{file_name}" and "{
            parents[0]}" in parents and trashed = false'

        r = drive.files().list(q=initial_check, spaces='drive', fields='files(id)').execute()
        if r.get('files'):
            log.warn(f"File: '{file_name}' exsist. Skipping")
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

    def get_csv(self, file_contains_string: str, parent_id: str) -> dict:
        q = f"'{parent_id}' in parents and name contains '{
            file_contains_string}' and mimeType='text/csv'"
        try:
            r = self.drive.files().list(q=q, spaces='drive', fields='files(id,name)').execute()
            files = r.get('files')
            if isinstance(files, list):
                return files[0]
            if isinstance(files, dict):
                return files
            return None
        except Exception as e:
            print(f"failure with csv: {e}")
            raise ValueError("Couldn't find CSV")

    def get_failed_pdfs(self, parent_id) -> list[dict]:
        """Retrieves all .PDFs in folder that in _incomplete folders"""
        drive = self.drive
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
            assert True == False

    def get_failures_csv(self, parent_id) -> Union[list[dict], None]:
        """Gets all csvs with _failures in the name in the appropriate folder"""
        try:
            query = f"'{
                parent_id}' in parents and name contains '_failures' and mimeType='text/csv'"
            res = self.drive.files().list(q=query, spaces='drive',
                                          fields='files(id,name)').execute()
            files = res.get('files')
            if len(files) == 0:
                raise ValueError(f"query returned empty:\n{query}")
            if len(files) > 1:
                files.sort(key=lambda x: x.get('name'), reverse=True)
            return files
        except Exception as e:
            print(f"Failed to grab failures, ironic huh? {e}")
            raise e

    def drive_file_to_bytes(self, file_id) -> Union[io.BytesIO, None]:
        """Reads a file from drive and returns a BytesIO object for manipulation"""
        drive = self.drive
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

    def get_invoice_folders(self, parent_id):
        drive = self.drive
        try:
            q = f"'{
                parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
            r = drive.files().list(q=q, spaces='drive', fields='files(id,name)').execute()
            return r.get('files')
        except Exception as e:
            log.error(f"Could not retrieve drive folders: {e}")

    def get_failed_invoice_data(self, parent_folder) -> (pd.DataFrame, pd.DataFrame):
        """Returns the failed invoice data from the parent folder. CSV data and PDF listing"""
        pdfs = pd.DataFrame(self.get_failed_pdfs(parent_folder))
        failed_invoices = self.get_failures_csv(parent_folder)
        if not failed_invoices:
            raise ValueError("Failures_CSV is not found")

        failed_invoice = failed_invoices[0]
        file_csv_bytes = self.drive_file_to_bytes(
            failed_invoice.get('id')
        )
        file_csv_bytes.seek(0)
        csv_string = file_csv_bytes.getvalue().decode('utf-8')

        data = pd.read_csv(io.StringIO(csv_string))
        data, pdfs = add_invoices_col(data, pdfs)

        return data, pdfs

    def get_messages_from(self, folder_name, days_ago: int = None) -> list[dict]:
        """Returns all messages in given folder for further processing"""
        gmail = self.gmail
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

    def process_invoices(self, messages: list[dict], folder_name: str, from_label: str, to_label: str, debugging: bool = False) -> bool:
        animals = get_all_animals(get_login_data())
        stats = Statistics(emails_count=len(messages))

        invoice_folder = self.get_drive_folder(folder_name)
        unprocessed_folder = self.get_drive_folder(
            'unprocessed_invoices', invoice_folder)

        success_list, fail_list = [], []
        batch_gmail = self.gmail.new_batch_http_request()
        for message in messages:
            msg_id = message['id']
            try:
                msg = self.gmail.users().messages().get(userId='me', id=msg_id).execute()
                headers = msg.get('payload', {}).get('headers', [])
                sender_email, date_str = get_email_dates_sender(headers)
            except Exception as e:
                log.error(f"Error processing email_id={msg_id}: {e}")
                continue

            attachments = [
                p for p in msg.get('payload', {}).get('parts', [])
                if p.get('filename') and 'attachmentId' in p.get('body', {})
            ]

            for p in attachments:
                normalized_name = p['filename'].replace(' ', '_')
                filename = f"{date_str}_{
                    sender_email}_{normalized_name}.pdf"
                if re.search(NON_INVOICE_REGEXES, filename.lower()):
                    stats.non_invoices.append(filename)
                    continue
                attachment = self.gmail.users().messages().attachments().get(
                    userId='me', messageId=msg_id, id=p['body']['attachmentId']
                ).execute()

                file = io.BytesIO(base64.urlsafe_b64decode(
                    attachment['data'].encode('UTF-8')))
                try:
                    parser = get_parser(
                        file, filename=filename, is_drive=True)
                    parser.parse_invoice()
                    filename = parser.name

                    parsed_items = match_animals(parser.items, animals)
                    success_condition = parsed_items['ANIMALCODE'] != 'ERROR_CODE'
                    if not parsed_items.empty:
                        success_list.append(
                            parsed_items[success_condition])
                        fail_list.append(parsed_items[~success_condition])

                    if parsed_items[~success_condition].empty:
                        output_path = self.get_drive_folder(
                            parser.drive_completed, invoice_folder)
                        stats.successes.append(filename)
                        batch_gmail.add(
                            self.gmail.users().messages().modify(
                                id=msg_id,
                                userId='me',
                                body={
                                    'removeLabelIds': [from_label],
                                    'addLabelIds': [to_label],
                                }
                            )
                        )
                    else:
                        stats.fails.append(filename)
                        output_path = self.get_drive_folder(
                            parser.drive_incomplete, invoice_folder)

                    if not debugging:
                        self.upload_drive(
                            file, filename, [output_path], p['mimeType'])
                except Exception as e:
                    log.error(f"{filename} with {
                              msg_id} could not process: {e}")
                    stats.fails.append(filename)
                    if not debugging:
                        self.upload_drive(
                            file, filename, [unprocessed_folder], p['mimeType'])

        timestamp = dt.now().strftime("%Y-%m-%d-%H:%M:%S")
        if success_list:
            success_df = pd.concat(success_list)
            print('---Successes----')
            print(success_df)
            if not debugging:
                stats.upload_success = upload_dataframe_to_database(success_df)
                success_csv = self.get_csv('successes', invoice_folder)
                self.update_csv_in_drive(
                    success_csv.get('id'), success_df, f'{timestamp}_successes.csv', [
                        invoice_folder], 'text/csv'
                )

        if fail_list:
            fail_df = pd.concat(fail_list)
            print('---Failures----')
            print(fail_df)
            if not debugging:
                failures_csv = self.get_csv('failures', invoice_folder)
                self.update_csv_in_drive(
                    failures_csv.get('id'), fail_df, f'{timestamp}_failures.csv', [
                        invoice_folder], 'text/csv'
                )
                self.upload_drive(
                    fail_df, f'{timestamp}_failures.csv', [invoice_folder], 'text/csv')

        if not debugging:
            batch_gmail.execute()

        return stats.send_summary(self.gmail)


def prune_by_threadId(messages: list[dict]) -> list[dict]:
    """Prunes messages belonging to the same conversation"""
    assert messages != None
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


class Statistics:
    upload_success = False

    def __init__(self, emails_count: int):
        self.emails_count = emails_count
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
