import base64
import logging
import io
import re
import pandas as pd
from typing import Tuple, Union, Optional, Dict, List
from datetime import datetime as dt, timedelta as td
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from parsers.invoices import get_parser
from animal_db_handler import add_invoices_col, match_animals, upload_dataframe_to_database
from utils import error_logger, get_email_dates_sender, Folders, EmailLabels
from constants.regex import NON_INVOICE_REGEXES
from constants.dates import (
    GMAIL_DATE,
    GMAIL_DATE_ZONE
)





log = logging.getLogger(__name__)



class GmailService:
    def __init__(self, creds):
        self.service = build("gmail", "v1", credentials=creds)

    @error_logger()
    def get_user_email(self):
        profile = self.service.users().getProfile(userId="me").execute()
        return profile["emailAddress"]

    @error_logger()
    def get_messages(self, label_name, days_ago=None) -> list[dict]:
        labels = self.service.users().labels().list(userId="me").execute().get("labels", [])
        label_id = next((label["id"] for label in labels if label["name"] == label_name), None)
        if not label_id:
            log.warning(f"Label {label_name} not found")
            return []

        query = f"after:{(dt.now() - td(days=days_ago)).strftime('%Y/%m/%d')}" if days_ago else ""

        messages, page_token = [], None
        while True:
            resp = self.service.users().messages().list(
                userId="me", labelIds=[label_id], q=query, pageToken=page_token
            ).execute()
            messages.extend(resp.get("messages", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return messages

    @error_logger()
    def get_message(self, msg_id):
        return self.service.users().messages().get(userId="me", id=msg_id).execute()

    @error_logger()
    def get_attachment(self, msg_id, att_id):
        att = self.service.users().messages().attachments().get(
            userId="me", messageId=msg_id, id=att_id
        ).execute()
        return io.BytesIO(base64.urlsafe_b64decode(att["data"]))


    @error_logger() 
    def move_message(self, msg_id: str, from_label: str, to: str, execute: bool =False):
        body = { 'removeLabelIds': [from_label], 'addLabelIds': [to] }
        move = self.service.users().messages().modify(
            id=msg_id,
            userId='me',
            body=body
        )
        if execute:
            return move.execute()
        else:
            return move
        
    @error_logger()
    def batch_modify_labels(self, requests):
        batch = self.service.new_batch_http_request()
        for req in requests:
            batch.add(self.service.users().messages().modify(**req))
        batch.execute()

    @error_logger()
    def send_email_summary(self, summary_html: str, to_email: str) -> bool:
        msg = MIMEMultipart()
        msg['to'] = to_email
        msg['from'] = 'me'
        msg['subject'] = f"Invoice Processor: {dt.now().strftime('%Y-%m-%d')} | Summary"
        msg.attach(MIMEText(summary_html, "html"))
        raw = {"raw":base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')}
        ret = self.service.users().messages().send(userId='me', body=raw).execute()
        if ret:
            return True
        return False
        

class DriveService:
    def __init__(self, creds):
        self.service = build("drive", "v3", credentials=creds)
        self.mimetypes = {
            'folder': 'application/vnd.google-apps.folder',
            'pdf': 'application/pdf',
            'csv': 'text/csv'
        }

    @error_logger()
    def get_or_create_folder(self, name, parent_id=None):
        folder_mime = self.mimetypes['folder']
        q = f"name='{name}' and mimeType='{folder_mime}'"
        if parent_id:
            q += f" and '{parent_id}' in parents"
        results = self.service.files().list(q=q, spaces="drive").execute().get("files", [])

        if results:
            return results[0]["id"]

        metadata = {"name": name, "mimeType": folder_mime}
        if parent_id:
            metadata["parents"] = [parent_id]
        folder = self.service.files().create(body=metadata, fields="id").execute()
        return folder["id"]

    @error_logger()
    def move_file(self, id: str, old_parents: List[str], new_parents: List[str], new_name: Optional[str] = None, execute=False) -> Optional[str]:
        body = None
        if new_name:
            body = {"name" : new_name}
        query = self.service.files().update(
            fileId=id,
            body=body,
            removeParents=','.join(old_parents),
            addParents=','.join(new_parents)
        )
        if execute:
            return query.execute()
        return query
        


    @error_logger()
    def upload_file(self, name: str, data: Union[io.BytesIO, pd.DataFrame], mime_type: str, parents: List[str]):
        if isinstance(data, pd.DataFrame):
            data = io.BytesIO(data.to_csv(index=False).encode())
        metadata = {"name": name, "parents": parents}
        mime = self.mimetypes.get(mime_type, mime_type)
        media = MediaIoBaseUpload(data, mime)
        file = self.service.files().create(body=metadata, media_body=media, fields="id").execute()
        return file["id"]

    @error_logger()
    def update_csv_file(self, file_id: str, new_data: Union[io.BufferedReader,pd.DataFrame], new_name : Optional[str]):
        mime = self.mimetypes['csv']
        if isinstance(new_data, io.BufferedReader):
            new = pd.read_csv(new_data)
        elif isinstance(new_data, pd.DataFrame):
            new = new_data
        else:
            raise Exception("Only accepts Bytes or pd.Dataframe objects!")
        old_data = self.download_file(file_id).getvalue().decode("utf-8")
        df = pd.read_csv(io.StringIO(old_data))
        combined = pd.concat([df, new], ignore_index=True)
        buffer = io.BytesIO(combined.to_csv(index=False).encode())
        media = MediaIoBaseUpload(buffer, mime)

        updates = {}
        if new_name:
            updates['name'] = new_name
        return self.service.files().update(
            fileId=file_id,
            body=updates if updates else None,
            media_body=media, fields="id"
        ).execute().get("id")

    @error_logger()
    def download_file(self, file_id):
        request = self.service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)
        return buffer

    @error_logger()
    def list_files(self, query: str, fields: str= "files(id,name,webViewLink,mimeType)"):
        page_token, files = None, []
        while True:
            resp = self.service.files().list(
                q = query,
                spaces = "drive",
                fields=f'nextPageToken,{fields}',
                pageToken=page_token,
            ).execute()
            files.extend(resp.get('files', []))
            page_token = resp.get('nextPageToken')
            if not page_token:
                break
        return files

    @error_logger()
    def list_files_in_folder(self, parent_id, name_contains: Optional[str] = None, mime_type: Optional[str] = None):
        q_parts = [f"'{parent_id}' in parents"]
        if mime_type:
            mime = self.mimetypes.get(mime_type, mime_type)
            q_parts.append(f"mimeType='{mime}'")
        if name_contains:
            q_parts.append(f"name contains '{name_contains}'")

        query = " and ".join(q_parts)
        return self.list_files(query=query)

    @error_logger()
    def get_folders(self, parent_id, name_contains: Optional[str] = None):
        return self.list_files_in_folder(parent_id, mime_type='folder', name_contains=name_contains)

    @error_logger()
    def get_failed_pdfs(
            self,
            parent_id: str,
            incomplete_folder_suffix: str = "_incomplete",
    ):
        return self.get_all_files_in_matching_folder(parent_id, incomplete_folder_suffix, 'pdf')

    @error_logger()
    def get_csv(self, parent_id, name_contains: str) -> List[Dict]:
        return self.list_files_in_folder(parent_id, name_contains, 'csv')

    
    @error_logger()
    def get_all_files_in_matching_folder(
            self,
            parent_id: str,
            folder_suffix: str,
            mime_type: str,
    ) -> List[Dict]:

        folders = self.get_folders(parent_id, name_contains=folder_suffix)
        files = []
        for folder in folders:
            folder_id = folder['id']
            files_in_folder = self.list_files_in_folder(folder_id, mime_type)
            files.extend(files_in_folder)
        return files

    @error_logger(reraise=True)
    def get_all_failed_invoice_data(self, parent_id) -> Tuple[pd.DataFrame, pd.DataFrame]:
        failed_pdfs = self.get_all_files_in_matching_folder(parent_id, '_incomplete', 'pdf')
        failed_csvs = self.list_files_in_folder(parent_id, '_failures', 'csv' )
        assert len(failed_csvs) == 1, "Too many failure CSVs in the [Invoices] Folder"
        failure_csv = failed_csvs.pop()
        csv_bytes = self.download_file(failure_csv.get('id'))
        csv_string = csv_bytes.getvalue().decode('utf-8')
        df_csv = pd.read_csv(io.StringIO(csv_string))
        df_pdf = pd.DataFrame(failed_pdfs)
        df_csv, df_pdf = add_invoices_col(df_csv, df_pdf)
        return df_csv, df_pdf




class Statistics:
    upload_success = False

    def __init__(self, emails_count: int) -> None:
        self.emails_count = emails_count
        self.entries = 0
        self.success_list: List[Union[pd.DataFrame,pd.Series]] = []
        self.failure_list: List[Union[pd.DataFrame,pd.Series]] = []
        self.successful_names = []
        self.failure_names = []
        self.non_invoices = []

    def summary(self) -> str:
        s = len(self.successful_names)
        f = len(self.failure_names)
        n = len(self.non_invoices)
        s_table, f_table = "", ""
        non_table = ""
        if self.successful_names:
            sframe = pd.DataFrame(self.successful_names, columns=["Successes"])
            sframe = sframe.sort_values(by="Successes")
            s_table = sframe.to_html(index=False)
        if self.failure_names:
            fframe = pd.DataFrame(
                self.failure_names, columns=["Name Conflicts or Database Errors"],
            )
            fframe = fframe.sort_values(by="Name Conflicts or Database Errors")
            f_table = fframe.to_html(index=False)
        if self.non_invoices:
            nonframe = pd.DataFrame(self.non_invoices, columns=["Non-Invoices"])
            nonframe = nonframe.sort_values(by="Non-Invoices")
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

    def send_summary(self, gmail) -> bool:
        return gmail.send_email_summary(self.summary(), gmail.get_user_email())

"""
So we need this function to:
    - Loop through emails
    - Find attachements
    - Skip bad attachments
    - Process attachment
"""
class Processor:
    def __init__(self, creds):
        self.drive = DriveService(creds)
        self.gmail = GmailService(creds)


    @error_logger()
    def process_invoices(
        self,
        messages: list[dict],
        folder_ids: Folders,
        email_labels: EmailLabels,
        animals: pd.DataFrame,
    ) -> bool:
        """Processes invoice attachments from Gmail messages, uploads them to Drive,
        and updates Gmail labels.
        """
        stats = Statistics(emails_count=len(messages))

        batch_gmail = self.gmail.service.new_batch_http_request()

        for message in messages:
            self.process_message(
                message=message,
                stats=stats,
                folder_ids=folder_ids,
                labels=email_labels,
                animals=animals,
                batch_gmail=batch_gmail,
            )

        timestamp = dt.now().strftime("%Y-%m-%d-%H:%M:%S")

        if stats.success_list:
            success_df = pd.concat(stats.success_list)
            self._update_csv_report(
                df=success_df,
                folder_id=folder_ids.invoice,
                name_contains='successes',
                timestamp=timestamp,
            )
            stats.upload_success = upload_dataframe_to_database(success_df)

        if stats.failure_list:
            fail_df = pd.concat(stats.failure_list)
            self._update_csv_report(
                df=fail_df,
                folder_id=folder_ids.invoice,
                name_contains='failures',
                timestamp=timestamp,
            )
        batch_gmail.execute()

        return self.gmail.send_email_summary(stats.summary(), self.gmail.get_user_email())


    def process_message(self, message: Dict, stats: Statistics, folder_ids: Folders, labels: EmailLabels, animals: pd.DataFrame,  batch_gmail):
        msg_id = message.get("id")
        add_to_gmail = False
        try:
            msg = self.gmail.get_message(msg_id)
            headers = msg.get("payload", {}).get("headers", [])
            sender_email, date_str = get_email_dates_sender(headers, [GMAIL_DATE, GMAIL_DATE_ZONE])
        except Exception as e:
            log.exception(f"Error processing email_id={msg_id}: {e}")
            return None
        attachments = [
            p
            for p in msg.get("payload", {}).get("parts", [])
            if p.get("filename") and "attachmentId" in p.get("body", {})
        ]

        for attachment in attachments:
            normalized_name = attachment["filename"].replace(" ", "_")
            ext = '.pdf' if not normalized_name.endswith('.pdf') else ''
            filename = f"{date_str}_{sender_email}_{normalized_name}{ext}"

            if re.search(NON_INVOICE_REGEXES, filename.lower()):
                stats.non_invoices.append(filename)
                continue

            attachment_data = self.gmail.get_attachment(msg_id, attachment["body"]["attachmentId"])
            try:
                drive_folder_name, add_to_gmail = self.process_invoiced_attachment(
                    filename=filename,
                    filedata=attachment_data,
                    stats=stats,
                    animals=animals
                )
                drive_folder_id = self.drive.get_or_create_folder(
                    name=drive_folder_name,
                    parent_id=folder_ids.invoice
                )
                if add_to_gmail:
                    batch_gmail.add(self.gmail.move_message(
                        msg_id = msg_id,
                        from_label=labels.from_label,
                        to = labels.to_label
                    ))



                self.drive.upload_file(
                    name=filename, data=attachment_data,
                    parents=[drive_folder_id],
                    mime_type=attachment['mimeType']
                    )
            except Exception as e:
                log.exception(f"{filename} with msg_id={msg_id} could not process: {e}")
                self.drive.upload_file(name=filename, data=attachment_data, parents=[folder_ids.unprocessed], mime_type=attachment["mimeType"])


    def _update_csv_report(self, df: pd.DataFrame, folder_id:str, name_contains: str, timestamp:str,):
        csv_files = self.drive.get_csv(folder_id, name_contains=name_contains)
        assert len(csv_files) == 1, "Too many csvs to return"
        csv_file = csv_files.pop()
        file_id = self.drive.update_csv_file(
            file_id = csv_file.get('id'),
            new_data=df,
            new_name=f"{timestamp}_{name_contains}.csv"
        )
        if not file_id:
            log.error(f"Couldn't update {name_contains} CSV: {timestamp}")

    @staticmethod
    def process_invoiced_attachment(
        filename: str,
        filedata: io.BytesIO,
        stats : Statistics,
        animals: pd.DataFrame
    ) -> Tuple[str, bool]:
        ## Return Values ##
        add_to_gmail_batch = False
        output_path = None
        parser = get_parser(filedata, filename, True)
        parser.parse_invoice()
        parsed_items = match_animals(parser.items, animals)
        success_condition = parsed_items['ANIMALCODE'] != 'ERROR_CODE'

        if not parsed_items.empty:
            stats.success_list.append(parsed_items[success_condition])
            stats.failure_list.append(parsed_items[~success_condition])

        # If there are NO failed items -- adjust output
        if parsed_items[~success_condition].empty:
            stats.successful_names.append(filename)
            add_to_gmail_batch = True
            output_path = parser.drive_completed
        else:
            stats.failure_names.append(filename)
            output_path = parser.drive_incomplete
        return output_path, add_to_gmail_batch



