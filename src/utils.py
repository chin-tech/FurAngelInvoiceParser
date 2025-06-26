import functools
import logging
from collections import defaultdict
from datetime import datetime as dt 
from typing import NamedTuple, Optional
from constants.project import (
    GMAIL_INVOICE_LABEL,
    GMAIL_TO_LABEL,
    GMAIL_FROM_LABEL,
    DRIVE_INVOICES_FOLDER,
)
from constants.database import DB_LOGIN_DATA
from animal_db_handler import get_all_animals

log = logging.getLogger(__name__)


def error_logger(default=None, reraise=False, context=""):
    """
    Decorator to catch, log and optionally supress exceptions
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.exception(f"{context or func.__name__} Failed {e}")
                if reraise:
                    raise
                return default
        return wrapper
    return decorator



def prune_by_threadId(messages: list[dict]) -> list[dict]:
    """Prunes messages belonging to the same conversation."""
    assert messages is not None
    assert len(messages) != 0
    msgs = defaultdict(list)
    for m in messages:
        msgs[m["threadId"]].append(m["id"])
    return [{"id": v[0], "threadId": k} for k, v in msgs.items()]


def get_email_dates_sender(headers, date_formats: list[str]) -> tuple[str, str]:
    sender = "unknown_sender"
    date = "1999-01-01"
    # formats = [GMAIL_DATE, GMAIL_DATE_ZONE]
    for header in headers:
        if header["name"] == "Date":
            for format in date_formats:
                try:
                    email_date = dt.strptime(header["value"], format)
                    date = email_date.strftime("%Y-%m-%d")
                except Exception:
                    continue
                if date == "1999-01-01":
                    log.warning(f"TIME FORMAT IS UNKNOWN! : {header['value']}")
        if header["name"] == "From":
            sender_email = header["value"].split("<")[-1].replace(">", "").strip()
            sender = sender_email.replace("@", "_at_")  # Avoid issues in filenames
    return sender, date



class Folders(NamedTuple):
    invoice: str
    unprocessed: str

class EmailLabels(NamedTuple):
    from_label: str
    to_label: str



def process_invoices(processor, days_ago: Optional[int] = None) -> bool:
    messages = prune_by_threadId(processor.gmail.get_messages(GMAIL_INVOICE_LABEL, days_ago))
    invoice_folder_id = processor.drive.get_or_create_folder(DRIVE_INVOICES_FOLDER)
    unproccessed_folder_id = processor.drive.get_or_create_folder(
            'unprocessed_invoices', invoice_folder_id
        )
    folder_ids = Folders(invoice_folder_id, unproccessed_folder_id)
    email_labels = EmailLabels(GMAIL_FROM_LABEL, GMAIL_TO_LABEL)
    animals = get_all_animals(DB_LOGIN_DATA)
    if not messages:
        log.info(f"No messages in folder! {GMAIL_INVOICE_LABEL} ")
        return "No messages in specified folder!", 404
    log.info(f"Starting processing of {len(messages)}")
    return processor.process_invoices(
        messages=messages,
        folder_ids=folder_ids,
        email_labels=email_labels,
        animals=animals
    )
