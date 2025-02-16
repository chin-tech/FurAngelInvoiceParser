import re
import pandas as pd
import logging
import io
from typing import Union
from typing import Protocol
from pypdf import PdfReader
from pathlib import Path
from parsers import Cost, Test, Medication, Vaccine
from constants import DATE_MDY, DATE_M_D_Y, DATE_MDYYYY, INVOICE_DIR, BAD_INVOICE_PATH, DATA_DIR, PROCEDURE_MAP, Regex
from datetime import datetime as dt

DATE_FORMATS = [DATE_MDY, DATE_M_D_Y, DATE_MDYYYY]
log = logging.getLogger(__name__)
current_invoice = ""


def get_description(option: str, cost_dict: dict, date: dt) -> dict:
    # option = match.group(1)
    medication = Medication()
    medical_test = Test()
    vaccination = Vaccine()
    date_string = date.strftime(DATE_M_D_Y)
    for pattern, (cost_type, fields) in PROCEDURE_MAP.items():
        matched = re.search(pattern, option)
        if matched:
            cost_dict['COSTTYPE'] = cost_type
            if cost_dict.get('COSTDESCRIPTION'):
                cost_dict['COSTDESCRIPTION'] += option
            else:
                cost_dict['COSTDESCRIPTION'] = option
            if not fields:
                return cost_dict
            for field in fields:
                if 'DATE' in field:
                    cost_dict[field] = date_string
                if 'COMMENT' in field:
                    cost_dict[field] = option
                if 'TYPE' in field:
                    cost_dict[field] = medical_test.parse(
                        option) if 'TEST' in field else vaccination.parse(option)
                if 'NAME' in field:
                    cost_dict[field] = medication.parse(option)
                if 'DOSAGE' in field:
                    cost_dict[field] = matched.group(1)
            return cost_dict
    cost_dict["COSTTYPE"] = Cost.OTHER
    cost_dict["COSTDESCRIPTION"] += f"{option}"
    return cost_dict


class InvoiceParser(Protocol):
    """Creates an InvoiceParser that accepts the text from an invoice"""
    clinic = ""
    clinic_abrv = ""
    invoice_pattern = r"Invoice:\s*?(\d+)"
    invoice_date_pattern = r"Printed:\s*?(\d{2}-\d{2}-\d{2})"
    dog_name_pattern = r"^\d{2}-\d{2}-d{2}\s+([A-Z].+?)  \s+?\d"
    charges_dog_pattern = ""
    price_pattern = r"(\d+\.\d{2})"
    charges_pattern = ""
    charges_date_pattern = ""
    itemized_begin_pattern = r"^\s+(Description.*)"
    itemized_end_pattern = "Patient Subtotal:"
    section_reduce_pattern = ""
    invoice_date_format = DATE_MDY
    charge_date_format = DATE_MDY

    def __init__(self, txt: str, invoice_path: Path, is_drive: bool = False):
        self.text = txt
        self.invoice = invoice_path
        self.name = invoice_path.name
        self.good = pd.DataFrame()
        self.bad = pd.DataFrame()
        self.success_dir = INVOICE_DIR / Path(self.clinic_abrv)
        self.fail_dir = INVOICE_DIR / Path(f"{self.clinic_abrv}_incomplete")
        self.drive_completed = f"{self.clinic_abrv}_completed"
        self.drive_incomplete = f"{self.clinic_abrv}_incomplete"

        if not is_drive:
            self.success_dir.mkdir(exist_ok=True)
            self.fail_dir.mkdir(exist_ok=True)

    def get_itemized_section(self) -> list[str]:
        sections = list()
        for match in re.finditer(self.itemized_begin_pattern, self.text, re.M):
            start_text = self.text[match.start():]
            end_index = start_text.find(self.itemized_end_pattern)
            new_text = start_text[:end_index]
            # line_reduce = r"\n(?=\S)"
            if self.section_reduce_pattern:
                while re.search(self.section_reduce_pattern, new_text, re.M):
                    new_text = re.sub(
                        self.section_reduce_pattern, " ", new_text, re.M)
            sections.append(new_text)
        return sections

    def get_dog_names(self) -> list[str]:
        names = re.findall(self.dog_name_pattern, self.text, re.M)
        if not names:
            raise ValueError(
                f"{self.name}: {self.clinic_abrv} | No Dog Names!")
        return names

    def get_invoice_id(self) -> str:
        match = re.search(self.invoice_pattern, self.text, re.M)
        if not match:
            raise ValueError(f"{self.name}: Unable to parse invoice ID")
        return match.group(1)

    def get_invoiced_date(self, date_formats: list[str] = DATE_FORMATS) -> dt:
        match = re.search(self.invoice_date_pattern, self.text, re.M)
        if not match:
            raise ValueError(f"{self.name}: Invoice has no match with given regex: {
                             self.invoice_date_pattern}")
        for format in date_formats:
            try:
                date = dt.strptime(match.group(1), format)
                return date
            except Exception:
                continue
        raise ValueError(f"{self.name}: Found {match.group(
            1)}: Couldn't parse with {date_formats}")

    def get_date(self, txt: str, date_formats: list[str] = DATE_FORMATS) -> dt:
        match = re.search(self.charges_date_pattern, txt)
        if not match:
            return None
        for format in date_formats:
            try:
                date = dt.strptime(match.group(1), format)
                return date
            except Exception:
                continue
        raise ValueError(f"{self.name}: [get_date()] Found: {
                         match.group(1)} Couldn't parse with {date_formats}")
        ...

    def get_price(self, txt: str) -> float:
        match = re.findall(self.price_pattern, txt)
        if not match:
            return 0.00
        return float(match[-1])

    def get_charge(self, txt: str) -> str:
        match = re.search(self.charges_pattern, txt)
        if not match:
            return None
        return match.group(1)

    def get_animal_name_charge(self, txt: str, curr_name: str) -> str:
        if self.charges_dog_pattern:
            match = re.search(self.charges_date_pattern, txt)
            if not match:
                return curr_name
            else:
                return match.group(1)
        return curr_name

    def finish(self, filename: str, items: dict):
        self.items = pd.DataFrame(items)
        self.name = filename
        if not self.bad.empty:
            self.local_dir = self.fail_dir
            self.drive_dir = self.drive_incomplete
        else:
            self.local_dir = self.success_dir
            self.drive_dir = self.drive_completed

    def parse_item(self, item: str, id: str, dog_name: str) -> dict:
        item = item.lower()
        charges = {}
        self.charge_date = self.get_date(
            item) if self.get_date(item) else self.charge_date
        date = self.charge_date
        price = self.get_price(item)
        charge = self.get_charge(item)
        if not charge and price <= 0:
            return None
        charges['COSTDATE'] = date.strftime(DATE_M_D_Y)
        charges['COSTDESCRIPTION'] = f"[{self.clinic} - {id} - {date.date()}] "
        charges['COSTAMOUNT'] = price
        charges['ANIMALNAME'] = self.get_animal_name_charge(item, dog_name)
        return get_description(charge, charges, date)

    def parse_invoice(self) -> None:
        """Parse the self.text of the InvoiceParser. Sets the self.name, self.good, self.bad and self.local_dir"""
        items = []
        dog_names = self.get_dog_names()
        invoice_id = self.get_invoice_id()
        invoice_datetime = self.get_invoiced_date()
        sections = self.get_itemized_section()
        new_name = f"{self.clinic_abrv}_{
            invoice_id}_{invoice_datetime.date()}.pdf"
        self.charge_date = invoice_datetime
        for index, section in enumerate(sections):
            dog_name = dog_names[index]
            for i, lines in enumerate(section.splitlines()):
                if i == 0 or len(lines) < 60:
                    continue
                charges = self.parse_item(
                    lines, invoice_id, dog_name)
                if not charges:
                    continue
                items.append(charges)
        self.finish(new_name, items)


class WaipioParser(InvoiceParser):
    clinic = "Waipio Pet Clinic"
    clinic_abrv = "WPC"
    charges_date_pattern = r"^(\d{2}-\d{2}-\d{2})"
    name_pattern = r"\d{2}-\d{2}-\d{2} ([a-z].+?) +\d{1,2}"
    charges_pattern = r"\s{2,}(?:\d+\.\d{1,2}|\d+)\s+?(\w.*?)\*"
    dog_name_pattern = r"\d{2}-\d{2}-\d{2}\s+?([A-Z].*?)\s{2,}\d"
    itemized_begin_pattern = r"^\s+(Date.*)"
    itemized_end_pattern = r"payment"


class VCAParser(InvoiceParser):
    clinic = "Veterinary Centers of America"
    clinic_abrv = "VCA"
    dog_name_pattern = r"^ (.*) \(\#\d+\)"
    price_pattern = r"\$(\d+\.\d+)"
    charges_pattern = r"(?:^\s{1}|\d{1,2}\/\d{1,2}\/\d{4}\s+)?(\w.*?) \$"

    invoice_pattern = r"Invoice:\s*?(\d+)"
    invoice_date_pattern = r"\| Date: (\d{1,2}/\d{1,2}/\d{1,4})"
    # dog_name_pattern = r"^\d{2}-\d{2}-d{2}\s+([A-Z].+?)  \s+?\d"
    price_pattern = r"(\d+\.\d{2})"
    charges_date_pattern = r"(\d{1,2}\/\d{1,2}\/\d{2,4})"
    itemized_begin_pattern = r"^\s+(Date.*)"
    itemized_end_pattern = "Subtotal:"
    section_reduce_pattern = r"\n(?=\S)"


class AnimalHouseVetParser(InvoiceParser):
    clinic = "Animal House Veterinary Center"
    clinic_abrv = "AHVC"
    invoice_pattern = r"Invoice #:\s+?(\d+)"
    invoice_date_pattern = r"\s{2,} Date:\s+?(\d{1,2}/\d{1,2}/\d{1,4})"
    dog_name_pattern = r"Patient Name: (.+?)  +?"
    price_pattern = r"\$(\d+\.\d+)"
    charges_date_pattern = r"(\d{1,2}\/\d{1,2}\/\d{4})"
    charges_pattern = r" \s+(\S.+?  )\s+\S{1,2}"
    itemized_begin_pattern = r"^\s+(Description.*)"
    itemized_end_pattern = "Patient Subtotal:"


class WahiawaParser(InvoiceParser):
    clinic = "Wahiawa Pet Hospital"
    clinic_abrv = "WPH"
    charges_date_pattern = r"^(\d{2}-\d{2}-\d{2})"
    name_pattern = r"\d{2}-\d{2}-\d{2} ([a-z].+?) +\d{1,2}"
    charges_pattern = r"\s{2,}(?:\d+\.\d{1,2}|\d+)\s+?(\w.*?)\*"
    dog_name_pattern = r"\d{2}-\d{2}-\d{2}\s+?([A-Z].*?)\s{2,}\d"
    itemized_begin_pattern = r"^\s+(Date.*)"
    itemized_end_pattern = r"payment"


class MMVCParser(InvoiceParser):
    clinic = "Mililani Mauka Veterinary Clinic"
    clinic_abrv = "MMVC"
    invoice_pattern = r"Invoice #:\s+?(\d+)"
    invoice_date_pattern = r"Invoice date:\s+?(\d{1,2}-\d{1,2}-\d{1,4})"
    dog_name_pattern = r"Animal Name:\s+(.+?)\s{2,}"
    price_pattern = r"\$(\d+\.\d+)"
    charges_date_pattern = r"(\d{1,2}\/\d{1,2}\/\d{4})"
    charges_pattern = r" \s+(\S.+?  )\s+\S{1,2}"
    itemized_begin_pattern = r"\s+(Qty.*)"
    itemized_end_pattern = "Subtotal:"


# class AlohaAffordableParser(InvoiceParser):
#     clinic = "Aloha Affordable"
#     clinic_abrv = "AAVC"
#     invoice_pattern = r"^\d{2}-\d{2}-\d{4}\s+(\d+)\s+Invoice"
#     invoice_date_pattern = r"(^\d{2}-\d{2}-\d{4})\s+(\d+)\s+Invoice"
#     dog_name_pattern = r"Animal Name: (.+?)  +?"
#     price_pattern = r"\$(\d+\.\d+)"
#     charges_date_pattern = r"(\d{1,2}\/\d{1,2}\/\d{4})"
#     charges_pattern = r" \s+(\S.+?  )\s+\S{1,2}"
#     itemized_begin_pattern = r"^\s+(Qty.*)"
#     itemized_end_pattern = "Subtotal:"


class EzyVetParser:

    def parse_invoice(self) -> None:
        raise NotImplementedError(
            "This invoice type has not been implemented!")
        ...


class EVetParser:

    def parse_invoice(self, txt: str, invoice_path: Path, good: pd.DataFrame = None, bad: pd.DataFrame = None) -> None:
        raise NotImplementedError(
            "This invoice type has not been implemented!")
        ...


def extract_text(pdf_path: Path, mode=None):
    text = ""
    reader = PdfReader(pdf_path)
    if not mode:
        mode = 'plain'
    text = '\n'.join([p.extract_text(extraction_mode=mode)
                     for p in reader.pages])
    return text


def get_parser(invoice_path: Union[Path, io.BytesIO], filename: str = None, is_drive: bool = False) -> InvoiceParser:
    txt = extract_text(invoice_path)
    parser_map = {
        r"Waipio Pet Clinic": WaipioParser,
        r"Wahiawa Pet Hospital": WahiawaParser,
        r"VCA ": VCAParser,
        r"Animal House Veterinary Center": AnimalHouseVetParser,
        r"Mililani Mauka Veterinary Clinic": MMVCParser,
        r"E Vet": EVetParser,
        r"EzyVet Clinic": EzyVetParser,
    }
    for clinic_regex, parser in parser_map.items():
        if re.search(clinic_regex, txt):
            if re.search(r'Animal|Waipio|Wahiawa|Mililani', clinic_regex):
                txt = extract_text(invoice_path, mode='layout')
            if filename:
                invoice_path = Path(filename)
            return parser(txt, invoice_path, is_drive)
    else:
        raise Exception(
            f"{filename if filename else invoice_path.name}: No Available Parser!")


def process_invoice(invoice: Union[io.BytesIO, Path], filename: str, is_drive: bool = False) -> (str, str, pd.DataFrame):
    parser = get_parser(invoice, filename=filename, is_drive=is_drive)
    parser.parse_invoice()
    if is_drive:
        return parser.drive_dir, parser.name, parser.items
    return parser.local_dir, parser.name, parser.items
