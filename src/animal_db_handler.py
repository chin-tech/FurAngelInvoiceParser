import logging
import re
from datetime import datetime as dt
from datetime import timedelta as td
from io import StringIO

import pandas as pd
import requests

from constants.database import (
    LOGIN_URL,
    CSV_URL,
    CSV_UPLOAD_URL,
    DB_LOGIN_DATA
)

log = logging.getLogger(__name__)

# DATABASE_URL = "https://us06d.sheltermanager.com"
# LOGIN_URL = DATABASE_URL + "/login?smaccount="
# CSV_URL = DATABASE_URL + "/report_export_csv?id=216"
# CSV_UPLOAD_URL = DATABASE_URL + "/csvimport"
DATECOL = "DATEBROUGHTIN"
DAYCOL = "TOTALDAYSONSHELTER"
NAMECOL = "ANIMALNAME"


def get_all_animals(login_data: dict) -> pd.DataFrame:
    """Retrieves all animals from the sheltermanager DB with provided credentials
    Args:
        login_data: (dict): A dictionary of keys:  [database, username, password].

    Returns:
        pd.DataFrame: Dataframe containing all animals

    """
    try:
        session = requests.Session()
        session.post(LOGIN_URL + login_data["database"], data=login_data)

        resp = session.get(CSV_URL)

        csv_text = resp.text
        csv_text = csv_text[csv_text.find('"') :]
        try:
            df = pd.read_csv(StringIO(csv_text))
            return prep_animal_df(df, DATECOL, DAYCOL, NAMECOL)
        except pd.errors.EmptyDataError:
            pass
    except requests.exceptions.RequestException:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def upload_dataframe_to_database(df: pd.DataFrame, is_debug: bool = False) -> bool:
    """Uploads the given dataframe to the sheltermanager DB
    Args:
        df (pd.DataFrame): The provided dataframe
    Returns:
        bool: True if the operation succeeded, false otherwise.
    """
    login_data = DB_LOGIN_DATA
    try:
        session = requests.Session()
        session.post(LOGIN_URL + login_data["database"], data=login_data)

        csv_memfile = StringIO()
        df.to_csv(csv_memfile, index=False)
        csv_data = csv_memfile.getvalue().encode("utf-8")
        files = {
            "filechooser": ("invoice_uploader.csv", csv_data, "text/csv"),
            "encoding": (None, "utf-8-sig"),
        }
        if is_debug:
            log.info(f"Made {files} - Not uploading")
            return True
        resp = session.post(CSV_UPLOAD_URL, files=files)
        if resp.status_code != 200:
            msg = "Failed updating DB"
            raise Exception(msg)
        rows = df.shape[0]
        log.info(f"Success!: {rows} - Added to database!")
        return True
    except Exception as e:
        log.exception(f"Failed to update DB: {e}")
        return False


def prep_animal_df(
    df: pd.DataFrame, date_col: str, days_col: str, name_col: str,
) -> pd.DataFrame:
    """Prepares the animal dataframe retrieved from the `get_all_animals` function, by formatting datetime columns, normalizing dog names and calculating the end_date
    Args:
        df (pd.DataFrame): The dataframe of all animals
        date_col (str): The name of the date column in the dataframe
        days_col (str): The name of the days column in the dataframe
        name_col (str): The name of the `name` column in the dataframe
    Returns:
        pd.DataFrame: The normalized dataframe.

    """
    df[date_col] = pd.to_datetime(df[date_col], format="mixed").dt.date
    df[date_col] = pd.to_datetime(df[date_col])
    df["name"] = df[name_col].str.lower().replace(r"[,'\"]", regex=True)
    df[days_col] = pd.to_timedelta(df[days_col], unit="days")
    df["end_date"] = pd.to_datetime(df[date_col] + df[days_col] + td(days=1))
    df.sort_values(by="end_date", inplace=True)
    return df


def prepare_animals_for_failure_matching() -> pd.DataFrame:
    animals = get_all_animals(DB_LOGIN_DATA)
    assert isinstance(animals, pd.DataFrame)
    animals = animals.sort_values(by="DATEBROUGHTIN")
    animals["date_in"] = animals["DATEBROUGHTIN"].dt.date
    animals["last_day_on_shelter"] = animals["end_date"].dt.date
    return animals


def get_probable_matches(
    animal: str, df: pd.DataFrame, date: dt | None = None,
) -> pd.DataFrame:
    animal = re.sub(r"[?'\"]", "", animal.lower())
    pattern = r"\b" + r"\b|\b".join(animal.split()) + r"\b"
    of = df
    if date:
        tmp = df[(df["DATEBROUGHTIN"] <= date) & (df["end_date"] >= date)]
        if not tmp.empty:
            df = tmp
    tmp = df[df["name"].str.contains(animal)]
    if tmp.shape[0] == 1:
        return tmp
    df = df[df["name"].str.contains(pattern, regex=True)]
    if df.empty:
        df = of[of["name"].str.contains(pattern, regex=True)]
    return df


def get_likely_animal(animal: str, date: dt, df: pd.DataFrame) -> pd.Series:
    """Attempts to find the closest matching animal in the database
    Args:
        animal (str): The name of the animal to find in the database
        date (dt): The provided datetime of the invoiced charge, to help narrow down the search
        df (pd.DataFrame): The animal dataframe retrieved from the database
    Returns:
        pd.Series: A pd.Series with either a fixed name and sheltercode or the unedited animal with an ERROR_CODE.
    """
    cleaned_animal = re.sub(r"['?,\"]", "", animal.lower()).strip()
    pattern = r"\b" + r"\b|\b".join(cleaned_animal.split()) + r"\b"

    # Apply date filtering if a date is provided
    if date is not None:
        tmp = df[(df["DATEBROUGHTIN"] <= date) & (df["end_date"] >= date)]
        if not tmp.empty:
            filtered_df = tmp

    # Direct match on 'name'
    tmp = filtered_df[
        filtered_df["name"].str.contains(cleaned_animal, case=False, na=False)
    ]
    if tmp.shape[0] == 1:
        return tmp[["ANIMALNAME", "SHELTERCODE"]].iloc[0]

    # Regex match with pattern
    tmp = filtered_df[
        filtered_df["name"].str.contains(pattern, regex=True, case=False, na=False)
    ]
    if tmp.shape[0] == 1:
        return tmp[["ANIMALNAME", "SHELTERCODE"]].iloc[0]
    return pd.Series([animal, "ERROR_CODE"], index=["ANIMALNAME", "SHELTERCODE"])


def match_animals(cost_df: pd.DataFrame, animal_df: pd.DataFrame) -> pd.DataFrame:
    """Convenience function to prepare the dataframe for getting the likely animals, while removing duplicates
    Args:
        cost_df (pd.DataFrame): The resulting dataframe from a InvoiceParsers.items
        animal_df (pd.DataFrame): The sheltermanager DB, dataframe
    Returns:
        pd.DataFrame: The fixed dataframe with appropraite names and columns.
    """
    cost_df["date"] = pd.to_datetime(cost_df["COSTDATE"])
    cost_df[["ANIMALNAME", "ANIMALCODE"]] = cost_df.apply(
        lambda x: get_likely_animal(x["ANIMALNAME"], x["date"], animal_df), axis=1,
    )
    cost_df = cost_df[
        ~((cost_df["COSTTYPE"] == "Other") & (cost_df["COSTAMOUNT"] == 0))
    ].copy()
    cost_df = cost_df.sort_values(by="date")
    cost_df = cost_df.drop(columns=["date"])
    return cost_df.drop_duplicates()


def add_invoices_col(fails: pd.DataFrame, pdfs: pd.DataFrame):
    cols = ["invoice", "invoice_date"]
    fails[cols] = fails["COSTDESCRIPTION"].str.extract(
        r" - (\d+) - (\d{4}-\d{2}-\d{2})",
    )
    pdfs[cols] = pdfs["name"].str.extract(r"_(\d+)_(\d{4}-\d{2}-\d{2})")
    pdfs["cmp"] = pdfs["invoice"] + "_" + pdfs["invoice_date"]
    fails["cmp"] = fails["invoice"] + "_" + fails["invoice_date"]
    return fails, pdfs
