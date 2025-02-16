import pandas as pd
from io import StringIO
from datetime import timedelta as td
from datetime import datetime as dt
from envstuff import get_login_data
import re
import requests
import logging

log = logging.getLogger(__name__)

DATABASE_URL = "https://us06d.sheltermanager.com"
LOGIN_URL = DATABASE_URL + "/login?smaccount="
CSV_URL = DATABASE_URL + "/report_export_csv?id=216"
CSV_UPLOAD_URL = DATABASE_URL + "/csvimport"
DATECOL = 'DATEBROUGHTIN'
DAYCOL = 'TOTALDAYSONSHELTER'
NAMECOL = 'ANIMALNAME'


def get_all_animals(login_data: dict) -> pd.DataFrame:
    try:
        session = requests.Session()
        session.post(LOGIN_URL + login_data['database'], data=login_data)

        resp = session.get(CSV_URL)

        csv_text = resp.text
        csv_text = csv_text[csv_text.find('"'):]
        try:
            df = pd.read_csv(StringIO(csv_text))
            df = prep_animal_df(df, DATECOL, DAYCOL, NAMECOL)
            return df
        except pd.errors.EmptyDataError:
            print("[Error]: CSV Data was empty!")
    except requests.exceptions.RequestException as e:
        print(f"[Request Error]: {e}")
    except Exception as e:
        print(f"[Unexpected Error]: {e}")


def upload_dataframe_to_database(df: pd.DataFrame, is_debug: bool = False):
    login_data = get_login_data()
    try:
        session = requests.Session()
        session.post(LOGIN_URL + login_data['database'], data=login_data)

        csv_memfile = StringIO()
        df.to_csv(csv_memfile, index=False)
        csv_data = csv_memfile.getvalue().encode('utf-8')
        files = {
            'filechooser': ('invoice_uploader.csv', csv_data, 'text/csv'),
            'encoding': (None, 'utf-8-sig'),
        }
        if is_debug:
            log.info(f"Made {files} - Not uploading")
            return True
        resp = session.post(CSV_UPLOAD_URL, files=files)
        if resp.status_code != 200:
            print(resp.text)
            raise Exception('Failed updating DB')
        rows = df.shape[0]
        log.info(f'Success!: {rows} - Added to database!')
        return True
    except Exception as e:
        log.error(f"Failed to update DB: {e}")
        return False


def prep_animal_df(df: pd.DataFrame, date_col: str, days_col: str, name_col: str) -> pd.DataFrame:
    df[date_col] = pd.to_datetime(df[date_col], format='mixed').dt.date
    df[date_col] = pd.to_datetime(df[date_col])
    df['name'] = df[name_col].str.lower().replace(r"[,'\"]", regex=True)
    df[days_col] = pd.to_timedelta(df[days_col], unit='days')
    df['end_date'] = pd.to_datetime(df[date_col] + df[days_col] + td(days=1))
    df.sort_values(by='end_date')
    return df


def get_probable_matches(animal: str, df: pd.DataFrame, date: dt = None) -> pd.DataFrame:
    animal = re.sub(r"['\"]", '', animal.lower())
    pattern = r'\b' + r'\b|\b'.join(animal.split()) + r'\b'
    of = df
    if date:
        tmp = df[(df['DATEBROUGHTIN'] <= date) & (df['end_date'] >= date)]
        if not tmp.empty:
            df = tmp
    tmp = df[df['name'].str.contains(animal)]
    if tmp.shape[0] == 1:
        return tmp
    df = df[df['name'].str.contains(pattern, regex=True)]
    if df.empty:
        df = of[of['name'].str.contains(pattern, regex=True)]
    return df


def get_likely_animal(animal: str, date: dt, df: pd.DataFrame):

    cleaned_animal = re.sub(r"['?,\"]", '', animal.lower()).strip()
    pattern = r'\b' + r'\b|\b'.join(cleaned_animal.split()) + r'\b'

# Apply date filtering if a date is provided
    if date is not None:
        tmp = df[(df['DATEBROUGHTIN'] <= date) & (df['end_date'] >= date)]
        if not tmp.empty:
            filtered_df = tmp

    # Direct match on 'name'
    tmp = filtered_df[filtered_df['name'].str.contains(
        cleaned_animal, case=False, na=False)]
    if tmp.shape[0] == 1:
        return tmp[['ANIMALNAME', 'SHELTERCODE']].iloc[0]

    # Regex match with pattern
    tmp = filtered_df[filtered_df['name'].str.contains(
        pattern, regex=True, case=False, na=False)]
    if tmp.shape[0] == 1:
        return tmp[['ANIMALNAME', 'SHELTERCODE']].iloc[0]
    return pd.Series([animal, 'ERROR_CODE'], index=['ANIMALNAME', 'SHELTERCODE'])
    # return (animal, 'ERROR_CODE')  # No single match found


def match_animals(cost_df: pd.DataFrame, animal_df: pd.DataFrame) -> pd.DataFrame:
    cost_df['date'] = pd.to_datetime(cost_df['COSTDATE'])
    cost_df[['ANIMALNAME', 'ANIMALCODE']] = cost_df.apply(
        lambda x: get_likely_animal(x['ANIMALNAME'], x['date'], animal_df), axis=1)
    cost_df = cost_df[~((cost_df['COSTTYPE'] == 'Other')
                        & (cost_df['COSTAMOUNT'] == 0))].copy()
    cost_df.sort_values(by='date', inplace=True)
    cost_df.drop(columns=['date'], inplace=True)
    return cost_df
