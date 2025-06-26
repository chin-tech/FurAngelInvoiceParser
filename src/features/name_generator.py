import re
import pandas as pd
from constants.database import DB_LOGIN_DATA
from constants.project import ANIMALS_NAME_FILE
from animal_db_handler import get_all_animals
from typing import NamedTuple 

class Names(NamedTuple):
    name: str
    gender: str

def load_names(name_file: str) -> pd.DataFrame:
    all_names = []
    with open(name_file, 'r') as f:
        for lines in f:
            split = lines.split()
            if len(split) == 1:
                n = split[0]
                g = 'Unknown'
            elif len(split) == 2:
                n,g = split
            else:
                pass
            all_names.append(Names(n,g))

    return pd.DataFrame(all_names)

def extract_extra_names(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Extracts individual words from a Series of strings,
    cleaning common punctuation and splitting by whitespace.

    Args:
        series: A pandas Series containing strings (e.g., animal names).

    Returns:
        A pandas Series where each word from the original names
        is an individual entry.
    """


    df[col] = clean_and_split_names(df[col])
    df = df.explode(col)
    df = df[df[col] != ''].reset_index(drop=True).copy()

    return df


def clean_and_split_names(series: pd.Series) -> pd.Series:
    """
    Cleans strings and splits them into lists of words.
    """
    cleaned_series = series.astype(str).str.lower()
    cleaned_series = cleaned_series.apply(lambda x: re.sub(r"[^\w\s-]", " ", x))
    cleaned_series = cleaned_series.apply(lambda x: re.sub(r"[\u2018\u2019\u201c\u201d]", " ", x))
    cleaned_series = cleaned_series.str.replace(r"[',\"]", " ", regex=True)
    cleaned_series = cleaned_series.str.replace("-", " ", regex=False) # Split hyphenated words
    cleaned_series = cleaned_series.str.strip().str.replace(r"\s+", " ", regex=True) # Normalize whitespace
    return cleaned_series.str.split() # Return a Series of lists



def get_disjoint(animal_df: pd.DataFrame, unique_names: pd.DataFrame) -> pd.DataFrame:
    animal_names = extract_extra_names(animal_df, 'name')
    animal_names = animal_names['name']

    unique_names = unique_names['name'].str.lower()
    disjoint = unique_names[~unique_names.isin(animal_names)]
    disjoint = pd.DataFrame(disjoint.str.capitalize().drop_duplicates())
    return disjoint


def get_unique_animal_names() -> pd.DataFrame:
    animals = extract_extra_names(get_all_animals(DB_LOGIN_DATA), 'name')
    unique_names = load_names(ANIMALS_NAME_FILE)

    return get_disjoint(animals, unique_names)

    
