import unicodedata
from io import BytesIO, StringIO

import pandas as pd
import py7zr
import requests


def read_csv_from_url(url, **kwargs):
    """
    read a csv file from an url with custom http headers

    :param url: URL pointing to the csv file to load
    :param kwargs: any keyword argument accepted by pandas.read_csv
    :return: a DataFrame instance
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0"
    }
    req = requests.get(url, headers=headers)
    archive = py7zr.SevenZipFile(BytesIO(req.content), mode='r')
    archive.extractall(path="./data")
    df = pd.read_csv("./data/vacunas_covid.csv", **kwargs)
    return df


def strip_accents_spain(
    string, accents=("COMBINING ACUTE ACCENT", "COMBINING GRAVE ACCENT")
):
    accents = set(map(unicodedata.lookup, accents))
    chars = [c for c in unicodedata.normalize("NFD", string) if c not in accents]
    return unicodedata.normalize("NFC", "".join(chars))
