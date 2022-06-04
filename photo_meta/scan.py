import os
import re

import pandas as pd

from tqdm.notebook import tqdm

import utils as u

from utils import log
from docs import Doc

REGEX = re.compile(r"(?P<year>\d{4})(_(?P<month>\d{2}))?(_(?P<day>\d{2}))?(.*)")

PATH_FILES = f"{u.PATH_DROPBOX}/files.parquet"
PATH_FILES_XLSX = f"{u.PATH_DROPBOX}/files.xlsx"
PATH_SUMMARY = f"{u.PATH_DROPBOX}/summary.parquet"
PATH_SUMMARY_XLSX = f"{u.PATH_DROPBOX}/summary.xlsx"


def get_folder_date(path):

    log.info(f"Extracting date from {path=}")

    # Find the first match going from the deepest folder
    for level, folder in enumerate(path.split("/")[::-1]):
        data = REGEX.match(folder)

        if data:
            data = data.groupdict()
            break

    else:
        log.error(f"Unable to extract data for {folder=}")
        return -1, False

    log.debug(f"Result: {data=}")

    out = str(data["year"])

    for key in ["month", "day"]:
        value = data[key]

        if (value is not None) and (int(value) > 0):
            out += f":{value}"

    return level, out


def read_everything(base_path):

    log.debug(f"Starting process for {base_path=}")

    # Get total number of steps to do
    count = len([0 for _ in os.walk(base_path)])

    out = []

    for path, _, files in tqdm(os.walk(base_path), total=count):

        path = path.replace("\\", "/")
        log.debug(f"Scanning {path=} with {len(files)} files")

        level, folder_date = get_folder_date(path)

        for file in files:
            log.debug(f"Loading {path=} {file=}")

            data = Doc(folder=path, name=file, folder_date=folder_date, level=level).load()
            out.append(data)

    return pd.DataFrame(out)


def scan_path(path):

    df = read_everything(path)

    vdp = u.get_vdropbox()

    if vdp.file_exists(PATH_FILES):
        df_history = vdp.read_parquet(PATH_FILES)

        # Exclude what needs to be updated
        df_history = df_history[~df_history["folder"].str.startswith(path)]
        df = pd.concat([df_history, df]).reset_index(drop=True).sort_values(["folder", "name"])

    log.info("Exporting data for scanned files")
    vdp.write_parquet(df, PATH_FILES)
    vdp.write_excel(df, PATH_FILES_XLSX)
    return df


def summarize():

    vdp = u.get_vdropbox()

    df = vdp.read_parquet(PATH_FILES)

    for col in ["is_image", "error_dt", "error_dt_original", "missing_meta"]:
        df.loc[df[col] == False, col] = None

    aggs = {
        "dt_min": pd.NamedAgg(column="datetime", aggfunc="min"),
        "dt_max": pd.NamedAgg(column="datetime", aggfunc="max"),
        "dt_original_min": pd.NamedAgg(column="datetime_original", aggfunc="min"),
        "dt_original_max": pd.NamedAgg(column="datetime_original", aggfunc="max"),
        "extensions": pd.NamedAgg(column="extension", aggfunc="unique"),
        "images": pd.NamedAgg(column="is_image", aggfunc="count"),
        "files": pd.NamedAgg(column="name", aggfunc="count"),
        "level": pd.NamedAgg(column="level", aggfunc="max"),
        "error_dt": pd.NamedAgg(column="error_dt", aggfunc="count"),
        "error_dt_original": pd.NamedAgg(column="error_dt_original", aggfunc="count"),
        "missing_meta": pd.NamedAgg(column="missing_meta", aggfunc="count"),
        "updated_at_min": pd.NamedAgg(column="updated_at", aggfunc="min"),
        "updated_at": pd.NamedAgg(column="updated_at", aggfunc="max"),
    }

    # Add scan_time_seconds
    dfo = df.groupby("folder").agg(**aggs).reset_index()
    dfo["scan_time_seconds"] = (dfo["updated_at"] - dfo["updated_at_min"]).dt.total_seconds()
    dfo = dfo.drop("updated_at_min", axis=1)

    log.info("Exporting summary")
    vdp.write_parquet(dfo, PATH_SUMMARY)
    vdp.write_excel(dfo, PATH_SUMMARY_XLSX)
    return dfo
