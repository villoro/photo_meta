import os
import re

import pandas as pd

from tqdm.notebook import tqdm
from exif import Image

import utils as u
from utils import log
from docs import Doc

REGEX = re.compile(r"(?P<skip>_)?(?P<year>\d{4})(_(?P<month>\d{2}))?(_(?P<day>\d{2}))?(.*)")

PATH_FILES = f"{u.PATH_DROPBOX}/1_files.parquet"
PATH_FILES_XLSX = f"{u.PATH_DROPBOX}/1_files.xlsx"
PATH_SUMMARY_XLSX = f"{u.PATH_DROPBOX}/2_summary.xlsx"
PATH_RESULTS_XLSX = f"{u.PATH_DROPBOX}/3_results.xlsx"


def show_file_info(path):

    log.debug(f"Showing info for {path=}")

    with open(path, "rb") as stream:
        image = Image(stream)

    for name in image.list_all():
        print(name, "-", image.get(name))


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
        return -1, False, False

    log.debug(f"Result: {data=}")

    out = str(data["year"])

    for key in ["month", "day"]:
        value = data[key]

        if (value is not None) and (int(value) > 0):
            out += f":{value}"

    return level, out, bool(data["skip"])


def read_everything(base_path):

    log.debug(f"Starting process for {base_path=}")

    # Get total number of steps to do
    count = len([0 for _ in os.walk(base_path)])

    out = []

    for path, _, files in tqdm(os.walk(base_path), total=count):

        path = path.replace("\\", "/")
        log.debug(f"Scanning {path=} with {len(files)} files")

        level, folder_date, skip = get_folder_date(path)

        for file in files:
            log.debug(f"Loading {path=} {file=}")

            data = Doc(
                folder=path, name=file, folder_date=folder_date, level=level, skip=skip
            ).load()
            out.append(data)

    return pd.DataFrame(out)


def fix_unicode(df):
    return df.applymap(
        lambda x: x.encode("unicode_escape").decode("utf-8") if isinstance(x, str) else x
    )


def scan_path(path, export_excel=False):

    df = read_everything(path)

    vdp = u.get_vdropbox()

    if vdp.file_exists(PATH_FILES):
        df_history = vdp.read_parquet(PATH_FILES)

        # Exclude what needs to be updated
        df_history = df_history[~df_history["folder"].str.startswith(path)]
        df = pd.concat([df_history, df]).reset_index(drop=True).sort_values(["folder", "name"])

    # Path as index
    df["uri"] = df["folder"] + "/" + df["name"]
    df = df.set_index("uri")

    log.info("Exporting data for scanned files")
    vdp.write_parquet(df, PATH_FILES)
    if export_excel:
        vdp.write_excel(fix_unicode(df), PATH_FILES_XLSX)
    return df


def cast_dates(df_in):
    df = df_in.copy()

    for col in ["datetime", "datetime_original"]:
        dt = pd.to_datetime(df[col], format="%Y:%m:%d %H:%M:%S", errors="coerce")

        df[col] = pd.to_datetime(df[col], format="%Y:%m:%d", errors="coerce")

        mask = ~dt.isna()
        df.loc[mask, col] = dt.loc[mask]

    return df


def summarize():

    vdp = u.get_vdropbox()
    df = vdp.read_parquet(PATH_FILES)

    df = cast_dates(df)

    for col in ["is_image", "error_dt", "error_dt_original", "missing_meta", "missing_gps"]:
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
        "skip": pd.NamedAgg(column="skip", aggfunc="max"),
        "error_dt": pd.NamedAgg(column="error_dt", aggfunc="count"),
        "error_dt_original": pd.NamedAgg(column="error_dt_original", aggfunc="count"),
        "missing_meta": pd.NamedAgg(column="missing_meta", aggfunc="count"),
        "missing_gps": pd.NamedAgg(column="missing_gps", aggfunc="count"),
        "updated_at_min": pd.NamedAgg(column="updated_at", aggfunc="min"),
        "updated_at": pd.NamedAgg(column="updated_at", aggfunc="max"),
    }

    # Add scan_time_seconds
    dfo = df.groupby("folder").agg(**aggs).reset_index()
    dfo["scan_time_seconds"] = (dfo["updated_at"] - dfo["updated_at_min"]).dt.total_seconds()
    dfo = dfo.drop("updated_at_min", axis=1)

    log.info("Exporting summary")
    vdp.write_excel(fix_unicode(dfo.set_index("folder", drop=True)), PATH_SUMMARY_XLSX)
    return dfo


def get_results():
    vdp = u.get_vdropbox()
    df = vdp.read_excel(PATH_SUMMARY_XLSX)

    aggs = {
        "folders": pd.NamedAgg(column="folder", aggfunc="count"),
        "images": pd.NamedAgg(column="images", aggfunc="sum"),
        "files": pd.NamedAgg(column="files", aggfunc="sum"),
        "skip": pd.NamedAgg(column="skip", aggfunc="sum"),
        "error_dt": pd.NamedAgg(column="error_dt", aggfunc="sum"),
        "error_dt_original": pd.NamedAgg(column="error_dt_original", aggfunc="sum"),
        "missing_meta": pd.NamedAgg(column="missing_meta", aggfunc="sum"),
        "missing_gps": pd.NamedAgg(column="missing_gps", aggfunc="sum"),
        "scan_time_minutes": pd.NamedAgg(column="scan_time_seconds", aggfunc="sum"),
        "updated_at": pd.NamedAgg(column="updated_at", aggfunc="max"),
    }

    # Dummy groupby to get global aggregations
    df = df.groupby(lambda _: True).agg(**aggs).set_index("updated_at", drop=True)

    # Transform to minutes
    df["scan_time_minutes"] = df["scan_time_minutes"] / 60

    # Add percents
    df["images_percent"] = 100 * df["images"] / df["files"]
    df["error_dt_percent"] = 100 * df["error_dt"] / df["images"]
    df["error_dt_original_percent"] = 100 * df["error_dt_original"] / df["images"]
    df["missing_meta_percent"] = 100 * df["missing_meta"] / df["images"]
    df["missing_gps_percent"] = 100 * df["missing_gps"] / df["images"]

    # Add to history
    if vdp.file_exists(PATH_RESULTS_XLSX):
        df_history = vdp.read_excel(PATH_RESULTS_XLSX).set_index("updated_at")
        df = pd.concat([df_history, df]).sort_index()

        # Drop possible duplicated data
        df = df[~df.index.duplicated(keep="last")]

    log.info("Exporting summary")
    vdp.write_excel(fix_unicode(df), PATH_RESULTS_XLSX)
    return df
