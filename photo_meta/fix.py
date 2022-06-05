import os
import re

from exif import Image
from tqdm.notebook import tqdm

from utils import log
from docs import is_image
from scan import get_folder_date, scan_path


def get_approx_date(value):

    # Add month
    if len(value) < 5:
        value += ":01"

    # Add day
    if len(value) < 8:
        value += ":01"

    return value


def set_dt_from_dt_original(folder):

    updated = 0

    for file in os.listdir(folder):
        path = f"{folder}/{file}"

        if not is_image(file):
            log.debug(f"Skipping {file=}")
            continue

        with open(path, "rb") as stream:
            image = Image(stream)

        dt_original = image.get("datetime_original")
        dt = image.get("datetime")

        if dt_original and dt and not dt.startswith(dt_original):
            updated += 1
            image["datetime"] = dt_original

            with open(path, "wb") as stream:
                stream.write(image.get_file())

    log.success(f"{folder=} {updated=}")
    scan_path(folder)


def update_one_meta(path, set_date, dry_run=True):

    log.debug(f"Updating metadata for {path=} with {set_date=}")

    with open(path, "rb") as stream:
        image = Image(stream)

    updated = 0

    for field in ["datetime", "datetime_original"]:

        try:
            value = image[field]
        except (KeyError, AttributeError):
            approx_date = get_approx_date(set_date)

            log.warning(f"{field=} not found in {path=}. Setting default '{approx_date}'")
            image[field] = approx_date
            updated += 1
            continue

        if value:
            if not value.startswith(set_date):
                updated += 1

            # Add new value
            image[field] = set_date + value[len(set_date) :]

    if (not dry_run) and updated:
        with open(path, "wb") as stream:
            stream.write(image.get_file())

        log.debug(f"{updated} updates to {path=}")

    return bool(updated)


def update_all_in_path(base_path, verbose=False, dry_run=True):

    log.info(f"Starting process for {base_path=} and {dry_run=}")

    # Get total number of steps to do
    count = len([0 for _ in os.walk(base_path)])

    out = []

    for root, _, files in tqdm(os.walk(base_path), total=count):

        log.debug(f"Scanning {root=} with {len(files)} files")

        if not files:
            log.info(f"Skipping {root=}")
            continue

        root = root.replace("\\", "/")

        _, folder_date, _ = get_folder_date(root)

        if not folder_date:
            continue

        files_updated = 0

        for file in files:
            if not is_image(file):
                log.info(f"Skipping {file=}")
                continue

            path = f"{root}/{file}"

            result = update_one_meta(path, folder_date, dry_run)
            files_updated += result

            if result:
                log.info(f"{file=} updated")

        log.success(f"In {root=} {files_updated=}")

    log.info(f"Exporting results")
