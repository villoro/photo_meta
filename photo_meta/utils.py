import sys

from pathlib import Path

from loguru import logger as log
from vcrypto import Cipher
from vdropbox import Vdropbox

# Base path of the repo.
# It need to go 2 times up since this file has the following relative path:
#   /src/utils.py
PATH_ROOT = Path(__file__).parent.parent


def get_path(path_relative):
    """Returns absolute path using PATH_ROOT"""

    path_out = PATH_ROOT

    for x in path_relative.split("/"):
        path_out /= x

    return str(path_out)


CONFIG = {
    "handlers": [
        {"sink": sys.stdout, "level": "ERROR"},
        {"sink": "logs/info.log", "level": "INFO"},
        {"sink": "logs/debug.log", "level": "DEBUG"},
    ]
}

log.configure(**CONFIG)
log.enable("metadata")


CIPHER = None
CIPHER_KWARGS = {"secrets_file": get_path("secrets.yaml"), "environ_var_name": "VTASKS_TOKEN"}


def get_secret(key, encoding="utf8"):
    """Retrives one encrypted secret"""

    global CIPHER
    if CIPHER is None:
        CIPHER = Cipher(**CIPHER_KWARGS)

    return CIPHER.get_secret(key, encoding=encoding)


def save_secret(key, value):
    """Retrives one encrypted secret"""

    global CIPHER
    if CIPHER is None:
        CIPHER = Cipher(**CIPHER_KWARGS)

    return CIPHER.save_secret(key, value)


VDROPBOX = None


def get_vdropbox():
    """Creates a vdropbox instance"""

    global VDROPBOX
    if VDROPBOX is None:
        VDROPBOX = Vdropbox(get_secret("DROPBOX_TOKEN"), log=log)

    return VDROPBOX
