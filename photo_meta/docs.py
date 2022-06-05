from datetime import datetime as dt  # To avoid collisions with 'datetime' attr

from pydantic import BaseModel, validator
from exif import Image

IMAGE_FORMATS = ["jpg", "jpeg"]  # , "png"]


def is_image(name):
    return name.split(".")[-1].lower() in IMAGE_FORMATS


class Doc(BaseModel):
    folder: str
    name: str
    extension: str = None
    is_image: bool = False
    level: int = -1
    skip: bool = False
    # Dates
    datetime: str = ""
    datetime_original: str = ""
    datetime_taken: str = ""
    folder_date: str = None
    # GPS
    gps_latitude: str = None
    gps_longitude: str = None
    # Validations
    missing_meta: bool = False
    missing_gps: bool = False
    error_dt: bool = None
    error_dt_original: bool = None
    updated_at: dt = None

    @validator("updated_at", always=True)
    def set_updated_at(cls, v, values):
        return dt.now()

    @validator("extension", always=True)
    def get_extension(cls, v, values):
        """Used for extracting the extension from the file name"""
        return values["name"].split(".")[-1].lower()

    @validator("is_image", always=True)
    def get_is_image(cls, v, values):
        return is_image(values["name"])

    def load(self):
        """Retrive metadata from document"""
        path = f"{self.folder}/{self.name}"

        fields_to_extracts = [
            "datetime",
            "datetime_original",
            "datetime_taken",
            "gps_latitude",
            "gps_longitude",
        ]

        if self.is_image:
            with open(path, "rb") as stream:
                image = Image(stream)

            self.missing_meta = len(image.list_all()) == 0

            for field in fields_to_extracts:
                try:
                    value = image.get(field)
                except KeyError:
                    continue

                if value:
                    # Cast it so that it can be stored in a parquet easily
                    if field.startswith("gps_"):
                        value = str(value)

                    setattr(self, field, value)

            self.missing_gps = not (self.gps_latitude or self.gps_longitude)
            self.error_dt = not (self.datetime.startswith(self.folder_date) or self.skip)
            self.error_dt_original = not (
                self.datetime_original.startswith(self.folder_date) or self.skip
            )

        # Simplify processing by returning the data as dict here
        return self.dict()
