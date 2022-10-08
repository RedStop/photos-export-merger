# Author: C.F. Wagner
# Date: 2022/09/24
# Title: JsonPropertyMerger

from email.mime import image
from pathlib import Path
from typing import Any, Union, Optional, List, Dict
import datetime
import exiftool
import json
import logging


class JsonPropertyMerger:
    def __init__(this):
        this.logger = logging.Logger("JsonPropertyMerger", level=logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S'))
        this.logger.addHandler(handler)

    def ExtracJsonData(this, jsonFile=""):
        data = None
        jsonFile = str(jsonFile)
        try:
            with open(jsonFile, mode="r") as file:
                data = json.load(file)
        except Exception as e:
            this.logger.error("Failed to open file. Error: %s", str(e))

        if (this.logger.isEnabledFor(logging.DEBUG) and data is not None):
            this.logger.debug("Data extracted: %s", str(data))

        return data

    def ExtractMetaData(this, file: Union[str, List[str]] = ""):
        metadata = None
        try:
            with exiftool.ExifToolHelper() as et:
                metadata = et.get_metadata(file)
        except Exception as e:
            this.logger.error("Failed to extract metadata from file: %s Error: %s", str(file), str(e))

        if (this.logger.isEnabledFor(logging.DEBUG) and metadata is not None):
            this.logger.debug("Metadata: %s", str(metadata))

        if (type(file) == "str" and len(metadata) == 1):
            return metadata[0]
        else:
            return metadata

    def UpdateImageMetaDataWithJson(this, jsonFile: str, imagefile: str = ""):
        googleMetadata = this.ExtracJsonData(jsonFile)
        if (googleMetadata is None):
            return None

        if (imagefile == ""):
            # Get the file name from the googleMetaData
            imagefile = googleMetadata.get("title")
            if (imagefile is None):
                this.logger.error("Title did not obtain the image name for the jsonFile: %s", str(jsonFile))
                return None
            # Add the prefix if there is one, since it is assumed that the json and image files are in the same directory.
            imageFile = str(Path(jsonFile).with_name(imagefile))
            if (this.logger.isEnabledFor(logging.DEBUG)):
                this.logger.debug("Image file location: %s", imageFile)

        description = googleMetadata.get("description")
        photoTakenTime = googleMetadata.get("photoTakenTime")
        if (description is not None and len(description) > 0):
            this.logger.debug("Description: %s", str(description))
        else:
            description = None

        photoTakenTimeEpoch = None
        if (photoTakenTime is not None):
            photoTakenTimeEpoch = photoTakenTime.get("timestamp")
            if (photoTakenTimeEpoch is not None):
                this.logger.debug("PhotoTakenTimeEpoch: %s", str(photoTakenTimeEpoch))
                # Assume that the photo taken time from the googleMetadata is more correct (in all cases) than the photo taken date stored in the photo.

        if (photoTakenTimeEpoch is not None or description is not None):
            # Update the imageFile
            pass


if __name__ == "__main__":
    jsonPropertyMerger = JsonPropertyMerger()
    # json = jsonPropertyMerger.ExtracJsonData("TestPhotos/IMG_20220102_094708117.jpg.json")
    # jsonPropertyMerger.ExtracJsonData("TestPhotos/IMG_20220102_094728921.jpg.json")

    jsonPropertyMerger.UpdateImageMetaDataWithJson("TestPhotos/IMG_20220102_094708117.jpg.json")

    # jsonPropertyMerger.ExtractMetaData(["TestPhotos/IMG_20220102_094708117.jpg", "TestPhotos/IMG_20221001_091508974.jpg"])

    # jsonPropertyMerger.ExtractMetaData("TestPhotos/IMG_20220102_094728921.jpg")

    # jsonPropertyMerger.ExtractMetaData(["TestPhotos/IMG_20220102_094728921.jpg", "TestPhotos/IMG_20220102_094708117.jpg"])
