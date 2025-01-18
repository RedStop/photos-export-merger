# Author: C.F. Wagner
# Date: 2022/09/24
# Title: JsonPropertyMerger

from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple, Union, Optional, List
import enum
import exiftool
import json
import logging


class TagUpdateMode(enum.Enum):
    OVERWRITE = 1  # Overwrite the original tag with the new tag.
    KEEP = 2  # Keep the original tag.
    APPEND = 3  # Append (if possible, else default to KEEP) to the original tag.
    PROMPT_USER = 4  # Ask the user what to do.


class JsonPropertyMerger:
    def __init__(this, descriptionTagUpdateMode=TagUpdateMode.APPEND):
        this.logger = logging.Logger("JsonPropertyMerger", level=logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S'))
        this.logger.addHandler(handler)

        this.descriptionTagUpdateMode = descriptionTagUpdateMode

        this.et = exiftool.ExifToolHelper()
        this.et.run()

    def __del__(this):
        this.et.terminate()

    def __HandleParamThatAlreadyExist(this, newTagsDict: dict, tagUpdateMode: TagUpdateMode, tagKey: str, newTagValue: str | None, imageMetaData: dict | None, imageFile: str, appendable: bool = False) -> Tuple[dict | None, dict | None]:
        if (newTagValue is not None):
            # Determine if there are already a description
            if (imageMetaData is None):
                imageMetaData = this.ExtractMetaData(imageFile)
                if (imageMetaData is None):
                    # This file does not exist
                    return None, None

            # Try to find the user comment tag
            if (tagKey in imageMetaData and len(oldTagValue := str(imageMetaData[tagKey])) > 0):
                this.logger.warning("The \"%s\" tag with value \"%s\" already exists in %s.", tagKey, oldTagValue, imageFile)

                if (tagUpdateMode == TagUpdateMode.APPEND):
                    if (not appendable):
                        tagUpdateMode = TagUpdateMode.PROMPT_USER
                        this.logger.warning("This tag value cannot be appened. The user will be prompted.")

                if (tagUpdateMode != TagUpdateMode.OVERWRITE and tagUpdateMode != TagUpdateMode.KEEP and tagUpdateMode != TagUpdateMode.APPEND):
                    # Prompt the user.
                    userMessage = "Should the original value in the image's metadata be [O]verwritten or [K]ept"
                    if (appendable):
                        userMessage += " or should the new value be [A]ppended to the original value? [O/K/A]:"
                    else:
                        userMessage += "? [O/K]:"
                    print(userMessage)

                    userInput = input().lower()
                    while (userInput != "o" and userInput != "k" and ((not appendable) or userInput != "a")):
                        print("Invalid input: \"" + userInput + "\". Try again.")
                        print(userMessage)
                        userInput = input().lower()

                    if (userInput == "o"):
                        tagUpdateMode = TagUpdateMode.OVERWRITE
                    elif (userInput == "k"):
                        tagUpdateMode = TagUpdateMode.KEEP
                    elif (userInput == "a"):
                        tagUpdateMode = TagUpdateMode.APPEND

                if (tagUpdateMode == TagUpdateMode.OVERWRITE):
                    newTagsDict[tagKey] = newTagValue
                    this.logger.warning("The tag value will be overwritten: \"%s\" -> \"%s\"", oldTagValue, newTagValue)
                elif (tagUpdateMode == TagUpdateMode.KEEP):
                    this.logger.info("The old tag value will be left unchanged.")
                elif (tagUpdateMode == TagUpdateMode.APPEND):
                    newTagsDict[tagKey] = oldTagValue + " || " + newTagValue
                    this.logger.info("The new tag value will be appended to the old value: \"%s\" -> \"%s\"", oldTagValue, newTagsDict[tagKey])
            else:
                # The tag does not exist.
                newTagsDict[tagKey] = newTagValue

        return newTagsDict, imageMetaData

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
            metadata = this.et.get_metadata(file)
        except Exception as e:
            this.logger.error("Failed to extract metadata from file: %s Error: %s", str(file), str(e))

        if (this.logger.isEnabledFor(logging.DEBUG) and metadata is not None):
            this.logger.debug("Metadata: %s", str(metadata))

        if (type(file) == str and len(metadata) == 1):
            return metadata[0]
        else:
            return metadata

    '''
    Update the the image refrenced in the Google Photo's json file with the parameters in the file.
    @param jsonFile (str): path to the jsonFile that should be parsed and used to updated the image's metadata with.
    @param imageFile (str): Optional path to the image who's metadata should be updated. If not provided, the title
    field in the json file will be used, and the image must be in the same directory as the json file.
    @param descriptionTagUpdateMode (TagUpdateMode): Optionally overwrite the default (that can be set when creating
    this class) description tag update mode that is used.
    '''
    def UpdateImageMetaDataWithJson(this, jsonFile: str, imagefile: Optional[str] = None, descriptionTagUpdateMode: Optional[TagUpdateMode] = None):
        if (descriptionTagUpdateMode is None):
            descriptionTagUpdateMode = this.descriptionTagUpdateMode

        googleMetadata = this.ExtracJsonData(jsonFile)
        if (googleMetadata is None):
            return None

        if (imagefile is None):
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
            imageMetaData = None
            # Build dict
            tagsDict = {}
            if (photoTakenTimeEpoch is not None):
                takenDateTime = datetime.utcfromtimestamp(int(photoTakenTimeEpoch)).replace(tzinfo=timezone.utc).astimezone(tz=None)
                takenDateTimeStr = takenDateTime.strftime('%Y:%m:%d %H:%M:%S')
                takenDateTimeZoneStr = takenDateTime.strftime('%z')
                takenDateTimeZoneStr = takenDateTimeZoneStr[:-2] + ":" + takenDateTimeZoneStr[-2:]  # Add colon in zone time.
                this.logger.debug("Date: %s Zone: %s", takenDateTimeStr, takenDateTimeZoneStr)

                tagsDict["alldates"] = takenDateTimeStr
                tagsDict["EXIF:ExifIFD:OffsetTime"] = takenDateTimeZoneStr
                tagsDict["EXIF:ExifIFD:OffsetTimeOriginal"] = takenDateTimeZoneStr
                tagsDict["EXIF:ExifIFD:OffsetTimeDigitized"] = takenDateTimeZoneStr

            tagsDict, imageMetaData = this.__HandleParamThatAlreadyExist(tagsDict, descriptionTagUpdateMode, "EXIF:ImageDescription", description, imageMetaData, imageFile, appendable=True)
            if (tagsDict is None):
                # The image file does not exist, so no use trying to update it.
                return None

            # TODO: Maybe also use this: EXIF:XPComment (Comment tag used by Windows, encoded in UCS2.)

            # TODO: GPS -> Only update if the GPS values does not exist.
            # Look at: https://exiftool.org/forum/index.php?topic=7826.0 (especially some of the last comments in 2021).
            # My phone stores the GPS location at: QuickTime:UserData:GPSCoordinates

            # Update the imageFile
            try:
                pass
                this.et.set_tags(imageFile, tagsDict)
            except Exception as e:
                this.logger.error("Failed to write metadata to %s. Error: %s", str(imageFile), str(e))


if __name__ == "__main__":
    jsonPropertyMerger = JsonPropertyMerger()
    # json = jsonPropertyMerger.ExtracJsonData("TestPhotos/IMG_20220102_094708117.jpg.json")
    # jsonPropertyMerger.ExtracJsonData("TestPhotos/IMG_20220102_094728921.jpg.json")

    jsonPropertyMerger.ExtractMetaData("TestPhotos/IMG_20221001_091508974.jpg")
    jsonPropertyMerger.UpdateImageMetaDataWithJson("TestPhotos/IMG_20220102_094728921.jpg.json", descriptionTagUpdateMode=TagUpdateMode.APPEND)

    # jsonPropertyMerger.ExtractMetaData(["TestPhotos/IMG_20220102_094708117.jpg", "TestPhotos/IMG_20221001_091508974.jpg"])

    # jsonPropertyMerger.ExtractMetaData(["TestPhotos/IMG_20220102_094728921.jpg", "TestPhotos/IMG_20220102_094708117.jpg"])
    del jsonPropertyMerger
