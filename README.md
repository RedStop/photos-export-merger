# Google Photos Export Merger

The purpose of this tool is to merge the json data acompanying an Google Photos Export into the Exif properties of the images and possibly the videos.  
No gurantees are made, so ensure you have a backup of your photos somewhere else before running this script.

## Requirments

- Python 3.10.11
- [ExifTool](https://exiftool.org/) 12.45 (should be available in the system Path) - I used the Windows executable.
- Only tested on Windows.

## Python packages

- PyExifTool 0.5.4 (use `pip install PyExifTool`)
- sortedcontainers 2.4.0 (use `pip install sortedcontainers`)