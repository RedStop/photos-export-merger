"""
test_merger.py — Comprehensive unit tests for GooglePhotosExportMerger.

This file is built in stages:
  Part 1  (current): File factories — minimal valid binary files + JSON files.
  Part 2  (future):  Test infrastructure — setUpClass, tearDownClass, summary runner.
  Part 3  (future):  All test_* methods.
"""

import json
import struct
import zlib
from pathlib import Path
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Internal binary builders (called once at import time)
# ---------------------------------------------------------------------------

def _make_jpeg() -> bytes:
    """Minimal valid JFIF JPEG (1×1 pixel, no actual image data needed by ExifTool)."""
    return bytes([
        0xFF, 0xD8,                          # SOI
        0xFF, 0xE0, 0x00, 0x10,              # APP0 marker, length=16
        0x4A, 0x46, 0x49, 0x46, 0x00,        # 'JFIF\0'
        0x01, 0x01,                          # version 1.1
        0x00,                                # pixel aspect ratio: no units
        0x00, 0x01, 0x00, 0x01,              # 1×1 density
        0x00, 0x00,                          # no embedded thumbnail
        0xFF, 0xD9,                          # EOI
    ])


def _make_png() -> bytes:
    """Minimal 1×1 white RGB PNG with valid CRCs."""
    def _chunk(tag: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack('>I', len(data)) + tag + data + struct.pack('>I', crc)

    sig  = b'\x89PNG\r\n\x1a\n'
    ihdr = _chunk(b'IHDR', struct.pack('>IIBBBBB', 1, 1, 8, 2, 0, 0, 0))  # 1×1 8-bit RGB
    idat = _chunk(b'IDAT', zlib.compress(b'\x00\xFF\xFF\xFF'))             # filter=none, white pixel
    iend = _chunk(b'IEND', b'')
    return sig + ihdr + idat + iend


def _make_gif() -> bytes:
    """Minimal GIF89a 1×1 (single white pixel)."""
    return bytes([
        0x47, 0x49, 0x46, 0x38, 0x39, 0x61,  # 'GIF89a'
        0x01, 0x00, 0x01, 0x00,              # 1×1 logical screen
        0x80,                                # GCT present, 1-bit colour depth (2 entries)
        0x00,                                # background colour index
        0x00,                                # pixel aspect ratio
        0xFF, 0xFF, 0xFF,                    # colour 0: white
        0x00, 0x00, 0x00,                    # colour 1: black (padding entry)
        0x2C,                                # image descriptor
        0x00, 0x00, 0x00, 0x00,              # left=0, top=0
        0x01, 0x00, 0x01, 0x00,              # width=1, height=1
        0x00,                                # packed: no local colour table
        0x02,                                # LZW minimum code size
        0x02, 0x4C, 0x01, 0x00,              # compressed image data (minimal LZW stream)
        0x3B,                                # GIF trailer
    ])


def _make_tiff() -> bytes:
    """Minimal little-endian TIFF with ImageWidth, ImageLength, BitsPerSample, SamplesPerPixel."""
    # TIFF IFD entry layout (12 bytes each): tag(u16) type(u16) count(u32) value/offset(u32)
    header = b'II' + struct.pack('<H', 42) + struct.pack('<I', 8)  # IFD0 at byte 8
    ifd = struct.pack('<H', 4)                                      # 4 entries
    ifd += struct.pack('<HHII', 256, 3, 1, 1)   # ImageWidth  = 1
    ifd += struct.pack('<HHII', 257, 3, 1, 1)   # ImageLength = 1
    ifd += struct.pack('<HHII', 258, 3, 1, 8)   # BitsPerSample = 8
    ifd += struct.pack('<HHII', 277, 3, 1, 1)   # SamplesPerPixel = 1
    ifd += struct.pack('<I', 0)                  # next IFD offset = 0 (end)
    return header + ifd


def _make_mp4(brand: bytes = b'isom') -> bytes:
    """Minimal ISO Base Media file (MP4 or MOV depending on brand)."""
    def _box(box_type: bytes, data: bytes = b'') -> bytes:
        return struct.pack('>I', 8 + len(data)) + box_type + data

    ftyp = _box(b'ftyp', brand + struct.pack('>I', 0) + b'isom' + b'iso2')
    free = _box(b'free')
    mdat = _box(b'mdat')
    return ftyp + free + mdat


def _make_avi() -> bytes:
    """Minimal RIFF AVI with a zeroed MainAVIHeader (enough for ExifTool to recognise it)."""
    avih = b'avih' + struct.pack('<I', 56) + b'\x00' * 56   # zeroed AVIMAINHEADER
    hdrl = b'LIST' + struct.pack('<I', 4 + len(avih)) + b'hdrl' + avih
    movi = b'LIST' + struct.pack('<I', 4) + b'movi'
    idx1 = b'idx1' + struct.pack('<I', 0)
    avi_data = b'AVI ' + hdrl + movi + idx1
    return b'RIFF' + struct.pack('<I', len(avi_data)) + avi_data


def _make_ebml(doc_type: bytes) -> bytes:
    """Minimal EBML header for Matroska/WebM containers."""
    def _vint(value: int) -> bytes:
        """1-byte VINT — sufficient for values 0–126."""
        return bytes([0x80 | value])

    def _elem(elem_id: bytes, data: bytes) -> bytes:
        return elem_id + _vint(len(data)) + data

    body = (
        _elem(b'\x42\x86', b'\x01') +          # EBMLVersion = 1
        _elem(b'\x42\xF7', b'\x01') +          # EBMLReadVersion = 1
        _elem(b'\x42\xF2', b'\x04') +          # EBMLMaxIDLength = 4
        _elem(b'\x42\xF3', b'\x08') +          # EBMLMaxSizeLength = 8
        _elem(b'\x42\x82', doc_type) +         # DocType
        _elem(b'\x42\x87', b'\x04') +          # DocTypeVersion = 4
        _elem(b'\x42\x85', b'\x02')            # DocTypeReadVersion = 2
    )
    ebml_header = b'\x1A\x45\xDF\xA3' + _vint(len(body)) + body
    # Segment element with unknown-size marker (0xFF = 1-byte unknown VINT)
    segment = b'\x18\x53\x80\x67\xFF'
    return ebml_header + segment


def _make_heic() -> bytes:
    """Minimal HEIC file (ftyp with 'heic' brand + empty meta box)."""
    def _box(box_type: bytes, data: bytes = b'') -> bytes:
        return struct.pack('>I', 8 + len(data)) + box_type + data

    ftyp = _box(b'ftyp', b'heic' + struct.pack('>I', 0) + b'heic' + b'mif1')
    meta = _box(b'meta', struct.pack('>I', 0))   # version=0, flags=0
    return ftyp + meta


def _make_dng() -> bytes:
    """Minimal DNG: little-endian TIFF extended with DNGVersion + DNGBackwardVersion tags."""
    # IFD0 at byte 8; tags must be in ascending numerical order per TIFF spec
    header = b'II' + struct.pack('<H', 42) + struct.pack('<I', 8)
    ifd = struct.pack('<H', 6)
    ifd += struct.pack('<HHII', 256,   3, 1, 1)              # ImageWidth = 1
    ifd += struct.pack('<HHII', 257,   3, 1, 1)              # ImageLength = 1
    ifd += struct.pack('<HHII', 258,   3, 1, 16)             # BitsPerSample = 16 (raw)
    ifd += struct.pack('<HHII', 277,   3, 1, 1)              # SamplesPerPixel = 1
    # DNGVersion (50706): BYTE[4], value fits in 4-byte offset field → 1.4.0.0
    ifd += struct.pack('<HHI4s', 50706, 1, 4, b'\x01\x04\x00\x00')
    # DNGBackwardVersion (50707): BYTE[4] → 1.1.0.0
    ifd += struct.pack('<HHI4s', 50707, 1, 4, b'\x01\x01\x00\x00')
    ifd += struct.pack('<I', 0)   # next IFD offset = 0
    return header + ifd


def _make_cr2() -> bytes:
    """Minimal CR2: TIFF header with CR2 signature at bytes 8–11, IFD0 at offset 16."""
    # Bytes 0–3:  'II' + magic 42
    # Bytes 4–7:  IFD0 offset → 16
    # Bytes 8–9:  'CR' (Canon CR2 identifier)
    # Bytes 10:   CR2 major version (2)
    # Bytes 11:   CR2 minor version (0)
    # Bytes 12–15: Offset to RAW IFD (0 for stub)
    header = b'II' + struct.pack('<H', 42) + struct.pack('<I', 16)
    cr2_sig = b'CR\x02\x00'
    raw_ifd_offset = struct.pack('<I', 0)

    ifd = struct.pack('<H', 4)
    ifd += struct.pack('<HHII', 256, 3, 1, 1)   # ImageWidth
    ifd += struct.pack('<HHII', 257, 3, 1, 1)   # ImageLength
    ifd += struct.pack('<HHII', 258, 3, 1, 8)   # BitsPerSample
    ifd += struct.pack('<HHII', 277, 3, 1, 1)   # SamplesPerPixel
    ifd += struct.pack('<I', 0)

    return header + cr2_sig + raw_ifd_offset + ifd


# ---------------------------------------------------------------------------
# Pre-built byte sequences (computed once at import time)
# ---------------------------------------------------------------------------

_MEDIA_BYTES: Dict[str, bytes] = {
    '.jpg':  _make_jpeg(),
    '.jpeg': _make_jpeg(),
    '.png':  _make_png(),
    '.gif':  _make_gif(),
    '.tiff': _make_tiff(),
    '.tif':  _make_tiff(),
    '.mp4':  _make_mp4(b'isom'),
    '.mov':  _make_mp4(b'qt  '),   # QuickTime brand
    '.avi':  _make_avi(),
    '.mkv':  _make_ebml(b'matroska'),
    '.webm': _make_ebml(b'webm'),
    '.heic': _make_heic(),
    '.dng':  _make_dng(),
    '.cr2':  _make_cr2(),
}


# ---------------------------------------------------------------------------
# Public factory functions
# ---------------------------------------------------------------------------

def make_media_file(path: Path) -> Path:
    """Write a minimal valid media file at *path* based on its suffix.

    Creates any missing parent directories.  Returns *path*.
    Raises ``ValueError`` for unsupported extensions.
    """
    ext = path.suffix.lower()
    data = _MEDIA_BYTES.get(ext)
    if data is None:
        raise ValueError(f"No file factory for extension: {ext!r}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def make_json_file(path: Path, **fields: Any) -> Path:
    """Write a Google Photos Takeout-style JSON metadata file at *path*.

    Sensible defaults are provided for every field; pass keyword arguments
    to override individual top-level keys.  Nested dicts (geoData, geoDataExif,
    photoTakenTime) should be passed as complete dicts when overriding.

    The ``title`` default is derived from *path* by stripping the trailing
    ``.json`` suffix (e.g. ``photo.jpg.json`` → title ``"photo.jpg"``).

    Default ``photoTakenTime.timestamp`` is ``"1723113846"``
    (2024-08-08 09:04:06 UTC).

    Creates any missing parent directories.  Returns *path*.
    """
    # path.stem strips only the last suffix, giving the media filename:
    #   "photo.jpg.json"  →  stem = "photo.jpg"
    media_name: str = path.stem

    defaults: Dict[str, Any] = {
        'title': media_name,
        'description': '',
        'photoTakenTime': {
            'timestamp': '1723113846',
            'formatted': 'Aug 8, 2024, 9:04:06 AM UTC',
        },
        'creationTime': {
            'timestamp': '1723113846',
            'formatted': 'Aug 8, 2024, 9:04:06 AM UTC',
        },
        'geoData': {
            'latitude': 0.0,
            'longitude': 0.0,
            'altitude': 0.0,
            'latitudeSpan': 0.0,
            'longitudeSpan': 0.0,
        },
        'geoDataExif': {
            'latitude': 0.0,
            'longitude': 0.0,
            'altitude': 0.0,
            'latitudeSpan': 0.0,
            'longitudeSpan': 0.0,
        },
    }
    defaults.update(fields)

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(defaults, ensure_ascii=False, indent=2), encoding='utf-8')
    return path
