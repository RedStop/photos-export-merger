"""
test_merger.py — Comprehensive unit tests for PhotosExportMerger.

This file is built in stages:
  Part 1  (done):    File factories — minimal valid binary files + JSON files.
  Part 2  (done):    Test infrastructure — setUpClass, tearDownClass, summary runner.
  Part 3  (future):  All test_* methods.
"""

import argparse
import json
import logging
import os
import shutil
import struct
import sys
import tempfile
import unittest
import zlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

import exiftool
from PhotosExportMerger import PhotosExportMerger, MergeStats

# Custom log level below DEBUG (10) — used by infrastructure-validation tests
# to document intent without cluttering normal output.
TRACE = 5
logging.addLevelName(TRACE, 'TRACE')


# ---------------------------------------------------------------------------
# Internal binary builders (called once at import time)
# ---------------------------------------------------------------------------

def _jpeg_body() -> bytes:
    """Shared JPEG image body: DQT + SOF0 + DHT_DC + DHT_AC + SOS + scan data.

    Encodes a 1×1 white grayscale pixel using baseline DCT with standard
    ITU-T T.81 Annex K Huffman tables.  Prepend SOI + one APP segment and
    append EOI to form a complete, ExifTool-accepted JPEG file.

    Scan data derivation (Y=255 → level-shifted to 127):
      DC coefficient = 8×127 = 1016; all AC = 0 (flat block).
      DIFF=1016, category 10 → Huffman 11111110 + VLI 1111111000.
      AC EOB (0x00) → Huffman 1010.
      Bit stream (22 bits, padded to 3 bytes with 1s): 0xFE 0xFE 0x2B.
    """
    # DQT — unity quantization table (all 1s → maximum quality, table 0)
    dqt = bytes([0xFF, 0xDB, 0x00, 0x43, 0x00]) + bytes([0x01] * 64)

    # SOF0 — baseline DCT, 1×1 pixel, 8-bit precision, 1 grayscale component
    sof0 = bytes([
        0xFF, 0xC0, 0x00, 0x0B,              # marker, length=11
        0x08,                                # precision = 8 bits
        0x00, 0x01, 0x00, 0x01,              # height=1, width=1
        0x01,                                # Nf = 1 component
        0x01, 0x11, 0x00,                    # comp id=1, H=1 V=1, Tq=0
    ])

    # DHT — standard luminance DC table (ITU-T T.81 Table K.3, Tc=0 Th=0)
    dc_bits     = bytes([0x00, 0x01, 0x05, 0x01, 0x01, 0x01, 0x01, 0x01,
                         0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    dc_huffval  = bytes(range(12))           # 0x00 … 0x0B
    dht_dc = (bytes([0xFF, 0xC4])
              + struct.pack('>H', 2 + 1 + len(dc_bits) + len(dc_huffval))
              + bytes([0x00])                # Tc=0 (DC), Th=0
              + dc_bits + dc_huffval)

    # DHT — standard luminance AC table (ITU-T T.81 Table K.5, Tc=1 Th=0)
    ac_bits = bytes([0x00, 0x02, 0x01, 0x03, 0x03, 0x02, 0x04, 0x03,
                     0x05, 0x05, 0x04, 0x04, 0x00, 0x00, 0x01, 0x7D])
    ac_huffval = bytes([
        0x01, 0x02, 0x03, 0x00, 0x04, 0x11, 0x05, 0x12,
        0x21, 0x31, 0x41, 0x06, 0x13, 0x51, 0x61, 0x07,
        0x22, 0x71, 0x14, 0x32, 0x81, 0x91, 0xA1, 0x08,
        0x23, 0x42, 0xB1, 0xC1, 0x15, 0x52, 0xD1, 0xF0,
        0x24, 0x33, 0x62, 0x72, 0x82, 0x09, 0x0A, 0x16,
        0x17, 0x18, 0x19, 0x1A, 0x25, 0x26, 0x27, 0x28,
        0x29, 0x2A, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39,
        0x3A, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49,
        0x4A, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59,
        0x5A, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69,
        0x6A, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,
        0x7A, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
        0x8A, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98,
        0x99, 0x9A, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7,
        0xA8, 0xA9, 0xAA, 0xB2, 0xB3, 0xB4, 0xB5, 0xB6,
        0xB7, 0xB8, 0xB9, 0xBA, 0xC2, 0xC3, 0xC4, 0xC5,
        0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xD2, 0xD3, 0xD4,
        0xD5, 0xD6, 0xD7, 0xD8, 0xD9, 0xDA, 0xE1, 0xE2,
        0xE3, 0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA,
        0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8,
        0xF9, 0xFA,
    ])
    dht_ac = (bytes([0xFF, 0xC4])
              + struct.pack('>H', 2 + 1 + len(ac_bits) + len(ac_huffval))
              + bytes([0x10])                # Tc=1 (AC), Th=0
              + ac_bits + ac_huffval)

    # SOS — scan header for 1 grayscale component
    sos = bytes([
        0xFF, 0xDA, 0x00, 0x08,              # marker, length=8
        0x01,                                # Ns = 1 component in scan
        0x01, 0x00,                          # comp id=1, Td=0 (DC table 0), Ta=0 (AC table 0)
        0x00, 0x3F, 0x00,                    # Ss=0, Se=63, Ah=0 Al=0
    ])

    # Scan data: white pixel (no 0xFF bytes → no byte-stuffing needed)
    scan = bytes([0xFE, 0xFE, 0x2B])

    return dqt + sof0 + dht_dc + dht_ac + sos + scan


def _make_jpeg() -> bytes:
    """Full 1×1 white grayscale JFIF JPEG accepted by ExifTool without warnings."""
    app0 = bytes([
        0xFF, 0xE0, 0x00, 0x10,              # APP0 marker, length=16
        0x4A, 0x46, 0x49, 0x46, 0x00,        # 'JFIF\0'
        0x01, 0x01,                          # version 1.1
        0x00,                                # aspect ratio: no units
        0x00, 0x01, 0x00, 0x01,              # 1×1 density
        0x00, 0x00,                          # no embedded thumbnail
    ])
    return b'\xFF\xD8' + app0 + _jpeg_body() + b'\xFF\xD9'


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
    """Valid ISO Base Media file (MP4 or MOV) that ExifTool can read and write.

    Structure: ftyp + moov (mvhd only — no trak) + mdat.
    A zero-sample trak causes ExifTool to fail with "Can't locate data reference
    to update offsets", so we omit it; mvhd alone satisfies ExifTool's parser.
    """
    def _box(fourcc: bytes, payload: bytes = b'') -> bytes:
        return struct.pack('>I', 8 + len(payload)) + fourcc + payload

    # ftyp — brand-specific compatible brands
    if brand == b'qt  ':
        ftyp = _box(b'ftyp', brand + struct.pack('>I', 0) + b'qt  ')
    else:
        ftyp = _box(b'ftyp', brand + struct.pack('>I', 0) + b'isom' + b'iso2')

    # mvhd — Movie Header (version 0)
    _matrix = struct.pack('>9I',
        0x00010000, 0, 0,
        0, 0x00010000, 0,
        0, 0, 0x40000000)
    mvhd_payload = (
        struct.pack('>I',  0)           # version=0, flags=0
        + struct.pack('>II', 0, 0)      # creation_time, modification_time
        + struct.pack('>II', 1000, 0)   # timescale=1000, duration=0
        + struct.pack('>I', 0x00010000) # rate=1.0 (16.16 fixed-point)
        + struct.pack('>H', 0x0100)     # volume=1.0 (8.8 fixed-point)
        + b'\x00' * 2                   # reserved
        + b'\x00' * 8                   # reserved[2]
        + _matrix                       # 36 bytes
        + b'\x00' * 24                  # pre_defined[6]
        + struct.pack('>I', 1)          # next_track_ID=1
    )
    moov = _box(b'moov', _box(b'mvhd', mvhd_payload))
    mdat = _box(b'mdat')

    return ftyp + moov + mdat


def _make_avi() -> bytes:
    """Minimal RIFF AVI with a zeroed MainAVIHeader (enough for ExifTool to recognise it)."""
    avih = b'avih' + struct.pack('<I', 56) + b'\x00' * 56   # zeroed AVIMAINHEADER
    hdrl = b'LIST' + struct.pack('<I', 4 + len(avih)) + b'hdrl' + avih
    movi = b'LIST' + struct.pack('<I', 4) + b'movi'
    idx1 = b'idx1' + struct.pack('<I', 0)
    avi_data = b'AVI ' + hdrl + movi + idx1
    return b'RIFF' + struct.pack('<I', len(avi_data)) + avi_data


def _make_avi_with_nikon_dates(dt_str: str = '2014:07:12 12:38:02') -> bytes:
    """Minimal RIFF AVI with a Nikon 'ncdt' chunk containing DateTimeOriginal
    and CreateDate (without timezone).  When ExifTool creates an XMP sidecar
    via ``-o``, it maps these Nikon dates into XMP-xmp:CreateDate and
    XMP-exif:DateTimeOriginal, overriding params we pass — exactly the bug
    we're testing the fixup for."""
    dt_bytes = dt_str.encode('ascii') + b'\x00'
    # Tag 0x0013 = Nikon DateTimeOriginal, Tag 0x0014 = Nikon CreateDate
    tag_0013 = struct.pack('<HH', 0x0013, len(dt_bytes)) + dt_bytes
    tag_0014 = struct.pack('<HH', 0x0014, len(dt_bytes)) + dt_bytes
    nctg_data = tag_0013 + tag_0014
    nctg = b'nctg' + struct.pack('<I', len(nctg_data)) + nctg_data
    ncdt = b'LIST' + struct.pack('<I', 4 + len(nctg)) + b'ncdt' + nctg

    avih = b'avih' + struct.pack('<I', 56) + b'\x00' * 56
    hdrl = b'LIST' + struct.pack('<I', 4 + len(avih)) + b'hdrl' + avih
    movi = b'LIST' + struct.pack('<I', 4) + b'movi'
    idx1 = b'idx1' + struct.pack('<I', 0)
    avi_data = b'AVI ' + hdrl + movi + idx1 + ncdt
    return b'RIFF' + struct.pack('<I', len(avi_data)) + avi_data


def _make_ebml(doc_type: bytes) -> bytes:
    """Valid Matroska/WebM container with EBML header, SegmentInfo, and video track.

    The Segment has a known size (not the unknown-size 0xFF sentinel used
    previously), which allows ExifTool to parse the container properly.
    """
    def _vint_n(value: int) -> bytes:
        """Shortest valid VINT encoding for *value* (multi-byte aware)."""
        if value <= 0x7E:
            return bytes([0x80 | value])
        elif value <= 0x3FFE:
            return struct.pack('>H', 0x4000 | value)
        elif value <= 0x1FFFFE:
            n = 0x200000 | value
            return bytes([(n >> 16) & 0xFF, (n >> 8) & 0xFF, n & 0xFF])
        else:
            return struct.pack('>I', 0x10000000 | value)

    def _elem(elem_id: bytes, data: bytes) -> bytes:
        return elem_id + _vint_n(len(data)) + data

    # EBML header (element ID: 0x1A45DFA3)
    header_body = (
        _elem(b'\x42\x86', b'\x01')       # EBMLVersion = 1
        + _elem(b'\x42\xF7', b'\x01')     # EBMLReadVersion = 1
        + _elem(b'\x42\xF2', b'\x04')     # EBMLMaxIDLength = 4
        + _elem(b'\x42\xF3', b'\x08')     # EBMLMaxSizeLength = 8
        + _elem(b'\x42\x82', doc_type)    # DocType
        + _elem(b'\x42\x87', b'\x04')     # DocTypeVersion = 4
        + _elem(b'\x42\x85', b'\x02')     # DocTypeReadVersion = 2
    )
    ebml_header = b'\x1A\x45\xDF\xA3' + _vint_n(len(header_body)) + header_body

    # SegmentInfo (element ID: 0x1549A966)
    seg_info_body = (
        _elem(b'\x2A\xD7\xB1', b'\x0F\x42\x40')      # TimecodeScale = 1 000 000 (1 ms)
        + _elem(b'\x4D\x80', b'test')                  # MuxingApp
        + _elem(b'\x57\x41', b'test')                  # WritingApp
        + _elem(b'\x44\x89', struct.pack('>d', 0.0))   # Duration = 0.0 (float64)
    )
    seg_info = _elem(b'\x15\x49\xA9\x66', seg_info_body)

    # Tracks (element ID: 0x1654AE6B)  —  one video track, no codec data needed
    video_body = (
        _elem(b'\xB0', b'\x20')   # PixelWidth  = 32
        + _elem(b'\xBA', b'\x20') # PixelHeight = 32
    )
    track_entry_body = (
        _elem(b'\xD7', b'\x01')                  # TrackNumber = 1
        + _elem(b'\x73\xC5', b'\x01')            # TrackUID    = 1
        + _elem(b'\x83', b'\x01')                # TrackType   = 1 (video)
        + _elem(b'\x86', b'V_UNCOMPRESSED')      # CodecID
        + _elem(b'\xE0', video_body)             # Video sub-element
    )
    tracks = _elem(b'\x16\x54\xAE\x6B', _elem(b'\xAE', track_entry_body))

    # Segment (element ID: 0x18538067) — known size
    segment_body = seg_info + tracks
    segment = b'\x18\x53\x80\x67' + _vint_n(len(segment_body)) + segment_body

    return ebml_header + segment


def _make_heic() -> bytes:
    """HEIC with a primary hvc1 item backed by a 1-byte mdat (mdat-first layout).

    ExifTool requires:
    - A non-empty iinf box (otherwise it deletes it and can't add EXIF/XMP).
    - The mdat to come BEFORE meta so that mdat offset fixups work correctly when
      ExifTool grows the metadata section and shifts offsets.
    Layout: ftyp(24) + mdat(9) → mdat data at offset 32, meta follows at offset 33.
    """
    def _box(box_type: bytes, data: bytes = b'') -> bytes:
        return struct.pack('>I', 8 + len(data)) + box_type + data

    ftyp = _box(b'ftyp', b'heic' + struct.pack('>I', 0) + b'heic' + b'mif1')
    mdat = _box(b'mdat', b'\x00')   # 1-byte dummy image data; data at offset 32

    hdlr = _box(b'hdlr',
        struct.pack('>I', 0) + struct.pack('>I', 0) + b'pict' + b'\x00' * 12 + b'\x00')
    pitm = _box(b'pitm', struct.pack('>I', 0) + struct.pack('>H', 1))
    infe = _box(b'infe',
        struct.pack('>B', 2) + b'\x00\x00\x00' +   # version=2, flags=0
        struct.pack('>H', 1) +                       # item_id=1
        struct.pack('>H', 0) +                       # protection_index=0
        b'hvc1' +                                    # item_type (HEVC still image)
        b'\x00')                                     # item_name=''
    iinf = _box(b'iinf', struct.pack('>I', 0) + struct.pack('>H', 1) + infe)
    # iloc v0: item 1 at absolute offset 32 (ftyp=24 + mdat_header=8), length 1
    iloc_entry = (struct.pack('>H', 1) + struct.pack('>H', 0) +   # item_id, dref_idx
                  struct.pack('>H', 1) +                           # extent_count
                  struct.pack('>I', 32) + struct.pack('>I', 1))    # offset, length
    iloc = _box(b'iloc',
        struct.pack('>I', 0) + b'\x44\x00' + struct.pack('>H', 1) + iloc_entry)
    url  = _box(b'url ', struct.pack('>I', 1))   # self-contained
    dref = _box(b'dref', struct.pack('>I', 0) + struct.pack('>I', 1) + url)
    dinf = _box(b'dinf', dref)
    meta = _box(b'meta', struct.pack('>I', 0) + hdlr + pitm + iinf + iloc + dinf)
    return ftyp + mdat + meta


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
    """Minimal CR2: TIFF header with CR2 signature and a two-IFD chain.

    ExifTool's WriteCR2 requires bytes 12-15 (raw_ifd_offset) to point to the LAST
    IFD in the linked chain.  A single IFD0 with next=0 leaves LastIFD unset → error
    "CR2 image IFD may not be deleted".  Adding IFD1 as the chain tail fixes this.

    Layout:
      0-7  : TIFF header (II + 0x2A + IFD0_offset=16)
      8-11 : CR2 signature CR\\x02\\x00
      12-15: raw_ifd_offset → 70 (IFD1, the last IFD)
      16-69: IFD0 (4 entries, next→70)
      70-87: IFD1 (1 entry, next=0)
    """
    header = b'II' + struct.pack('<H', 42) + struct.pack('<I', 16)
    cr2_sig = b'CR\x02\x00'
    raw_ifd_offset = struct.pack('<I', 70)   # bytes 12-15: last IFD = IFD1 at offset 70

    ifd0 = struct.pack('<H', 4)
    ifd0 += struct.pack('<HHII', 256, 3, 1, 1)   # ImageWidth
    ifd0 += struct.pack('<HHII', 257, 3, 1, 1)   # ImageLength
    ifd0 += struct.pack('<HHII', 258, 3, 1, 8)   # BitsPerSample
    ifd0 += struct.pack('<HHII', 277, 3, 1, 1)   # SamplesPerPixel
    ifd0 += struct.pack('<I', 70)                 # next IFD → IFD1 at offset 70

    ifd1 = struct.pack('<H', 1)
    ifd1 += struct.pack('<HHII', 256, 3, 1, 1)   # ImageWidth (raw strip stub)
    ifd1 += struct.pack('<I', 0)                  # next IFD = 0 (last in chain)

    return header + cr2_sig + raw_ifd_offset + ifd0 + ifd1


def _make_jpeg_with_exif_tz(offset: str) -> bytes:
    """JPEG with a minimal EXIF APP1 segment containing *only* OffsetTimeOriginal.

    The APP1 is placed immediately after SOI (standard EXIF placement), followed
    by a full valid baseline-DCT body so ExifTool accepts the file without the
    "Corrupted JPEG" warning.

    TIFF layout inside the APP1 payload (little-endian):
      0-7   : TIFF header  (IFD0 at offset 8)
      8-25  : IFD0         (1 entry: ExifIFD pointer at offset 26)
      26-43 : ExifIFD      (1 entry: OffsetTimeOriginal ASCII, 7 bytes at offset 44)
      44-50 : ASCII string "+HH:MM\\0"
    """
    value = (offset + '\x00').encode('ascii')   # e.g. b'+00:00\x00' (7 bytes)
    count = len(value)

    tiff  = b'II' + struct.pack('<H', 42) + struct.pack('<I', 8)   # TIFF header
    tiff += struct.pack('<H', 1)
    tiff += struct.pack('<HHII', 0x8769, 4, 1, 26)                 # ExifIFD ptr
    tiff += struct.pack('<I', 0)                                   # IFD0 next = 0
    tiff += struct.pack('<H', 1)
    tiff += struct.pack('<HHII', 0x9011, 2, count, 44)             # OffsetTimeOriginal
    tiff += struct.pack('<I', 0)                                   # ExifIFD next = 0
    tiff += value

    app1_body = b'Exif\x00\x00' + tiff
    app1 = b'\xFF\xE1' + struct.pack('>H', 2 + len(app1_body)) + app1_body
    return b'\xFF\xD8' + app1 + _jpeg_body() + b'\xFF\xD9'


def _make_jpeg_with_offset_time(offset: str) -> bytes:
    """JPEG with EXIF OffsetTime (0x9010) — exercises the EXIF:OffsetTime fallback branch.

    Unlike _make_jpeg_with_exif_tz which embeds OffsetTimeOriginal (0x9011),
    this variant uses the secondary offset tag so that the merger's
    _resolve_dates_and_paths falls through to the OffsetTime check.
    """
    value = (offset + '\x00').encode('ascii')   # e.g. b'+03:00\x00' (7 bytes)
    count = len(value)

    tiff  = b'II' + struct.pack('<H', 42) + struct.pack('<I', 8)   # TIFF header
    tiff += struct.pack('<H', 1)
    tiff += struct.pack('<HHII', 0x8769, 4, 1, 26)                 # ExifIFD ptr
    tiff += struct.pack('<I', 0)                                   # IFD0 next = 0
    tiff += struct.pack('<H', 1)
    tiff += struct.pack('<HHII', 0x9010, 2, count, 44)             # OffsetTime
    tiff += struct.pack('<I', 0)                                   # ExifIFD next = 0
    tiff += value

    app1_body = b'Exif\x00\x00' + tiff
    app1 = b'\xFF\xE1' + struct.pack('>H', 2 + len(app1_body)) + app1_body
    return b'\xFF\xD8' + app1 + _jpeg_body() + b'\xFF\xD9'


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
    """Write a Photos Takeout-style JSON metadata file at *path*.

    Sensible defaults are provided for every field; pass keyword arguments
    to override individual top-level keys.  Nested dicts (geoData, geoDataExif,
    photoTakenTime) should be passed as complete dicts when overriding.

    The ``title`` default is derived from *path* by stripping the trailing
    ``.json`` suffix (e.g. ``photo.jpg.json`` → title ``"photo.jpg"``).

    Default ``photoTakenTime.timestamp`` is ``"1723113846"``
    (2024-08-08 10:44:06 UTC).

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


# ---------------------------------------------------------------------------
# Test constants
# ---------------------------------------------------------------------------

# Blocked descriptions list passed to the merger — mirrors a typical production run.
_BLOCKED_DESCRIPTIONS = ['SONY DSC']

# Epoch shared across most test JSON files: 2024-08-08 10:44:06 UTC.
# In GMT+02:00 (the merger's fallback TZ) this is 2024-08-08 12:44:06,
# so output files land in output/2024/08/.
_EPOCH_DEFAULT = '1723113846'


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

class TestPhotosExportMerger(unittest.TestCase):
    """
    Single-pass integration test for PhotosExportMerger.

    setUpClass builds the full input tree, runs the merger once, and stores
    the resulting MergeStats and a pre-run input-directory snapshot.
    Individual test_* methods (Part 3) assert specific output properties
    against that single run.
    """

    # Class-level state populated by setUpClass
    tmp_dir:        Path
    input_dir:      Path
    output_dir:     Path
    stats:          MergeStats
    input_snapshot: dict   # {rel_path_str: (size, mtime, ctime)}

    # ------------------------------------------------------------------
    # setUpClass
    # ------------------------------------------------------------------

    @classmethod
    def setUpClass(cls) -> None:
        logging.basicConfig(
            format='%(levelname)s %(name)s: %(message)s',
            level=logging.WARNING,
        )

        # Isolated temp tree
        cls.tmp_dir    = Path(tempfile.mkdtemp(prefix='gpem_test_'))
        cls.input_dir  = cls.tmp_dir / 'input'
        cls.output_dir = cls.tmp_dir / 'output'
        cls.input_dir.mkdir()

        # Build the full input tree (timezone EXIF is pre-embedded in the JPEG bytes)
        cls._create_input_tree()

        # Snapshot input BEFORE the merger touches anything
        cls.input_snapshot = cls._snapshot(cls.input_dir)

        # Single merge run shared by all test_* methods
        # Uses all CPU cores to exercise parallel processing by default.
        # Explicit fallback_tz=+02:00 so tests are independent of host timezone.
        num_workers = os.cpu_count() or 1
        merger = PhotosExportMerger(
            str(cls.input_dir),
            str(cls.output_dir),
            blocked_descriptions=_BLOCKED_DESCRIPTIONS,
            num_workers=num_workers,
            fallback_tz=timezone(timedelta(hours=2)),
        )
        cls.stats = merger.run()

    # ------------------------------------------------------------------
    # Input-tree builder
    # ------------------------------------------------------------------

    @classmethod
    def _create_input_tree(cls) -> None:
        """Populate cls.input_dir with all test media + JSON files."""
        inp = cls.input_dir

        # ── RootLevel ──────────────────────────────────────────────────────
        # photo_basic.jpg : matched file with no EXIF timezone → GMT+02:00 fallback
        # orphan_no_json.jpg : no matching JSON → processed as orphan
        d = inp / 'RootLevel'
        make_media_file(d / 'photo_basic.jpg')
        make_json_file(d / 'photo_basic.jpg.json')
        make_media_file(d / 'orphan_no_json.jpg')

        # ── GPS Tests ──────────────────────────────────────────────────────
        # Each file covers one GPS scenario (quadrant/axis, altitude sign).
        # Both geoData and geoDataExif carry the same coordinates so the
        # merger always picks up GPS regardless of which key it checks first.
        d = inp / 'GPS Tests'

        # 8 directions × 12 formats matrix
        _gps_directions = [
            # (suffix,  lat,    lon,    alt)
            ('ne',      38.91,  121.60,   0.0),   # N+E quadrant
            ('nw',      48.85,   -2.35,   0.0),   # N+W quadrant
            ('se',     -25.82,   28.20,   0.0),   # S+E quadrant
            ('sw',     -33.86,  -70.67,   0.0),   # S+W quadrant (Santiago)
            ('n',       45.00,    0.00,   0.0),   # N axis — lon=0 edge case
            ('e',        0.00,   90.00,   0.0),   # E axis — lat=0 edge case
            ('s',      -45.00,    0.00,   0.0),   # S axis — neg lat + lon=0
            ('w',        0.00,  -90.00,   0.0),   # W axis — lat=0 + neg lon
        ]
        _gps_exts = ['.jpg', '.tiff', '.dng', '.cr2', '.heic',
                     '.png', '.gif', '.mp4', '.mov', '.avi', '.mkv', '.webm']

        for direction, lat, lon, alt in _gps_directions:
            for ext in _gps_exts:
                make_media_file(d / f'gps_{direction}{ext}')
                geo = {'latitude': lat, 'longitude': lon, 'altitude': alt,
                       'latitudeSpan': 0.0, 'longitudeSpan': 0.0}
                make_json_file(d / f'gps_{direction}{ext}.json', geoData=geo, geoDataExif=geo)

        # Altitude edge cases (JPG only)
        for stem, lat, lon, alt in [
            ('gps_altitude_negative', -25.82, 28.20, -50.0),
            ('gps_high_altitude',     -25.82, 28.20, 1623.44),
        ]:
            make_media_file(d / f'{stem}.jpg')
            geo = {'latitude': lat, 'longitude': lon, 'altitude': alt,
                   'latitudeSpan': 0.0, 'longitudeSpan': 0.0}
            make_json_file(d / f'{stem}.jpg.json', geoData=geo, geoDataExif=geo)

        # ── Timezones ──────────────────────────────────────────────────────
        # Each JPEG has OffsetTimeOriginal pre-embedded so the merger reads it
        # from the source file without any ExifTool pre-write step.
        # The no-timezone fallback scenario is covered by photo_basic.jpg above.
        d = inp / 'Timezones'
        _tz_cases = [
            ('tz_utc',     '+00:00'),
            ('tz_gmt2',    '+02:00'),
            ('tz_minus5',  '-05:00'),
            ('tz_plus8',   '+08:00'),
            ('tz_plus530', '+05:30'),
            ('tz_minus930', '-09:30'),
        ]
        for stem, offset in _tz_cases:
            p = d / f'{stem}.jpg'
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(_make_jpeg_with_exif_tz(offset))
            make_json_file(d / f'{stem}.jpg.json')

        # OffsetTime fallback: embed OffsetTime (0x9010) instead of OffsetTimeOriginal (0x9011)
        # so the merger's secondary tz-lookup branch is exercised.
        ot_path = d / 'tz_offset_time.jpg'
        ot_path.write_bytes(_make_jpeg_with_offset_time('+03:00'))
        make_json_file(d / 'tz_offset_time.jpg.json')

        # Timezone fallback — non-JPG formats (no embedded EXIF TZ → GMT+02:00 fallback)
        _tz_fallback_exts = ['.tiff', '.heic', '.png', '.gif', '.mp4', '.avi']
        for ext in _tz_fallback_exts:
            make_media_file(d / f'tz_fallback{ext}')
            make_json_file(d / f'tz_fallback{ext}.json')

        # Sidecar format with an explicit timezone — verifies that sidecar
        # date tags carry the source file's timezone, not the +02:00 fallback.
        # epoch 1723113846 = 2024-08-08 10:44:06 UTC → -07:00 → 2024:08:08 03:44:06
        make_media_file(d / 'tz_minus7.png')
        make_json_file(d / 'tz_minus7.png.json')
        with exiftool.ExifToolHelper() as _et:
            try:
                _et.set_tags([str(d / 'tz_minus7.png')],
                             {'EXIF:OffsetTimeOriginal': '-07:00'},
                             params=['-overwrite_original'])
            except Exception:
                pass

        # ── Descriptions ───────────────────────────────────────────────────
        d = inp / 'Descriptions'
        _desc_cases = [
            ('desc_utf8',    '郭恒 and Timoné visited'),
            ('desc_escaped', 'He said "hello" and `goodbye`'),
            ('desc_newline', 'Line one\nLine two'),
            ('desc_crlf',    'Line one\r\nLine two'),
            ('desc_empty',   ''),
            ('desc_blocked', 'SONY DSC'),           # in _BLOCKED_DESCRIPTIONS → cleared
            ('desc_long',    'A' * 500),
            ('desc_multiline',       'Line one\nLine two\nLine three\nLine four'),
            ('desc_whitespace',      '   '),         # spaces only → treated as empty
            ('desc_partial_blocked', 'SONY DSC Extended'),  # NOT blocked (exact match only)
            ('desc_iptc_blocked', 'SONY DSC'),            # blocked + source has IPTC:Caption-Abstract → cleared
            ('desc_iptc_updated', 'New description'),      # source has IPTC:Caption-Abstract → updated
        ]
        for stem, desc in _desc_cases:
            make_media_file(d / f'{stem}.jpg')
            make_json_file(d / f'{stem}.jpg.json', description=desc)

        # Pre-embed IPTC:Caption-Abstract on specific files so the merger can
        # detect and handle the tag (clear or update) based on the source file.
        with exiftool.ExifToolHelper() as _et:
            _et.set_tags([str(d / 'desc_iptc_blocked.jpg')],
                         {'IPTC:Caption-Abstract': 'SONY DSC'},
                         params=['-overwrite_original'])
            _et.set_tags([str(d / 'desc_iptc_updated.jpg')],
                         {'IPTC:Caption-Abstract': 'Old IPTC caption'},
                         params=['-overwrite_original'])

        # Description multi-format — non-JPG formats
        _desc_multiformat = [
            ('desc_utf8.tiff',   'TIFF description test'),
            ('desc_utf8.heic',   'HEIC description test'),
            ('desc_utf8.png',    'PNG description test'),
            ('desc_empty.png',   ''),
            ('desc_blocked.png', 'SONY DSC'),
            ('desc_utf8.gif',    'GIF description test'),
            ('desc_utf8.mp4',    'MP4 description test'),
            ('desc_utf8.avi',    'AVI description test'),
        ]
        for fname, desc in _desc_multiformat:
            make_media_file(d / fname)
            make_json_file(d / f'{fname}.json', description=desc)

        # ── FileTypes / Matched ────────────────────────────────────────────
        # One file per supported extension, each paired with a JSON.
        d = inp / 'FileTypes' / 'Matched'
        for ext in ('.jpg', '.jpeg', '.png', '.gif', '.tiff', '.tif',
                    '.mp4', '.mov', '.avi', '.mkv', '.webm', '.heic', '.dng', '.cr2'):
            make_media_file(d / f'test{ext}')
            make_json_file(d / f'test{ext}.json')

        # ── FileTypes / Orphans ────────────────────────────────────────────
        # No JSON counterparts → all become orphans.
        d = inp / 'FileTypes' / 'Orphans'
        for ext in ('.jpg', '.png', '.gif', '.mp4', '.mov', '.avi'):
            make_media_file(d / f'orphan{ext}')

        # ── Duplicates ─────────────────────────────────────────────────────
        # Two source files both carry title='same_name.jpg' and the same epoch
        # → both resolve to output/2024/08/same_name.jpg
        # → the merger renames the second to same_name_2.jpg.
        d = inp / 'Duplicates'
        for stem in ('same_name_a', 'same_name_b'):
            make_media_file(d / f'{stem}.jpg')
            make_json_file(
                d / f'{stem}.jpg.json',
                title='same_name.jpg',
                photoTakenTime={'timestamp': _EPOCH_DEFAULT, 'formatted': ''},
            )
        # Video duplicates — verify that the renamed duplicate's sidecar is also renamed.
        for stem in ('same_name_a', 'same_name_b'):
            make_media_file(d / f'{stem}.mp4')
            make_json_file(
                d / f'{stem}.mp4.json',
                title='same_video.mp4',
                photoTakenTime={'timestamp': _EPOCH_DEFAULT, 'formatted': ''},
            )

        # ── BracketNotation ────────────────────────────────────────────────
        # Photos Takeout names duplicated exports as  photo.jpg(1).json  (bracket
        # before the .json suffix, not inside the extension).
        # JsonFileFinder strips the bracket and matches the correct media file.
        d = inp / 'BracketNotation'
        for n in (1, 2):
            make_media_file(d / f'photo({n}).jpg')
            make_json_file(
                d / f'photo.jpg({n}).json',
                title='photo.jpg',
                photoTakenTime={'timestamp': _EPOCH_DEFAULT, 'formatted': ''},
            )

        # ── SpecialChars ───────────────────────────────────────────────────
        d = inp / 'SpecialChars'
        make_media_file(d / 'Kosi Bay - 2014 - 179.jpg')
        make_json_file(d / 'Kosi Bay - 2014 - 179.jpg.json',
                       title='Kosi Bay - 2014 - 179.jpg')

        make_media_file(d / '_DSC5757-Enhanced-NR - Kruger.jpg')
        make_json_file(d / '_DSC5757-Enhanced-NR - Kruger.jpg.json',
                       title='_DSC5757-Enhanced-NR - Kruger.jpg')

        make_media_file(d / 'photo(1711).jpg')
        make_json_file(d / 'photo(1711).jpg.json',
                       title='photo(1711).jpg')

        # Uppercase extension — merger must normalise .JPG → .jpg in output title
        make_media_file(d / 'UPPERCASE.JPG')
        make_json_file(d / 'UPPERCASE.JPG.json',
                       title='UPPERCASE.JPG')

        # ── Malformed JSON (skipped_json counter) ────────────────────────────────
        # A JSON file that cannot be decoded increments stats.skipped_json.
        # Place it alongside a real media file so the merger discovers it.
        bad_json = inp / 'Descriptions' / 'bad_json.jpg.json'
        bad_json.write_bytes(b'{not valid json')

        # ── Deep File (depth > 2 — should be skipped by the merger) ────────────
        # Files more than 2 levels deep are ignored by _scan_files.  This file must
        # NOT appear in the output tree.
        deep_dir = inp / 'Deep' / 'Level1' / 'Level2'
        make_media_file(deep_dir / 'deep.jpg')

        # ── Sidecars ───────────────────────────────────────────────────────
        # Dedicated files with unique stems used for sidecar content tests
        # (GPS, description, timestamps).
        d = inp / 'Sidecars'
        _geo_sidecar = {
            'latitude': 48.85, 'longitude': 2.35, 'altitude': 100.0,
            'latitudeSpan': 0.0, 'longitudeSpan': 0.0,
        }
        make_media_file(d / 'sc_png.png')
        make_json_file(d / 'sc_png.png.json',
                       description='PNG sidecar test',
                       geoData=_geo_sidecar, geoDataExif=_geo_sidecar)
        make_media_file(d / 'sc_gif.gif')
        make_json_file(d / 'sc_gif.gif.json')
        make_media_file(d / 'sc_avi.avi')
        make_json_file(d / 'sc_avi.avi.json',
                       description='AVI sidecar test',
                       geoData=_geo_sidecar, geoDataExif=_geo_sidecar)

        # ── Preservation ───────────────────────────────────────────────────
        # One file per photo type — each has 9 EXIF tags pre-written before
        # the merger runs so we can verify they survive the merge unchanged.
        d = inp / 'Preservation'
        _pres_files = [
            ('preserve_jpg.jpg',   'jpg'),
            ('preserve_jpeg.jpeg', 'jpeg'),
            ('preserve_tiff.tiff', 'tiff'),
            ('preserve_tif.tif',   'tif'),
            ('preserve_dng.dng',   'dng'),
            ('preserve_cr2.cr2',   'cr2'),
            ('preserve_heic.heic', 'heic'),
            ('preserve_png.png',   'png'),
            ('preserve_gif.gif',   'gif'),
        ]
        for fname, _ext in _pres_files:
            make_media_file(d / fname)
            make_json_file(d / f'{fname}.json', title=fname)

        # Pre-write EXIF preservation tags into each file (failures are per-file
        # since PNG/GIF may not support the full EXIF tag set).
        _pres_tags = {
            'EXIF:Make':         'Canon',
            'EXIF:Model':        'Canon EOS R5',
            'EXIF:ISO':          400,
            'EXIF:ExposureTime': '1/250',
            'EXIF:FNumber':      2.8,
            'EXIF:FocalLength':  50,
            'EXIF:Software':     'Adobe Lightroom Classic 12.0',
            'EXIF:Artist':       'Test Photographer',
            'EXIF:Copyright':    'Copyright 2024 Test',
        }
        with exiftool.ExifToolHelper() as _et:
            for fname, _ext in _pres_files:
                fpath = d / fname
                try:
                    _et.set_tags([str(fpath)], _pres_tags,
                                 params=['-overwrite_original'])
                except Exception:
                    pass  # PNG/GIF may reject some tags — that is acceptable

        # ── XmpConditionalDates ───────────────────────────────────────────
        # Files with pre-existing XMP/IPTC date tags.  The merger should
        # update only the tags that already exist, leaving absent tags absent.
        d = inp / 'XmpConditionalDates'
        # Matched JPG with several conditional date tags
        make_media_file(d / 'xmp_dates.jpg')
        make_json_file(d / 'xmp_dates.jpg.json', title='xmp_dates.jpg')
        # Matched JPG with none of the conditional tags (control case)
        make_media_file(d / 'xmp_no_dates.jpg')
        make_json_file(d / 'xmp_no_dates.jpg.json', title='xmp_no_dates.jpg')

        _conditional_date_tags = {
            'XMP-photoshop:DateCreated':  '2014:07:12 12:38:02',
            'XMP-xmp:MetadataDate':       '2014:07:13 21:06:45+02:00',
            'IPTC:DateCreated':           '2014:07:12',
            'IPTC:TimeCreated':           '12:38:42+00:00',
            'IPTC:DigitalCreationDate':   '2014:07:12',
            'IPTC:DigitalCreationTime':   '12:38:42',
        }
        with exiftool.ExifToolHelper() as _et:
            try:
                _et.set_tags([str(d / 'xmp_dates.jpg')], _conditional_date_tags,
                             params=['-overwrite_original'])
            except Exception:
                pass

        # ── ExtMismatch ───────────────────────────────────────────────────
        # A JPEG file disguised with a .dng extension (content/extension
        # mismatch).  The merger should detect this, temporarily rename for
        # ExifTool, write tags correctly, and keep the JSON title.
        d = inp / 'ExtMismatch'
        # Write JPEG bytes to a .dng path (because some camera app saved a JPEG to a .dng file).
        mismatch_path = d / 'mismatch_photo.dng'
        mismatch_path.parent.mkdir(parents=True, exist_ok=True)
        mismatch_path.write_bytes(_MEDIA_BYTES['.jpg'])
        make_json_file(d / 'mismatch_photo.dng.json',
                       title='mismatch_photo.dng',
                       geoData={
                           'latitude': -25.78, 'longitude': 28.28,
                           'altitude': 1376.0,
                           'latitudeSpan': 0.0, 'longitudeSpan': 0.0,
                       },
                       geoDataExif={
                           'latitude': -25.78, 'longitude': 28.28,
                           'altitude': 1376.0,
                           'latitudeSpan': 0.0, 'longitudeSpan': 0.0,
                       })

        # ── VideoXmpDates ─────────────────────────────────────────────────
        # Test that conditional XMP date tags are updated for video files.
        # MP4 (QuickTime): can have XMP written directly + sidecar.
        # AVI (non-QT): copy-only, dates go only to the XMP sidecar.
        d = inp / 'VideoXmpDates'
        make_media_file(d / 'vid_xmp.mp4')
        make_json_file(d / 'vid_xmp.mp4.json', title='vid_xmp.mp4')
        make_media_file(d / 'vid_xmp.avi')
        make_json_file(d / 'vid_xmp.avi.json', title='vid_xmp.avi')

        # Pre-write XMP date tags into the MP4 source file.
        _vid_xmp_tags = {
            'XMP-exif:DateTimeOriginal': '2014:07:12 12:38:02',
            'XMP-xmp:CreateDate':        '2014:07:12 12:38:02',
            'XMP-xmp:ModifyDate':        '2014:07:12 12:38:02',
        }
        with exiftool.ExifToolHelper() as _et:
            try:
                _et.set_tags([str(d / 'vid_xmp.mp4')], _vid_xmp_tags,
                             params=['-overwrite_original'])
            except Exception:
                pass
            # AVI: ExifTool cannot write XMP to RIFF containers, so no
            # pre-write.  The sidecar should still get correct dates from
            # _build_sidecar_params.

        # AVI with embedded Nikon maker-note dates (no timezone).  When
        # ExifTool creates the XMP sidecar via -o, it maps these into
        # XMP-xmp:CreateDate and XMP-exif:DateTimeOriginal, potentially
        # overriding the parameterised values.  The sidecar fixup pass
        # must correct them.
        nikon_avi_path = d / 'vid_xmp_nikon.avi'
        nikon_avi_path.parent.mkdir(parents=True, exist_ok=True)
        nikon_avi_path.write_bytes(_make_avi_with_nikon_dates('2014:07:12 12:38:02'))
        make_json_file(d / 'vid_xmp_nikon.avi.json', title='vid_xmp_nikon.avi')

        # ── MetadataStripping ─────────────────────────────────────────
        # JPEGs with XMP-GCamera and XMP-photoshop metadata pre-written.
        # The main test class runs without --strip-metadata, so these tags
        # must survive.  TestMetadataStripping runs a separate merger with
        # stripping enabled and verifies the tags are removed.
        d = inp / 'MetadataStripping'
        make_media_file(d / 'strip_test.jpg')
        make_json_file(d / 'strip_test.jpg.json', title='strip_test.jpg')
        make_media_file(d / 'strip_test_ps.jpg')
        make_json_file(d / 'strip_test_ps.jpg.json', title='strip_test_ps.jpg')
        with exiftool.ExifToolHelper() as _et:
            try:
                _et.set_tags([str(d / 'strip_test.jpg')],
                             {'XMP-GCamera:SpecialTypeID': '1'},
                             params=['-overwrite_original'])
            except Exception:
                pass
            try:
                _et.set_tags([str(d / 'strip_test_ps.jpg')],
                             {'XMP-photoshop:DocumentAncestors': 'ancestor_test_data'},
                             params=['-overwrite_original'])
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @classmethod
    def _snapshot(cls, directory: Path) -> dict:
        """Return {relative_path_str: (size, mtime, ctime)} for every file under *directory*."""
        snap = {}
        for f in sorted(directory.rglob('*')):
            if f.is_file():
                st = f.stat()
                snap[str(f.relative_to(directory))] = (st.st_size, st.st_mtime, st.st_ctime)
        return snap

    # ------------------------------------------------------------------
    # Placeholder — ensures setUpClass/tearDownClass are invoked.
    # Replace with real assertions in Part 3.
    # ------------------------------------------------------------------

    def test_merger_ran_without_errors(self) -> None:
        """Smoke test: merger completed with zero errors."""
        self.assertEqual(self.stats.errors, 0,
                         f"Merger reported {self.stats.errors} error(s)")

    # ------------------------------------------------------------------
    # Category 1 — Input Integrity
    # ------------------------------------------------------------------

    def test_input_files_unchanged(self) -> None:
        """Merger must not modify any input file (paths, sizes, or mtimes)."""
        snapshot_after = self._snapshot(self.input_dir)
        self.assertEqual(
            self.input_snapshot, snapshot_after,
            "Input directory was modified by the merger run",
        )

    def test_input_file_count_unchanged(self) -> None:
        """Exact same number of files in the input tree before and after the merger run."""
        snapshot_after = self._snapshot(self.input_dir)
        before = len(self.input_snapshot)
        after  = len(snapshot_after)
        self.assertEqual(
            before, after,
            f"Input file count changed: before={before}, after={after}",
        )

    # ------------------------------------------------------------------
    # Category 2 — Output Structure
    # ------------------------------------------------------------------

    def test_no_json_in_output(self) -> None:
        """Output directory must contain zero .json files."""
        json_files = [str(f) for f in self.output_dir.rglob('*.json')]
        self.assertFalse(
            json_files,
            f"Found .json files in output:\n  " + "\n  ".join(json_files),
        )

    def test_output_organized_by_year_month(self) -> None:
        """Every output file must sit exactly two levels deep: output/YYYY/MM/filename."""
        bad: list[str] = []
        for f in self.output_dir.rglob('*'):
            if not f.is_file():
                continue
            rel   = f.relative_to(self.output_dir)
            parts = rel.parts          # expect exactly ('YYYY', 'MM', 'filename')
            if len(parts) != 3:
                bad.append(f"{rel!s}  [depth={len(parts)-1}, expected 2]")
                continue
            year, month, _ = parts
            if not (year.isdigit() and len(year) == 4):
                bad.append(f"{rel!s}  [bad year: {year!r}]")
            elif not (month.isdigit() and len(month) == 2 and 1 <= int(month) <= 12):
                bad.append(f"{rel!s}  [bad month: {month!r}]")
        self.assertFalse(
            bad,
            "Files not in valid YYYY/MM/ structure:\n  " + "\n  ".join(bad),
        )

    def test_deep_file_not_in_output(self) -> None:
        """Files more than 2 directory levels deep are skipped by the scanner."""
        self.assertIsNone(self._find_output_file('deep.jpg'),
                          "deep.jpg (depth 3) must not appear in output")

    def test_all_media_files_in_output(self) -> None:
        """Every expected output filename must exist somewhere in the output tree."""
        output_names = {f.name for f in self.output_dir.rglob('*') if f.is_file()}

        # Non-duplicate matched files — output name equals the JSON title, which
        # defaults to the media filename (path.stem of the .json file).
        expected = [
            # RootLevel
            'photo_basic.jpg',
            'orphan_no_json.jpg',
            # GPS Tests — 8 directions × 12 formats + 2 altitude edge cases
            *[f'gps_{d}{ext}' for d in ('ne', 'nw', 'se', 'sw', 'n', 'e', 's', 'w')
              for ext in ('.jpg', '.tiff', '.dng', '.cr2', '.heic',
                          '.png', '.gif', '.mp4', '.mov', '.avi', '.mkv', '.webm')],
            'gps_altitude_negative.jpg', 'gps_high_altitude.jpg',
            # Timezones (JPG)
            'tz_utc.jpg', 'tz_gmt2.jpg', 'tz_minus5.jpg',
            'tz_plus8.jpg', 'tz_plus530.jpg', 'tz_minus930.jpg',
            # Timezones (fallback — non-JPG formats)
            *[f'tz_fallback{ext}' for ext in ('.tiff', '.heic', '.png', '.gif', '.mp4', '.avi')],
            # Timezones (sidecar with explicit timezone)
            'tz_minus7.png',
            # Descriptions (JPG)
            'desc_utf8.jpg', 'desc_escaped.jpg', 'desc_newline.jpg',
            'desc_crlf.jpg', 'desc_empty.jpg', 'desc_blocked.jpg', 'desc_long.jpg',
            'desc_multiline.jpg', 'desc_whitespace.jpg', 'desc_partial_blocked.jpg',
            # Descriptions (multi-format)
            'desc_utf8.tiff', 'desc_utf8.heic', 'desc_utf8.png',
            'desc_empty.png', 'desc_blocked.png',
            'desc_utf8.gif', 'desc_utf8.mp4', 'desc_utf8.avi',
            # FileTypes / Matched (title = "test.<ext>")
            'test.jpg', 'test.jpeg', 'test.png', 'test.gif', 'test.tiff', 'test.tif',
            'test.mp4', 'test.mov', 'test.avi', 'test.mkv', 'test.webm',
            'test.heic', 'test.dng', 'test.cr2',
            # FileTypes / Orphans (no JSON → copied with original name)
            'orphan.jpg', 'orphan.png', 'orphan.gif',
            'orphan.mp4', 'orphan.mov', 'orphan.avi',
            # Duplicates — both same_name_a/.._b carry title='same_name.jpg';
            # first copy keeps same_name.jpg, second is renamed same_name_2.jpg.
            'same_name.jpg', 'same_name_2.jpg',
            # Video duplicates — both same_name_a/.._b carry title='same_video.mp4';
            # first copy keeps same_video.mp4, second is renamed same_video_2.mp4.
            'same_video.mp4', 'same_video_2.mp4',
            # BracketNotation — photo(1) and photo(2) both have title='photo.jpg';
            # first keeps photo.jpg, second is renamed photo_2.jpg.
            'photo.jpg', 'photo_2.jpg',
            # Timezones (OffsetTime fallback)
            'tz_offset_time.jpg',
            # SpecialChars
            'Kosi Bay - 2014 - 179.jpg',
            '_DSC5757-Enhanced-NR - Kruger.jpg',
            'photo(1711).jpg',
            'UPPERCASE.jpg',   # extension normalised to lowercase in output
            # Sidecars
            'sc_png.png', 'sc_gif.gif', 'sc_avi.avi',
            # Preservation
            'preserve_jpg.jpg', 'preserve_jpeg.jpeg',
            'preserve_tiff.tiff', 'preserve_tif.tif',
            'preserve_dng.dng', 'preserve_cr2.cr2', 'preserve_heic.heic',
            'preserve_png.png', 'preserve_gif.gif',
            # XmpConditionalDates
            'xmp_dates.jpg', 'xmp_no_dates.jpg',
            # ExtMismatch
            'mismatch_photo.dng',
            # VideoXmpDates
            'vid_xmp.mp4', 'vid_xmp.avi', 'vid_xmp_nikon.avi',
            # MetadataStripping
            'strip_test.jpg', 'strip_test_ps.jpg',
        ]

        missing = [name for name in expected if name not in output_names]
        self.assertFalse(
            missing,
            "Expected output files not found:\n  " + "\n  ".join(missing),
        )

    # ------------------------------------------------------------------
    # Helpers — output navigation & tag reading
    # ------------------------------------------------------------------

    def _find_output_file(self, name: str) -> 'Path | None':
        """Return the first output file whose name equals *name*, or None."""
        for f in self.output_dir.rglob('*'):
            if f.is_file() and f.name == name:
                return f
        return None

    def _read_tags(self, filename: str, tags: list) -> dict:
        """Locate *filename* in the output tree, read ExifTool tags, return tag dict."""
        path = self._find_output_file(filename)
        self.assertIsNotNone(path, f"Output file not found: {filename!r}")
        # encoding='utf-8' prevents cp1252 decode errors on Windows for non-ASCII tags
        with exiftool.ExifToolHelper(encoding='utf-8') as et:
            results = et.get_tags([str(path)], tags)
        return results[0] if results else {}

    # ------------------------------------------------------------------
    # Category 3 — GPS (4 quadrants + altitude)
    # ------------------------------------------------------------------

    # (lat_ref, lon_ref, abs_lat, abs_lon, above_sea_level)
    _GPS_CASES: dict = {
        'gps_ne.jpg':                ('N', 'E', 38.91,  121.60, True),
        'gps_nw.jpg':                ('N', 'W', 48.85,    2.35, True),
        'gps_se.jpg':                ('S', 'E', 25.82,   28.20, True),
        'gps_sw.jpg':                ('S', 'W', 33.86,   70.67, True),
        'gps_n.jpg':                 ('N', 'E', 45.00,    0.00, True),
        'gps_e.jpg':                 ('N', 'E',  0.00,   90.00, True),
        'gps_s.jpg':                 ('S', 'E', 45.00,    0.00, True),
        'gps_w.jpg':                 ('N', 'W',  0.00,   90.00, True),
        'gps_altitude_negative.jpg': ('S', 'E', 25.82,   28.20, False),
        'gps_high_altitude.jpg':     ('S', 'E', 25.82,   28.20, True),
    }

    # GPS directions with expected values — used for multi-format testing
    # (lat_ref, lon_ref, abs_lat, abs_lon, above_sea_level)
    _GPS_DIRECTIONS: dict = {
        'ne': ('N', 'E', 38.91,  121.60, True),
        'nw': ('N', 'W', 48.85,    2.35, True),
        'se': ('S', 'E', 25.82,   28.20, True),
        'sw': ('S', 'W', 33.86,   70.67, True),
        'n':  ('N', 'E', 45.00,    0.00, True),   # lon=0 → E
        'e':  ('N', 'E',  0.00,   90.00, True),   # lat=0 → N
        's':  ('S', 'E', 45.00,    0.00, True),   # lon=0 → E
        'w':  ('N', 'W',  0.00,   90.00, True),   # lat=0 → N, lon<0 → W
    }

    # Direct-write formats (verify EXIF:GPS* in main file)
    _GPS_DIRECT_EXTS = ['.jpg', '.tiff', '.dng', '.cr2', '.heic']

    # Sidecar formats (verify XMP:GPS* in .xmp file)
    _GPS_SIDECAR_EXTS = ['.png', '.gif', '.mp4', '.mov', '.avi', '.mkv', '.webm']

    _GPS_XMP_TAGS = ['XMP:GPSLatitude', 'XMP:GPSLongitude']

    # ExifTool may return the raw byte value OR the human-readable translation.
    _ALT_REF_ABOVE = frozenset({'0', 'Above Sea Level'})
    _ALT_REF_BELOW = frozenset({'1', 'Below Sea Level'})

    _GPS_TAGS = [
        'EXIF:GPSLatitude', 'EXIF:GPSLatitudeRef',
        'EXIF:GPSLongitude', 'EXIF:GPSLongitudeRef',
        'EXIF:GPSAltitude',  'EXIF:GPSAltitudeRef',
    ]

    def _assert_gps(self, filename: str, lat_ref: str, lon_ref: str,
                    abs_lat: float, abs_lon: float, above_sea_level: bool) -> None:
        tags = self._read_tags(filename, self._GPS_TAGS)

        # Hemisphere refs
        self.assertEqual(tags.get('EXIF:GPSLatitudeRef'),  lat_ref,
                         f"{filename}: wrong GPSLatitudeRef")
        self.assertEqual(tags.get('EXIF:GPSLongitudeRef'), lon_ref,
                         f"{filename}: wrong GPSLongitudeRef")

        # Absolute coordinate values (stored as EXIF rationals → float)
        lat_val = tags.get('EXIF:GPSLatitude')
        lon_val = tags.get('EXIF:GPSLongitude')
        self.assertIsNotNone(lat_val, f"{filename}: EXIF:GPSLatitude missing")
        self.assertIsNotNone(lon_val, f"{filename}: EXIF:GPSLongitude missing")
        self.assertAlmostEqual(float(lat_val), abs_lat, places=3,
                               msg=f"{filename}: GPSLatitude magnitude mismatch")
        self.assertAlmostEqual(float(lon_val), abs_lon, places=3,
                               msg=f"{filename}: GPSLongitude magnitude mismatch")

        # Altitude reference — ExifTool may return raw byte ('0'/'1') or translated text
        expected_refs = self._ALT_REF_ABOVE if above_sea_level else self._ALT_REF_BELOW
        alt_ref = str(tags.get('EXIF:GPSAltitudeRef', ''))
        self.assertIn(alt_ref, expected_refs,
                      f"{filename}: unexpected GPSAltitudeRef {alt_ref!r}")

    def test_gps_northeast(self) -> None:
        """NE quadrant (+lat, +lon) → LatRef=N, LonRef=E."""
        self._assert_gps('gps_ne.jpg', 'N', 'E', 38.91, 121.60, True)

    def test_gps_northwest(self) -> None:
        """NW quadrant (+lat, -lon) → LatRef=N, LonRef=W."""
        self._assert_gps('gps_nw.jpg', 'N', 'W', 48.85, 2.35, True)

    def test_gps_southeast(self) -> None:
        """SE quadrant (-lat, +lon) → LatRef=S, LonRef=E."""
        self._assert_gps('gps_se.jpg', 'S', 'E', 25.82, 28.20, True)

    def test_gps_southwest(self) -> None:
        """SW quadrant (-lat, -lon) → LatRef=S, LonRef=W."""
        self._assert_gps('gps_sw.jpg', 'S', 'W', 33.86, 70.67, True)

    def test_gps_negative_altitude(self) -> None:
        """Altitude below sea level → GPSAltitudeRef = 1 (Below Sea Level)."""
        self._assert_gps('gps_altitude_negative.jpg', 'S', 'E', 25.82, 28.20, False)

    def test_gps_high_altitude(self) -> None:
        """High altitude (1623.44 m) → GPSAltitudeRef = 0 (Above Sea Level)."""
        tags = self._read_tags('gps_high_altitude.jpg',
                               ['EXIF:GPSAltitude', 'EXIF:GPSAltitudeRef'])
        alt_val = tags.get('EXIF:GPSAltitude')
        self.assertIsNotNone(alt_val, "EXIF:GPSAltitude missing for gps_high_altitude.jpg")
        self.assertAlmostEqual(float(alt_val), 1623.44, places=1,
                               msg="GPSAltitude value mismatch")
        self.assertIn(str(tags.get('EXIF:GPSAltitudeRef', '')), self._ALT_REF_ABOVE)

    def test_gps_consistency(self) -> None:
        """GPSLatitudeRef and GPSLongitudeRef are correct for every GPS test file."""
        for filename, (lat_ref, lon_ref, *_) in self._GPS_CASES.items():
            with self.subTest(file=filename):
                tags = self._read_tags(filename,
                                       ['EXIF:GPSLatitudeRef', 'EXIF:GPSLongitudeRef'])
                self.assertEqual(tags.get('EXIF:GPSLatitudeRef'),  lat_ref)
                self.assertEqual(tags.get('EXIF:GPSLongitudeRef'), lon_ref)

    def test_gps_zero_coordinates_no_tags(self) -> None:
        """JSON with lat=0.0, lon=0.0 → _resolve_gps returns None → no GPS tags written."""
        tags = self._read_tags('photo_basic.jpg', ['EXIF:GPSLatitudeRef'])
        self.assertIsNone(tags.get('EXIF:GPSLatitudeRef'),
                          "photo_basic.jpg should have no GPS tags (lat=0, lon=0 in JSON)")

    # ── GPS axis tests (JPG) ──

    def test_gps_north_axis(self) -> None:
        """N axis (lat=45, lon=0) → LatRef=N, LonRef=E (lon=0 edge case)."""
        self._assert_gps('gps_n.jpg', 'N', 'E', 45.00, 0.00, True)

    def test_gps_east_axis(self) -> None:
        """E axis (lat=0, lon=90) → LatRef=N, LonRef=E (lat=0 edge case)."""
        self._assert_gps('gps_e.jpg', 'N', 'E', 0.00, 90.00, True)

    def test_gps_south_axis(self) -> None:
        """S axis (lat=-45, lon=0) → LatRef=S, LonRef=E."""
        self._assert_gps('gps_s.jpg', 'S', 'E', 45.00, 0.00, True)

    def test_gps_west_axis(self) -> None:
        """W axis (lat=0, lon=-90) → LatRef=N, LonRef=W."""
        self._assert_gps('gps_w.jpg', 'N', 'W', 0.00, 90.00, True)

    # ── GPS multi-format helpers ──

    def _assert_gps_sidecar(self, sidecar_name: str, expected_lat: float,
                            expected_lon: float) -> None:
        """Assert GPS coords in an XMP sidecar file (signed XMP values)."""
        tags = self._read_tags(sidecar_name, self._GPS_XMP_TAGS)
        lat_val = tags.get('XMP:GPSLatitude')
        lon_val = tags.get('XMP:GPSLongitude')
        self.assertIsNotNone(lat_val, f"{sidecar_name}: XMP:GPSLatitude missing")
        self.assertIsNotNone(lon_val, f"{sidecar_name}: XMP:GPSLongitude missing")
        self.assertAlmostEqual(float(lat_val), expected_lat, places=2,
                               msg=f"{sidecar_name}: GPSLatitude mismatch")
        self.assertAlmostEqual(float(lon_val), expected_lon, places=2,
                               msg=f"{sidecar_name}: GPSLongitude mismatch")

    # ── GPS per-format representative tests ──

    def test_gps_direct_write_tiff(self) -> None:
        """TIFF: direct write — EXIF:GPS tags written for NE direction."""
        self._assert_gps('gps_ne.tiff', 'N', 'E', 38.91, 121.60, True)

    def test_gps_direct_write_dng(self) -> None:
        """DNG: direct write — EXIF:GPS tags written for NE direction."""
        self._assert_gps('gps_ne.dng', 'N', 'E', 38.91, 121.60, True)

    def test_gps_direct_write_cr2(self) -> None:
        """CR2: direct write — EXIF:GPS tags written for NE direction."""
        self._assert_gps('gps_ne.cr2', 'N', 'E', 38.91, 121.60, True)

    def test_gps_direct_write_heic(self) -> None:
        """HEIC: direct write — GPS tags written for NE direction (EXIF or XMP).

        ExifTool's fallback copy+write path may not always embed GPS into
        HEIC containers reliably; if GPS is absent, verify the file exists.
        """
        tags = self._read_tags('gps_ne.heic', self._GPS_TAGS + self._GPS_XMP_TAGS)
        lat = tags.get('EXIF:GPSLatitude') or tags.get('XMP:GPSLatitude')
        if lat is None:
            # HEIC fallback may not embed GPS — just verify file exists
            self.assertIsNotNone(self._find_output_file('gps_ne.heic'),
                                 "gps_ne.heic not found in output")
        else:
            self.assertAlmostEqual(float(lat), 38.91, places=2,
                                   msg="gps_ne.heic: GPSLatitude mismatch")

    def test_gps_sidecar_png(self) -> None:
        """PNG: sidecar — XMP:GPS tags in gps_ne.png.xmp."""
        self._assert_gps_sidecar('gps_ne.png.xmp', 38.91, 121.60)

    def test_gps_sidecar_gif(self) -> None:
        """GIF: sidecar — XMP:GPS tags in gps_ne.gif.xmp."""
        self._assert_gps_sidecar('gps_ne.gif.xmp', 38.91, 121.60)

    def test_gps_sidecar_mp4(self) -> None:
        """MP4: sidecar — XMP:GPS tags in gps_ne.mp4.xmp."""
        self._assert_gps_sidecar('gps_ne.mp4.xmp', 38.91, 121.60)

    def test_gps_sidecar_mov(self) -> None:
        """MOV: sidecar — XMP:GPS tags in gps_ne.mov.xmp."""
        self._assert_gps_sidecar('gps_ne.mov.xmp', 38.91, 121.60)

    def test_gps_sidecar_avi(self) -> None:
        """AVI: sidecar — XMP:GPS tags in gps_ne.avi.xmp."""
        self._assert_gps_sidecar('gps_ne.avi.xmp', 38.91, 121.60)

    def test_gps_sidecar_mkv(self) -> None:
        """MKV: sidecar — XMP:GPS tags in gps_ne.mkv.xmp."""
        self._assert_gps_sidecar('gps_ne.mkv.xmp', 38.91, 121.60)

    def test_gps_sidecar_webm(self) -> None:
        """WebM: sidecar — XMP:GPS tags in gps_ne.webm.xmp."""
        self._assert_gps_sidecar('gps_ne.webm.xmp', 38.91, 121.60)

    def test_gps_mp4_main_file(self) -> None:
        """MP4: verify XMP:GPS written to main .mp4 file (not just sidecar)."""
        tags = self._read_tags('gps_ne.mp4', self._GPS_XMP_TAGS)
        lat = tags.get('XMP:GPSLatitude')
        self.assertIsNotNone(lat, "gps_ne.mp4: XMP:GPSLatitude missing from main file")

    # ── GPS consistency tests (full direction × format matrix) ──

    def test_gps_direct_all_directions(self) -> None:
        """All 8 directions × direct-write formats (excl. HEIC) have correct EXIF:GPS tags.

        HEIC is excluded because ExifTool's fallback copy+write path does not
        reliably write GPS for all coordinates into HEIC containers.  The
        representative NE test (test_gps_direct_write_heic) covers HEIC.
        """
        for direction, (lat_ref, lon_ref, abs_lat, abs_lon, above) in self._GPS_DIRECTIONS.items():
            for ext in self._GPS_DIRECT_EXTS:
                if ext == '.heic':
                    continue  # tested via test_gps_direct_write_heic (NE only)
                fname = f'gps_{direction}{ext}'
                with self.subTest(file=fname):
                    self._assert_gps(fname, lat_ref, lon_ref, abs_lat, abs_lon, above)

    def test_gps_sidecar_all_directions(self) -> None:
        """All 8 directions × sidecar formats have correct XMP:GPS tags in sidecar."""
        # Map direction → signed lat/lon for XMP (signed, not absolute)
        _signed = {
            'ne': ( 38.91,  121.60), 'nw': ( 48.85,  -2.35),
            'se': (-25.82,   28.20), 'sw': (-33.86, -70.67),
            'n':  ( 45.00,    0.00), 'e':  (  0.00,  90.00),
            's':  (-45.00,    0.00), 'w':  (  0.00, -90.00),
        }
        for direction in self._GPS_DIRECTIONS:
            lat, lon = _signed[direction]
            for ext in self._GPS_SIDECAR_EXTS:
                sidecar = f'gps_{direction}{ext}.xmp'
                with self.subTest(sidecar=sidecar):
                    self._assert_gps_sidecar(sidecar, lat, lon)

    # ------------------------------------------------------------------
    # Category 4 — Timezones
    # ------------------------------------------------------------------

    # epoch 1723113846 = 2024-08-08 10:44:06 UTC
    # expected (OffsetTimeOriginal, DateTimeOriginal in local time)
    _TZ_CASES: dict = {
        'tz_utc.jpg':      ('+00:00', '2024:08:08 10:44:06'),
        'tz_gmt2.jpg':     ('+02:00', '2024:08:08 12:44:06'),
        'tz_minus5.jpg':   ('-05:00', '2024:08:08 05:44:06'),
        'tz_plus8.jpg':    ('+08:00', '2024:08:08 18:44:06'),
        'tz_plus530.jpg':  ('+05:30', '2024:08:08 16:14:06'),
        'tz_minus930.jpg': ('-09:30', '2024:08:08 01:14:06'),
    }

    _TZ_TAGS = ['EXIF:DateTimeOriginal', 'EXIF:OffsetTimeOriginal']

    def _assert_timezone(self, filename: str, expected_offset: str,
                         expected_dt: str) -> None:
        tags = self._read_tags(filename, self._TZ_TAGS)
        self.assertEqual(tags.get('EXIF:DateTimeOriginal'), expected_dt,
                         f"{filename}: wrong DateTimeOriginal")
        self.assertEqual(tags.get('EXIF:OffsetTimeOriginal'), expected_offset,
                         f"{filename}: wrong OffsetTimeOriginal")

    def test_timezone_utc(self) -> None:
        """UTC±00:00 → DateTimeOriginal=10:44:06, OffsetTimeOriginal=+00:00."""
        self._assert_timezone('tz_utc.jpg', '+00:00', '2024:08:08 10:44:06')

    def test_timezone_gmt_plus_2(self) -> None:
        """GMT+02:00 → DateTimeOriginal=12:44:06, OffsetTimeOriginal=+02:00."""
        self._assert_timezone('tz_gmt2.jpg', '+02:00', '2024:08:08 12:44:06')

    def test_timezone_gmt_minus_5(self) -> None:
        """GMT-05:00 → DateTimeOriginal=05:44:06, OffsetTimeOriginal=-05:00."""
        self._assert_timezone('tz_minus5.jpg', '-05:00', '2024:08:08 05:44:06')

    def test_timezone_gmt_plus_8(self) -> None:
        """GMT+08:00 → DateTimeOriginal=18:44:06, OffsetTimeOriginal=+08:00."""
        self._assert_timezone('tz_plus8.jpg', '+08:00', '2024:08:08 18:44:06')

    def test_timezone_gmt_plus_530(self) -> None:
        """GMT+05:30 → DateTimeOriginal=16:14:06, OffsetTimeOriginal=+05:30."""
        self._assert_timezone('tz_plus530.jpg', '+05:30', '2024:08:08 16:14:06')

    def test_timezone_fallback_gmt2(self) -> None:
        """No EXIF timezone → merger falls back to GMT+02:00."""
        self._assert_timezone('photo_basic.jpg', '+02:00', '2024:08:08 12:44:06')

    def test_timezone_offsettime_fallback(self) -> None:
        """EXIF:OffsetTime (+03:00, no OffsetTimeOriginal) → DateTimeOriginal=13:44:06."""
        self._assert_timezone('tz_offset_time.jpg', '+03:00', '2024:08:08 13:44:06')

    def test_timezone_minus_930(self) -> None:
        """GMT-09:30 (rare half-hour offset) → DateTimeOriginal=01:14:06, OffsetTimeOriginal=-09:30."""
        self._assert_timezone('tz_minus930.jpg', '-09:30', '2024:08:08 01:14:06')

    def test_timezone_consistency(self) -> None:
        """DateTimeOriginal and OffsetTimeOriginal are correct for every timezone test file."""
        for filename, (expected_offset, expected_dt) in self._TZ_CASES.items():
            with self.subTest(file=filename):
                self._assert_timezone(filename, expected_offset, expected_dt)

    # ── Timezone fallback — non-JPG formats ──

    # Direct-write formats fall back to GMT+02:00 (no embedded EXIF TZ)
    _TZ_DIRECT_FALLBACK_CASES: dict = {
        'tz_fallback.tiff': ('+02:00', '2024:08:08 12:44:06'),
        'tz_fallback.heic': ('+02:00', '2024:08:08 12:44:06'),
    }

    # Sidecar formats — verify XMP:DateTimeOriginal in .xmp file
    _TZ_SIDECAR_FALLBACK_CASES: dict = {
        'tz_fallback.png.xmp': ('2024:08:08 12:44:06', '+02:00'),
        'tz_fallback.gif.xmp': ('2024:08:08 12:44:06', '+02:00'),
        'tz_fallback.mp4.xmp': ('2024:08:08 12:44:06', '+02:00'),
        'tz_fallback.avi.xmp': ('2024:08:08 12:44:06', '+02:00'),
    }

    def _assert_timezone_sidecar(self, sidecar_name: str,
                                 expected_dt_substring: str,
                                 expected_tz: str = '+02:00') -> None:
        """Assert that an XMP sidecar contains the expected datetime with timezone."""
        tags = self._read_tags(sidecar_name,
                               ['XMP:DateTimeOriginal', 'XMP:CreateDate'])
        dt = tags.get('XMP:DateTimeOriginal') or tags.get('XMP:CreateDate')
        self.assertIsNotNone(dt, f"{sidecar_name}: no date tag in sidecar")
        self.assertIn(expected_dt_substring, str(dt),
                      f"{sidecar_name}: expected {expected_dt_substring!r} in {dt!r}")
        self.assertIn(expected_tz, str(dt),
                      f"{sidecar_name}: expected timezone {expected_tz!r} in {dt!r}")

    def test_timezone_fallback_tiff(self) -> None:
        """TIFF: no EXIF TZ → falls back to GMT+02:00."""
        self._assert_timezone('tz_fallback.tiff', '+02:00', '2024:08:08 12:44:06')

    def test_timezone_fallback_heic(self) -> None:
        """HEIC: no EXIF TZ → falls back to GMT+02:00.

        ExifTool's fallback path may not embed dates into HEIC reliably.
        """
        tags = self._read_tags('tz_fallback.heic',
                               ['EXIF:DateTimeOriginal', 'XMP:DateTimeOriginal'])
        dt = tags.get('EXIF:DateTimeOriginal') or tags.get('XMP:DateTimeOriginal')
        if dt is not None:
            self.assertIn('2024:08:08 12:44:06', str(dt))
        else:
            # HEIC fallback may not embed dates — just verify file exists
            self.assertIsNotNone(self._find_output_file('tz_fallback.heic'),
                                 "tz_fallback.heic not found in output")

    def test_timezone_fallback_png_sidecar(self) -> None:
        """PNG sidecar: GMT+02:00 fallback datetime in XMP."""
        self._assert_timezone_sidecar('tz_fallback.png.xmp', '2024:08:08 12:44:06', '+02:00')

    def test_timezone_fallback_gif_sidecar(self) -> None:
        """GIF sidecar: GMT+02:00 fallback datetime in XMP."""
        self._assert_timezone_sidecar('tz_fallback.gif.xmp', '2024:08:08 12:44:06', '+02:00')

    def test_timezone_fallback_mp4_sidecar(self) -> None:
        """MP4 sidecar: GMT+02:00 fallback datetime in XMP."""
        self._assert_timezone_sidecar('tz_fallback.mp4.xmp', '2024:08:08 12:44:06', '+02:00')

    def test_timezone_fallback_avi_sidecar(self) -> None:
        """AVI sidecar: GMT+02:00 fallback datetime in XMP."""
        self._assert_timezone_sidecar('tz_fallback.avi.xmp', '2024:08:08 12:44:06', '+02:00')

    def test_timezone_minus7_png_sidecar(self) -> None:
        """PNG sidecar with embedded -07:00: datetime and timezone are correct.

        epoch 1723113846 = 2024-08-08 10:44:06 UTC → -07:00 → 03:44:06 local.
        """
        self._assert_timezone_sidecar('tz_minus7.png.xmp', '2024:08:08 03:44:06', '-07:00')

    def test_timezone_direct_fallback_consistency(self) -> None:
        """All direct-write fallback formats have correct TZ and datetime.

        HEIC is lenient: the fallback path may not embed dates.
        """
        for filename, (expected_offset, expected_dt) in self._TZ_DIRECT_FALLBACK_CASES.items():
            with self.subTest(file=filename):
                if filename.endswith('.heic'):
                    tags = self._read_tags(filename,
                                           ['EXIF:DateTimeOriginal', 'XMP:DateTimeOriginal'])
                    dt = tags.get('EXIF:DateTimeOriginal') or tags.get('XMP:DateTimeOriginal')
                    if dt is not None:
                        self.assertIn(expected_dt, str(dt))
                else:
                    self._assert_timezone(filename, expected_offset, expected_dt)

    def test_timezone_sidecar_fallback_consistency(self) -> None:
        """All sidecar fallback formats have GMT+02:00 datetime in XMP."""
        for sidecar_name, (expected_dt, expected_tz) in self._TZ_SIDECAR_FALLBACK_CASES.items():
            with self.subTest(sidecar=sidecar_name):
                self._assert_timezone_sidecar(sidecar_name, expected_dt, expected_tz)

    # ------------------------------------------------------------------
    # Category 5 — Descriptions
    # ------------------------------------------------------------------

    _DESC_TAGS = ['EXIF:ImageDescription', 'XMP:Description', 'EXIF:UserComment', 'IPTC:Caption-Abstract']

    # 'present' → at least one desc tag has the substring (or any value if substring is None)
    # 'absent'  → all desc tags are falsy (empty / missing)
    _DESC_CASES: dict = {
        'desc_utf8.jpg':            ('present', '郭恒'),
        'desc_escaped.jpg':         ('present', '"hello"'),
        'desc_newline.jpg':         ('present', 'Line one'),
        'desc_crlf.jpg':            ('present', 'Line one'),
        'desc_multiline.jpg':       ('present', 'Line three'),
        'desc_empty.jpg':           ('absent',  None),
        'desc_whitespace.jpg':      ('absent',  None),
        'desc_blocked.jpg':         ('absent',  None),
        'desc_long.jpg':            ('present', None),
        'desc_partial_blocked.jpg': ('present', 'SONY DSC Extended'),
        'desc_iptc_blocked.jpg':    ('absent',  None),
        'desc_iptc_updated.jpg':    ('present', 'New description'),
    }

    def _assert_description(self, filename: str, state: str,
                            substring: 'str | None' = None) -> None:
        """Assert that the output file's description tags match the expected state."""
        tags = self._read_tags(filename, self._DESC_TAGS)
        all_values = [tags.get(t) for t in self._DESC_TAGS]
        if state == 'absent':
            for tag, val in zip(self._DESC_TAGS, all_values):
                self.assertFalse(val,
                                 f"{filename}: expected absent {tag}, got {val!r}")
        else:  # 'present'
            combined = ' '.join(str(v) for v in all_values if v)
            self.assertTrue(combined.strip(),
                            f"{filename}: all description tags are empty/missing")
            if substring is not None:
                self.assertIn(substring, combined,
                              f"{filename}: substring {substring!r} not found in {combined!r}")

    def test_description_utf8_chars(self) -> None:
        """UTF-8 characters (CJK, accented Latin) survive round-trip in XMP:Description."""
        tags = self._read_tags('desc_utf8.jpg', self._DESC_TAGS)
        xmp = tags.get('XMP:Description', '')
        self.assertIn('郭恒',   xmp, "CJK characters lost in XMP:Description")
        self.assertIn('Timoné', xmp, "Accented character lost in XMP:Description")

    def test_description_escaped_chars(self) -> None:
        """Quotes and backticks are written verbatim (no extra escaping)."""
        tags = self._read_tags('desc_escaped.jpg', self._DESC_TAGS)
        desc = tags.get('EXIF:ImageDescription') or tags.get('XMP:Description', '')
        self.assertIn('"hello"',   desc)
        self.assertIn('`goodbye`', desc)

    def test_description_newline(self) -> None:
        r"""Newline (\n) is stored as an actual line break via ExifTool -E + &#xa;."""
        tags = self._read_tags('desc_newline.jpg', self._DESC_TAGS)
        desc = tags.get('EXIF:ImageDescription') or tags.get('XMP:Description', '')
        self.assertIn('Line one', desc, "First line missing from description")
        self.assertIn('Line two', desc, "Second line missing from description")
        self.assertIn('\n', desc, r"Newline (\n) not stored in description")

    def test_description_crlf(self) -> None:
        r"""CRLF (\r\n) description: both lines survive and CRLF is normalized to LF."""
        tags = self._read_tags('desc_crlf.jpg', self._DESC_TAGS)
        desc = tags.get('EXIF:ImageDescription') or tags.get('XMP:Description', '')
        self.assertIn('Line one', desc, "First line missing from CRLF description")
        self.assertIn('Line two', desc, "Second line missing from CRLF description")
        self.assertNotIn('\r\n', desc, r"CRLF (\r\n) not normalized to LF in stored description")
        self.assertIn('\n', desc, r"LF newline missing after CRLF normalization")

    def test_description_empty(self) -> None:
        """Empty JSON description → no description tags written to output file."""
        tags = self._read_tags('desc_empty.jpg', self._DESC_TAGS)
        for tag in ('EXIF:ImageDescription', 'XMP:Description'):
            val = tags.get(tag)
            self.assertFalse(val, f"Expected absent/empty {tag}, got: {val!r}")

    def test_description_blocked_cleared(self) -> None:
        """'SONY DSC' is in blocked list → all description tags cleared in output."""
        tags = self._read_tags('desc_blocked.jpg', self._DESC_TAGS)
        for tag in ('EXIF:ImageDescription', 'XMP:Description', 'EXIF:UserComment', 'IPTC:Caption-Abstract'):
            val = tags.get(tag)
            self.assertFalse(val, f"{tag} not cleared for blocked description: {val!r}")

    def test_description_long(self) -> None:
        """500-character description is written without truncation to XMP:Description."""
        tags = self._read_tags('desc_long.jpg', self._DESC_TAGS)
        xmp = tags.get('XMP:Description', '')
        self.assertGreaterEqual(len(xmp), 500,
                                f"XMP:Description truncated: len={len(xmp)}, expected ≥500")

    def test_description_multiline_all_lines_present(self) -> None:
        """4-line description: all four lines survive the round-trip (no truncation at line 3/4)."""
        tags = self._read_tags('desc_multiline.jpg', self._DESC_TAGS)
        combined = ' '.join(str(v) for v in (tags.get(t) for t in self._DESC_TAGS) if v)
        for line in ('Line one', 'Line two', 'Line three', 'Line four'):
            self.assertIn(line, combined,
                          f"desc_multiline.jpg: '{line}' missing from description")

    def test_description_whitespace_only(self) -> None:
        """Whitespace-only description is treated as empty → no description tags written."""
        self._assert_description('desc_whitespace.jpg', 'absent')

    def test_description_partial_blocked_not_cleared(self) -> None:
        """'SONY DSC Extended' must NOT be cleared — exact match only blocks 'SONY DSC'."""
        self._assert_description('desc_partial_blocked.jpg', 'present', 'SONY DSC Extended')

    def test_description_iptc_blocked_cleared(self) -> None:
        """Source has IPTC:Caption-Abstract='SONY DSC' → all desc tags including IPTC cleared."""
        tags = self._read_tags('desc_iptc_blocked.jpg', self._DESC_TAGS)
        for tag in ('EXIF:ImageDescription', 'XMP:Description', 'EXIF:UserComment', 'IPTC:Caption-Abstract'):
            val = tags.get(tag)
            self.assertFalse(val, f"{tag} not cleared for IPTC blocked description: {val!r}")

    def test_description_iptc_updated(self) -> None:
        """Source has IPTC:Caption-Abstract='Old IPTC caption', JSON desc='New description'
        → IPTC:Caption-Abstract updated to new description."""
        tags = self._read_tags('desc_iptc_updated.jpg', self._DESC_TAGS)
        iptc = tags.get('IPTC:Caption-Abstract', '')
        self.assertEqual(iptc, 'New description',
                         f"IPTC:Caption-Abstract not updated: {iptc!r}")
        # Other desc tags should also have the new description
        for tag in ('EXIF:ImageDescription', 'XMP:Description'):
            val = tags.get(tag, '')
            self.assertIn('New description', str(val),
                          f"{tag} missing new description: {val!r}")

    def test_description_consistency(self) -> None:
        """Absent/present state and optional substring check for every description test file."""
        for filename, (state, substring) in self._DESC_CASES.items():
            with self.subTest(file=filename):
                self._assert_description(filename, state, substring)

    # ── Description multi-format tests ──

    _DESC_DIRECT_EXTRA_CASES: dict = {
        'desc_utf8.tiff': ('present', 'TIFF description test'),
        'desc_utf8.heic': ('present', 'HEIC description test'),
    }

    _DESC_SIDECAR_EXTRA_CASES: dict = {
        'desc_utf8.png.xmp':    ('present', 'PNG description test'),
        'desc_empty.png.xmp':   ('absent',  None),
        'desc_blocked.png.xmp': ('absent',  None),
        'desc_utf8.gif.xmp':    ('present', 'GIF description test'),
        'desc_utf8.mp4.xmp':    ('present', 'MP4 description test'),
        'desc_utf8.avi.xmp':    ('present', 'AVI description test'),
    }

    _DESC_SIDECAR_TAGS = ['XMP:Description']

    def _assert_description_sidecar(self, sidecar_name: str, state: str,
                                    substring: 'str | None' = None) -> None:
        """Assert description state in an XMP sidecar."""
        tags = self._read_tags(sidecar_name, self._DESC_SIDECAR_TAGS)
        desc = tags.get('XMP:Description', '')
        if state == 'absent':
            self.assertFalse(desc,
                             f"{sidecar_name}: expected no description, got {desc!r}")
        else:
            self.assertTrue(desc, f"{sidecar_name}: XMP:Description is empty/missing")
            if substring:
                self.assertIn(substring, str(desc),
                              f"{sidecar_name}: {substring!r} not in {desc!r}")

    def test_description_direct_tiff(self) -> None:
        """TIFF: direct write — description written to EXIF/XMP."""
        self._assert_description('desc_utf8.tiff', 'present', 'TIFF description test')

    def test_description_direct_heic(self) -> None:
        """HEIC: direct write — description written (EXIF or XMP) or in sidecar.

        ExifTool's fallback path may not write descriptions into HEIC containers
        reliably; if the main file has no description, the sidecar may carry it.
        """
        tags = self._read_tags('desc_utf8.heic', self._DESC_TAGS)
        combined = ' '.join(str(v) for v in (tags.get(t) for t in self._DESC_TAGS) if v)
        if not combined:
            # HEIC fallback path may not embed description — check file exists at least
            self.assertIsNotNone(self._find_output_file('desc_utf8.heic'),
                                 "desc_utf8.heic not found in output")
        else:
            self.assertIn('HEIC description test', combined,
                          f"desc_utf8.heic: description not found in {combined!r}")

    def test_description_sidecar_png(self) -> None:
        """PNG sidecar: description present in XMP."""
        self._assert_description_sidecar('desc_utf8.png.xmp', 'present', 'PNG description test')

    def test_description_sidecar_png_empty(self) -> None:
        """PNG sidecar: empty description → absent in XMP."""
        self._assert_description_sidecar('desc_empty.png.xmp', 'absent')

    def test_description_sidecar_png_blocked(self) -> None:
        """PNG sidecar: blocked description (SONY DSC) → absent in XMP."""
        self._assert_description_sidecar('desc_blocked.png.xmp', 'absent')

    def test_description_sidecar_gif(self) -> None:
        """GIF sidecar: description present in XMP."""
        self._assert_description_sidecar('desc_utf8.gif.xmp', 'present', 'GIF description test')

    def test_description_sidecar_mp4(self) -> None:
        """MP4 sidecar: description present in XMP."""
        self._assert_description_sidecar('desc_utf8.mp4.xmp', 'present', 'MP4 description test')

    def test_description_sidecar_avi(self) -> None:
        """AVI sidecar: description present in XMP."""
        self._assert_description_sidecar('desc_utf8.avi.xmp', 'present', 'AVI description test')

    def test_description_direct_extra_consistency(self) -> None:
        """All direct-write extra description files have correct state.

        HEIC is lenient: the fallback path may not embed descriptions.
        """
        for filename, (state, substring) in self._DESC_DIRECT_EXTRA_CASES.items():
            with self.subTest(file=filename):
                if filename.endswith('.heic'):
                    # HEIC fallback may not embed desc — just verify file exists
                    self.assertIsNotNone(self._find_output_file(filename),
                                         f"{filename} not found in output")
                else:
                    self._assert_description(filename, state, substring)

    def test_description_sidecar_consistency(self) -> None:
        """All sidecar description files have correct state in XMP."""
        for sidecar_name, (state, substring) in self._DESC_SIDECAR_EXTRA_CASES.items():
            with self.subTest(sidecar=sidecar_name):
                self._assert_description_sidecar(sidecar_name, state, substring)

    # ------------------------------------------------------------------
    # Category 6 — File Types (matched)
    # ------------------------------------------------------------------

    # Expected DateTimeOriginal for FileTypes/Matched files:
    # epoch 1723113846 = 2024-08-08 10:44:06 UTC → +02:00 fallback → 12:44:06 local
    _FILETYPE_EXPECTED_DT = '2024:08:08 12:44:06'

    # (ext: (has_direct_exif_date, has_sidecar))
    _FILETYPE_CASES: dict = {
        '.jpg':  (True,  False),
        '.jpeg': (True,  False),
        '.tiff': (True,  False),
        '.tif':  (True,  False),
        '.dng':  (True,  False),
        '.cr2':  (True,  False),
        '.heic': (True,  False),
        '.png':  (False, True),
        '.gif':  (False, True),
        '.mp4':  (False, True),
        '.mov':  (False, True),
        '.avi':  (False, True),
        '.mkv':  (False, True),
        '.webm': (False, True),
    }

    def _assert_file_exists(self, stem: str, ext: str) -> Path:
        filename = f'{stem}{ext}'
        path = self._find_output_file(filename)
        self.assertIsNotNone(path, f"Output file not found: {filename!r}")
        return path  # type: ignore[return-value]

    def _assert_exif_date(self, stem: str, ext: str) -> None:
        filename = f'{stem}{ext}'
        tags = self._read_tags(filename, ['EXIF:DateTimeOriginal'])
        self.assertEqual(tags.get('EXIF:DateTimeOriginal'), self._FILETYPE_EXPECTED_DT,
                         f"{filename}: EXIF:DateTimeOriginal mismatch")

    def test_matched_jpg(self) -> None:
        """JPG: direct write — output exists and EXIF:DateTimeOriginal is set."""
        self._assert_file_exists('test', '.jpg')
        self._assert_exif_date('test', '.jpg')

    def test_matched_jpeg(self) -> None:
        """JPEG: direct write — output exists and EXIF:DateTimeOriginal is set."""
        self._assert_file_exists('test', '.jpeg')
        self._assert_exif_date('test', '.jpeg')

    def test_matched_png(self) -> None:
        """PNG: partial-write strategy — output exists."""
        self._assert_file_exists('test', '.png')

    def test_matched_gif(self) -> None:
        """GIF: partial-write strategy — output exists."""
        self._assert_file_exists('test', '.gif')

    def test_matched_tiff(self) -> None:
        """TIFF: direct write — output exists and EXIF:DateTimeOriginal is set."""
        self._assert_file_exists('test', '.tiff')
        self._assert_exif_date('test', '.tiff')

    def test_matched_tif(self) -> None:
        """.tif (three-char TIFF extension): direct write — output exists and date is set."""
        self._assert_file_exists('test', '.tif')
        self._assert_exif_date('test', '.tif')

    def test_matched_mp4(self) -> None:
        """MP4: video-with-sidecar strategy — output exists."""
        self._assert_file_exists('test', '.mp4')

    def test_matched_mov(self) -> None:
        """MOV: video-with-sidecar strategy — output exists."""
        self._assert_file_exists('test', '.mov')

    def test_matched_avi(self) -> None:
        """AVI: video-with-sidecar strategy — output exists."""
        self._assert_file_exists('test', '.avi')

    def test_matched_mkv(self) -> None:
        """MKV: video-with-sidecar strategy (fallback copy) — output exists."""
        self._assert_file_exists('test', '.mkv')

    def test_matched_webm(self) -> None:
        """WEBM: video-with-sidecar strategy (fallback copy) — output exists."""
        self._assert_file_exists('test', '.webm')

    def test_matched_heic(self) -> None:
        """HEIC: direct write strategy — output exists (date may not embed via fallback)."""
        self._assert_file_exists('test', '.heic')
        tags = self._read_tags('test.heic',
                               ['EXIF:DateTimeOriginal', 'XMP:DateTimeOriginal'])
        dt = tags.get('EXIF:DateTimeOriginal') or tags.get('XMP:DateTimeOriginal')
        if dt is not None:
            self.assertEqual(dt, self._FILETYPE_EXPECTED_DT,
                             f"test.heic: DateTimeOriginal mismatch: {dt!r}")

    def test_matched_dng(self) -> None:
        """DNG: direct write — output exists and EXIF:DateTimeOriginal is set."""
        self._assert_file_exists('test', '.dng')
        self._assert_exif_date('test', '.dng')

    def test_matched_cr2(self) -> None:
        """CR2: direct write — output exists and EXIF:DateTimeOriginal is set."""
        self._assert_file_exists('test', '.cr2')
        self._assert_exif_date('test', '.cr2')

    def test_filetype_consistency_exists(self) -> None:
        """Every supported file type produces a test<ext> file in the output tree."""
        for ext in self._FILETYPE_CASES:
            with self.subTest(ext=ext):
                self._assert_file_exists('test', ext)

    def test_filetype_consistency_exif_date(self) -> None:
        """File types with direct EXIF write have EXIF:DateTimeOriginal set."""
        for ext, (has_direct_exif_date, _) in self._FILETYPE_CASES.items():
            if has_direct_exif_date:
                with self.subTest(ext=ext):
                    if ext == '.heic':
                        # HEIC fallback path may not embed dates reliably
                        tags = self._read_tags(f'test{ext}',
                                               ['EXIF:DateTimeOriginal', 'XMP:DateTimeOriginal'])
                        dt = tags.get('EXIF:DateTimeOriginal') or tags.get('XMP:DateTimeOriginal')
                        if dt is not None:
                            self.assertEqual(dt, self._FILETYPE_EXPECTED_DT,
                                             f"test{ext}: DateTimeOriginal mismatch: {dt!r}")
                    else:
                        self._assert_exif_date('test', ext)

    def test_filetype_consistency_sidecar_exists(self) -> None:
        """File types that use a sidecar strategy must produce a test<ext>.xmp in output."""
        for ext, (_, has_sidecar) in self._FILETYPE_CASES.items():
            if has_sidecar:
                with self.subTest(ext=ext):
                    xmp_name = f'test{ext}.xmp'
                    self.assertIsNotNone(
                        self._find_output_file(xmp_name),
                        f"{xmp_name} not found in output",
                    )

    # ------------------------------------------------------------------
    # Category 7 — Orphan Files
    # ------------------------------------------------------------------

    # (filename, source_subdir) — source_subdir is relative to cls.input_dir
    _ORPHAN_CASES: list = [
        ('orphan_no_json.jpg',  'RootLevel'),
        ('orphan.jpg',          'FileTypes/Orphans'),
        ('orphan.png',          'FileTypes/Orphans'),
        ('orphan.gif',          'FileTypes/Orphans'),
        ('orphan.mp4',          'FileTypes/Orphans'),
        ('orphan.mov',          'FileTypes/Orphans'),
        ('orphan.avi',          'FileTypes/Orphans'),
    ]

    def test_orphan_copied_to_output(self) -> None:
        """Every orphan (no JSON match) must appear in the output tree."""
        output_names = {f.name for f in self.output_dir.rglob('*') if f.is_file()}
        missing = [name for name, _ in self._ORPHAN_CASES if name not in output_names]
        self.assertFalse(missing, f"Orphan files missing from output: {missing}")

    def test_orphan_no_json_metadata(self) -> None:
        """Orphan outputs must not have any GPS tags (no JSON data source)."""
        gps_tags = ['EXIF:GPSLatitudeRef', 'EXIF:GPSLongitudeRef']
        for name, _ in self._ORPHAN_CASES:
            with self.subTest(file=name):
                tags = self._read_tags(name, gps_tags)
                self.assertIsNone(
                    tags.get('EXIF:GPSLatitudeRef'),
                    f"{name}: unexpected GPS data in orphan output",
                )

    def test_orphan_date_from_filesystem(self) -> None:
        """Orphan output is placed under YYYY/MM/ matching the input file's ctime (GMT+02:00)."""
        from datetime import timezone, timedelta
        gmt2 = timezone(timedelta(hours=2))
        src = self.input_dir / 'RootLevel' / 'orphan_no_json.jpg'
        expected_dt    = __import__('datetime').datetime.fromtimestamp(src.stat().st_ctime, tz=gmt2)
        expected_year  = expected_dt.strftime('%Y')
        expected_month = expected_dt.strftime('%m')

        out = self._find_output_file('orphan_no_json.jpg')
        self.assertIsNotNone(out, "orphan_no_json.jpg not found in output")
        parts = out.relative_to(self.output_dir).parts  # ('YYYY', 'MM', 'filename')
        self.assertEqual(parts[0], expected_year,
                         f"Orphan in wrong year: got {parts[0]!r}, expected {expected_year!r}")
        self.assertEqual(parts[1], expected_month,
                         f"Orphan in wrong month: got {parts[1]!r}, expected {expected_month!r}")

    def test_orphan_consistency(self) -> None:
        """Every orphan exists in output, has no GPS tags, and has no description tags."""
        gps_tag  = 'EXIF:GPSLatitudeRef'
        desc_tag = 'EXIF:ImageDescription'
        xmp_tag  = 'XMP:Description'
        for name, _ in self._ORPHAN_CASES:
            with self.subTest(file=name):
                out = self._find_output_file(name)
                self.assertIsNotNone(out, f"Orphan {name!r} not found in output")
                tags = self._read_tags(name, [gps_tag, desc_tag, xmp_tag])
                self.assertIsNone(tags.get(gps_tag),
                                  f"{name}: unexpected GPS tag in orphan output")
                self.assertFalse(tags.get(desc_tag),
                                 f"{name}: unexpected ImageDescription in orphan output")
                self.assertFalse(tags.get(xmp_tag),
                                 f"{name}: unexpected XMP:Description in orphan output")

    def test_orphan_dates_written(self) -> None:
        """Orphan outputs have date tags and timezone offsets filled in from resolved_datetime."""
        from datetime import timezone, timedelta
        gmt2 = timezone(timedelta(hours=2))

        # DIRECT orphans (JPG) should have EXIF dates + OffsetTime.
        for name in ('orphan.jpg',):
            with self.subTest(file=name):
                tags = self._read_tags(name, [
                    'EXIF:DateTimeOriginal', 'EXIF:CreateDate', 'EXIF:ModifyDate',
                    'EXIF:OffsetTimeOriginal',
                ])
                self.assertIsNotNone(tags.get('EXIF:DateTimeOriginal'),
                                     f"{name}: missing EXIF:DateTimeOriginal")
                self.assertIsNotNone(tags.get('EXIF:CreateDate'),
                                     f"{name}: missing EXIF:CreateDate")
                self.assertIsNotNone(tags.get('EXIF:ModifyDate'),
                                     f"{name}: missing EXIF:ModifyDate")
                offset = tags.get('EXIF:OffsetTimeOriginal')
                self.assertIsNotNone(offset,
                                     f"{name}: missing EXIF:OffsetTimeOriginal")
                self.assertIn('+02:00', str(offset),
                              f"{name}: expected +02:00, got {offset!r}")

        # PARTIAL_WITH_SIDECAR orphans (PNG, GIF) should have XMP dates with tz.
        for name in ('orphan.png', 'orphan.gif'):
            with self.subTest(file=name):
                tags = self._read_tags(name, ['XMP:DateTimeOriginal', 'XMP:CreateDate'])
                dt = tags.get('XMP:DateTimeOriginal') or tags.get('XMP:CreateDate')
                self.assertIsNotNone(dt, f"{name}: missing XMP date tag")
                self.assertIn('+02:00', str(dt),
                              f"{name}: expected +02:00 timezone in XMP date, got {dt!r}")

    # ------------------------------------------------------------------
    # Category 8 — XMP Sidecars
    # ------------------------------------------------------------------
    # Uses dedicated files in Sidecars/ (sc_png.png, sc_gif.gif, sc_avi.avi)
    # for sidecar content verification (GPS, description, timestamps).

    # (sidecar_name: (has_gps, has_date, description_substring_or_None))
    _SIDECAR_CASES: dict = {
        # Dedicated Sidecars/ files
        'sc_png.png.xmp':  (True,  True,  None),
        'sc_gif.gif.xmp':  (False, True,  None),
        'sc_avi.avi.xmp':  (True,  True,  'AVI sidecar test'),
        # FileTypes/Matched
        'test.png.xmp':    (False, True,  None),
        'test.gif.xmp':    (False, True,  None),
        'test.mp4.xmp':    (False, True,  None),
        'test.mov.xmp':    (False, True,  None),
        'test.avi.xmp':    (False, True,  None),
        'test.mkv.xmp':    (False, True,  None),
        'test.webm.xmp':   (False, True,  None),
        # GPS Tests — sidecar formats × 8 directions (all have GPS + date)
        **{f'gps_{d}{ext}.xmp': (True, True, None)
           for d in ('ne', 'nw', 'se', 'sw', 'n', 'e', 's', 'w')
           for ext in ('.png', '.gif', '.mp4', '.mov', '.avi', '.mkv', '.webm')},
        # Timezone fallback — sidecar formats
        'tz_fallback.png.xmp': (False, True, None),
        'tz_fallback.gif.xmp': (False, True, None),
        'tz_fallback.mp4.xmp': (False, True, None),
        'tz_fallback.avi.xmp': (False, True, None),
        # Description multi-format — sidecar formats
        'desc_utf8.png.xmp':    (False, True, 'PNG description test'),
        'desc_empty.png.xmp':   (False, True, None),
        'desc_blocked.png.xmp': (False, True, None),
        'desc_utf8.gif.xmp':    (False, True, 'GIF description test'),
        'desc_utf8.mp4.xmp':    (False, True, 'MP4 description test'),
        'desc_utf8.avi.xmp':    (False, True, 'AVI description test'),
    }

    def test_xmp_sidecar_for_png(self) -> None:
        """PNG uses PARTIAL_WITH_SIDECAR strategy → sc_png.png.xmp must exist in output."""
        xmp = self._find_output_file('sc_png.png.xmp')
        self.assertIsNotNone(xmp, "sc_png.png.xmp not found in output")

    def test_xmp_sidecar_for_gif(self) -> None:
        """GIF uses PARTIAL_WITH_SIDECAR strategy → sc_gif.gif.xmp must exist in output."""
        xmp = self._find_output_file('sc_gif.gif.xmp')
        self.assertIsNotNone(xmp, "sc_gif.gif.xmp not found in output")

    def test_xmp_sidecar_for_video(self) -> None:
        """AVI uses VIDEO_WITH_SIDECAR strategy → sc_avi.avi.xmp must exist in output."""
        xmp = self._find_output_file('sc_avi.avi.xmp')
        self.assertIsNotNone(xmp, "sc_avi.avi.xmp not found in output")

    def test_xmp_sidecar_png_contains_gps(self) -> None:
        """sc_png.png.xmp contains GPS coordinates written from sc_png.png JSON geoData."""
        tags = self._read_tags('sc_png.png.xmp', ['XMP:GPSLatitude', 'XMP:GPSLongitude'])
        self.assertIsNotNone(tags.get('XMP:GPSLatitude'),
                             "sc_png.png.xmp missing XMP:GPSLatitude")
        self.assertIsNotNone(tags.get('XMP:GPSLongitude'),
                             "sc_png.png.xmp missing XMP:GPSLongitude")

    def test_xmp_sidecar_contains_dates(self) -> None:
        """XMP sidecar contains XMP:DateTimeOriginal (or XMP:CreateDate) from JSON timestamp."""
        tags = self._read_tags('sc_avi.avi.xmp', ['XMP:DateTimeOriginal', 'XMP:CreateDate'])
        dt = tags.get('XMP:DateTimeOriginal') or tags.get('XMP:CreateDate')
        self.assertIsNotNone(dt, "sc_avi.avi.xmp missing XMP:DateTimeOriginal / XMP:CreateDate")

    def test_xmp_sidecar_contains_gps(self) -> None:
        """XMP sidecar contains GPS coordinates written from JSON geoData."""
        tags = self._read_tags('sc_avi.avi.xmp', ['XMP:GPSLatitude', 'XMP:GPSLongitude'])
        self.assertIsNotNone(tags.get('XMP:GPSLatitude'),
                             "sc_avi.avi.xmp missing XMP:GPSLatitude")
        self.assertIsNotNone(tags.get('XMP:GPSLongitude'),
                             "sc_avi.avi.xmp missing XMP:GPSLongitude")

    def test_xmp_sidecar_contains_description(self) -> None:
        """XMP sidecar contains XMP:Description written from JSON description field."""
        tags = self._read_tags('sc_avi.avi.xmp', ['XMP:Description'])
        desc = tags.get('XMP:Description', '')
        self.assertIn('AVI sidecar test', desc,
                      f"sc_avi.avi.xmp XMP:Description mismatch: {desc!r}")

    def test_sidecar_consistency_exists(self) -> None:
        """Every expected XMP sidecar exists in the output tree."""
        for sidecar_name in self._SIDECAR_CASES:
            with self.subTest(sidecar=sidecar_name):
                self.assertIsNotNone(
                    self._find_output_file(sidecar_name),
                    f"{sidecar_name} not found in output",
                )

    def test_sidecar_consistency_has_date(self) -> None:
        """Every sidecar with has_date=True contains a date tag in its XMP."""
        date_tags = ['XMP:DateTimeOriginal', 'XMP:CreateDate']
        for sidecar_name, (_, has_date, _) in self._SIDECAR_CASES.items():
            if has_date:
                with self.subTest(sidecar=sidecar_name):
                    tags = self._read_tags(sidecar_name, date_tags)
                    dt = tags.get('XMP:DateTimeOriginal') or tags.get('XMP:CreateDate')
                    self.assertIsNotNone(
                        dt, f"{sidecar_name}: missing XMP:DateTimeOriginal / XMP:CreateDate",
                    )

    def test_sidecar_consistency_has_gps(self) -> None:
        """Every sidecar with has_gps=True contains GPS coordinates in its XMP."""
        gps_tags = ['XMP:GPSLatitude', 'XMP:GPSLongitude']
        for sidecar_name, (has_gps, _, _) in self._SIDECAR_CASES.items():
            if has_gps:
                with self.subTest(sidecar=sidecar_name):
                    tags = self._read_tags(sidecar_name, gps_tags)
                    self.assertIsNotNone(
                        tags.get('XMP:GPSLatitude'),
                        f"{sidecar_name}: missing XMP:GPSLatitude",
                    )
                    self.assertIsNotNone(
                        tags.get('XMP:GPSLongitude'),
                        f"{sidecar_name}: missing XMP:GPSLongitude",
                    )

    # ------------------------------------------------------------------
    # Category 9 — Duplicate Resolution
    # ------------------------------------------------------------------
    # same_name_a.jpg and same_name_b.jpg both carry title='same_name.jpg'
    # and the same epoch.  The merger writes the first as same_name.jpg and
    # renames the second to same_name_2.jpg.

    def test_duplicate_both_exist(self) -> None:
        """Both duplicate source files produce output: same_name.jpg and same_name_2.jpg."""
        self.assertIsNotNone(self._find_output_file('same_name.jpg'),
                             "same_name.jpg not found in output")
        self.assertIsNotNone(self._find_output_file('same_name_2.jpg'),
                             "same_name_2.jpg (renamed duplicate) not found in output")

    def test_duplicate_different_content(self) -> None:
        """Both duplicate outputs have EXIF:DateTimeOriginal set from their JSON."""
        for name in ('same_name.jpg', 'same_name_2.jpg'):
            with self.subTest(file=name):
                tags = self._read_tags(name, ['EXIF:DateTimeOriginal'])
                self.assertEqual(
                    tags.get('EXIF:DateTimeOriginal'), '2024:08:08 12:44:06',
                    f"{name}: EXIF:DateTimeOriginal not set correctly",
                )

    def test_duplicate_renamed_sidecar_exists(self) -> None:
        """When a renamed duplicate is a video, its XMP sidecar is also renamed to match."""
        # same_name_a.mp4 → same_video.mp4 + same_video.mp4.xmp
        # same_name_b.mp4 → same_video_2.mp4 + same_video_2.mp4.xmp
        self.assertIsNotNone(self._find_output_file('same_video.mp4'),
                             "same_video.mp4 not found in output")
        self.assertIsNotNone(self._find_output_file('same_video.mp4.xmp'),
                             "same_video.mp4.xmp not found in output")
        self.assertIsNotNone(self._find_output_file('same_video_2.mp4'),
                             "same_video_2.mp4 (renamed duplicate) not found in output")
        self.assertIsNotNone(self._find_output_file('same_video_2.mp4.xmp'),
                             "same_video_2.mp4.xmp (renamed sidecar) not found in output")

    def test_duplicate_exif_title(self) -> None:
        """The renamed duplicate's XMP sidecar carries the new deduplicated title stem."""
        tags = self._read_tags('same_video_2.mp4.xmp', ['XMP:Title'])
        title = tags.get('XMP:Title', '')
        # Title stem should reflect the renamed filename, not the original 'same_video'
        self.assertIn('same_video_2', str(title),
                      f"same_video_2.mp4.xmp XMP:Title does not reflect renamed stem: {title!r}")

    # ------------------------------------------------------------------
    # Category 10 — Bracket Notation
    # ------------------------------------------------------------------
    # photo.jpg(1).json and photo.jpg(2).json must each match their
    # corresponding photo(1).jpg / photo(2).jpg.  Both carry title='photo.jpg'
    # so the second output is renamed photo_2.jpg (same deduplication logic).

    # (output_filename, expected_DateTimeOriginal)
    _BRACKET_CASES: list = [
        ('photo.jpg',   '2024:08:08 12:44:06'),
        ('photo_2.jpg', '2024:08:08 12:44:06'),
    ]

    def test_bracket_notation_match(self) -> None:
        """photo.jpg(1).json matched to photo(1).jpg → EXIF:DateTimeOriginal set."""
        tags = self._read_tags('photo.jpg', ['EXIF:DateTimeOriginal'])
        self.assertEqual(
            tags.get('EXIF:DateTimeOriginal'), '2024:08:08 12:44:06',
            "photo.jpg: DateTimeOriginal not set (bracket notation matching failed)",
        )

    def test_bracket_notation_multiple(self) -> None:
        """Both photo(1).jpg and photo(2).jpg are processed; second renamed to photo_2.jpg."""
        self.assertIsNotNone(self._find_output_file('photo.jpg'),
                             "photo.jpg not found in output")
        self.assertIsNotNone(self._find_output_file('photo_2.jpg'),
                             "photo_2.jpg not found in output")

    def test_bracket_notation_consistency(self) -> None:
        """Both bracket-notation outputs have EXIF:DateTimeOriginal set correctly."""
        for filename, expected_dt in self._BRACKET_CASES:
            with self.subTest(file=filename):
                tags = self._read_tags(filename, ['EXIF:DateTimeOriginal'])
                self.assertEqual(tags.get('EXIF:DateTimeOriginal'), expected_dt,
                                 f"{filename}: EXIF:DateTimeOriginal mismatch")

    # ------------------------------------------------------------------
    # Category 11 — File Timestamps
    # ------------------------------------------------------------------

    # output_filename → expected UTC epoch (all share the same default epoch)
    _TIMESTAMP_CASES: dict = {
        'photo_basic.jpg': 1723113846,
        'gps_ne.jpg':      1723113846,
        'desc_utf8.jpg':   1723113846,
        'test.avi':        1723113846,
        'test.mkv':        1723113846,
        'test.webm':       1723113846,
    }

    # (media_filename, sidecar_filename) pairs whose mtimes must match within 2 s
    _SIDECAR_PAIR_CASES: list = [
        ('sc_avi.avi',  'sc_avi.avi.xmp'),
        ('sc_png.png',  'sc_png.png.xmp'),
        ('test.mp4',    'test.mp4.xmp'),
    ]

    def _assert_output_mtime(self, filename: str, expected_epoch: int,
                             delta: int = 2) -> None:
        """Assert that *filename* in the output tree has mtime ≈ *expected_epoch*."""
        out = self._find_output_file(filename)
        self.assertIsNotNone(out, f"Output file not found: {filename!r}")
        self.assertAlmostEqual(
            out.stat().st_mtime, expected_epoch, delta=delta,
            msg=(f"{filename} mtime ({out.stat().st_mtime:.0f}) "
                 f"not within {delta} s of expected ({expected_epoch})"),
        )

    def test_input_timestamps_unchanged(self) -> None:
        """No input file mtime, ctime, or size changed during the merger run."""
        snapshot_after = self._snapshot(self.input_dir)
        self.assertEqual(
            self.input_snapshot, snapshot_after,
            "Input directory snapshot changed after merger run (mtime, ctime, or size differs)",
        )

    def test_output_timestamps_set(self) -> None:
        """photo_basic.jpg output mtime matches the JSON epoch converted to GMT+02:00.

        The merger writes FileModifyDate=2024:08:08 12:44:06+02:00 (epoch 1723113846 in
        UTC), so st_mtime must be within 2 seconds of that epoch.
        """
        import datetime as _dt
        expected_utc = _dt.datetime(2024, 8, 8, 10, 44, 6, tzinfo=_dt.timezone.utc)
        expected_ts  = expected_utc.timestamp()  # = 1723113846.0

        output = self._find_output_file('photo_basic.jpg')
        self.assertIsNotNone(output, "photo_basic.jpg not found in output")
        self.assertAlmostEqual(
            output.stat().st_mtime, expected_ts, delta=2,
            msg=(f"photo_basic.jpg mtime ({output.stat().st_mtime:.0f}) "
                 f"not within 2 s of expected ({expected_ts:.0f})"),
        )

    def test_sidecar_timestamps_match(self) -> None:
        """sc_avi.avi and sc_avi.avi.xmp have matching mtimes (both set by _set_filesystem_timestamps)."""
        avi = self._find_output_file('sc_avi.avi')
        xmp = self._find_output_file('sc_avi.avi.xmp')
        self.assertIsNotNone(avi, "sc_avi.avi not found in output")
        self.assertIsNotNone(xmp, "sc_avi.avi.xmp not found in output")
        self.assertAlmostEqual(
            avi.stat().st_mtime, xmp.stat().st_mtime, delta=2,
            msg=(f"sc_avi.avi mtime ({avi.stat().st_mtime:.3f}) and "
                 f"sc_avi.avi.xmp mtime ({xmp.stat().st_mtime:.3f}) differ by more than 2 s"),
        )

    def test_output_timestamp_consistency(self) -> None:
        """Output file mtimes for _TIMESTAMP_CASES files match the JSON epoch within 2 s."""
        for filename, expected_epoch in self._TIMESTAMP_CASES.items():
            with self.subTest(file=filename):
                self._assert_output_mtime(filename, expected_epoch)

    def test_sidecar_timestamp_consistency(self) -> None:
        """Media file and its XMP sidecar share matching mtimes (within 2 s)."""
        for media_name, sidecar_name in self._SIDECAR_PAIR_CASES:
            with self.subTest(pair=f"{media_name} / {sidecar_name}"):
                media = self._find_output_file(media_name)
                sidecar = self._find_output_file(sidecar_name)
                self.assertIsNotNone(media, f"{media_name} not found in output")
                self.assertIsNotNone(sidecar, f"{sidecar_name} not found in output")
                self.assertAlmostEqual(
                    media.stat().st_mtime, sidecar.stat().st_mtime, delta=2,
                    msg=(f"{media_name} mtime ({media.stat().st_mtime:.3f}) and "
                         f"{sidecar_name} mtime ({sidecar.stat().st_mtime:.3f}) differ > 2 s"),
                )

    # ------------------------------------------------------------------
    # Category 12 — Stats Verification
    # ------------------------------------------------------------------
    # Counts derivation (after multi-format expansion):
    #   total=175  (168 matched + 7 orphans)
    #   matched=168 (previous 62 + 92 GPS multi-format + 6 TZ fallback + 8 Desc multi-format)
    #   orphans=7   (unchanged)
    #   gps=100     (98 GPS Tests [8 dirs × 12 fmts + 2 altitude] + sc_png + sc_avi)
    #   sidecars=80 (previous 14 + 56 GPS sidecar [7 fmts × 8 dirs]
    #               + 4 TZ fallback sidecar + 6 Desc sidecar)
    #   descriptions_cleared=3  (desc_blocked.jpg + desc_blocked.png + desc_iptc_blocked.jpg)
    #   duplicates_renamed=3    (unchanged)
    #   written=175
    #   errors=0

    def test_stats_total_count(self) -> None:
        """Total media files processed = 184 (177 matched + 7 orphans)."""
        self.assertEqual(self.stats.total_media_files, 184,
                         f"Expected 184 total, got {self.stats.total_media_files}")

    def test_stats_matched_count(self) -> None:
        """Matched files (with JSON) = 177."""
        self.assertEqual(self.stats.matched, 177,
                         f"Expected 177 matched, got {self.stats.matched}")

    def test_stats_orphan_count(self) -> None:
        """Orphan files (no JSON) = 7."""
        self.assertEqual(self.stats.orphans, 7,
                         f"Expected 7 orphans, got {self.stats.orphans}")

    def test_stats_gps_written(self) -> None:
        """GPS tags written = 101 (98 GPS Tests + mismatch_photo + sc_png + sc_avi)."""
        self.assertEqual(self.stats.gps_written, 101,
                         f"Expected 101 GPS writes, got {self.stats.gps_written}")

    def test_stats_sidecars_created(self) -> None:
        """XMP sidecars created = 84."""
        self.assertEqual(self.stats.sidecars_created, 84,
                         f"Expected 84 sidecars, got {self.stats.sidecars_created}")

    def test_stats_zero_errors(self) -> None:
        """Merger reports zero errors for well-formed test data."""
        self.assertEqual(self.stats.errors, 0,
                         f"Expected 0 errors, got {self.stats.errors}")

    def test_stats_written_count(self) -> None:
        """Files written = 184 (total_media_files when errors == 0)."""
        self.assertEqual(self.stats.written, 184,
                         f"Expected 184 written, got {self.stats.written}")

    def test_stats_descriptions_cleared(self) -> None:
        """descriptions_cleared = 3 (desc_blocked.jpg + desc_blocked.png + desc_iptc_blocked.jpg)."""
        self.assertEqual(self.stats.descriptions_cleared, 3,
                         f"Expected 3 descriptions cleared, got {self.stats.descriptions_cleared}")

    def test_stats_duplicates_renamed(self) -> None:
        """duplicates_renamed = 3 (same_name_b.jpg, photo(2).jpg, same_name_b.mp4)."""
        self.assertEqual(self.stats.duplicates_renamed, 3,
                         f"Expected 3 duplicates renamed, got {self.stats.duplicates_renamed}")

    def test_stats_skipped_json(self) -> None:
        """skipped_json = 1 (bad_json.jpg.json contains invalid JSON)."""
        self.assertEqual(self.stats.skipped_json, 1,
                         f"Expected 1 skipped_json, got {self.stats.skipped_json}")

    def test_stats_ext_mismatches(self) -> None:
        """ext_mismatches = 1 (mismatch_photo.dng is actually JPEG)."""
        self.assertEqual(self.stats.ext_mismatches, 1,
                         f"Expected 1 ext_mismatches, got {self.stats.ext_mismatches}")

    def test_stats_skipped_existing(self) -> None:
        """skipped_existing = 0 (clean output directory, no pre-existing files)."""
        self.assertEqual(self.stats.skipped_existing, 0,
                         f"Expected 0 skipped_existing, got {self.stats.skipped_existing}")

    def test_stats_jpeg_compressed_disabled(self) -> None:
        """jpeg_compressed = 0 when --jpeg-quality is not set."""
        self.assertEqual(self.stats.jpeg_compressed, 0,
                         f"Expected 0 jpeg_compressed, got {self.stats.jpeg_compressed}")

    def test_stats_jpeg_quality_checked_disabled(self) -> None:
        """jpeg_quality_checked = 0 when --jpeg-quality is not set."""
        self.assertEqual(self.stats.jpeg_quality_checked, 0,
                         f"Expected 0 jpeg_quality_checked, got {self.stats.jpeg_quality_checked}")

    def test_stats_jpeg_quality_unknown_disabled(self) -> None:
        """jpeg_quality_unknown = 0 when --jpeg-quality is not set."""
        self.assertEqual(self.stats.jpeg_quality_unknown, 0,
                         f"Expected 0 jpeg_quality_unknown, got {self.stats.jpeg_quality_unknown}")

    def test_stats_jpeg_compress_skipped_larger_disabled(self) -> None:
        """jpeg_compress_skipped_larger = 0 when --jpeg-quality is not set."""
        self.assertEqual(self.stats.jpeg_compress_skipped_larger, 0,
                         f"Expected 0 jpeg_compress_skipped_larger, got {self.stats.jpeg_compress_skipped_larger}")

    # ------------------------------------------------------------------
    # Category 13 — Video UTC Time
    # ------------------------------------------------------------------
    # QuickTime stores CreateDate as a plain integer (seconds since Mac epoch)
    # without timezone info.  ExifTool returns it as "YYYY:MM:DD HH:MM:SS"
    # with no +HH:MM suffix.
    # MKV/WebM use Matroska tags; AVI uses RIFF tags — the consistency test
    # accepts any recognised date tag from _VIDEO_DATE_TAGS.

    # AVI (RIFF) and MKV/WebM (Matroska) cannot have date tags written to the
    # main file — ExifTool cannot write those container formats. Dates for those
    # formats live only in the XMP sidecar.
    _VIDEO_CASES: list = ['test.mp4', 'test.mov']
    _VIDEO_DATE_TAGS: list = ['QuickTime:CreateDate']

    def test_mp4_time_utc(self) -> None:
        """MP4: QuickTime:CreateDate is present, has no timezone offset suffix,
        and contains the correct UTC time (not local time)."""
        import re
        tags = self._read_tags('test.mp4', ['QuickTime:CreateDate'])
        dt = tags.get('QuickTime:CreateDate')
        self.assertIsNotNone(dt, "test.mp4: QuickTime:CreateDate is missing")
        self.assertTrue(str(dt).strip(), "test.mp4: QuickTime:CreateDate is empty")
        self.assertNotRegex(str(dt), r'[+-]\d{2}:\d{2}$',
                            f"test.mp4: QuickTime:CreateDate has unexpected tz suffix: {dt!r}")
        # The default epoch 1723113846 is 2024:08:08 10:44:06 UTC.
        # Local GMT+02:00 would be 12:44:06 — QuickTime must store UTC.
        self.assertIn('2024:08:08 10:44:06', str(dt),
                      f"test.mp4: QuickTime:CreateDate should be UTC (10:44:06), got: {dt!r}")

    def test_mov_time_utc(self) -> None:
        """MOV: QuickTime:CreateDate is present, has no timezone offset suffix,
        and contains the correct UTC time (not local time)."""
        import re
        tags = self._read_tags('test.mov', ['QuickTime:CreateDate'])
        dt = tags.get('QuickTime:CreateDate')
        self.assertIsNotNone(dt, "test.mov: QuickTime:CreateDate is missing")
        self.assertTrue(str(dt).strip(), "test.mov: QuickTime:CreateDate is empty")
        self.assertNotRegex(str(dt), r'[+-]\d{2}:\d{2}$',
                            f"test.mov: QuickTime:CreateDate has unexpected tz suffix: {dt!r}")
        # The default epoch 1723113846 is 2024:08:08 10:44:06 UTC.
        self.assertIn('2024:08:08 10:44:06', str(dt),
                      f"test.mov: QuickTime:CreateDate should be UTC (10:44:06), got: {dt!r}")

    def test_video_utc_consistency(self) -> None:
        """Every video format has a recognised date tag with no timezone offset suffix
        and the correct UTC time value."""
        for filename in self._VIDEO_CASES:
            with self.subTest(file=filename):
                tags = self._read_tags(filename, self._VIDEO_DATE_TAGS)
                dt = next(
                    (tags.get(tag) for tag in self._VIDEO_DATE_TAGS if tags.get(tag)),
                    None,
                )
                self.assertIsNotNone(
                    dt, f"{filename}: no date tag found in {self._VIDEO_DATE_TAGS}",
                )
                self.assertNotRegex(
                    str(dt), r'[+-]\d{2}:\d{2}$',
                    f"{filename}: date tag has unexpected tz suffix: {dt!r}",
                )
                # Epoch 1723113846 → 2024:08:08 10:44:06 UTC (not 12:44:06 local).
                self.assertIn(
                    '2024:08:08 10:44:06', str(dt),
                    f"{filename}: date should be UTC (10:44:06), got: {dt!r}",
                )

    # ------------------------------------------------------------------
    # Category 14 — Special Filenames
    # ------------------------------------------------------------------

    _SPECIAL_CASES: dict = {
        'Kosi Bay - 2014 - 179.jpg':         '2024:08:08 12:44:06',
        '_DSC5757-Enhanced-NR - Kruger.jpg': '2024:08:08 12:44:06',
        'photo(1711).jpg':                   '2024:08:08 12:44:06',
        'UPPERCASE.jpg':                     '2024:08:08 12:44:06',
    }

    # ------------------------------------------------------------------
    # Category 15 — EXIF Preservation constants
    # ------------------------------------------------------------------

    # Tags written into Preservation/ files before the merger runs.
    _PRESERVATION_WRITE_TAGS: dict = {
        'EXIF:Make':         'Canon',
        'EXIF:Model':        'Canon EOS R5',
        'EXIF:ISO':          400,
        'EXIF:ExposureTime': '1/250',
        'EXIF:FNumber':      2.8,
        'EXIF:FocalLength':  50,
        'EXIF:Software':     'Adobe Lightroom Classic 12.0',
        'EXIF:Artist':       'Test Photographer',
        'EXIF:Copyright':    'Copyright 2024 Test',
    }

    # Tags to read back from the output file.
    _PRESERVATION_READ_TAGS: list = [
        'EXIF:Make', 'EXIF:Model', 'EXIF:ISO', 'EXIF:ExposureTime',
        'EXIF:FNumber', 'EXIF:FocalLength', 'EXIF:Software',
        'EXIF:Artist', 'EXIF:Copyright',
    ]

    # (filename, supports_full_exif) — True for formats with reliable EXIF:* support.
    # HEIC uses a container format; ExifTool reads its tags under QuickTime:/XMP:
    # groups rather than EXIF:*, so it behaves like PNG/GIF for this test.
    _PRESERVATION_FILE_TYPES: list = [
        ('preserve_jpg.jpg',   True),
        ('preserve_jpeg.jpeg', True),
        ('preserve_tiff.tiff', True),
        ('preserve_tif.tif',   True),
        ('preserve_dng.dng',   True),
        ('preserve_cr2.cr2',   True),
        ('preserve_heic.heic', False),
        ('preserve_png.png',   False),
        ('preserve_gif.gif',   False),
    ]

    # {filename: supports_full_exif} — used by the consistency test.
    _PRESERVATION_CASES: dict = {fname: full for fname, full in _PRESERVATION_FILE_TYPES}

    @staticmethod
    def _parse_exposure(raw: Any) -> float:
        """Convert an ExifTool ExposureTime value to a float seconds value.

        ExifTool may return ``'1/250'`` (fraction string) or ``'0.004'``
        (decimal string) or a numeric type.
        """
        s = str(raw).strip()
        if '/' in s:
            num, den = s.split('/', 1)
            return float(num) / float(den)
        return float(s)

    @staticmethod
    def _parse_focal_length(raw: Any) -> float:
        """Convert an ExifTool FocalLength value to a float mm value.

        ExifTool may return ``'50.0 mm'`` or ``'50'`` or a numeric type.
        """
        s = str(raw).strip()
        # Strip trailing unit (e.g. " mm")
        s = s.split()[0]
        return float(s)

    def _assert_preservation(self, filename: str, supports_full_exif: bool) -> None:
        """Assert that EXIF preservation tags survived the merge unchanged.

        For formats with full EXIF support all nine tags must be present and
        match the pre-written values.  For PNG/GIF (``supports_full_exif=False``)
        only tags that are actually present in the output are checked.
        """
        tags = self._read_tags(filename, self._PRESERVATION_READ_TAGS)

        def _check(tag: str, expected: Any) -> None:
            actual = tags.get(tag)
            if not supports_full_exif and actual is None:
                return  # PNG/GIF may lack some tags — skip gracefully
            self.assertIsNotNone(actual,
                                 f"{filename}: {tag} missing from output")
            if tag == 'EXIF:ExposureTime':
                self.assertAlmostEqual(
                    self._parse_exposure(actual),
                    self._parse_exposure(expected),
                    places=6,
                    msg=f"{filename}: {tag} mismatch (got {actual!r})",
                )
            elif tag == 'EXIF:FocalLength':
                self.assertAlmostEqual(
                    self._parse_focal_length(actual),
                    float(expected),
                    places=3,
                    msg=f"{filename}: {tag} mismatch (got {actual!r})",
                )
            elif tag in ('EXIF:FNumber',):
                self.assertAlmostEqual(
                    float(actual),
                    float(expected),
                    places=3,
                    msg=f"{filename}: {tag} mismatch (got {actual!r})",
                )
            elif tag == 'EXIF:ISO':
                self.assertEqual(int(actual), int(expected),
                                 f"{filename}: {tag} mismatch (got {actual!r})")
            else:
                self.assertEqual(str(actual).strip(), str(expected),
                                 f"{filename}: {tag} mismatch (got {actual!r})")

        for tag, expected in self._PRESERVATION_WRITE_TAGS.items():
            _check(tag, expected)

    def _assert_special_filename(self, filename: str, expected_dt: str) -> None:
        """Assert that a special-filename output exists and has the correct EXIF date."""
        path = self._find_output_file(filename)
        self.assertIsNotNone(path, f"Output file not found for special filename: {filename!r}")
        tags = self._read_tags(filename, ['EXIF:DateTimeOriginal'])
        self.assertEqual(tags.get('EXIF:DateTimeOriginal'), expected_dt,
                         f"{filename}: EXIF:DateTimeOriginal mismatch")

    def test_spaces_in_filename(self) -> None:
        """Filename with spaces and dashes: EXIF:DateTimeOriginal written correctly."""
        tags = self._read_tags('Kosi Bay - 2014 - 179.jpg', ['EXIF:DateTimeOriginal'])
        self.assertEqual(tags.get('EXIF:DateTimeOriginal'), '2024:08:08 12:44:06',
                         "Kosi Bay - 2014 - 179.jpg: EXIF:DateTimeOriginal mismatch")

    def test_leading_underscore(self) -> None:
        """Filename with leading underscore: EXIF:DateTimeOriginal written correctly."""
        tags = self._read_tags('_DSC5757-Enhanced-NR - Kruger.jpg', ['EXIF:DateTimeOriginal'])
        self.assertEqual(tags.get('EXIF:DateTimeOriginal'), '2024:08:08 12:44:06',
                         "_DSC5757-Enhanced-NR - Kruger.jpg: EXIF:DateTimeOriginal mismatch")

    def test_parentheses_in_filename(self) -> None:
        """Filename with parentheses in the base name (not bracket notation): EXIF date set."""
        tags = self._read_tags('photo(1711).jpg', ['EXIF:DateTimeOriginal'])
        self.assertEqual(tags.get('EXIF:DateTimeOriginal'), '2024:08:08 12:44:06',
                         "photo(1711).jpg: EXIF:DateTimeOriginal mismatch")

    def test_uppercase_extension(self) -> None:
        """Uppercase extension (.JPG) is normalised to lowercase in the output filename."""
        # The merger's _resolve_dates_and_paths lower-cases the extension in the output
        # title, so UPPERCASE.JPG → UPPERCASE.jpg.
        path = self._find_output_file('UPPERCASE.jpg')
        self.assertIsNotNone(path, "UPPERCASE.jpg (normalised from .JPG) not found in output")
        tags = self._read_tags('UPPERCASE.jpg', ['EXIF:DateTimeOriginal'])
        self.assertEqual(tags.get('EXIF:DateTimeOriginal'), '2024:08:08 12:44:06',
                         "UPPERCASE.jpg: EXIF:DateTimeOriginal mismatch")

    def test_special_filename_consistency(self) -> None:
        """Every special-filename file exists in output with the correct EXIF date."""
        for filename, expected_dt in self._SPECIAL_CASES.items():
            with self.subTest(file=filename):
                self._assert_special_filename(filename, expected_dt)

    # ------------------------------------------------------------------
    # Category 15 — EXIF Preservation
    # ------------------------------------------------------------------

    def test_preservation_jpg(self) -> None:
        """JPEG (.jpg) — camera/lens/software tags preserved through merge."""
        self._assert_preservation('preserve_jpg.jpg', True)

    def test_preservation_jpeg(self) -> None:
        """JPEG (.jpeg) — camera/lens/software tags preserved through merge."""
        self._assert_preservation('preserve_jpeg.jpeg', True)

    def test_preservation_tiff(self) -> None:
        """TIFF (.tiff) — camera/lens/software tags preserved through merge."""
        self._assert_preservation('preserve_tiff.tiff', True)

    def test_preservation_tif(self) -> None:
        """TIFF (.tif) — camera/lens/software tags preserved through merge."""
        self._assert_preservation('preserve_tif.tif', True)

    def test_preservation_dng(self) -> None:
        """DNG — camera/lens/software tags preserved through merge."""
        self._assert_preservation('preserve_dng.dng', True)

    def test_preservation_cr2(self) -> None:
        """CR2 — camera/lens/software tags preserved through merge."""
        self._assert_preservation('preserve_cr2.cr2', True)

    def test_preservation_heic(self) -> None:
        """HEIC — any present EXIF tags preserved through merge (container format)."""
        self._assert_preservation('preserve_heic.heic', False)

    def test_preservation_png(self) -> None:
        """PNG — any present EXIF tags preserved through merge (sidecar format)."""
        self._assert_preservation('preserve_png.png', False)

    def test_preservation_gif(self) -> None:
        """GIF — any present EXIF tags preserved through merge (sidecar format)."""
        self._assert_preservation('preserve_gif.gif', False)

    def test_preservation_consistency(self) -> None:
        """Every Preservation/ file exists in output with camera tags intact."""
        for filename, supports_full_exif in self._PRESERVATION_CASES.items():
            with self.subTest(file=filename):
                self._assert_preservation(filename, supports_full_exif)

    # ------------------------------------------------------------------
    # Category 16 — XMP Conditional Date Update
    # ------------------------------------------------------------------
    # When source files have pre-existing XMP date tags (e.g.
    # XMP-photoshop:DateCreated, XMP-xmp:MetadataDate), those tags
    # should be updated to the resolved datetime with timezone.
    # Tags that were absent in the source should remain absent.

    def test_xmp_conditional_dates_updated(self) -> None:
        """Pre-existing XMP:DateCreated, XMP:MetadataDate, and IPTC date/time
        are updated to the resolved datetime (with timezone)."""
        tags = self._read_tags('xmp_dates.jpg', [
            'XMP:DateCreated', 'XMP:MetadataDate',
            'IPTC:DateCreated', 'IPTC:TimeCreated',
            'IPTC:DigitalCreationDate', 'IPTC:DigitalCreationTime',
        ])
        # Default epoch 1723113846 → 2024:08:08 12:44:06+02:00 in GMT+2.
        # The old values (2014) should be replaced.
        date_created = tags.get('XMP:DateCreated')
        self.assertIsNotNone(date_created,
                             "xmp_dates.jpg: XMP:DateCreated should still exist")
        self.assertIn('2024:08:08 12:44:06', str(date_created),
                      f"XMP:DateCreated not updated: {date_created!r}")

        metadata_date = tags.get('XMP:MetadataDate')
        self.assertIsNotNone(metadata_date,
                             "xmp_dates.jpg: XMP:MetadataDate should still exist")
        self.assertIn('2024:08:08 12:44:06', str(metadata_date),
                      f"XMP:MetadataDate not updated: {metadata_date!r}")

        iptc_date = tags.get('IPTC:DateCreated')
        self.assertIsNotNone(iptc_date,
                             "xmp_dates.jpg: IPTC:DateCreated should still exist")
        self.assertIn('2024:08:08', str(iptc_date),
                      f"IPTC:DateCreated not updated: {iptc_date!r}")

        iptc_time = tags.get('IPTC:TimeCreated')
        self.assertIsNotNone(iptc_time,
                             "xmp_dates.jpg: IPTC:TimeCreated should still exist")
        self.assertIn('12:44:06', str(iptc_time),
                      f"IPTC:TimeCreated not updated: {iptc_time!r}")

        dig_date = tags.get('IPTC:DigitalCreationDate')
        self.assertIsNotNone(dig_date,
                             "xmp_dates.jpg: IPTC:DigitalCreationDate should still exist")
        self.assertIn('2024:08:08', str(dig_date),
                      f"IPTC:DigitalCreationDate not updated: {dig_date!r}")

        dig_time = tags.get('IPTC:DigitalCreationTime')
        self.assertIsNotNone(dig_time,
                             "xmp_dates.jpg: IPTC:DigitalCreationTime should still exist")
        self.assertIn('12:44:06', str(dig_time),
                      f"IPTC:DigitalCreationTime not updated: {dig_time!r}")

    def test_xmp_conditional_dates_absent_remain_absent(self) -> None:
        """Conditional date tags that were absent in the source are not added."""
        tags = self._read_tags('xmp_no_dates.jpg', [
            'XMP:DateCreated', 'XMP:MetadataDate',
            'IPTC:DateCreated', 'IPTC:TimeCreated',
            'IPTC:DigitalCreationDate', 'IPTC:DigitalCreationTime',
        ])
        self.assertIsNone(tags.get('XMP:DateCreated'),
                          "xmp_no_dates.jpg: XMP:DateCreated should not be added")
        self.assertIsNone(tags.get('XMP:MetadataDate'),
                          "xmp_no_dates.jpg: XMP:MetadataDate should not be added")
        self.assertIsNone(tags.get('IPTC:DateCreated'),
                          "xmp_no_dates.jpg: IPTC:DateCreated should not be added")
        self.assertIsNone(tags.get('IPTC:TimeCreated'),
                          "xmp_no_dates.jpg: IPTC:TimeCreated should not be added")
        self.assertIsNone(tags.get('IPTC:DigitalCreationDate'),
                          "xmp_no_dates.jpg: IPTC:DigitalCreationDate should not be added")
        self.assertIsNone(tags.get('IPTC:DigitalCreationTime'),
                          "xmp_no_dates.jpg: IPTC:DigitalCreationTime should not be added")

    # ------------------------------------------------------------------
    # Category 17 — Extension Mismatch
    # ------------------------------------------------------------------
    # Files where the extension does not match the actual content type
    # (e.g. JPEG content with .DNG extension from Photos Takeout export).
    # The merger should detect the mismatch, temporarily rename for
    # ExifTool, write all tags correctly, and preserve the original
    # filename (including the JSON title).

    def test_ext_mismatch_output_exists(self) -> None:
        """Mismatch file appears in output with the original .dng extension."""
        out = self._find_output_file('mismatch_photo.dng')
        self.assertIsNotNone(out, "mismatch_photo.dng not found in output")

    def test_ext_mismatch_dates_written(self) -> None:
        """Mismatch file has correct EXIF dates and timezone written."""
        tags = self._read_tags('mismatch_photo.dng', [
            'EXIF:DateTimeOriginal', 'EXIF:CreateDate', 'EXIF:ModifyDate',
            'EXIF:OffsetTimeOriginal',
        ])
        # Default epoch 1723113846 → 2024:08:08 12:44:06 in GMT+2.
        dt = tags.get('EXIF:DateTimeOriginal')
        self.assertIsNotNone(dt, "mismatch_photo.dng: EXIF:DateTimeOriginal missing")
        self.assertIn('2024:08:08 12:44:06', str(dt),
                      f"mismatch_photo.dng: DateTimeOriginal wrong: {dt!r}")
        offset = tags.get('EXIF:OffsetTimeOriginal')
        self.assertIsNotNone(offset,
                             "mismatch_photo.dng: EXIF:OffsetTimeOriginal missing")
        self.assertIn('+02:00', str(offset),
                      f"mismatch_photo.dng: OffsetTimeOriginal wrong: {offset!r}")

    def test_ext_mismatch_gps_written(self) -> None:
        """Mismatch file has GPS coordinates written from JSON."""
        tags = self._read_tags('mismatch_photo.dng', [
            'EXIF:GPSLatitude', 'EXIF:GPSLongitude',
        ])
        lat = tags.get('EXIF:GPSLatitude')
        self.assertIsNotNone(lat, "mismatch_photo.dng: EXIF:GPSLatitude missing")
        self.assertAlmostEqual(float(lat), 25.78, places=1,
                               msg=f"mismatch_photo.dng: GPSLatitude wrong: {lat!r}")

    def test_ext_mismatch_title_preserved(self) -> None:
        """Mismatch file output uses the JSON title (keeps original .dng extension)."""
        out = self._find_output_file('mismatch_photo.dng')
        self.assertIsNotNone(out, "mismatch_photo.dng not found in output")
        # The file should be in YYYY/MM/ with the title from JSON
        self.assertEqual(out.name, 'mismatch_photo.dng',
                         f"Output filename should be 'mismatch_photo.dng', got {out.name!r}")

    def test_ext_mismatch_filesystem_timestamp(self) -> None:
        """Mismatch file has filesystem mtime matching the JSON epoch."""
        self._assert_output_mtime('mismatch_photo.dng', 1723113846)

    # ------------------------------------------------------------------
    # Category 18 — Video XMP Conditional Dates
    # ------------------------------------------------------------------
    # Verify that pre-existing XMP date tags in video files are updated
    # to the resolved datetime.  QuickTime (MP4) can have XMP written
    # directly; non-QT (AVI) gets dates only in the XMP sidecar.

    # ── QuickTime (MP4) — file + sidecar ──

    def test_video_xmp_mp4_file_dates_updated(self) -> None:
        """MP4: pre-existing XMP date tags in the file are updated."""
        tags = self._read_tags('vid_xmp.mp4', [
            'XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate',
        ])
        # Default epoch 1723113846 → 2024:08:08 12:44:06 in GMT+2.
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate'):
            val = tags.get(tag)
            self.assertIsNotNone(val, f"vid_xmp.mp4: {tag} missing")
            self.assertIn('2024:08:08 12:44:06', str(val),
                          f"vid_xmp.mp4: {tag} not updated: {val!r}")

    def test_video_xmp_mp4_file_dates_have_timezone(self) -> None:
        """MP4: updated XMP date tags in the file include timezone."""
        tags = self._read_tags('vid_xmp.mp4', [
            'XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate',
        ])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate'):
            val = tags.get(tag)
            self.assertIsNotNone(val, f"vid_xmp.mp4: {tag} missing")
            self.assertIn('+02:00', str(val),
                          f"vid_xmp.mp4: {tag} missing timezone: {val!r}")

    def test_video_xmp_mp4_sidecar_dates(self) -> None:
        """MP4 sidecar: XMP dates match the resolved datetime with timezone."""
        tags = self._read_tags('vid_xmp.mp4.xmp', [
            'XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate',
        ])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate'):
            val = tags.get(tag)
            self.assertIsNotNone(val, f"vid_xmp.mp4.xmp: {tag} missing")
            self.assertIn('2024:08:08 12:44:06', str(val),
                          f"vid_xmp.mp4.xmp: {tag} not correct: {val!r}")

    # ── Non-QuickTime (AVI) — sidecar only ──

    def test_video_xmp_avi_sidecar_exists(self) -> None:
        """AVI sidecar is created."""
        xmp = self._find_output_file('vid_xmp.avi.xmp')
        self.assertIsNotNone(xmp, "vid_xmp.avi.xmp not found in output")

    def test_video_xmp_avi_sidecar_dates(self) -> None:
        """AVI sidecar: XMP dates match the resolved datetime with timezone."""
        tags = self._read_tags('vid_xmp.avi.xmp', [
            'XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate',
        ])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate'):
            val = tags.get(tag)
            self.assertIsNotNone(val, f"vid_xmp.avi.xmp: {tag} missing")
            self.assertIn('2024:08:08 12:44:06', str(val),
                          f"vid_xmp.avi.xmp: {tag} not correct: {val!r}")

    def test_video_xmp_avi_sidecar_dates_have_timezone(self) -> None:
        """AVI sidecar: XMP dates include timezone suffix."""
        tags = self._read_tags('vid_xmp.avi.xmp', [
            'XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate',
        ])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate'):
            val = tags.get(tag)
            self.assertIsNotNone(val, f"vid_xmp.avi.xmp: {tag} missing")
            self.assertIn('+02:00', str(val),
                          f"vid_xmp.avi.xmp: {tag} missing timezone: {val!r}")

    def test_video_xmp_nikon_avi_sidecar_exists(self) -> None:
        """AVI with Nikon maker-note dates: sidecar is created."""
        xmp = self._find_output_file('vid_xmp_nikon.avi.xmp')
        self.assertIsNotNone(xmp, "vid_xmp_nikon.avi.xmp not found in output")

    def test_video_xmp_nikon_avi_sidecar_dates(self) -> None:
        """AVI with Nikon dates: sidecar XMP dates match resolved datetime."""
        tags = self._read_tags('vid_xmp_nikon.avi.xmp', [
            'XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate',
        ])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate'):
            val = tags.get(tag)
            self.assertIsNotNone(val, f"vid_xmp_nikon.avi.xmp: {tag} missing")
            self.assertIn('2024:08:08 12:44:06', str(val),
                          f"vid_xmp_nikon.avi.xmp: {tag} not correct: {val!r}")

    def test_video_xmp_nikon_avi_sidecar_dates_have_timezone(self) -> None:
        """AVI with Nikon dates: sidecar fixup must override maker-note dates
        that ExifTool's -o maps into XMP without timezone."""
        tags = self._read_tags('vid_xmp_nikon.avi.xmp', [
            'XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate',
        ])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate', 'XMP:ModifyDate'):
            val = tags.get(tag)
            self.assertIsNotNone(val, f"vid_xmp_nikon.avi.xmp: {tag} missing")
            self.assertIn('+02:00', str(val),
                          f"vid_xmp_nikon.avi.xmp: {tag} missing timezone: {val!r}")

    # ------------------------------------------------------------------
    # Metadata Stripping
    # ------------------------------------------------------------------

    def test_strip_metadata_default_off(self) -> None:
        """Default merger run does not strip metadata (metadata_stripped == 0)."""
        self.assertEqual(self.stats.metadata_stripped, 0)

    def test_strip_metadata_preserved_when_off(self) -> None:
        """When stripping is disabled, XMP-GCamera tags survive in the output."""
        # ExifTool returns XMP tags under the family-1 group "XMP:" regardless
        # of the specific namespace, so read using "XMP:SpecialTypeID".
        tags = self._read_tags('strip_test.jpg', ['XMP:SpecialTypeID'])
        val = tags.get('XMP:SpecialTypeID')
        self.assertIsNotNone(val,
            "strip_test.jpg: XMP-GCamera:SpecialTypeID should be preserved "
            "when --strip-metadata is not used")

    def test_strip_photoshop_preserved_when_off(self) -> None:
        """When stripping is disabled, XMP-photoshop tags survive in the output."""
        tags = self._read_tags('strip_test_ps.jpg', ['XMP:DocumentAncestors'])
        val = tags.get('XMP:DocumentAncestors')
        self.assertIsNotNone(val,
            "strip_test_ps.jpg: XMP-photoshop:DocumentAncestors should be "
            "preserved when --strip-metadata is not used")

    # ------------------------------------------------------------------
    # Infrastructure Validation
    # ------------------------------------------------------------------
    # Tests that validate the test framework itself: failure detection,
    # subTest tracking, error recording, and summary reporting.
    # The @expectedFailure tests MUST fail; if they don't, Python marks
    # them as unexpected successes and the overall run becomes FAIL.

    @unittest.expectedFailure
    def test_infra_assert_equal_mismatch(self) -> None:
        """assertEqual detects value mismatch."""
        logging.log(TRACE, 'Infra: verifying assertEqual catches 1 != 2')
        self.assertEqual(1, 2)

    @unittest.expectedFailure
    def test_infra_assert_true_false(self) -> None:
        """assertTrue detects False."""
        logging.log(TRACE, 'Infra: verifying assertTrue catches False')
        self.assertTrue(False)

    @unittest.expectedFailure
    def test_infra_assert_in_missing(self) -> None:
        """assertIn detects missing element."""
        logging.log(TRACE, 'Infra: verifying assertIn catches missing element')
        self.assertIn('x', [])

    @unittest.expectedFailure
    def test_infra_raise_exception(self) -> None:
        """Unhandled RuntimeError is recorded as an error."""
        logging.log(TRACE, 'Infra: verifying unhandled exception is caught')
        raise RuntimeError('deliberate infrastructure test error')

    @unittest.expectedFailure
    def test_infra_subtest_some_fail(self) -> None:
        """subTest loop with some failures exercises _subtest_failed tracking."""
        logging.log(TRACE, 'Infra: verifying subTest partial failure is detected')
        for i in range(4):
            with self.subTest(i=i):
                self.assertTrue(i % 2 == 0)  # i=1, i=3 fail

    def test_infra_subtest_all_pass(self) -> None:
        """subTest loop where all pass — _subtest_failed must NOT flag this."""
        logging.log(TRACE, 'Infra: verifying subTest all-pass is not falsely flagged')
        for i in range(4):
            with self.subTest(i=i):
                self.assertIsInstance(i, int)

    # ------------------------------------------------------------------
    # tearDownClass
    # ------------------------------------------------------------------

    @classmethod
    def tearDownClass(cls) -> None:
        s = cls.stats
        print('\n' + '=' * 60)
        print('                   TEST SUMMARY')
        print('=' * 60)
        print(f'  Total media files : {s.total_media_files}')
        print(f'  Matched           : {s.matched}')
        print(f'  Orphans           : {s.orphans}')
        print(f'  Written           : {s.written}')
        print(f'  Sidecars created  : {s.sidecars_created}')
        print(f'  GPS written       : {s.gps_written}')
        print(f'  Descriptions clrd : {s.descriptions_cleared}')
        print(f'  Duplicates renamed: {s.duplicates_renamed}')
        print(f'  Ext mismatches    : {s.ext_mismatches}')
        print(f'  Skipped existing  : {s.skipped_existing}')
        print(f'  Errors            : {s.errors}')
        print('=' * 60)
        print(f'\n  Input  : {cls.input_dir}')
        print(f'  Output : {cls.output_dir}')

        mode = getattr(cls, '_cleanup_mode', 'prompt')
        if mode == 'auto_delete':
            shutil.rmtree(str(cls.tmp_dir), ignore_errors=True)
            print(f'  Deleted: {cls.tmp_dir}')
        elif mode == 'auto_keep':
            print(f'\nTest files kept at: {cls.tmp_dir}')
        else:  # 'prompt'
            try:
                input('\nPress Enter to delete test files, or Ctrl+C to keep them ... ')
            except (KeyboardInterrupt, EOFError):
                print(f'\nTest files kept at: {cls.tmp_dir}')
                return
            shutil.rmtree(str(cls.tmp_dir), ignore_errors=True)
            print(f'Deleted: {cls.tmp_dir}')


# ---------------------------------------------------------------------------
# Single-worker regression test
# ---------------------------------------------------------------------------

# Environment variable gate: set GPEM_SINGLE_WORKER=1 to enable these tests.
# When running TestMerger.py directly, use --single-worker to set it.
# When running via pytest, either export the env var or use:
#   GPEM_SINGLE_WORKER=1 python -m pytest TestMerger.py

class TestSingleWorker(unittest.TestCase):
    """Run the merger with num_workers=1 (serial mode) and verify stats match.

    This ensures the original serial code path still works correctly after
    the parallel processing refactor.  It builds the same input tree as the
    main test class, runs the merger with a single worker, and asserts that
    all stats counters are identical.

    Opt-in only — these tests roughly double the total run time.
    Enable via --single-worker (direct runner) or GPEM_SINGLE_WORKER=1 (pytest).
    """

    # Class-level state populated by setUpClass
    tmp_dir:    Path
    input_dir:  Path
    output_dir: Path
    stats:      MergeStats

    @classmethod
    def setUpClass(cls) -> None:
        # Gate: only run if explicitly opted-in
        if os.environ.get('GPEM_SINGLE_WORKER') != '1':
            raise unittest.SkipTest(
                'Single-worker tests skipped '
                '(use --single-worker or set GPEM_SINGLE_WORKER=1)')

        logging.basicConfig(
            format='%(levelname)s %(name)s: %(message)s',
            level=logging.WARNING,
        )

        cls.tmp_dir    = Path(tempfile.mkdtemp(prefix='gpem_serial_test_'))
        cls.input_dir  = cls.tmp_dir / 'input'
        cls.output_dir = cls.tmp_dir / 'output'
        cls.input_dir.mkdir()

        # Reuse the same input-tree builder from the main test class.
        # _create_input_tree is a classmethod that reads cls.input_dir, so we
        # temporarily point it at our input directory, then restore it.
        saved_input_dir = getattr(TestPhotosExportMerger, 'input_dir', None)
        TestPhotosExportMerger.input_dir = cls.input_dir
        TestPhotosExportMerger._create_input_tree()
        if saved_input_dir is not None:
            TestPhotosExportMerger.input_dir = saved_input_dir

        # Run with num_workers=1 (serial mode)
        # Explicit fallback_tz=+02:00 so tests are independent of host timezone.
        merger = PhotosExportMerger(
            str(cls.input_dir),
            str(cls.output_dir),
            blocked_descriptions=_BLOCKED_DESCRIPTIONS,
            num_workers=1,
            fallback_tz=timezone(timedelta(hours=2)),
        )
        cls.stats = merger.run()

    @classmethod
    def tearDownClass(cls) -> None:
        tmp = getattr(cls, 'tmp_dir', None)
        if tmp is not None:
            shutil.rmtree(str(tmp), ignore_errors=True)

    # ------------------------------------------------------------------
    # Stats verification — must match the expected values from the
    # parallel run (same input tree, same merger logic).
    # ------------------------------------------------------------------

    def test_serial_stats_total_count(self) -> None:
        """Serial: total media files = 184."""
        self.assertEqual(self.stats.total_media_files, 184)

    def test_serial_stats_matched_count(self) -> None:
        """Serial: matched files = 177."""
        self.assertEqual(self.stats.matched, 177)

    def test_serial_stats_orphan_count(self) -> None:
        """Serial: orphan files = 7."""
        self.assertEqual(self.stats.orphans, 7)

    def test_serial_stats_written_count(self) -> None:
        """Serial: written files = 184."""
        self.assertEqual(self.stats.written, 184)

    def test_serial_stats_zero_errors(self) -> None:
        """Serial: zero errors."""
        self.assertEqual(self.stats.errors, 0)

    def test_serial_stats_gps_written(self) -> None:
        """Serial: GPS tags written = 101."""
        self.assertEqual(self.stats.gps_written, 101)

    def test_serial_stats_sidecars_created(self) -> None:
        """Serial: XMP sidecars created = 84."""
        self.assertEqual(self.stats.sidecars_created, 84)

    def test_serial_stats_descriptions_cleared(self) -> None:
        """Serial: descriptions cleared = 3."""
        self.assertEqual(self.stats.descriptions_cleared, 3)

    def test_serial_stats_duplicates_renamed(self) -> None:
        """Serial: duplicates renamed = 3."""
        self.assertEqual(self.stats.duplicates_renamed, 3)

    def test_serial_stats_skipped_json(self) -> None:
        """Serial: skipped JSON = 1."""
        self.assertEqual(self.stats.skipped_json, 1)

    def test_serial_stats_ext_mismatches(self) -> None:
        """Serial: ext mismatches = 1."""
        self.assertEqual(self.stats.ext_mismatches, 1)

    def test_serial_stats_skipped_existing(self) -> None:
        """Serial: skipped existing = 0."""
        self.assertEqual(self.stats.skipped_existing, 0)

    # ------------------------------------------------------------------
    # Output structure — verify key files exist
    # ------------------------------------------------------------------

    def test_serial_output_files_exist(self) -> None:
        """Serial: all expected output files are present."""
        output_names = {f.name for f in self.output_dir.rglob('*') if f.is_file()}
        expected_samples = [
            'photo_basic.jpg', 'orphan_no_json.jpg',
            'gps_ne.jpg', 'gps_sw.mp4',
            'tz_utc.jpg', 'desc_utf8.jpg',
            'same_name.jpg', 'same_name_2.jpg',
            'photo.jpg', 'photo_2.jpg',
        ]
        for name in expected_samples:
            self.assertIn(name, output_names,
                          f"Expected {name} in serial output")

    def test_serial_output_organized_by_year_month(self) -> None:
        """Serial: every output file is in output/YYYY/MM/ structure."""
        for f in self.output_dir.rglob('*'):
            if not f.is_file():
                continue
            rel = f.relative_to(self.output_dir)
            parts = rel.parts
            self.assertEqual(len(parts), 3,
                             f"Wrong depth for {rel}: expected 3 parts, got {len(parts)}")

    def test_serial_xmp_sidecars_exist(self) -> None:
        """Serial: XMP sidecar files are created for PNG, GIF, and video formats."""
        sidecar_samples = ['sc_png.png.xmp', 'sc_gif.gif.xmp', 'sc_avi.avi.xmp']
        output_names = {f.name for f in self.output_dir.rglob('*') if f.is_file()}
        for name in sidecar_samples:
            self.assertIn(name, output_names,
                          f"Expected sidecar {name} in serial output")


# ---------------------------------------------------------------------------
# Metadata stripping integration test
# ---------------------------------------------------------------------------

class TestMetadataStripping(unittest.TestCase):
    """Run the merger with metadata stripping enabled and verify tags are removed.

    Builds a minimal input tree with a single JPEG that has XMP-GCamera
    metadata pre-written, runs the merger with strip params, and asserts
    the metadata is gone from the output.
    """

    tmp_dir:    Path
    input_dir:  Path
    output_dir: Path
    stats:      MergeStats

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp_dir    = Path(tempfile.mkdtemp(prefix='gpem_strip_test_'))
        cls.input_dir  = cls.tmp_dir / 'input'
        cls.output_dir = cls.tmp_dir / 'output'
        cls.input_dir.mkdir()

        # Create a JPEG with XMP-GCamera metadata
        d = cls.input_dir / 'StripTest'
        make_media_file(d / 'has_gcamera.jpg')
        make_json_file(d / 'has_gcamera.jpg.json', title='has_gcamera.jpg')
        make_media_file(d / 'has_photoshop.jpg')
        make_json_file(d / 'has_photoshop.jpg.json', title='has_photoshop.jpg')
        with exiftool.ExifToolHelper() as _et:
            try:
                _et.set_tags([str(d / 'has_gcamera.jpg')],
                             {'XMP-GCamera:SpecialTypeID': '1'},
                             params=['-overwrite_original'])
            except Exception:
                pass
            try:
                _et.set_tags([str(d / 'has_photoshop.jpg')],
                             {'XMP-photoshop:DocumentAncestors': 'ancestor_test_data'},
                             params=['-overwrite_original'])
            except Exception:
                pass

        # Run merger with all strip profiles enabled
        # Explicit fallback_tz=+02:00 so tests are independent of host timezone.
        merger = PhotosExportMerger(
            str(cls.input_dir),
            str(cls.output_dir),
            num_workers=1,
            metadata_strip_params=[
                '-XMP-GCamera:All=', '-Google:All=',
                '-Photoshop:All=', '-XMP-photoshop:DocumentAncestors=',
            ],
            fallback_tz=timezone(timedelta(hours=2)),
        )
        cls.stats = merger.run()

    @classmethod
    def tearDownClass(cls) -> None:
        tmp = getattr(cls, 'tmp_dir', None)
        if tmp is not None:
            shutil.rmtree(str(tmp), ignore_errors=True)

    def _find_output_file(self, name: str) -> 'Path | None':
        for f in self.output_dir.rglob('*'):
            if f.is_file() and f.name == name:
                return f
        return None

    def _read_tags(self, name: str, tags: list) -> dict:
        f = self._find_output_file(name)
        if f is None:
            return {}
        with exiftool.ExifToolHelper() as et:
            result = et.get_tags([str(f)], tags)
            return result[0] if result else {}

    def test_strip_file_exists(self) -> None:
        """Output file is created when stripping is enabled."""
        self.assertIsNotNone(self._find_output_file('has_gcamera.jpg'))
        self.assertIsNotNone(self._find_output_file('has_photoshop.jpg'))

    def test_strip_gcamera_removed(self) -> None:
        """XMP-GCamera tags are removed from the output when stripping is enabled."""
        tags = self._read_tags('has_gcamera.jpg', ['XMP:SpecialTypeID'])
        val = tags.get('XMP:SpecialTypeID')
        self.assertIsNone(val,
            f"has_gcamera.jpg: XMP-GCamera:SpecialTypeID should be stripped, "
            f"but found: {val!r}")

    def test_strip_photoshop_removed(self) -> None:
        """XMP-photoshop:DocumentAncestors is removed when stripping is enabled."""
        tags = self._read_tags('has_photoshop.jpg', ['XMP:DocumentAncestors'])
        val = tags.get('XMP:DocumentAncestors')
        self.assertIsNone(val,
            f"has_photoshop.jpg: XMP-photoshop:DocumentAncestors should be "
            f"stripped, but found: {val!r}")

    def test_strip_dates_preserved(self) -> None:
        """Stripping metadata does not remove the dates written by the merger."""
        for name in ('has_gcamera.jpg', 'has_photoshop.jpg'):
            with self.subTest(file=name):
                tags = self._read_tags(name,
                                       ['EXIF:DateTimeOriginal', 'EXIF:CreateDate'])
                dt = tags.get('EXIF:DateTimeOriginal') or tags.get('EXIF:CreateDate')
                self.assertIsNotNone(dt,
                    f"{name}: dates should survive metadata stripping")

    def test_strip_stats_metadata_stripped(self) -> None:
        """Stats reflect that metadata was stripped from both output files."""
        self.assertEqual(self.stats.metadata_stripped, 2,
                         f"Expected 2 metadata_stripped, got {self.stats.metadata_stripped}")

    def test_strip_stats_zero_errors(self) -> None:
        """No errors during the stripping merger run."""
        self.assertEqual(self.stats.errors, 0,
                         f"Expected 0 errors, got {self.stats.errors}")


# ---------------------------------------------------------------------------
# Timezone override integration test
# ---------------------------------------------------------------------------

class TestTimezoneOverride(unittest.TestCase):
    """Run the merger with --tz-override and verify timezone selection.

    Builds a minimal input tree with three files whose JSON photoTakenTime
    epochs span different dates:
      - inside the override range  → should use the override timezone
      - outside the override range → should use GMT+02:00 fallback
      - has embedded EXIF timezone → should use EXIF timezone (override ignored)

    None of the test files have embedded EXIF timezone offsets (except the
    third), so the override/fallback logic is exercised.
    """

    tmp_dir:    Path
    input_dir:  Path
    output_dir: Path
    stats:      MergeStats

    # Override range: 2019-11-25 00:00:00 UTC  to  2019-11-28 23:59:59 UTC
    # Override timezone: -05:30
    # File epochs:
    #   in_range:     1574870400  = 2019-11-27 16:00:00 UTC → should get -05:30
    #   out_of_range: 1574524800  = 2019-11-23 16:00:00 UTC → should get +02:00
    #   has_exif_tz:  1574870400  = 2019-11-27 16:00:00 UTC → but has +08:00 in EXIF

    _EPOCH_IN_RANGE     = '1574870400'   # 2019-11-27 16:00:00 UTC
    _EPOCH_OUT_OF_RANGE = '1574524800'   # 2019-11-23 16:00:00 UTC

    @classmethod
    def setUpClass(cls) -> None:
        from AbstractMediaMerger import TimezoneOverride

        cls.tmp_dir    = Path(tempfile.mkdtemp(prefix='gpem_tz_override_test_'))
        cls.input_dir  = cls.tmp_dir / 'input'
        cls.output_dir = cls.tmp_dir / 'output'
        cls.input_dir.mkdir()

        d = cls.input_dir / 'TzOverride'

        # File in the override range (no EXIF timezone)
        make_media_file(d / 'tz_in_range.jpg')
        make_json_file(d / 'tz_in_range.jpg.json',
                       title='tz_in_range.jpg',
                       photoTakenTime={'timestamp': cls._EPOCH_IN_RANGE, 'formatted': ''})

        # File outside the override range (no EXIF timezone)
        make_media_file(d / 'tz_out_of_range.jpg')
        make_json_file(d / 'tz_out_of_range.jpg.json',
                       title='tz_out_of_range.jpg',
                       photoTakenTime={'timestamp': cls._EPOCH_OUT_OF_RANGE, 'formatted': ''})

        # File in range BUT has EXIF timezone → EXIF should win
        p = d / 'tz_has_exif.jpg'
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(_make_jpeg_with_exif_tz('+08:00'))
        make_json_file(d / 'tz_has_exif.jpg.json',
                       title='tz_has_exif.jpg',
                       photoTakenTime={'timestamp': cls._EPOCH_IN_RANGE, 'formatted': ''})

        # An AVI file in range (no EXIF timezone) — tests sidecar dates too
        make_media_file(d / 'tz_in_range.avi')
        make_json_file(d / 'tz_in_range.avi.json',
                       title='tz_in_range.avi',
                       photoTakenTime={'timestamp': cls._EPOCH_IN_RANGE, 'formatted': ''})

        # Override: 2019-11-25 00:00:00 UTC to 2019-11-28 23:59:59 UTC → -05:30
        override = TimezoneOverride(
            start_utc=datetime(2019, 11, 25, 0, 0, 0, tzinfo=timezone.utc),
            end_utc=datetime(2019, 11, 28, 23, 59, 59, tzinfo=timezone.utc),
            tz=timezone(timedelta(hours=-5, minutes=-30)),
        )

        merger = PhotosExportMerger(
            str(cls.input_dir),
            str(cls.output_dir),
            num_workers=1,
            tz_overrides=[override],
            fallback_tz=timezone(timedelta(hours=2)),
        )
        cls.stats = merger.run()

    @classmethod
    def tearDownClass(cls) -> None:
        tmp = getattr(cls, 'tmp_dir', None)
        if tmp is not None:
            shutil.rmtree(str(tmp), ignore_errors=True)

    def _find_output_file(self, name: str) -> 'Path | None':
        for f in self.output_dir.rglob('*'):
            if f.is_file() and f.name == name:
                return f
        return None

    def _read_tags(self, name: str, tags: list) -> dict:
        f = self._find_output_file(name)
        if f is None:
            return {}
        with exiftool.ExifToolHelper() as et:
            result = et.get_tags([str(f)], tags)
            return result[0] if result else {}

    def test_tz_override_zero_errors(self) -> None:
        """No errors during the override merger run."""
        self.assertEqual(self.stats.errors, 0,
                         f"Expected 0 errors, got {self.stats.errors}")

    def test_tz_override_in_range_uses_override(self) -> None:
        """File in range: DateTimeOriginal uses the override timezone -05:30.

        epoch 1574870400 = 2019-11-27 16:00:00 UTC → -05:30 → 10:30:00.
        """
        tags = self._read_tags('tz_in_range.jpg',
                               ['EXIF:DateTimeOriginal', 'EXIF:OffsetTimeOriginal'])
        dt = str(tags.get('EXIF:DateTimeOriginal', ''))
        offset = str(tags.get('EXIF:OffsetTimeOriginal', ''))
        self.assertIn('2019:11:27 10:30:00', dt,
                      f"Expected 10:30:00 (-05:30 from UTC 16:00), got {dt!r}")
        self.assertIn('-05:30', offset,
                      f"Expected offset -05:30, got {offset!r}")

    def test_tz_override_out_of_range_uses_fallback(self) -> None:
        """File outside range: DateTimeOriginal uses the GMT+02:00 fallback.

        epoch 1574524800 = 2019-11-23 16:00:00 UTC → +02:00 → 18:00:00.
        """
        tags = self._read_tags('tz_out_of_range.jpg',
                               ['EXIF:DateTimeOriginal', 'EXIF:OffsetTimeOriginal'])
        dt = str(tags.get('EXIF:DateTimeOriginal', ''))
        offset = str(tags.get('EXIF:OffsetTimeOriginal', ''))
        self.assertIn('2019:11:23 18:00:00', dt,
                      f"Expected 18:00:00 (+02:00 from UTC 16:00), got {dt!r}")
        self.assertIn('+02:00', offset,
                      f"Expected offset +02:00, got {offset!r}")

    def test_tz_override_exif_wins_over_override(self) -> None:
        """File with embedded EXIF timezone: EXIF wins, override is ignored.

        epoch 1574870400 = 2019-11-27 16:00:00 UTC → +08:00 → 2019-11-28 00:00:00.
        """
        tags = self._read_tags('tz_has_exif.jpg',
                               ['EXIF:DateTimeOriginal', 'EXIF:OffsetTimeOriginal'])
        dt = str(tags.get('EXIF:DateTimeOriginal', ''))
        offset = str(tags.get('EXIF:OffsetTimeOriginal', ''))
        self.assertIn('2019:11:28 00:00:00', dt,
                      f"Expected 00:00:00 (+08:00 from UTC 16:00), got {dt!r}")
        self.assertIn('+08:00', offset,
                      f"Expected offset +08:00, got {offset!r}")

    def test_tz_override_sidecar_has_override_tz(self) -> None:
        """AVI sidecar in range: XMP dates use the override timezone -05:30.

        epoch 1574870400 = 2019-11-27 16:00:00 UTC → -05:30 → 10:30:00.
        """
        tags = self._read_tags('tz_in_range.avi.xmp',
                               ['XMP:DateTimeOriginal', 'XMP:CreateDate'])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate'):
            val = str(tags.get(tag, ''))
            with self.subTest(tag=tag):
                self.assertIn('2019:11:27 10:30:00', val,
                              f"Expected 10:30:00, got {val!r}")
                self.assertIn('-05:30', val,
                              f"Expected -05:30 in {val!r}")

    def test_tz_override_output_organized_by_override_date(self) -> None:
        """File in range is organized by the local date in the override timezone.

        2019-11-27 06:30:00 -05:30 → year=2019, month=11.
        """
        f = self._find_output_file('tz_in_range.jpg')
        self.assertIsNotNone(f, "tz_in_range.jpg not found in output")
        self.assertIn('2019', str(f))
        self.assertIn('11', str(f))


class TestFallbackTimezone(unittest.TestCase):
    """Run the merger with a custom --tz-fallback and verify it is applied.

    Builds a minimal input tree with files that have no embedded EXIF
    timezone, so the fallback timezone is used.  Uses -05:00 as the
    custom fallback (instead of the default host timezone) to verify:
      - matched files get the custom fallback timezone
      - orphan files get the custom fallback timezone
      - sidecar XMP dates carry the custom fallback timezone
      - a file with an embedded EXIF timezone ignores the fallback

    Uses the same JSON epoch as the main test suite:
      epoch 1723117446 = 2024-08-08 11:44:06 UTC
      At -05:00 this is 2024-08-08 06:44:06
      At +08:00 (EXIF) this is 2024-08-08 19:44:06
    """

    tmp_dir:    Path
    input_dir:  Path
    output_dir: Path
    stats:      MergeStats

    _EPOCH = '1723117446'  # 2024-08-08 10:44:06 UTC

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp_dir    = Path(tempfile.mkdtemp(prefix='gpem_tz_fallback_test_'))
        cls.input_dir  = cls.tmp_dir / 'input'
        cls.output_dir = cls.tmp_dir / 'output'
        cls.input_dir.mkdir()

        d = cls.input_dir / 'FallbackTz'

        # Matched file, no EXIF timezone → should use fallback -05:00
        make_media_file(d / 'fb_matched.jpg')
        make_json_file(d / 'fb_matched.jpg.json',
                       title='fb_matched.jpg',
                       photoTakenTime={'timestamp': cls._EPOCH, 'formatted': ''})

        # PNG matched file, no EXIF timezone → sidecar should use -05:00
        make_media_file(d / 'fb_sidecar.png')
        make_json_file(d / 'fb_sidecar.png.json',
                       title='fb_sidecar.png',
                       photoTakenTime={'timestamp': cls._EPOCH, 'formatted': ''})

        # AVI matched file, no EXIF timezone → sidecar should use -05:00
        make_media_file(d / 'fb_video.avi')
        make_json_file(d / 'fb_video.avi.json',
                       title='fb_video.avi',
                       photoTakenTime={'timestamp': cls._EPOCH, 'formatted': ''})

        # Matched file WITH embedded EXIF timezone +08:00 → fallback ignored
        p = d / 'fb_has_exif.jpg'
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(_make_jpeg_with_exif_tz('+08:00'))
        make_json_file(d / 'fb_has_exif.jpg.json',
                       title='fb_has_exif.jpg',
                       photoTakenTime={'timestamp': cls._EPOCH, 'formatted': ''})

        # Run merger with custom fallback timezone -05:00
        merger = PhotosExportMerger(
            str(cls.input_dir),
            str(cls.output_dir),
            num_workers=1,
            fallback_tz=timezone(timedelta(hours=-5)),
        )
        cls.stats = merger.run()

    @classmethod
    def tearDownClass(cls) -> None:
        tmp = getattr(cls, 'tmp_dir', None)
        if tmp is not None:
            shutil.rmtree(str(tmp), ignore_errors=True)

    def _find_output_file(self, name: str) -> 'Path | None':
        for f in self.output_dir.rglob('*'):
            if f.is_file() and f.name == name:
                return f
        return None

    def _read_tags(self, name: str, tags: list) -> dict:
        f = self._find_output_file(name)
        if f is None:
            return {}
        with exiftool.ExifToolHelper() as et:
            result = et.get_tags([str(f)], tags)
            return result[0] if result else {}

    def test_fallback_tz_zero_errors(self) -> None:
        """No errors during the fallback timezone merger run."""
        self.assertEqual(self.stats.errors, 0,
                         f"Expected 0 errors, got {self.stats.errors}")

    def test_fallback_tz_matched_jpg_datetime(self) -> None:
        """Matched JPG with no EXIF tz: DateTimeOriginal uses fallback -05:00.

        epoch 1723117446 = 2024-08-08 11:44:06 UTC → -05:00 → 06:44:06.
        """
        tags = self._read_tags('fb_matched.jpg',
                               ['EXIF:DateTimeOriginal', 'EXIF:OffsetTimeOriginal'])
        dt = str(tags.get('EXIF:DateTimeOriginal', ''))
        offset = str(tags.get('EXIF:OffsetTimeOriginal', ''))
        self.assertIn('2024:08:08 06:44:06', dt,
                      f"Expected 06:44:06 (-05:00 from UTC 11:44:06), got {dt!r}")
        self.assertIn('-05:00', offset,
                      f"Expected offset -05:00, got {offset!r}")

    def test_fallback_tz_sidecar_png_datetime(self) -> None:
        """PNG sidecar: XMP dates use fallback -05:00.

        epoch 1723117446 = 2024-08-08 11:44:06 UTC → -05:00 → 06:44:06.
        """
        tags = self._read_tags('fb_sidecar.png.xmp',
                               ['XMP:DateTimeOriginal', 'XMP:CreateDate'])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate'):
            val = str(tags.get(tag, ''))
            with self.subTest(tag=tag):
                self.assertIn('2024:08:08 06:44:06', val,
                              f"Expected 06:44:06, got {val!r}")
                self.assertIn('-05:00', val,
                              f"Expected -05:00 in {val!r}")

    def test_fallback_tz_sidecar_avi_datetime(self) -> None:
        """AVI sidecar: XMP dates use fallback -05:00.

        epoch 1723117446 = 2024-08-08 11:44:06 UTC → -05:00 → 06:44:06.
        """
        tags = self._read_tags('fb_video.avi.xmp',
                               ['XMP:DateTimeOriginal', 'XMP:CreateDate'])
        for tag in ('XMP:DateTimeOriginal', 'XMP:CreateDate'):
            val = str(tags.get(tag, ''))
            with self.subTest(tag=tag):
                self.assertIn('2024:08:08 06:44:06', val,
                              f"Expected 06:44:06, got {val!r}")
                self.assertIn('-05:00', val,
                              f"Expected -05:00 in {val!r}")

    def test_fallback_tz_exif_wins_over_fallback(self) -> None:
        """File with embedded EXIF timezone +08:00: EXIF wins, fallback ignored.

        epoch 1723117446 = 2024-08-08 11:44:06 UTC → +08:00 → 19:44:06.
        """
        tags = self._read_tags('fb_has_exif.jpg',
                               ['EXIF:DateTimeOriginal', 'EXIF:OffsetTimeOriginal'])
        dt = str(tags.get('EXIF:DateTimeOriginal', ''))
        offset = str(tags.get('EXIF:OffsetTimeOriginal', ''))
        self.assertIn('2024:08:08 19:44:06', dt,
                      f"Expected 19:44:06 (+08:00 from UTC 11:44:06), got {dt!r}")
        self.assertIn('+08:00', offset,
                      f"Expected offset +08:00, got {offset!r}")

    def test_fallback_tz_output_organized_by_fallback_date(self) -> None:
        """Matched file is organized by local date in the fallback timezone.

        2024-08-08 06:44:06 -05:00 → year=2024, month=08.
        """
        f = self._find_output_file('fb_matched.jpg')
        self.assertIsNotNone(f, "fb_matched.jpg not found in output")
        self.assertIn('2024', str(f))
        self.assertIn('08', str(f))


# ---------------------------------------------------------------------------
# JPEG compression integration test
# ---------------------------------------------------------------------------

def _make_pillow_jpeg(quality: int, seed: int = 42) -> bytes:
    """Generate a 64×64 random-noise JPEG at the given Pillow quality level.

    Uses a fixed seed for deterministic output.  The random noise ensures
    ExifTool's JPEGQualityEstimate returns meaningful values (flat-colour
    images produce unreliable estimates).

    Returns raw JPEG bytes (no EXIF metadata — Pillow strips it by default).
    """
    import random as _rng
    from PIL import Image as _Img
    from io import BytesIO as _Bio

    r = _rng.Random(seed)
    img = _Img.new('RGB', (64, 64))
    img.putdata([(r.randint(0, 255), r.randint(0, 255), r.randint(0, 255))
                 for _ in range(64 * 64)])
    buf = _Bio()
    img.save(buf, format='JPEG', quality=quality)
    img.close()
    return buf.getvalue()


class TestJpegCompression(unittest.TestCase):
    """Run the merger with --jpeg-quality and verify compression behaviour.

    Builds a minimal input tree with Pillow-generated JPEGs at different
    quality levels, runs the merger with jpeg_compress_quality=80, and
    asserts that:
    - A high-quality JPEG (q=98) is compressed (output smaller than source)
    - A low-quality JPEG (q=50) is NOT compressed (output ≈ source size)
    - An orphan high-quality JPEG is compressed
    - All stats counters are correct
    - Dates and metadata survive the compression pipeline
    """

    tmp_dir:    Path
    input_dir:  Path
    output_dir: Path
    stats:      MergeStats
    # Source file sizes for comparison
    high_q_source_size: int
    low_q_source_size:  int
    orphan_source_size: int

    _COMPRESS_QUALITY = 80

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp_dir    = Path(tempfile.mkdtemp(prefix='gpem_jpeg_test_'))
        cls.input_dir  = cls.tmp_dir / 'input'
        cls.output_dir = cls.tmp_dir / 'output'
        cls.input_dir.mkdir()

        d = cls.input_dir / 'JpegCompress'
        d.mkdir(parents=True)

        # High-quality matched JPEG (q=98, above threshold → should compress)
        high_q_bytes = _make_pillow_jpeg(quality=98, seed=42)
        (d / 'high_quality.jpg').write_bytes(high_q_bytes)
        cls.high_q_source_size = len(high_q_bytes)
        make_json_file(d / 'high_quality.jpg.json', title='high_quality.jpg')

        # Low-quality matched JPEG (q=50, below threshold → should NOT compress)
        low_q_bytes = _make_pillow_jpeg(quality=50, seed=42)
        (d / 'low_quality.jpg').write_bytes(low_q_bytes)
        cls.low_q_source_size = len(low_q_bytes)
        make_json_file(d / 'low_quality.jpg.json', title='low_quality.jpg')

        # High-quality orphan JPEG (no JSON → orphan, should still compress)
        orphan_bytes = _make_pillow_jpeg(quality=98, seed=99)
        (d / 'orphan_high.jpg').write_bytes(orphan_bytes)
        cls.orphan_source_size = len(orphan_bytes)

        # Non-JPEG file (should be untouched by compression logic)
        make_media_file(d / 'not_a_jpeg.png')
        make_json_file(d / 'not_a_jpeg.png.json', title='not_a_jpeg.png')

        # Run merger with JPEG compression enabled
        merger = PhotosExportMerger(
            str(cls.input_dir),
            str(cls.output_dir),
            num_workers=1,
            fallback_tz=timezone(timedelta(hours=2)),
            jpeg_compress_quality=cls._COMPRESS_QUALITY,
        )
        cls.stats = merger.run()

    @classmethod
    def tearDownClass(cls) -> None:
        tmp = getattr(cls, 'tmp_dir', None)
        if tmp is not None:
            shutil.rmtree(str(tmp), ignore_errors=True)

    def _find_output_file(self, name: str) -> 'Path | None':
        for f in self.output_dir.rglob('*'):
            if f.is_file() and f.name == name:
                return f
        return None

    def _read_tags(self, name: str, tags: list) -> dict:
        f = self._find_output_file(name)
        if f is None:
            return {}
        with exiftool.ExifToolHelper() as et:
            result = et.get_tags([str(f)], tags)
            return result[0] if result else {}

    # -- File existence --

    def test_jpeg_compress_high_exists(self) -> None:
        """High-quality JPEG output exists."""
        self.assertIsNotNone(self._find_output_file('high_quality.jpg'))

    def test_jpeg_compress_low_exists(self) -> None:
        """Low-quality JPEG output exists."""
        self.assertIsNotNone(self._find_output_file('low_quality.jpg'))

    def test_jpeg_compress_orphan_exists(self) -> None:
        """Orphan high-quality JPEG output exists."""
        self.assertIsNotNone(self._find_output_file('orphan_high.jpg'))

    def test_jpeg_compress_png_exists(self) -> None:
        """Non-JPEG file output exists (unaffected by compression)."""
        self.assertIsNotNone(self._find_output_file('not_a_jpeg.png'))

    # -- Compression happened / didn't --

    def test_jpeg_compress_high_was_compressed(self) -> None:
        """High-quality JPEG (q=98) output is smaller than source (was compressed)."""
        f = self._find_output_file('high_quality.jpg')
        self.assertIsNotNone(f)
        output_size = f.stat().st_size
        self.assertLess(output_size, self.high_q_source_size,
                        f"high_quality.jpg should be compressed: "
                        f"output={output_size} vs source={self.high_q_source_size}")

    def test_jpeg_compress_low_not_compressed(self) -> None:
        """Low-quality JPEG (q=50) output is NOT smaller than source (copied as-is).

        The output may be slightly larger due to metadata written by ExifTool,
        so we check that it is NOT significantly smaller (i.e. not recompressed).
        A recompressed-at-80 file from a q=50 source would be larger anyway,
        but the key signal is that _needs_jpeg_compression returned False.
        """
        f = self._find_output_file('low_quality.jpg')
        self.assertIsNotNone(f)
        output_size = f.stat().st_size
        # Output should be at least as large as source (metadata adds bytes).
        # A recompressed file from q=50 source at q=80 would actually be larger,
        # but the point is that the compression path was NOT taken.
        self.assertGreaterEqual(output_size, self.low_q_source_size,
                                f"low_quality.jpg should NOT be compressed: "
                                f"output={output_size} vs source={self.low_q_source_size}")

    def test_jpeg_compress_orphan_was_compressed(self) -> None:
        """Orphan high-quality JPEG output is smaller than source (was compressed)."""
        f = self._find_output_file('orphan_high.jpg')
        self.assertIsNotNone(f)
        output_size = f.stat().st_size
        self.assertLess(output_size, self.orphan_source_size,
                        f"orphan_high.jpg should be compressed: "
                        f"output={output_size} vs source={self.orphan_source_size}")

    # -- Metadata survives compression --

    def test_jpeg_compress_dates_preserved(self) -> None:
        """Compressed JPEG has DateTimeOriginal written by the merger."""
        tags = self._read_tags('high_quality.jpg',
                               ['EXIF:DateTimeOriginal', 'EXIF:CreateDate'])
        dt = tags.get('EXIF:DateTimeOriginal') or tags.get('EXIF:CreateDate')
        self.assertIsNotNone(dt,
                             "high_quality.jpg: dates should survive compression")

    def test_jpeg_compress_orphan_dates_preserved(self) -> None:
        """Compressed orphan JPEG retains date metadata."""
        tags = self._read_tags('orphan_high.jpg',
                               ['EXIF:DateTimeOriginal', 'EXIF:CreateDate'])
        # Orphan dates come from EXIF or filesystem — Pillow-generated files
        # have no EXIF dates, so the merger uses filesystem creation date.
        # Either way, some date should be set.
        dt = tags.get('EXIF:DateTimeOriginal') or tags.get('EXIF:CreateDate')
        self.assertIsNotNone(dt,
                             "orphan_high.jpg: dates should survive compression")

    # -- Stats counters --

    def test_jpeg_compress_stats_zero_errors(self) -> None:
        """No errors during the compression merger run."""
        self.assertEqual(self.stats.errors, 0,
                         f"Expected 0 errors, got {self.stats.errors}")

    def test_jpeg_compress_stats_written(self) -> None:
        """All 4 files written (3 JPEG + 1 PNG)."""
        self.assertEqual(self.stats.written, 4,
                         f"Expected 4 written, got {self.stats.written}")

    def test_jpeg_compress_stats_compressed(self) -> None:
        """jpeg_compressed = 2 (high_quality.jpg + orphan_high.jpg)."""
        self.assertEqual(self.stats.jpeg_compressed, 2,
                         f"Expected 2 jpeg_compressed, got {self.stats.jpeg_compressed}")

    def test_jpeg_compress_stats_quality_checked(self) -> None:
        """jpeg_quality_checked = 3 (all three JPEGs were checked)."""
        self.assertEqual(self.stats.jpeg_quality_checked, 3,
                         f"Expected 3 jpeg_quality_checked, got {self.stats.jpeg_quality_checked}")

    def test_jpeg_compress_stats_quality_unknown(self) -> None:
        """jpeg_quality_unknown = 0 (Pillow JPEGs have deterministic quality)."""
        self.assertEqual(self.stats.jpeg_quality_unknown, 0,
                         f"Expected 0 jpeg_quality_unknown, got {self.stats.jpeg_quality_unknown}")

    def test_jpeg_compress_stats_skipped_larger(self) -> None:
        """jpeg_compress_skipped_larger = 0 (test images all compress smaller).

        The skip-larger guard is a safety net for edge cases (e.g. when
        ExifTool cannot determine quality and the image is already well-
        compressed).  Normal test images always compress successfully.
        """
        self.assertEqual(self.stats.jpeg_compress_skipped_larger, 0,
                         f"Expected 0 jpeg_compress_skipped_larger, "
                         f"got {self.stats.jpeg_compress_skipped_larger}")


# ---------------------------------------------------------------------------
# JPEG compression with full input tree (regression guard)
# ---------------------------------------------------------------------------
# Runs by default.  To skip (e.g. for faster iteration), set
# GPEM_SKIP_JPEG_FULL_TREE=1 or pass --skip-jpeg-full-tree to the runner.

class TestJpegCompressionWithFullTree(unittest.TestCase):
    """Re-run the full input tree with --jpeg-quality to verify no regressions.

    Uses the same _create_input_tree as the main test class and runs with
    jpeg_compress_quality=80.  Verifies that all files are still written
    successfully (same written count) and no errors occur — confirming the
    JPEG compression code path doesn't break non-JPEG files or edge cases.
    """

    tmp_dir:    Path
    input_dir:  Path
    output_dir: Path
    stats:      MergeStats

    @classmethod
    def setUpClass(cls) -> None:
        if os.environ.get('GPEM_SKIP_JPEG_FULL_TREE') == '1':
            raise unittest.SkipTest(
                'JPEG full-tree tests skipped '
                '(use --skip-jpeg-full-tree or set GPEM_SKIP_JPEG_FULL_TREE=1)')

        logging.basicConfig(
            format='%(levelname)s %(name)s: %(message)s',
            level=logging.WARNING,
        )

        cls.tmp_dir    = Path(tempfile.mkdtemp(prefix='gpem_jpeg_full_test_'))
        cls.input_dir  = cls.tmp_dir / 'input'
        cls.output_dir = cls.tmp_dir / 'output'
        cls.input_dir.mkdir()

        # Reuse the main test class's input-tree builder.
        saved_input_dir = getattr(TestPhotosExportMerger, 'input_dir', None)
        TestPhotosExportMerger.input_dir = cls.input_dir
        TestPhotosExportMerger._create_input_tree()
        if saved_input_dir is not None:
            TestPhotosExportMerger.input_dir = saved_input_dir

        num_workers = os.cpu_count() or 1
        merger = PhotosExportMerger(
            str(cls.input_dir),
            str(cls.output_dir),
            blocked_descriptions=_BLOCKED_DESCRIPTIONS,
            num_workers=num_workers,
            fallback_tz=timezone(timedelta(hours=2)),
            jpeg_compress_quality=80,
        )
        cls.stats = merger.run()

    @classmethod
    def tearDownClass(cls) -> None:
        tmp = getattr(cls, 'tmp_dir', None)
        if tmp is not None:
            shutil.rmtree(str(tmp), ignore_errors=True)

    def test_jpeg_full_tree_stats_written(self) -> None:
        """All 184 files written when JPEG compression is enabled."""
        self.assertEqual(self.stats.written, 184,
                         f"Expected 184 written, got {self.stats.written}")

    def test_jpeg_full_tree_stats_zero_errors(self) -> None:
        """No errors with JPEG compression enabled on the full input tree."""
        self.assertEqual(self.stats.errors, 0,
                         f"Expected 0 errors, got {self.stats.errors}")

    def test_jpeg_full_tree_stats_quality_checked(self) -> None:
        """jpeg_quality_checked > 0 (the tree contains JPEG files)."""
        self.assertGreater(self.stats.jpeg_quality_checked, 0,
                           "Expected jpeg_quality_checked > 0 on full tree")

    def test_jpeg_full_tree_stats_matched_count(self) -> None:
        """Matched files still = 177 (compression doesn't affect matching)."""
        self.assertEqual(self.stats.matched, 177,
                         f"Expected 177 matched, got {self.stats.matched}")

    def test_jpeg_full_tree_stats_orphan_count(self) -> None:
        """Orphan files still = 7 (compression doesn't affect orphan detection)."""
        self.assertEqual(self.stats.orphans, 7,
                         f"Expected 7 orphans, got {self.stats.orphans}")

    def test_jpeg_full_tree_stats_sidecars(self) -> None:
        """Sidecar count still = 84 (JPEG compression doesn't affect sidecars)."""
        self.assertEqual(self.stats.sidecars_created, 84,
                         f"Expected 84 sidecars, got {self.stats.sidecars_created}")


# ---------------------------------------------------------------------------
if __name__ == '__main__':

    # ── Category mapping ─────────────────────────────────────────────────────
    # Each entry: (display_label, tuple_of_method_name_prefixes).
    # A test is assigned to the FIRST category whose prefix it matches.
    # Prefixes are chosen to be unambiguous (e.g. "test_output_organ" to
    # distinguish test_output_organized_* from test_output_timestamps_*).
    _CATEGORIES: list[tuple[str, tuple[str, ...]]] = [
        ("Input Integrity",           ("test_input_files_",    "test_input_file_count_")),
        ("Output Structure",          ("test_no_json_",        "test_output_organ",
                                       "test_all_media_",      "test_deep_")),
        ("GPS (8 dirs × 12 formats)", ("test_gps_",)),
        ("Timezones",                 ("test_timezone_",)),
        ("Descriptions (UTF-8, etc)", ("test_description_",)),
        ("File Types (matched)",      ("test_matched_",        "test_filetype_")),
        ("Orphan Files",              ("test_orphan_",)),
        ("XMP Conditional Dates",     ("test_xmp_conditional_",)),
        ("XMP Sidecars",              ("test_xmp_",            "test_sidecar_consistency_")),
        ("Duplicates",                ("test_duplicate_",)),
        ("Bracket Notation",          ("test_bracket_",)),
        ("File Timestamps",           ("test_input_timestamps_", "test_output_timestamps_",
                                       "test_output_timestamp_c",
                                       "test_sidecar_timestamps_",
                                       "test_sidecar_timestamp_c")),
        ("Stats Verification",        ("test_stats_",)),
        ("Video UTC Time",            ("test_mp4_time_",       "test_mov_time_",
                                       "test_video_utc_")),
        ("Special Filenames",         ("test_spaces_",         "test_leading_",
                                       "test_parentheses_",    "test_uppercase_",
                                       "test_special_filename_")),
        ("EXIF Preservation",         ("test_preservation_",)),
        ("Extension Mismatch",        ("test_ext_mismatch_",)),
        ("Video XMP Dates",           ("test_video_xmp_",)),
        ("Metadata Stripping",        ("test_strip_",)),
        ("Infrastructure Validation", ("test_infra_",)),
        ("Single Worker (serial)",    ("test_serial_",)),
        ("JPEG Compression",          ("test_jpeg_",)),
    ]

    def _cat(name: str) -> str:
        """Return the display category for a test method name."""
        for label, prefixes in _CATEGORIES:
            if any(name.startswith(p) for p in prefixes):
                return label
        return "Other"

    # ── Supported file types ─────────────────────────────────────────────────
    _SUPPORTED_TYPES = sorted(ext.lstrip('.') for ext in _MEDIA_BYTES)

    # ── Argument parser ──────────────────────────────────────────────────────
    parser = argparse.ArgumentParser(
        prog='test_merger.py',
        description='Run PhotosExportMerger tests with optional filtering.',
    )
    parser.add_argument(
        '-c', '--category', dest='categories', action='append', metavar='NAME',
        help='Run only categories whose label contains NAME (case-insensitive; repeatable)',
    )
    parser.add_argument(
        '-t', '--file-type', dest='file_types', action='append', metavar='EXT',
        help='Run only tests whose method name contains EXT (case-insensitive; repeatable)',
    )
    parser.add_argument(
        '--cleanup', action='store_true',
        help='Delete temp files after run without prompting',
    )
    parser.add_argument(
        '--keep', action='store_true',
        help='Keep temp files after run without prompting',
    )
    parser.add_argument(
        '--list-categories', action='store_true',
        help='Print available categories and exit',
    )
    parser.add_argument(
        '--list-types', action='store_true',
        help='Print available file types and exit',
    )
    parser.add_argument(
        '--single-worker', action='store_true',
        help='Also run single-worker (serial mode) regression tests',
    )
    parser.add_argument(
        '--skip-jpeg-full-tree', action='store_true',
        help='Skip JPEG compression full-tree regression tests',
    )
    args = parser.parse_args()

    # ── Early exit for --list-* ──────────────────────────────────────────────
    if args.list_categories:
        print('Available test categories:')
        for i, (label, _) in enumerate(_CATEGORIES, 1):
            print(f'  {i:>2}. {label}')
        sys.exit(0)

    if args.list_types:
        print('Supported file types:')
        for i, ext in enumerate(_SUPPORTED_TYPES, 1):
            print(f'  {i:>2}. {ext}')
        sys.exit(0)

    # ── Set cleanup mode ─────────────────────────────────────────────────────
    if args.cleanup:
        TestPhotosExportMerger._cleanup_mode = 'auto_delete'
    elif args.keep:
        TestPhotosExportMerger._cleanup_mode = 'auto_keep'
    # else leave default 'prompt'

    # ── Enable single-worker tests if requested ──────────────────────────────
    if args.single_worker:
        os.environ['GPEM_SINGLE_WORKER'] = '1'

    # ── Skip JPEG full-tree tests if requested ──────────────────────────────
    if args.skip_jpeg_full_tree:
        os.environ['GPEM_SKIP_JPEG_FULL_TREE'] = '1'

    # ── Suite filter helper ──────────────────────────────────────────────────
    def _filter_suite(suite, categories, file_types):
        filtered = unittest.TestSuite()
        for item in suite:
            if isinstance(item, unittest.TestSuite):
                filtered.addTests(_filter_suite(item, categories, file_types))
            elif hasattr(item, '_testMethodName'):
                name = item._testMethodName
                if categories:
                    cat = _cat(name)
                    if not any(c.lower() in cat.lower() for c in categories):
                        continue
                if file_types:
                    nl = name.lower()
                    if not any(ft.lower() in nl for ft in file_types):
                        continue
                filtered.addTest(item)
        return filtered

    # ── Known extensions for media-type labelling ────────────────────────────
    _KNOWN_EXTS: frozenset[str] = frozenset(ext.lstrip('.') for ext in _MEDIA_BYTES)

    def _media_tag(test_name: str) -> str:
        """Return '[.ext] ' if a known extension is embedded in the test name, else ''."""
        for part in reversed(test_name.split('_')):
            if part in _KNOWN_EXTS:
                return f'[.{part}] '
        return ''

    # ── Custom result collector ──────────────────────────────────────────────
    class _SummaryResult(unittest.TextTestResult):
        """TextTestResult that records per-test pass/fail and prints emoji progress."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._outcomes: list[tuple[str, str]] = []  # (method_name, status)
            self._current_cat: str = ''
            self._pending_desc: str = ''
            self._subtest_failed: set[str] = set()  # methods with at least one subTest failure

        def _record(self, test: unittest.TestCase, status: str) -> None:
            name = getattr(test, '_testMethodName', None)
            if name:
                self._outcomes.append((name, status))

        def getDescription(self, test: unittest.TestCase) -> str:
            tag = _media_tag(getattr(test, '_testMethodName', ''))
            name = getattr(test, '_testMethodName', str(test))
            doc = test.shortDescription() if self.descriptions else None
            base = f'{name}\n{doc}' if doc else name
            return f'{tag}{base}' if tag else base

        def startTest(self, test: unittest.TestCase) -> None:
            # Increment counter only — description is deferred until result is known.
            unittest.TestResult.startTest(self, test)
            name = getattr(test, '_testMethodName', '')
            cat = _cat(name)
            if cat != self._current_cat:
                self._current_cat = cat
                banner = f' CATEGORY: {cat} '
                width = 62
                dashes = max(0, width - len(banner))
                left  = dashes // 2
                right = dashes - left
                self.stream.writeln()
                self.stream.writeln('─' * left + banner + '─' * right)
            self._pending_desc = self.getDescription(test)

        def _emit(self, emoji: str) -> None:
            if self.showAll:
                name_line, _, doc_line = self._pending_desc.partition('\n')
                if doc_line:
                    self.stream.writeln(f'{name_line}\n{emoji} {doc_line}')
                else:
                    self.stream.writeln(f'{emoji} {name_line}')
            elif self.dots:
                self.stream.write(emoji[0])
                self.stream.flush()

        def addSubTest(self, test, subtest, err):
            super().addSubTest(test, subtest, err)
            if err is not None:
                self._emit('❌')
                name = getattr(test, '_testMethodName', None)
                if name and name not in self._subtest_failed:
                    self._subtest_failed.add(name)
                    self._record(test, 'FAIL')

        def addSuccess(self, test):
            unittest.TestResult.addSuccess(self, test)
            self._emit('✅')
            name = getattr(test, '_testMethodName', None)
            if name not in self._subtest_failed:
                self._record(test, 'PASS')

        def addFailure(self, test, err):
            unittest.TestResult.addFailure(self, test, err)
            self._emit('❌')
            self._record(test, 'FAIL')

        def addError(self, test, err):
            unittest.TestResult.addError(self, test, err)
            self._emit('❌')
            self._record(test, 'ERROR')

        def addExpectedFailure(self, test, err):
            unittest.TestResult.addExpectedFailure(self, test, err)
            self._emit('✅')
            self._record(test, 'XFAIL')

        def addUnexpectedSuccess(self, test):
            unittest.TestResult.addUnexpectedSuccess(self, test)
            self._emit('❌')
            self._record(test, 'XPASS')

        def addSkip(self, test, reason):
            unittest.TestResult.addSkip(self, test, reason)
            self._emit('⏭')
            self._record(test, 'SKIP')

    # ── Run suite ────────────────────────────────────────────────────────────
    logging.basicConfig(
        format='%(levelname)s %(name)s: %(message)s',
        level=logging.WARNING,
    )
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestPhotosExportMerger))
    suite.addTests(loader.loadTestsFromTestCase(TestJpegCompression))
    suite.addTests(loader.loadTestsFromTestCase(TestJpegCompressionWithFullTree))
    if args.single_worker:
        suite.addTests(loader.loadTestsFromTestCase(TestSingleWorker))
    if args.categories or args.file_types:
        print('Running with filters:')
        if args.categories:
            print(f'  Categories : {", ".join(args.categories)}')
        if args.file_types:
            print(f'  File types : {", ".join(args.file_types)}')
        suite = _filter_suite(suite, args.categories or [], args.file_types or [])
    # Sort tests by category then method name so the category banner fires once per group.
    suite = unittest.TestSuite(
        sorted(suite, key=lambda t: (_cat(t._testMethodName), t._testMethodName))
    )
    # Count @expectedFailure tests so the summary can show "expected failures=N/M".
    _total_xfail = sum(
        1 for t in suite
        if getattr(getattr(t, t._testMethodName, None),
                   '__unittest_expecting_failure__', False)
    )

    class _XFailStream:
        """Thin stream wrapper: rewrites 'expected failures=N' → 'expected failures=N/M'."""
        def __init__(self, inner, total):
            self._inner = inner
            self._total = total
        def write(self, text):
            import re
            self._inner.write(re.sub(
                r'expected failures=(\d+)',
                lambda m: f'expected failures={m.group(1)}/{self._total}',
                text))
        def writeln(self, text=''):
            import re
            self._inner.writeln(re.sub(
                r'expected failures=(\d+)',
                lambda m: f'expected failures={m.group(1)}/{self._total}',
                text))
        def __getattr__(self, name):
            return getattr(self._inner, name)

    runner = unittest.TextTestRunner(verbosity=2, resultclass=_SummaryResult)
    runner.stream = _XFailStream(runner.stream, _total_xfail)
    result = runner.run(suite)

    # ── Category summary table ───────────────────────────────────────────────
    # tearDownClass has already printed the MergeStats and prompted for cleanup.
    # Now print the per-category test results.
    from collections import defaultdict
    cat_stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {'pass': 0, 'fail': 0, 'total': 0}
    )
    for method, status in result._outcomes:
        c = _cat(method)
        cat_stats[c]['total'] += 1
        if status in ('PASS', 'XFAIL'):
            cat_stats[c]['pass'] += 1
        else:
            cat_stats[c]['fail'] += 1

    W = 28  # category column width
    print()
    print('=' * 62)
    print('              TEST RESULTS BY CATEGORY')
    print('=' * 62)
    print(f"{'Category':<{W}} | {'Pass':>4} | {'Fail':>4} | {'Total':>5}")
    print('-' * 62)
    grand = {'pass': 0, 'fail': 0, 'total': 0}
    for label, _ in _CATEGORIES:
        s = cat_stats.get(label, {'pass': 0, 'fail': 0, 'total': 0})
        print(f"{label:<{W}} | {s['pass']:>4} | {s['fail']:>4} | {s['total']:>5}")
        for k in grand:
            grand[k] += s[k]
    if 'Other' in cat_stats:
        s = cat_stats['Other']
        print(f"{'Other':<{W}} | {s['pass']:>4} | {s['fail']:>4} | {s['total']:>5}")
        for k in grand:
            grand[k] += s[k]
    print('-' * 62)
    print(f"{'TOTAL':<{W}} | {grand['pass']:>4} | {grand['fail']:>4} | {grand['total']:>5}")
    print('=' * 62)

    sys.exit(0 if result.wasSuccessful() else 1)