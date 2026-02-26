"""
test_merger.py — Comprehensive unit tests for GooglePhotosExportMerger.

This file is built in stages:
  Part 1  (done):    File factories — minimal valid binary files + JSON files.
  Part 2  (done):    Test infrastructure — setUpClass, tearDownClass, summary runner.
  Part 3  (future):  All test_* methods.
"""

import argparse
import json
import logging
import shutil
import struct
import sys
import tempfile
import unittest
import zlib
from pathlib import Path
from typing import Any, Dict

import exiftool
from GooglePhotosExportMerger import GooglePhotosExportMerger, MergeStats


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
    """Write a Google Photos Takeout-style JSON metadata file at *path*.

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

class TestGooglePhotosExportMerger(unittest.TestCase):
    """
    Single-pass integration test for GooglePhotosExportMerger.

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
        merger = GooglePhotosExportMerger(
            str(cls.input_dir),
            str(cls.output_dir),
            blocked_descriptions=_BLOCKED_DESCRIPTIONS,
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
        # Each file covers one GPS scenario (quadrant, altitude sign).
        # Both geoData and geoDataExif carry the same coordinates so the
        # merger always picks up GPS regardless of which key it checks first.
        d = inp / 'GPS Tests'
        _gps_cases = [
            # (stem,                    lat,    lon,      alt)
            ('gps_ne',                 38.91,  121.60,    0.0),   # N+E quadrant
            ('gps_nw',                 48.85,   -2.35,    0.0),   # N+W quadrant
            ('gps_se',                -25.82,   28.20,    0.0),   # S+E quadrant
            ('gps_sw',                -33.86,  -70.67,    0.0),   # S+W quadrant (Santiago)
            ('gps_altitude_negative', -25.82,   28.20,  -50.0),   # below sea level
            ('gps_high_altitude',     -25.82,   28.20, 1623.44),  # high altitude
        ]
        for stem, lat, lon, alt in _gps_cases:
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
            ('tz_utc',    '+00:00'),
            ('tz_gmt2',   '+02:00'),
            ('tz_minus5', '-05:00'),
            ('tz_plus8',  '+08:00'),
            ('tz_plus530', '+05:30'),
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
        ]
        for stem, desc in _desc_cases:
            make_media_file(d / f'{stem}.jpg')
            make_json_file(d / f'{stem}.jpg.json', description=desc)

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

        # ── BracketNotation ────────────────────────────────────────────────
        # Google Photos names duplicated exports as  photo.jpg(1).json  (bracket
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
            # GPS Tests
            'gps_ne.jpg', 'gps_nw.jpg', 'gps_se.jpg', 'gps_sw.jpg',
            'gps_altitude_negative.jpg', 'gps_high_altitude.jpg',
            # Timezones
            'tz_utc.jpg', 'tz_gmt2.jpg', 'tz_minus5.jpg',
            'tz_plus8.jpg', 'tz_plus530.jpg',
            # Descriptions
            'desc_utf8.jpg', 'desc_escaped.jpg', 'desc_newline.jpg',
            'desc_crlf.jpg', 'desc_empty.jpg', 'desc_blocked.jpg', 'desc_long.jpg',
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
        'gps_altitude_negative.jpg': ('S', 'E', 25.82,   28.20, False),
        'gps_high_altitude.jpg':     ('S', 'E', 25.82,   28.20, True),
    }

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

    # ------------------------------------------------------------------
    # Category 4 — Timezones
    # ------------------------------------------------------------------

    # epoch 1723113846 = 2024-08-08 10:44:06 UTC
    # expected (OffsetTimeOriginal, DateTimeOriginal in local time)
    _TZ_CASES: dict = {
        'tz_utc.jpg':     ('+00:00', '2024:08:08 10:44:06'),
        'tz_gmt2.jpg':    ('+02:00', '2024:08:08 12:44:06'),
        'tz_minus5.jpg':  ('-05:00', '2024:08:08 05:44:06'),
        'tz_plus8.jpg':   ('+08:00', '2024:08:08 18:44:06'),
        'tz_plus530.jpg': ('+05:30', '2024:08:08 16:14:06'),
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

    # ------------------------------------------------------------------
    # Category 5 — Descriptions
    # ------------------------------------------------------------------

    _DESC_TAGS = ['EXIF:ImageDescription', 'XMP:Description', 'EXIF:UserComment']

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
        r"""CRLF (\r\n) description: both lines survive the round-trip."""
        tags = self._read_tags('desc_crlf.jpg', self._DESC_TAGS)
        desc = tags.get('EXIF:ImageDescription') or tags.get('XMP:Description', '')
        self.assertIn('Line one', desc, "First line missing from CRLF description")
        self.assertIn('Line two', desc, "Second line missing from CRLF description")

    def test_description_empty(self) -> None:
        """Empty JSON description → no description tags written to output file."""
        tags = self._read_tags('desc_empty.jpg', self._DESC_TAGS)
        for tag in ('EXIF:ImageDescription', 'XMP:Description'):
            val = tags.get(tag)
            self.assertFalse(val, f"Expected absent/empty {tag}, got: {val!r}")

    def test_description_blocked_cleared(self) -> None:
        """'SONY DSC' is in blocked list → all description tags cleared in output."""
        tags = self._read_tags('desc_blocked.jpg', self._DESC_TAGS)
        for tag in ('EXIF:ImageDescription', 'XMP:Description', 'EXIF:UserComment'):
            val = tags.get(tag)
            self.assertFalse(val, f"{tag} not cleared for blocked description: {val!r}")

    def test_description_long(self) -> None:
        """500-character description is written without truncation to XMP:Description."""
        tags = self._read_tags('desc_long.jpg', self._DESC_TAGS)
        xmp = tags.get('XMP:Description', '')
        self.assertGreaterEqual(len(xmp), 500,
                                f"XMP:Description truncated: len={len(xmp)}, expected ≥500")

    # ------------------------------------------------------------------
    # Category 6 — File Types (matched)
    # ------------------------------------------------------------------

    # Expected DateTimeOriginal for FileTypes/Matched files:
    # epoch 1723113846 = 2024-08-08 10:44:06 UTC → +02:00 fallback → 12:44:06 local
    _FILETYPE_EXPECTED_DT = '2024:08:08 12:44:06'

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
        """HEIC: direct write strategy — output exists and a date tag is set."""
        self._assert_file_exists('test', '.heic')
        tags = self._read_tags('test.heic',
                               ['EXIF:DateTimeOriginal', 'XMP:DateTimeOriginal'])
        dt = tags.get('EXIF:DateTimeOriginal') or tags.get('XMP:DateTimeOriginal')
        self.assertIsNotNone(dt, "test.heic: no DateTimeOriginal in EXIF or XMP")
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

    # ------------------------------------------------------------------
    # Category 7 — Orphan Files
    # ------------------------------------------------------------------

    _ORPHAN_NAMES = [
        'orphan_no_json.jpg',   # RootLevel — has no matching JSON
        'orphan.jpg',           # FileTypes/Orphans
        'orphan.png',
        'orphan.gif',
        'orphan.mp4',
        'orphan.mov',
        'orphan.avi',
    ]

    def test_orphan_copied_to_output(self) -> None:
        """Every orphan (no JSON match) must appear in the output tree."""
        output_names = {f.name for f in self.output_dir.rglob('*') if f.is_file()}
        missing = [n for n in self._ORPHAN_NAMES if n not in output_names]
        self.assertFalse(missing, f"Orphan files missing from output: {missing}")

    def test_orphan_no_json_metadata(self) -> None:
        """Orphan outputs must not have any GPS tags (no JSON data source)."""
        gps_tags = ['EXIF:GPSLatitudeRef', 'EXIF:GPSLongitudeRef']
        for name in self._ORPHAN_NAMES:
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

    # ------------------------------------------------------------------
    # Category 8 — XMP Sidecars
    # ------------------------------------------------------------------
    # Uses dedicated files in Sidecars/ (sc_png.png, sc_gif.gif, sc_avi.avi)
    # for sidecar content verification (GPS, description, timestamps).

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

    # ------------------------------------------------------------------
    # Category 10 — Bracket Notation
    # ------------------------------------------------------------------
    # photo.jpg(1).json and photo.jpg(2).json must each match their
    # corresponding photo(1).jpg / photo(2).jpg.  Both carry title='photo.jpg'
    # so the second output is renamed photo_2.jpg (same deduplication logic).

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

    # ------------------------------------------------------------------
    # Category 11 — File Timestamps
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Category 12 — Stats Verification
    # ------------------------------------------------------------------
    # Counts derivation:
    #   total=52  (45 matched + 7 orphans)
    #   matched=45 (all dirs except FileTypes/Orphans + orphan_no_json;
    #               includes test.cr2, test.tif, tz_offset_time.jpg, UPPERCASE.JPG)
    #   orphans=7  (orphan_no_json + 6 × FileTypes/Orphans)
    #   gps=8      (6 GPS Tests + sc_png + sc_avi)
    #   sidecars=10 (test.png.xmp + test.gif.xmp + test.mp4.xmp + test.mov.xmp +
    #               test.avi.xmp + test.mkv.xmp + test.webm.xmp from FileTypes/Matched,
    #               + sc_png.png.xmp + sc_gif.gif.xmp + sc_avi.avi.xmp)
    #   descriptions_cleared=1  (desc_blocked.jpg)
    #   duplicates_renamed=2    (same_name_b → same_name_2, photo(2) → photo_2)
    #   written=52
    #   errors=0

    def test_stats_total_count(self) -> None:
        """Total media files processed = 52."""
        self.assertEqual(self.stats.total_media_files, 52,
                         f"Expected 52 total, got {self.stats.total_media_files}")

    def test_stats_matched_count(self) -> None:
        """Matched files (with JSON) = 45."""
        self.assertEqual(self.stats.matched, 45,
                         f"Expected 45 matched, got {self.stats.matched}")

    def test_stats_orphan_count(self) -> None:
        """Orphan files (no JSON) = 7."""
        self.assertEqual(self.stats.orphans, 7,
                         f"Expected 7 orphans, got {self.stats.orphans}")

    def test_stats_gps_written(self) -> None:
        """GPS tags written = 8 (6 GPS Tests + sc_png + sc_avi)."""
        self.assertEqual(self.stats.gps_written, 8,
                         f"Expected 8 GPS writes, got {self.stats.gps_written}")

    def test_stats_sidecars_created(self) -> None:
        """XMP sidecars created = 10 (7 from FileTypes/Matched + sc_png.png, sc_gif.gif, sc_avi.avi)."""
        self.assertEqual(self.stats.sidecars_created, 10,
                         f"Expected 10 sidecars, got {self.stats.sidecars_created}")

    def test_stats_zero_errors(self) -> None:
        """Merger reports zero errors for well-formed test data."""
        self.assertEqual(self.stats.errors, 0,
                         f"Expected 0 errors, got {self.stats.errors}")

    def test_stats_written_count(self) -> None:
        """Files written = 51 (total_media_files when errors == 0)."""
        self.assertEqual(self.stats.written, 52,
                         f"Expected 52 written, got {self.stats.written}")

    def test_stats_descriptions_cleared(self) -> None:
        """descriptions_cleared = 1 (desc_blocked.jpg carries 'SONY DSC')."""
        self.assertEqual(self.stats.descriptions_cleared, 1,
                         f"Expected 1 description cleared, got {self.stats.descriptions_cleared}")

    def test_stats_duplicates_renamed(self) -> None:
        """duplicates_renamed = 2 (one from Duplicates/, one from BracketNotation/)."""
        self.assertEqual(self.stats.duplicates_renamed, 2,
                         f"Expected 2 duplicates renamed, got {self.stats.duplicates_renamed}")

    def test_stats_skipped_json(self) -> None:
        """skipped_json = 1 (bad_json.jpg.json contains invalid JSON)."""
        self.assertEqual(self.stats.skipped_json, 1,
                         f"Expected 1 skipped_json, got {self.stats.skipped_json}")

    # ------------------------------------------------------------------
    # Category 13 — Video UTC Time
    # ------------------------------------------------------------------
    # QuickTime stores CreateDate as a plain integer (seconds since Mac epoch)
    # without timezone info.  ExifTool returns it as "YYYY:MM:DD HH:MM:SS"
    # with no +HH:MM suffix.

    def test_mp4_time_utc(self) -> None:
        """MP4: QuickTime:CreateDate is present and has no timezone offset suffix."""
        import re
        tags = self._read_tags('test.mp4', ['QuickTime:CreateDate'])
        dt = tags.get('QuickTime:CreateDate')
        self.assertIsNotNone(dt, "test.mp4: QuickTime:CreateDate is missing")
        self.assertTrue(str(dt).strip(), "test.mp4: QuickTime:CreateDate is empty")
        self.assertNotRegex(str(dt), r'[+-]\d{2}:\d{2}$',
                            f"test.mp4: QuickTime:CreateDate has unexpected tz suffix: {dt!r}")

    def test_mov_time_utc(self) -> None:
        """MOV: QuickTime:CreateDate is present and has no timezone offset suffix."""
        import re
        tags = self._read_tags('test.mov', ['QuickTime:CreateDate'])
        dt = tags.get('QuickTime:CreateDate')
        self.assertIsNotNone(dt, "test.mov: QuickTime:CreateDate is missing")
        self.assertTrue(str(dt).strip(), "test.mov: QuickTime:CreateDate is empty")
        self.assertNotRegex(str(dt), r'[+-]\d{2}:\d{2}$',
                            f"test.mov: QuickTime:CreateDate has unexpected tz suffix: {dt!r}")

    # ------------------------------------------------------------------
    # Category 14 — Special Filenames
    # ------------------------------------------------------------------

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
        ("GPS (4 quadrants + alt)",   ("test_gps_",)),
        ("Timezones",                 ("test_timezone_",)),
        ("Descriptions (UTF-8, etc)", ("test_description_",)),
        ("File Types (matched)",      ("test_matched_",)),
        ("Orphan Files",              ("test_orphan_",)),
        ("XMP Sidecars",              ("test_xmp_",)),
        ("Duplicates",                ("test_duplicate_",)),
        ("Bracket Notation",          ("test_bracket_",)),
        ("File Timestamps",           ("test_input_timestamps_", "test_output_timestamps_",
                                       "test_sidecar_timestamps_")),
        ("Stats Verification",        ("test_stats_",)),
        ("Video UTC Time",            ("test_mp4_time_",       "test_mov_time_")),
        ("Special Filenames",         ("test_spaces_",         "test_leading_",
                                       "test_parentheses_",    "test_uppercase_")),
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
        description='Run GooglePhotosExportMerger tests with optional filtering.',
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
        TestGooglePhotosExportMerger._cleanup_mode = 'auto_delete'
    elif args.keep:
        TestGooglePhotosExportMerger._cleanup_mode = 'auto_keep'
    # else leave default 'prompt'

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

    # ── Custom result collector ──────────────────────────────────────────────
    class _SummaryResult(unittest.TextTestResult):
        """TextTestResult that additionally records per-test pass/fail status."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._outcomes: list[tuple[str, str]] = []  # (method_name, status)

        def _record(self, test: unittest.TestCase, status: str) -> None:
            name = getattr(test, '_testMethodName', None)
            if name:
                self._outcomes.append((name, status))

        def addSuccess(self, test):
            super().addSuccess(test)
            self._record(test, 'PASS')

        def addFailure(self, test, err):
            super().addFailure(test, err)
            self._record(test, 'FAIL')

        def addError(self, test, err):
            super().addError(test, err)
            self._record(test, 'ERROR')

        def addSkip(self, test, reason):
            super().addSkip(test, reason)
            self._record(test, 'SKIP')

    # ── Run suite ────────────────────────────────────────────────────────────
    logging.basicConfig(
        format='%(levelname)s %(name)s: %(message)s',
        level=logging.WARNING,
    )
    suite = unittest.TestLoader().loadTestsFromTestCase(TestGooglePhotosExportMerger)
    if args.categories or args.file_types:
        print('Running with filters:')
        if args.categories:
            print(f'  Categories : {", ".join(args.categories)}')
        if args.file_types:
            print(f'  File types : {", ".join(args.file_types)}')
        suite = _filter_suite(suite, args.categories or [], args.file_types or [])
    runner = unittest.TextTestRunner(verbosity=2, resultclass=_SummaryResult)
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
        if status == 'PASS':
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
