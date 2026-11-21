"""Minimal codec for Prometheus Remote Write 1.0 (snappy-compressed protobuf).

Only the subset needed to receive ordinary samples is implemented by hand,
so the receiver itself depends on no protobuf runtime:

    WriteRequest {
      repeated TimeSeries timeseries = 1;
    }
    TimeSeries {
      repeated Label labels   = 1; // sorted by name on the wire
      repeated Sample samples = 2;
    }
    Label  { string name = 1; string value = 2; }
    Sample { double value = 1; int64 timestamp = 2; } // timestamp: unix ms

The encoder is used by the test-suite to build requests byte-compatible with
the real Prometheus remote-write client.
"""
from __future__ import annotations

from dataclasses import dataclass

import cramjam

SNAPPY = "snappy"
PROTOBUF = "application/x-protobuf"
REMOTE_WRITE_VERSION = "1.0"


class RemoteWriteError(ValueError):
    """Raised when a remote-write payload cannot be decoded."""


# --------------------------------------------------------------------------- #
# Low level protobuf wire helpers
# --------------------------------------------------------------------------- #
_WT_VARINT = 0
_WT_FIXED64 = 1
_WT_LEN = 2


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise RemoteWriteError("truncated varint")
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift > 70:
            raise RemoteWriteError("varint too long")


def _iter_fields(buf: bytes):
    pos = 0
    n = len(buf)
    while pos < n:
        key, pos = _read_varint(buf, pos)
        field_num, wire_type = key >> 3, key & 0x07
        if wire_type == _WT_VARINT:
            value, pos = _read_varint(buf, pos)
            yield field_num, wire_type, value
        elif wire_type == _WT_FIXED64:
            if pos + 8 > n:
                raise RemoteWriteError("truncated fixed64")
            value = buf[pos : pos + 8]
            pos += 8
            yield field_num, wire_type, value
        elif wire_type == _WT_LEN:
            length, pos = _read_varint(buf, pos)
            if pos + length > n:
                raise RemoteWriteError("truncated length-delimited field")
            yield field_num, wire_type, buf[pos : pos + length]
            pos += length
        else:
            raise RemoteWriteError(f"unsupported wire type {wire_type}")


def _encode_varint(value: int) -> bytes:
    if value < 0:
        # int64 negative numbers are encoded as 10-byte two's complement.
        value &= (1 << 64) - 1
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _encode_key(field_num: int, wire_type: int) -> bytes:
    return _encode_varint((field_num << 3) | wire_type)


def _encode_string(field_num: int, value: str) -> bytes:
    raw = value.encode("utf-8")
    return _encode_key(field_num, _WT_LEN) + _encode_varint(len(raw)) + raw


def _encode_message(field_num: int, payload: bytes) -> bytes:
    return _encode_key(field_num, _WT_LEN) + _encode_varint(len(payload)) + payload


# --------------------------------------------------------------------------- #
# Domain types
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Label:
    name: str
    value: str


@dataclass(frozen=True)
class Sample:
    value: float
    timestamp_ms: int


@dataclass(frozen=True)
class TimeSeries:
    labels: tuple[Label, ...]
    samples: tuple[Sample, ...]

    def label_map(self) -> dict[str, str]:
        return {label.name: label.value for label in self.labels}


# --------------------------------------------------------------------------- #
# Decoding
# --------------------------------------------------------------------------- #
def _decode_label(buf: bytes) -> Label:
    name = ""
    value = ""
    for field_num, _wire, raw in _iter_fields(buf):
        if field_num == 1:
            name = raw.decode("utf-8")
        elif field_num == 2:
            value = raw.decode("utf-8")
    return Label(name, value)


def _decode_sample(buf: bytes) -> Sample:
    import struct

    value = 0.0
    timestamp_ms = 0
    for field_num, wire, raw in _iter_fields(buf):
        if field_num == 1 and wire == _WT_FIXED64:
            value = struct.unpack("<d", raw)[0]
        elif field_num == 2 and wire == _WT_VARINT:
            timestamp_ms = raw
            if timestamp_ms >= 1 << 63:
                timestamp_ms -= 1 << 64
    return Sample(value, timestamp_ms)


def _decode_timeseries(buf: bytes) -> TimeSeries:
    labels: list[Label] = []
    samples: list[Sample] = []
    for field_num, _wire, raw in _iter_fields(buf):
        if field_num == 1:
            labels.append(_decode_label(raw))
        elif field_num == 2:
            samples.append(_decode_sample(raw))
    return TimeSeries(tuple(labels), tuple(samples))


def decode_write_request(raw: bytes) -> list[TimeSeries]:
    """Decompress and parse a remote-write 1.0 request body."""
    try:
        payload = bytes(cramjam.snappy.decompress(raw))
    except Exception as exc:  # pragma: no cover - defensive
        raise RemoteWriteError(f"snappy decompression failed: {exc}") from exc

    series: list[TimeSeries] = []
    for field_num, wire, raw_field in _iter_fields(payload):
        if field_num == 1 and wire == _WT_LEN:
            series.append(_decode_timeseries(raw_field))
        # Unknown fields (e.g. metadata added by remote-write 2.x) are ignored.
    return series


# --------------------------------------------------------------------------- #
# Encoding (tests / demo tooling)
# --------------------------------------------------------------------------- #
def _encode_label(label: Label) -> bytes:
    return _encode_string(1, label.name) + _encode_string(2, label.value)


def _encode_sample(sample: Sample) -> bytes:
    import struct

    return (
        _encode_key(1, _WT_FIXED64)
        + struct.pack("<d", sample.value)
        + _encode_key(2, _WT_VARINT)
        + _encode_varint(sample.timestamp_ms)
    )


def _encode_timeseries(series: TimeSeries) -> bytes:
    out = bytearray()
    for label in sorted(series.labels, key=lambda item: item.name):
        out += _encode_message(1, _encode_label(label))
    for sample in series.samples:
        out += _encode_message(2, _encode_sample(sample))
    return bytes(out)


def encode_write_request(series_list: list[TimeSeries]) -> bytes:
    """Build a snappy-compressed remote-write 1.0 request body."""
    out = bytearray()
    for series in series_list:
        out += _encode_message(1, _encode_timeseries(series))
    return bytes(cramjam.snappy.compress(bytes(out)))
