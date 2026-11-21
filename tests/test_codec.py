"""Round-trip tests for the hand-written remote-write codec."""
from __future__ import annotations

import math
import struct

import pytest

from app.remote_write import (
    Label,
    RemoteWriteError,
    Sample,
    TimeSeries,
    _encode_varint,
    _read_varint,
    decode_write_request,
    encode_write_request,
)
from tests.conftest import body, labels, series


def test_varint_roundtrip():
    for value in (0, 1, 127, 128, 16383, 16384, 2**31, 2**63 - 1):
        raw = _encode_varint(value)
        decoded, pos = _read_varint(raw, 0)
        assert decoded == value
        assert pos == len(raw)


def test_negative_timestamp_varint():
    raw = _encode_varint(-1)
    decoded, _ = _read_varint(raw, 0)
    assert decoded == (1 << 64) - 1  # wire form of int64 -1


def test_encode_decode_roundtrip():
    request = [
        series("demo", "A", [(0, 1.0), (1000, 2.5), (2000, 3.25)]),
        series("demo", "B", [(0, 9.0)], name="demo_uptime_seconds_total"),
    ]
    decoded = decode_write_request(body(request))
    assert len(decoded) == 2
    first = decoded[0]
    assert first.label_map()["cluster"] == "demo"
    assert first.label_map()["replica"] == "A"
    assert [s.timestamp_ms for s in first.samples] == [
        s.timestamp_ms for s in request[0].samples
    ]
    assert [s.value for s in first.samples] == [1.0, 2.5, 3.25]


def test_special_float_values_preserve():
    ts = TimeSeries(
        labels=labels("demo", "A"),
        samples=(
            Sample(timestamp_ms=1, value=math.inf),
            Sample(timestamp_ms=2, value=-math.inf),
            Sample(timestamp_ms=3, value=math.nan),
        ),
    )
    decoded = decode_write_request(encode_write_request([ts]))[0]
    values = [s.value for s in decoded.samples]
    assert values[0] == math.inf
    assert values[1] == -math.inf
    assert math.isnan(values[2])


def test_utf8_label_values():
    ts = TimeSeries(
        labels=(Label("cluster", "demo"), Label("replica", "A"),
                Label("note", "中文标签 🚀"), Label("__name__", "m")),
        samples=(Sample(timestamp_ms=1, value=1.0),),
    )
    decoded = decode_write_request(encode_write_request([ts]))[0]
    assert decoded.label_map()["note"] == "中文标签 🚀"


def test_truncated_payload_rejected():
    good = encode_write_request([series("demo", "A", [(0, 1.0)])])
    with pytest.raises(RemoteWriteError):
        decode_write_request(good[:-3])


def test_garbage_payload_rejected():
    with pytest.raises(RemoteWriteError):
        decode_write_request(b"this is definitely not snappy" * 4)


def test_unknown_field_ignored():
    # Field number 99, varint wire type -> must be skipped, series still parsed.
    ts_bytes = encode_write_request([series("demo", "A", [(0, 1.0)])])
    import cramjam

    payload = bytes(cramjam.snappy.decompress(ts_bytes))
    payload += bytes([0x98, 0x06, 0x01])  # field 99, varint, value 1
    wrapped = bytes(cramjam.snappy.compress(payload))
    decoded = decode_write_request(wrapped)
    assert len(decoded) == 1
    assert decoded[0].samples[0].value == 1.0
