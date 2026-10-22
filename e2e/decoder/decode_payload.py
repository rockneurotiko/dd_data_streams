#!/usr/bin/env python3
"""Decode a flushed pipeline stats body with the reference tracer primitives.

The body is the exact bytes the aggregator would post to the agent: MsgPack for
the payload and DDSketch protobuf for the three distributions. `ddtrace` 3.17.0
pins the reference tracer; its own MsgPack is encode-only and its native
DDSketch has no decoder, so the body is read with the msgpack and ddsketch
packages that tracer is built around.

Usage: decode_payload.py <captured.bin>

The file may be gzipped (as the agent receives it) or already decompressed.
Exits non-zero when the body is not a valid pipeline stats payload.
"""

import gzip
import sys

import ddsketch.pb.ddsketch_pb2 as sketch_pb
import msgpack


def read_body(path):
    raw = open(path, "rb").read()
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


def sample_count(blob):
    sketch = sketch_pb.DDSketch.FromString(bytes(blob))
    return int(
        sketch.zeroCount
        + sum(sketch.positiveValues.contiguousBinCounts)
        + sum(sketch.negativeValues.contiguousBinCounts)
    )


def main(path):
    payload = msgpack.unpackb(read_body(path), raw=False, strict_map_key=False)

    print(f"service: {payload['Service']}")
    print(f"env: {payload['Env']}")
    print(f"lang: {payload['Lang']}")
    print(f"tracer_version: {payload['TracerVersion']}")
    print(f"version: {payload['Version']}")
    print(f"product_mask: {payload['ProductMask']}")
    print(f"buckets: {len(payload['Stats'])}")

    for bucket in payload["Stats"]:
        print(f"\nbucket start={bucket['Start']} duration={bucket['Duration']}")
        for point in bucket["Stats"]:
            samples = {
                name: sample_count(point[field])
                for name, field in (
                    ("pathway", "PathwayLatency"),
                    ("edge", "EdgeLatency"),
                    ("payload", "PayloadSize"),
                )
            }
            print(
                f"  tags={point['EdgeTags']} "
                f"hash={point['Hash']} "
                f"parent_hash={point['ParentHash']} "
                f"timestamp_type={point['TimestampType']} "
                f"samples={samples}"
            )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} <captured.bin>")
    main(sys.argv[1])
