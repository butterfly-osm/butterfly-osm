#!/usr/bin/env python3
"""Mix per-country JSONL corpora into one shuffled multi-country corpus.

Uses a deterministic seeded shuffle so the output is reproducible.
Reads streaming, but holds all line offsets in memory.
"""
import argparse
import os
import random
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--inputs", nargs="+", required=True, help="Input JSONL files")
    p.add_argument("--out", required=True, help="Output JSONL path")
    p.add_argument("--seed", type=int, default=0xB17EBAD0)
    args = p.parse_args()

    rng = random.Random(args.seed)
    # Read each input, line-buffered. Tag with file index so we can stream
    # back without keeping every line in RAM.
    # Shuffle indices, then write out.
    line_pointers = []  # (file_idx, byte_offset, byte_len)
    for fi, path in enumerate(args.inputs):
        print(f"[mix] indexing {path}", file=sys.stderr)
        sz = os.path.getsize(path)
        with open(path, "rb") as f:
            offset = 0
            for line in f:
                ln = len(line)
                line_pointers.append((fi, offset, ln))
                offset += ln
        print(f"[mix]   indexed {sum(1 for _ in [None] if line_pointers)} so far={len(line_pointers)} (file size {sz/1e9:.2f} GB)", file=sys.stderr)

    print(f"[mix] total lines={len(line_pointers)}", file=sys.stderr)
    rng.shuffle(line_pointers)
    print(f"[mix] shuffled, writing to {args.out}", file=sys.stderr)

    # Open all input files for random reads.
    file_handles = [open(p, "rb") for p in args.inputs]
    try:
        with open(args.out, "wb") as out:
            for i, (fi, off, ln) in enumerate(line_pointers):
                file_handles[fi].seek(off)
                buf = file_handles[fi].read(ln)
                out.write(buf)
                if (i + 1) % 1_000_000 == 0:
                    print(f"[mix]   wrote {i+1} / {len(line_pointers)}", file=sys.stderr)
    finally:
        for h in file_handles:
            h.close()
    print(f"[mix] DONE wrote {len(line_pointers)} records to {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
