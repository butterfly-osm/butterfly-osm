#!/usr/bin/env python3
"""Mix per-country JSONL corpora into a balanced multi-country corpus.

Subsamples each country to at most --max-per-country lines, then
shuffles the union deterministically. The cap balances country
contribution: small-corpus countries (LU, IN, JP) are NOT padded,
and large-corpus countries are capped so they don't dominate.

This yields a corpus that:
- is small enough to train in 30 min on d=256 / RTX 5060 Ti
- gives every country a fair share of the gradient
"""
import argparse
import os
import random
import sys


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--inputs", nargs="+", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--seed", type=lambda s: int(s, 0), default=0xB17EBAD0,
                   help="Seed (decimal or 0x-prefixed hex)")
    p.add_argument("--max-per-country", type=int, default=350_000,
                   help="Cap per-country line count")
    args = p.parse_args()

    rng = random.Random(args.seed)
    line_pointers = []  # (file_idx, byte_offset, byte_len)

    for fi, path in enumerate(args.inputs):
        per_file = []
        with open(path, "rb") as f:
            offset = 0
            for line in f:
                ln = len(line)
                per_file.append((fi, offset, ln))
                offset += ln
        # Subsample if over the cap
        if len(per_file) > args.max_per_country:
            rng.shuffle(per_file)
            per_file = per_file[: args.max_per_country]
        print(f"[mix] {path}: kept {len(per_file)}", file=sys.stderr)
        line_pointers.extend(per_file)

    print(f"[mix] total lines={len(line_pointers)}", file=sys.stderr)
    rng.shuffle(line_pointers)
    print(f"[mix] shuffled, writing to {args.out}", file=sys.stderr)

    file_handles = [open(p, "rb") for p in args.inputs]
    try:
        with open(args.out, "wb") as out:
            for i, (fi, off, ln) in enumerate(line_pointers):
                file_handles[fi].seek(off)
                buf = file_handles[fi].read(ln)
                out.write(buf)
                if (i + 1) % 1_000_000 == 0:
                    print(f"[mix]   wrote {i+1} / {len(line_pointers)}",
                          file=sys.stderr)
    finally:
        for h in file_handles:
            h.close()
    print(f"[mix] DONE wrote {len(line_pointers)} records to {args.out}",
          file=sys.stderr)


if __name__ == "__main__":
    main()
