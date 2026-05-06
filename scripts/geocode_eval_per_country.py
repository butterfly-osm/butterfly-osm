#!/usr/bin/env python3
"""Summarize per-country bio_acc from a training metrics JSONL.

Reads the JSONL telemetry emitted by `butterfly-geocode train --metrics-out`
and prints a table of best per-country bio_acc across all logged epochs.

Usage:
    python3 scripts/geocode_eval_per_country.py \
        --metrics geocode-research/training-runs/2026-05-06-multi-country-prod.jsonl
"""
import argparse
import json
import sys
from pathlib import Path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--metrics", required=True, help="Training metrics JSONL")
    p.add_argument("--baseline-be", type=float, default=0.870,
                   help="BE single-country baseline bio_acc to compare against")
    args = p.parse_args()

    rows = []
    with open(args.metrics) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    if not rows:
        print("[error] no rows in metrics file", file=sys.stderr)
        sys.exit(1)

    print(f"# Multi-country tagger training summary")
    print(f"# Source: {args.metrics}")
    print(f"# Architecture: d_model={rows[-1]['d_model']} n_layers={rows[-1]['n_layers']} n_countries={rows[-1]['n_countries']}")
    print(f"# Device: {rows[-1]['device']}")
    print(f"# Total wall clock: {rows[-1]['wall_seconds_elapsed']:.0f}s ({rows[-1]['wall_seconds_elapsed']/60:.1f}min)")
    print(f"# Epochs logged: {len(rows)}")
    print()

    print(f"## Per-epoch aggregate")
    print(f"| Epoch | train_loss | eval_loss | bio_acc (overall) | country_acc | wall (s) |")
    print(f"|------:|:-----------|:----------|:------------------|:------------|:---------|")
    for r in rows:
        print(f"| {r['epoch']} | {r['train_loss']:.4f} | {r['eval_loss']:.4f} | {r['bio_acc']:.4f} | {r['country_acc']:.4f} | {r['wall_seconds_elapsed']:.0f} |")
    print()

    # Best per-country bio_acc across epochs
    pc_keys = sorted(rows[-1]['per_country_bio_acc'].keys())
    print(f"## Per-country bio_acc")
    print(f"| Country | first_epoch | last_epoch | best | improvement | total_eval_examples |")
    print(f"|:--------|:------------|:-----------|:-----|:------------|:--------------------|")
    last_epoch = rows[-1]
    first_epoch = rows[0]
    best_per_country = {iso: 0.0 for iso in pc_keys}
    for r in rows:
        for iso in pc_keys:
            v = r['per_country_bio_acc'][iso]['bio_acc']
            if v > best_per_country[iso]:
                best_per_country[iso] = v
    for iso in pc_keys:
        first = first_epoch['per_country_bio_acc'][iso]['bio_acc']
        last = last_epoch['per_country_bio_acc'][iso]['bio_acc']
        best = best_per_country[iso]
        delta = last - first
        total = last_epoch['per_country_bio_acc'][iso]['total']
        print(f"| {iso} | {first:.4f} | {last:.4f} | {best:.4f} | {delta:+.4f} | {total} |")
    print()
    avg_best = sum(best_per_country.values()) / len(best_per_country)
    print(f"# Mean best bio_acc across countries: {avg_best:.4f}")
    print(f"# Belgium baseline (single-country, d=256): {args.baseline_be:.4f}")
    if best_per_country.get('BE', 0.0) >= args.baseline_be:
        print(f"# BE in multi-country >= baseline ({best_per_country.get('BE', 0.0):.4f} >= {args.baseline_be:.4f}): PASS")
    else:
        print(f"# BE in multi-country < baseline ({best_per_country.get('BE', 0.0):.4f} < {args.baseline_be:.4f}): regressed by {args.baseline_be - best_per_country.get('BE', 0.0):.4f}")


if __name__ == "__main__":
    main()
