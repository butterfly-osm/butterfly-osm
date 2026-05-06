#!/usr/bin/env python3
"""Chunked training driver for the byte-level tagger.

Runs `butterfly-geocode train` in 5-minute chunks (configurable via
`--chunk-seconds`). After each chunk, parses the JSONL telemetry
emitted via `--metrics-out` and decides whether to continue. The
discipline is: never train more than one chunk without re-evaluating
critically.

Decision rules:
- continue   : bio_acc improved by >= --min-bio-acc-delta over the
               most-recent chunk.
- continue   : eval_loss improved by >= --min-eval-loss-delta and
               train_loss is still falling — gradient-quality healthy.
- stop       : eval_loss has trended UP across the last two chunks
               (overfitting / divergence).
- stop       : bio_acc plateaued for >= --plateau-chunks-stop
               consecutive chunks.
- stop       : total wall clock >= --max-total-seconds.

When the trainer exits with status code 2 (wall-clock budget
exhausted, more work possible), this driver re-invokes it with
`--resume <out> --resume-step <last_global_step>` to keep the LR
schedule continuous.

Usage:
    python3 scripts/geocode_train_chunks.py \
        --binary ./target/release/butterfly-geocode \
        --corpus geocode-training/output/be-corpus-100k.jsonl \
        --out geocode/data/models/belgium-prod.safetensors \
        --metrics-out geocode-research/training-runs/2026-05-06-gpu-prod.jsonl \
        --countries BE \
        --architecture production \
        --device cuda \
        --batch-size 128 \
        --learning-rate 1e-3 \
        --warmup-steps 200 \
        --epochs 50 \
        --chunk-seconds 300 \
        --max-total-seconds 1800
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Chunked tagger training driver")
    p.add_argument("--binary", required=True, help="Path to butterfly-geocode")
    p.add_argument("--corpus", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--metrics-out", required=True)
    p.add_argument("--countries", default="BE")
    p.add_argument("--architecture", default="production",
                   choices=["tiny", "production", "large"])
    p.add_argument("--device", default="cuda", choices=["auto", "cuda", "cpu"])
    p.add_argument("--dtype", default="f32", choices=["f32", "bf16"])
    p.add_argument("--batch-size", type=int, default=128)
    p.add_argument("--learning-rate", type=float, default=1e-3)
    p.add_argument("--lr-schedule", default="cosine")
    p.add_argument("--warmup-steps", type=int, default=200)
    p.add_argument("--weight-decay", type=float, default=0.01)
    p.add_argument("--gradient-clip", type=float, default=1.0)
    p.add_argument("--epochs", type=int, default=50,
                   help="Total epochs across all chunks (LR schedule horizon).")
    p.add_argument("--seed", type=int, default=0xB17EBAD0)
    p.add_argument("--early-stop-patience", type=int, default=4,
                   help="Forwarded to the trainer.")
    p.add_argument("--early-stop-min-delta", type=float, default=1e-3)
    # Chunked driver knobs.
    p.add_argument("--chunk-seconds", type=int, default=300,
                   help="Wall-clock per chunk before the driver re-evaluates.")
    p.add_argument("--max-total-seconds", type=int, default=1800,
                   help="Hard cap on wall-clock training time.")
    p.add_argument("--min-bio-acc-delta", type=float, default=0.005,
                   help="Required bio_acc improvement over previous chunk.")
    p.add_argument("--min-eval-loss-delta", type=float, default=0.005,
                   help="Required eval_loss improvement over previous chunk.")
    p.add_argument("--plateau-chunks-stop", type=int, default=2,
                   help="Stop after this many consecutive non-improving chunks.")
    return p.parse_args()


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def chunk_summary(rows: list[dict]) -> dict | None:
    """Last row of telemetry, or None if empty."""
    if not rows:
        return None
    return rows[-1]


def first_after(rows: list[dict], baseline_count: int) -> dict | None:
    """First row with index >= baseline_count, or None."""
    if len(rows) <= baseline_count:
        return None
    return rows[baseline_count]


def main() -> int:
    args = parse_args()
    binary = Path(args.binary).resolve()
    if not binary.exists():
        print(f"ERROR: binary {binary} not found", file=sys.stderr)
        return 1
    out_path = Path(args.out).resolve()
    metrics_path = Path(args.metrics_out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    # Wipe any prior metrics file so we start fresh; the driver tracks
    # chunk boundaries by row count.
    if metrics_path.exists():
        backup = metrics_path.with_suffix(
            metrics_path.suffix + f".bak-{int(time.time())}")
        shutil.move(metrics_path, backup)
        print(f"[driver] backed up prior metrics → {backup}")

    total_start = time.monotonic()
    chunk_idx = 0
    last_global_step = 0
    plateau_streak = 0
    prev_bio_acc: float | None = None
    prev_eval_loss: float | None = None
    prev_eval_trend: list[float] = []
    prev_row_count = 0

    while True:
        elapsed_total = time.monotonic() - total_start
        remaining = args.max_total_seconds - elapsed_total
        if remaining <= 0:
            print(f"[driver] hard cap {args.max_total_seconds}s reached — stopping")
            break
        chunk_seconds = min(args.chunk_seconds, int(remaining))
        chunk_idx += 1
        print(f"\n[driver] === chunk {chunk_idx} === "
              f"budget={chunk_seconds}s elapsed_total={elapsed_total:.0f}s "
              f"resume_step={last_global_step}")

        cmd = [
            str(binary), "train",
            "--corpus", args.corpus,
            "--out", str(out_path),
            "--metrics-out", str(metrics_path),
            "--countries", args.countries,
            "--architecture", args.architecture,
            "--device", args.device,
            "--dtype", args.dtype,
            "--batch-size", str(args.batch_size),
            "--learning-rate", str(args.learning_rate),
            "--lr-schedule", args.lr_schedule,
            "--warmup-steps", str(args.warmup_steps),
            "--weight-decay", str(args.weight_decay),
            "--gradient-clip", str(args.gradient_clip),
            "--epochs", str(args.epochs),
            "--seed", str(args.seed),
            "--max-train-seconds", str(chunk_seconds),
            "--early-stop-patience", str(args.early_stop_patience),
            "--early-stop-min-delta", str(args.early_stop_min_delta),
        ]
        if chunk_idx > 1 and out_path.exists():
            cmd += ["--resume", str(out_path),
                    "--resume-step", str(last_global_step)]

        chunk_start = time.monotonic()
        env = os.environ.copy()
        env.setdefault("RUST_LOG", "info")
        proc = subprocess.run(cmd, env=env)
        chunk_dur = time.monotonic() - chunk_start
        print(f"[driver] chunk {chunk_idx} returned {proc.returncode} "
              f"after {chunk_dur:.1f}s")

        rows = read_jsonl(metrics_path)
        new_rows = rows[prev_row_count:]
        if not new_rows:
            print(f"[driver] chunk {chunk_idx} produced no telemetry rows — stopping")
            break

        last = new_rows[-1]
        last_global_step = last.get("global_step", last_global_step)
        bio_acc = last.get("bio_acc", float("nan"))
        eval_loss = last.get("eval_loss", float("nan"))
        train_loss = last.get("train_loss", float("nan"))
        print(f"[driver] chunk {chunk_idx} end-of-chunk: "
              f"bio_acc={bio_acc:.4f} eval_loss={eval_loss:.4f} "
              f"train_loss={train_loss:.4f} "
              f"step={last_global_step} epochs_in_chunk={len(new_rows)}")

        # Decision rules.
        prev_eval_trend.append(eval_loss)
        if len(prev_eval_trend) > 3:
            prev_eval_trend = prev_eval_trend[-3:]

        # Stop: eval_loss going up for two chunks in a row.
        if (len(prev_eval_trend) >= 3
                and prev_eval_trend[-1] > prev_eval_trend[-2] > prev_eval_trend[-3]):
            print("[driver] eval_loss trending UP for 2 consecutive chunks "
                  "— stopping (overfitting / divergence)")
            break

        # Stop: trainer exited cleanly via early-stop or completed epochs
        # (returncode 0, and not because of wall-clock).
        if proc.returncode == 0:
            print("[driver] trainer exited cleanly (returncode=0) — stopping")
            break
        if proc.returncode not in (0, 2):
            print(f"[driver] trainer exited with unexpected code {proc.returncode} "
                  "— stopping")
            return proc.returncode

        # Plateau check.
        improved_bio = (prev_bio_acc is None
                        or bio_acc - prev_bio_acc >= args.min_bio_acc_delta)
        improved_loss = (prev_eval_loss is None
                         or prev_eval_loss - eval_loss >= args.min_eval_loss_delta)
        if improved_bio or improved_loss:
            plateau_streak = 0
            print(f"[driver] chunk {chunk_idx} improved "
                  f"(bio_acc Δ={bio_acc - (prev_bio_acc or 0):+.4f}, "
                  f"eval_loss Δ={(prev_eval_loss or 0) - eval_loss:+.4f}) "
                  "— continuing")
        else:
            plateau_streak += 1
            print(f"[driver] chunk {chunk_idx} plateau "
                  f"(streak={plateau_streak}/{args.plateau_chunks_stop})")
            if plateau_streak >= args.plateau_chunks_stop:
                print("[driver] plateau threshold reached — stopping")
                break

        prev_bio_acc = bio_acc
        prev_eval_loss = eval_loss
        prev_row_count = len(rows)

    elapsed_total = time.monotonic() - total_start
    rows = read_jsonl(metrics_path)
    if not rows:
        print("[driver] FAIL: no telemetry rows produced")
        return 1
    last = rows[-1]
    print("\n[driver] FINAL")
    print(f"  total_wall_seconds = {elapsed_total:.1f}")
    print(f"  chunks_run         = {chunk_idx}")
    print(f"  epochs_total       = {len(rows)}")
    print(f"  bio_acc            = {last.get('bio_acc', float('nan')):.4f}")
    print(f"  country_acc        = {last.get('country_acc', float('nan')):.4f}")
    print(f"  eval_loss          = {last.get('eval_loss', float('nan')):.4f}")
    print(f"  train_loss         = {last.get('train_loss', float('nan')):.4f}")
    print(f"  best_eval_loss     = {last.get('best_eval_loss', float('nan')):.4f}")
    print(f"  global_step        = {last.get('global_step', 0)}")
    print(f"  out_path           = {out_path}")
    print(f"  metrics_path       = {metrics_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
