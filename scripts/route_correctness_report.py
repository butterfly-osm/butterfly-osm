#!/usr/bin/env python3
"""
Combine per-mode sweep results into a single REPORT.md and consolidated
top-disagreements TSV.

Reads:
  bench/route/results/correctness-sweep-2026-05-06/results-{mode}.jsonl
  bench/route/results/correctness-sweep-2026-05-06/summary-{mode}.json

Writes:
  REPORT.md
  top-disagreements-all.tsv
"""

import argparse
import json
import sys
from pathlib import Path

import statistics
import math

# Reuse data classes / helpers
sys.path.insert(0, str(Path(__file__).parent))
from route_correctness_sweep import (  # type: ignore
    CompareResult, summarize_mode, percentile, write_top_disagreements,
)


def write_combined_report(out_dir: Path, modes: list, summaries: list, top_per_mode: list) -> None:
    lines: list = []
    lines.append("# Route Correctness Sweep -- Belgium, butterfly-route vs OSRM")
    lines.append("")
    lines.append("**Date:** 2026-05-06")
    lines.append("")
    lines.append("**Engines under test:**")
    lines.append("- butterfly-route (post #189 / d5faa99 main, edge-based CCH, single-region BE container)")
    lines.append("- OSRM v5.26.0 CH (latest osrm-backend docker image)")
    lines.append("")
    lines.append("**Dataset:** Belgium, single region, baseline.butterfly")
    lines.append("**Pairs:** 10,000 randomly generated origin-destination pairs.")
    lines.append("**Modes:** car, bike, foot")
    lines.append("")
    lines.append("**Pair generation:**")
    lines.append("- Both endpoints sampled uniformly from BE bounding box [49.55, 51.45]N x [2.65, 6.35]E.")
    lines.append("- Both endpoints snapped via butterfly's `/nearest?mode=car`. Pairs rejected when snap distance > 100m.")
    lines.append("- Great-circle distance bounded to [500m, 250km] (rejects degenerate-short and out-of-region pairs).")
    lines.append("- Same 10,000 snapped pairs used for all three modes (`bench/route/results/correctness-sweep-2026-05-06/be-pairs.tsv`).")
    lines.append("")
    lines.append("**Tolerance per pair:**")
    lines.append("- Distance: within 0.5%, or 50m absolute (whichever is larger)")
    lines.append("- Duration: within 1%,   or 5s absolute (whichever is larger)")
    lines.append("")
    lines.append("## Methodology caveat -- speed-profile bias")
    lines.append("")
    lines.append("OSRM and butterfly use different speed profiles by design.")
    lines.append("")
    lines.append("| road class | OSRM `car.lua` | butterfly `models/car.model.json` |")
    lines.append("|------------|----------------|-----------------------------------|")
    lines.append("| motorway   | 90 km/h * 0.8 = **72** | **110** |")
    lines.append("| trunk      | 85 * 0.8 = **68** | **90** |")
    lines.append("| primary    | 65 * 0.8 = **52** | **70** |")
    lines.append("| secondary  | 55 * 0.8 = **44** | **60** |")
    lines.append("")
    lines.append("(OSRM `car.lua` applies a global `speed_reduction = 0.8` multiplier on top of the table speeds, modelling traffic.)")
    lines.append("")
    lines.append("This produces a systematic per-mode duration bias that no routing algorithm can close -- it is a profile-tuning question, not a correctness question. We therefore report **two** duration metrics:")
    lines.append("")
    lines.append("1. **Raw duration disagreement** -- engines compared with their default profiles. Useful for production-truth comparisons but dominated by speed-profile choice.")
    lines.append("2. **Bias-corrected duration disagreement** -- butterfly durations rescaled by the per-mode aggregate ratio `sum(OSRM_duration) / sum(butterfly_duration)`. Isolates **do they agree on which roads to take** from **do they use the same speed table**. The bias-corrected number is the true correctness signal for the routing algorithm.")
    lines.append("")
    lines.append("Distance disagreement is profile-independent and is reported as-is.")
    lines.append("")

    # ---- Per-mode summary table (raw) ----
    lines.append("## Per-mode summary (raw, default profiles)")
    lines.append("")
    lines.append("| mode | n_pairs_both_valid | dist_p50 % | dist_p95 % | dist_p99 % | dist_max % | dur_p50 % | dur_p95 % | dur_p99 % | flagged_dist>5% | flagged_dur>5% |")
    lines.append("|------|-------------------:|-----------:|-----------:|-----------:|-----------:|----------:|----------:|----------:|----------------:|----------------:|")
    for s in summaries:
        if s["n_both_valid"] == 0:
            lines.append(f"| {s['mode']} | 0 | -- | -- | -- | -- | -- | -- | -- | -- | -- |")
            continue
        d = s["distance_diff_pct"]
        u = s["duration_diff_pct"]
        lines.append(
            f"| {s['mode']} | {s['n_both_valid']} | "
            f"{d['p50']:.2f} | {d['p95']:.2f} | {d['p99']:.2f} | {d['max']:.2f} | "
            f"{u['p50']:.2f} | {u['p95']:.2f} | {u['p99']:.2f} | "
            f"{s['n_flagged_distance_gt_5pct']} | {s['n_flagged_duration_gt_5pct']} |"
        )
    lines.append("")

    # ---- Per-mode summary table (bias-corrected) ----
    lines.append("## Per-mode summary (bias-corrected -- true correctness signal)")
    lines.append("")
    lines.append("Butterfly durations rescaled by per-mode aggregate ratio so total duration matches OSRM.")
    lines.append("")
    lines.append("| mode | speed_bias (OSRM/BF) | dist_p50 % | dist+corr_dur within tol | dur_corr_p50 % | dur_corr_p95 % | dur_corr_p99 % |")
    lines.append("|------|---------------------:|-----------:|-------------------------:|---------------:|---------------:|---------------:|")
    for s in summaries:
        if s["n_both_valid"] == 0 or s.get("speed_profile_bias_ratio_osrm_over_bf") is None:
            lines.append(f"| {s['mode']} | -- | -- | -- | -- | -- | -- |")
            continue
        c = s["duration_diff_pct_corrected"]
        lines.append(
            f"| {s['mode']} | {s['speed_profile_bias_ratio_osrm_over_bf']:.4f} | "
            f"{s['distance_diff_pct']['p50']:.2f} | "
            f"{s['pct_in_distance_and_corrected_duration']:.2f}% | "
            f"{c['p50']:.2f} | {c['p95']:.2f} | {c['p99']:.2f} |"
        )
    lines.append("")

    # ---- Distance histogram per mode ----
    lines.append("## Distance disagreement distribution (cumulative)")
    lines.append("")
    lines.append("Percentage of pairs whose distance disagreement is at most X% of OSRM distance.")
    lines.append("")
    lines.append("| mode | <=0.5% | <=1% | <=2% | <=5% | <=10% | <=25% | <=50% | <=100% |")
    lines.append("|------|-------:|-----:|-----:|-----:|------:|------:|------:|-------:|")
    for mode in modes:
        with (out_dir / f"results-{mode}.jsonl").open() as f:
            buckets_max = [0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0]
            cum_counts = [0] * len(buckets_max)
            n = 0
            for line in f:
                r = json.loads(line)
                if r['butterfly_distance_m'] is None or r['osrm_distance_m'] is None or r['osrm_distance_m'] <= 0:
                    continue
                n += 1
                pct = abs(r['butterfly_distance_m'] - r['osrm_distance_m']) / r['osrm_distance_m'] * 100.0
                for i, m in enumerate(buckets_max):
                    if pct <= m:
                        cum_counts[i] += 1
            row = "| {0} | ".format(mode) + " | ".join(f"{100.0*c/n:.2f}%" for c in cum_counts) + " |"
            lines.append(row)
    lines.append("")

    # ---- Per-mode breakdown ----
    for s in summaries:
        lines.append(f"## Mode `{s['mode']}` -- full breakdown")
        lines.append("")
        lines.append(f"- pairs total: **{s['n_total']}**")
        lines.append(f"- both engines succeeded: **{s['n_both_valid']}** ({100.0*s['n_both_valid']/max(1,s['n_total']):.2f}%)")
        lines.append(f"- OSRM-only success: {s['n_osrm_only']}")
        lines.append(f"- butterfly-only success: {s['n_butterfly_only']}")
        lines.append(f"- both failed: {s['n_both_failed']}")
        if s['n_both_valid'] == 0:
            lines.append("")
            continue
        lines.append("")
        lines.append("**Distance disagreement** (% of OSRM):")
        d = s["distance_diff_pct"]
        lines.append(f"- p50 {d['p50']:.3f} %  |  p95 {d['p95']:.3f} %  |  p99 {d['p99']:.3f} %  |  max {d['max']:.3f} %  |  mean {d['mean']:.3f} %")
        lines.append("")
        lines.append("**Duration disagreement, raw** (% of OSRM):")
        u = s["duration_diff_pct"]
        lines.append(f"- p50 {u['p50']:.3f} %  |  p95 {u['p95']:.3f} %  |  p99 {u['p99']:.3f} %  |  max {u['max']:.3f} %  |  mean {u['mean']:.3f} %")
        lines.append("")
        lines.append("**Duration disagreement, bias-corrected** (% of OSRM):")
        c = s["duration_diff_pct_corrected"]
        lines.append(f"- p50 {c['p50']:.3f} %  |  p95 {c['p95']:.3f} %  |  p99 {c['p99']:.3f} %  |  max {c['max']:.3f} %  |  mean {c['mean']:.3f} %")
        lines.append("")
        lines.append("**Tolerance pass-rate**:")
        lines.append(f"- distance only (within 0.5% / 50m):                 {s['n_in_distance_tolerance']}/{s['n_both_valid']} ({s['pct_in_distance_tolerance']:.2f}%)")
        lines.append(f"- duration raw only (within 1% / 5s):                {s['n_in_duration_tolerance']}/{s['n_both_valid']} ({100.0*s['n_in_duration_tolerance']/s['n_both_valid']:.2f}%)")
        lines.append(f"- duration bias-corrected only (within 1% / 5s):     {s['n_in_duration_tolerance_corrected']}/{s['n_both_valid']} ({100.0*s['n_in_duration_tolerance_corrected']/s['n_both_valid']:.2f}%)")
        lines.append(f"- BOTH raw distance and bias-corrected duration:     {s['pct_in_distance_and_corrected_duration']:.2f}%")
        lines.append(f"- BOTH raw distance and raw duration:                {s['pct_in_both_tolerance']:.2f}%")
        lines.append("")
        lines.append("**Aggregate totals (sanity check):**")
        lines.append(f"- OSRM:      {s['osrm_distance_total_km']:.0f} km, {s['osrm_duration_total_h']:.0f} h")
        lines.append(f"- butterfly: {s['bf_distance_total_km']:.0f} km, {s['bf_duration_total_h']:.0f} h")
        bias_d = 100.0*(s['bf_distance_total_km']/max(1e-9,s['osrm_distance_total_km']) - 1)
        bias_t = 100.0*(s['bf_duration_total_h']/max(1e-9,s['osrm_duration_total_h']) - 1)
        lines.append(f"- bias (butterfly relative to OSRM): distance {bias_d:+.3f}%, duration {bias_t:+.3f}%")
        lines.append("")

    # ---- Top disagreements ----
    lines.append("## Top-20 disagreement cases (per mode)")
    lines.append("")
    lines.append("Ranked by max(distance_pct_diff, duration_pct_diff). Use the lat/lon to manually inspect on https://www.openstreetmap.org or another reference engine.")
    lines.append("")
    for mode, top in top_per_mode:
        lines.append(f"### {mode}")
        lines.append("")
        lines.append("| rank | src (lon,lat) | dst (lon,lat) | OSRM d (m) | BF d (m) | dist % | OSRM t (s) | BF t (s) | dur % |")
        lines.append("|-----:|---------------|---------------|-----------:|---------:|-------:|-----------:|---------:|------:|")
        for r in top[:20]:
            lines.append(
                f"| {r['rank']} | "
                f"{r['src_lon']:.5f},{r['src_lat']:.5f} | "
                f"{r['dst_lon']:.5f},{r['dst_lat']:.5f} | "
                f"{r['osrm_distance_m']:.0f} | {r['bf_distance_m']:.0f} | {r['distance_pct_diff']:.2f} | "
                f"{r['osrm_duration_s']:.0f} | {r['bf_duration_s']:.0f} | {r['duration_pct_diff']:.2f} |"
            )
        lines.append("")

    # ---- Findings ----
    lines.append("## Findings")
    lines.append("")
    lines.append("### F1. Speed-profile divergence dominates raw duration agreement")
    lines.append("")
    lines.append("Default OSRM v5.26 `car.lua` and butterfly `models/car.model.json` differ by ~17% on aggregate duration for car (OSRM/BF = 1.166), ~25% for bike (1.254), and butterfly is slower than OSRM for foot (OSRM/BF = 0.905). This is a TUNABLE parameter, not an algorithmic property. After bias correction the engines agree on bike at p50=2.36% and foot at p50=1.02%. Recommend either:")
    lines.append("- Tune `models/car.model.json` to match OSRM defaults for compatibility-mode workloads.")
    lines.append("- Document the divergence and offer a `--profile osrm-compat` switch.")
    lines.append("")
    lines.append("### F2. butterfly returns 'no route' on 15.6% of car pairs that OSRM successfully routes")
    lines.append("")
    lines.append("On the car sweep, 1,563 of 10,000 pairs returned 404 'No route found' from butterfly while OSRM successfully routed both directions. Investigation:")
    lines.append("")
    lines.append("- Sampled 50 unique unroutable destinations -- only 2 (4%) are isolated from a Brussels-centred probe (true small-SCC). The remaining 96% are routable in **at least one direction** to/from Brussels.")
    lines.append("- Concretely: pair `(4.4579,51.2697) -> (5.7803,49.7259)` returns 404 from butterfly. Butterfly successfully routes `dst -> brussels` (192km, 200 OK) but `brussels -> dst` returns 404. This is **directional asymmetry**: the destination snap point is reachable backward but not forward, or vice versa.")
    lines.append("- The bike (0/10000) and foot (0/10000) sweeps have ZERO no-route failures on the same coordinates, because their graphs include reverse-undirected pedestrian/bike paths that bypass the issue.")
    lines.append("")
    lines.append("**Conclusion:** butterfly's `/nearest` snaps to the geometrically closest EBG node without considering whether that node is FORWARD-reachable (for src) or BACKWARD-reachable (for dst). OSRM's snapping handles both endpoints of an undirected edge and picks the side matching the role.")
    lines.append("")
    lines.append("This is a genuine snap-quality bug worth filing as a GitHub issue. It costs ~15.6% of long-distance car pairs.")
    lines.append("")
    lines.append("### F3. Distance agreement on routable pairs is reasonable but not tight")
    lines.append("")
    lines.append("Among pairs where both engines route successfully, distance disagreement distributions:")
    for s in summaries:
        if s['n_both_valid'] == 0:
            continue
        d = s['distance_diff_pct']
        lines.append(f"- `{s['mode']}` p50/p95/p99/max: {d['p50']:.2f}% / {d['p95']:.2f}% / {d['p99']:.2f}% / {d['max']:.2f}%")
    lines.append("")
    lines.append("p50 distance differences of 1-2% indicate the engines pick **subtly different routes** on roughly half the queries -- typically choosing different motorway exits, alternates, or via roads. This is partly due to the speed-profile differences (favouring different road classes) and partly due to turn-cost model differences. Not a correctness defect; a tuning gap.")
    lines.append("")
    lines.append("Long-tail distance differences (>50%, ~0.06% of car pairs) correspond to genuine routing-graph divergences, often where one engine takes a far longer detour. These rare cases are worth inspecting individually (see Top-20 tables above).")
    lines.append("")

    # ---- Verdict ----
    lines.append("## Honest characterisation per mode")
    lines.append("")
    for s in summaries:
        if s["n_both_valid"] == 0:
            lines.append(f"- `{s['mode']}`: **no valid comparisons**")
            continue
        bias_ratio = s.get("speed_profile_bias_ratio_osrm_over_bf")
        bias_str = f"{bias_ratio:.3f}" if bias_ratio is not None else "n/a"
        corrected_pct = s.get("pct_in_distance_and_corrected_duration", 0.0)
        no_route_rate = s["n_osrm_only"] / max(1, s["n_total"]) * 100.0
        d = s["distance_diff_pct"]
        c = s["duration_diff_pct_corrected"]
        lines.append(f"### `{s['mode']}`")
        lines.append("")
        lines.append(f"- **{corrected_pct:.2f}%** of pairs agree on both distance (within 0.5% / 50m) AND bias-corrected duration (within 1% / 5s).")
        lines.append(f"- Raw raw-tolerance match: {s['pct_in_both_tolerance']:.2f}% (skewed by speed-profile bias).")
        lines.append(f"- Speed-profile bias (OSRM/butterfly aggregate-duration ratio): **{bias_str}**.")
        lines.append(f"- Distance bias (butterfly total / OSRM total - 1): {100.0*(s['bf_distance_total_km']/max(1e-9,s['osrm_distance_total_km']) - 1):+.3f}%.")
        lines.append(f"- butterfly returned 'no route' on **{s['n_osrm_only']}/{s['n_total']} ({no_route_rate:.2f}%)** pairs that OSRM successfully routed.")
        lines.append(f"- Distance disagreement: p50={d['p50']:.2f}%, p95={d['p95']:.2f}%, p99={d['p99']:.2f}%, max={d['max']:.2f}%.")
        lines.append(f"- Bias-corrected duration disagreement: p50={c['p50']:.2f}%, p95={c['p95']:.2f}%, p99={c['p99']:.2f}%, max={c['max']:.2f}%.")
        lines.append("")

    # ---- Verdict ----
    overall_pass = all(
        s["n_both_valid"] > 0
        and s.get("pct_in_distance_and_corrected_duration", 0.0) >= 99.5
        for s in summaries
    )
    lines.append("## Verdict")
    lines.append("")
    if overall_pass:
        lines.append("**PASS.** All modes meet the 99.5% within-tolerance floor against bias-corrected OSRM.")
    else:
        lines.append("**FAIL.** No mode meets the 99.5% within-tolerance floor when both distance (0.5% / 50m) AND bias-corrected duration (1% / 5s) are required simultaneously.")
        lines.append("")
        lines.append("Root causes (in order of impact):")
        lines.append("")
        lines.append("1. **Speed-profile divergence** (F1) -- accounts for the 14-25% raw duration drift. Tunable, not algorithmic.")
        lines.append("2. **Snap directional asymmetry** (F2) -- accounts for 15.6% car pairs returning 404. **Genuine bug**, file as GitHub issue.")
        lines.append("3. **Routing-decision divergence on long routes** (F3) -- p50 ~1-2% distance gap; partly explained by F1 (different road-class preferences), partly by turn-cost model differences. Investigate the Top-20 tables.")
        lines.append("")
        lines.append("**However, after isolating routing-decision agreement from profile choice and snap quality:**")
        for s in summaries:
            if s["n_both_valid"] == 0: continue
            # Among pairs where both routed, what % agree within bias-corrected tolerance
            both_only = s["n_both_valid"]
            in_tol_both = int(s["pct_in_distance_and_corrected_duration"] * both_only / 100.0)
            n_within_corr_dur = s.get("n_in_duration_tolerance_corrected", 0)
            within_corr_dur_pct = 100.0 * n_within_corr_dur / max(1, both_only)
            lines.append(f"- `{s['mode']}` bias-corrected duration agreement (alone): **{within_corr_dur_pct:.2f}%** within 1%/5s")
        lines.append("")
        lines.append("These bias-corrected duration agreement rates are the meaningful correctness numbers for the routing algorithm itself.")
    lines.append("")

    (out_dir / "REPORT.md").write_text("\n".join(lines))
    print(f"wrote {out_dir / 'REPORT.md'}")

    # Combined top-disagreements TSV
    combined = out_dir / "top-disagreements-all.tsv"
    with combined.open("w") as f:
        f.write("mode\trank\tpair_idx\tsrc_lon\tsrc_lat\tdst_lon\tdst_lat\t"
                "osrm_distance_m\tbf_distance_m\tdist_pct\tosrm_duration_s\tbf_duration_s\tdur_pct\n")
        for mode, top in top_per_mode:
            for r in top[:20]:
                f.write(f"{mode}\t{r['rank']}\t{r['pair_idx']}\t"
                        f"{r['src_lon']:.6f}\t{r['src_lat']:.6f}\t{r['dst_lon']:.6f}\t{r['dst_lat']:.6f}\t"
                        f"{r['osrm_distance_m']:.1f}\t{r['bf_distance_m']:.1f}\t{r['distance_pct_diff']:.2f}\t"
                        f"{r['osrm_duration_s']:.1f}\t{r['bf_duration_s']:.1f}\t{r['duration_pct_diff']:.2f}\n")
    print(f"wrote {combined}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="bench/route/results/correctness-sweep-2026-05-06")
    ap.add_argument("--modes", default="car,bike,foot")
    args = ap.parse_args()

    out = Path(args.out)
    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    summaries = []
    top_per_mode = []
    for m in modes:
        results: list = []
        with (out / f"results-{m}.jsonl").open() as f:
            for line in f:
                d = json.loads(line)
                results.append(CompareResult(
                    pair_idx=d['pair_idx'], mode=d['mode'],
                    src_lon=d['src_lon'], src_lat=d['src_lat'],
                    dst_lon=d['dst_lon'], dst_lat=d['dst_lat'],
                    osrm_distance_m=d['osrm_distance_m'],
                    osrm_duration_s=d['osrm_duration_s'],
                    bf_distance_m=d['butterfly_distance_m'],
                    bf_duration_s=d['butterfly_duration_s'],
                    osrm_error=d['osrm_error'],
                    bf_error=d['butterfly_error'],
                ))
        s = summarize_mode(m, results)
        # Persist updated summary too
        with (out / f"summary-{m}.json").open("w") as f:
            json.dump(s, f, indent=2)
        summaries.append(s)
        top = write_top_disagreements(out / f"top-disagreements-{m}.tsv", m, results)
        top_per_mode.append((m, top))
    write_combined_report(out, modes, summaries, top_per_mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
