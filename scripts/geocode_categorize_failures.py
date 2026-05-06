#!/usr/bin/env python3
"""Categorize Butterfly geocoder failures vs Nominatim ground truth.

Reads the rows JSONL produced by `geocode_multi_country_bench.py`
(`butterfly-rows.jsonl` and `nominatim-rows.jsonl`), classifies each
failing query by its quality_class plus a few derived heuristic types,
and emits:
  - failures-categorized.tsv: every failed butterfly query labeled
  - failure-summary.json: aggregate counts per category

Usage:
    python3 scripts/geocode_categorize_failures.py \
        --bf-rows bench/geocode/results/.../BE/butterfly-rows.jsonl \
        --nom-rows bench/geocode/results/.../BE/nominatim-rows.jsonl \
        --out-dir bench/geocode/results/2026-05-06-be-merged-shard/ \
        --radius-m 100
"""
import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path


def hav(a, b, c, d):
    R = 6371000.0
    p1, p2 = math.radians(a), math.radians(c)
    dphi, dlam = math.radians(c - a), math.radians(d - b)
    x = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(x), math.sqrt(1 - x))


def load_rows(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


# Heuristics for Belgium-style queries: detect missing postcode, ASCII-only,
# non-Latin diacritics, abbreviation markers, etc. Quality_class from corpus is
# the primary axis; we add secondary facets for "why did this one fail".
POSTCODE_RE = re.compile(r"\b\d{4,5}\b")
HOUSENUMBER_RE = re.compile(r"\b\d+[a-zA-Z]?\b")
ABBREV_RE = re.compile(r"\b\w+(\.|str\.|pl\.|av\.|bd\.|bvd\.|chee\.)", re.IGNORECASE)
DIACRITIC_RE = re.compile(r"[À-ɏḀ-ỿ]")


def classify_text(text):
    facets = []
    if not POSTCODE_RE.search(text):
        facets.append("no_postcode")
    if not HOUSENUMBER_RE.search(text):
        facets.append("no_housenumber")
    if ABBREV_RE.search(text):
        facets.append("has_abbreviation")
    if DIACRITIC_RE.search(text):
        facets.append("has_diacritics")
    if "/" in text:
        facets.append("has_slash_suffix")
    return facets


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bf-rows", required=True)
    ap.add_argument("--nom-rows", required=False, default=None)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--radius-m", type=float, default=100.0)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bf = load_rows(args.bf_rows)
    nom = load_rows(args.nom_rows) if args.nom_rows else None

    # Index nominatim by query_id for cross-reference.
    nom_by_id = {}
    if nom is not None:
        for row in nom:
            qid = row["q"]["query_id"]
            nom_by_id[qid] = row

    failures = []
    successes_by_cat = Counter()
    n_per_cat = Counter()
    nom_wins_when_bf_loses = 0
    bf_wins_when_nom_loses = 0
    both_lose = 0
    facets_in_failures = Counter()

    for row in bf:
        q = row["q"]
        r = row["r"]
        cat = q["quality_class"]
        n_per_cat[cat] += 1
        topk = r.get("topk", [])
        bf_top1_ok = False
        d1 = None
        if topk and topk[0].get("lat") is not None:
            d1 = hav(q["gold_lat"], q["gold_lon"], topk[0]["lat"], topk[0]["lon"])
            if d1 <= args.radius_m:
                bf_top1_ok = True
        if bf_top1_ok:
            successes_by_cat[cat] += 1
            continue

        # Failure path
        nom_top1_ok = False
        nom_top1 = None
        if nom_by_id and q["query_id"] in nom_by_id:
            nrow = nom_by_id[q["query_id"]]
            nt = nrow["r"].get("topk", [])
            if nt and nt[0].get("lat") is not None:
                nom_top1 = nt[0]
                nd1 = hav(q["gold_lat"], q["gold_lon"], nt[0]["lat"], nt[0]["lon"])
                if nd1 <= args.radius_m:
                    nom_top1_ok = True

        if nom_top1_ok:
            nom_wins_when_bf_loses += 1
        else:
            both_lose += 1

        facets = classify_text(q["query_text"])
        for f in facets:
            facets_in_failures[(cat, f)] += 1

        # Failure reason: no result vs wrong result
        if not topk or topk[0].get("lat") is None:
            reason = "no_result"
        elif d1 is None:
            reason = "no_result"
        elif d1 > args.radius_m * 100:  # wildly wrong (>10km)
            reason = "wrong_far"
        else:
            reason = "wrong_near"

        failures.append({
            "query_id": q["query_id"],
            "query_text": q["query_text"],
            "category": cat,
            "facets": ",".join(facets) if facets else "",
            "gold_lat": q["gold_lat"],
            "gold_lon": q["gold_lon"],
            "bf_top1_lat": topk[0].get("lat") if topk else None,
            "bf_top1_lon": topk[0].get("lon") if topk else None,
            "bf_d_m": round(d1, 1) if d1 is not None else "",
            "bf_reason": reason,
            "nom_ok": nom_top1_ok,
            "nom_top1_lat": nom_top1["lat"] if nom_top1 else None,
            "nom_top1_lon": nom_top1["lon"] if nom_top1 else None,
        })

    # Also count BF wins when nom loses
    if nom_by_id:
        for row in bf:
            q = row["q"]
            r = row["r"]
            topk = r.get("topk", [])
            bf_ok = False
            if topk and topk[0].get("lat") is not None:
                d1 = hav(q["gold_lat"], q["gold_lon"], topk[0]["lat"], topk[0]["lon"])
                if d1 <= args.radius_m:
                    bf_ok = True
            if not bf_ok:
                continue
            qid = q["query_id"]
            if qid not in nom_by_id:
                continue
            nrow = nom_by_id[qid]
            nt = nrow["r"].get("topk", [])
            nom_ok = False
            if nt and nt[0].get("lat") is not None:
                nd = hav(q["gold_lat"], q["gold_lon"], nt[0]["lat"], nt[0]["lon"])
                if nd <= args.radius_m:
                    nom_ok = True
            if not nom_ok:
                bf_wins_when_nom_loses += 1

    # Write TSV
    tsv_path = out_dir / "failures-categorized.tsv"
    with open(tsv_path, "w") as f:
        f.write("query_id\tcategory\tfacets\tquery_text\tbf_reason\tbf_d_m\tnom_ok\tgold_lat\tgold_lon\tbf_top1_lat\tbf_top1_lon\tnom_top1_lat\tnom_top1_lon\n")
        for r in failures:
            f.write("\t".join([
                r["query_id"], r["category"], r["facets"], r["query_text"],
                r["bf_reason"], str(r["bf_d_m"]), str(r["nom_ok"]),
                f"{r['gold_lat']:.7f}", f"{r['gold_lon']:.7f}",
                f"{r['bf_top1_lat']:.7f}" if r["bf_top1_lat"] is not None else "",
                f"{r['bf_top1_lon']:.7f}" if r["bf_top1_lon"] is not None else "",
                f"{r['nom_top1_lat']:.7f}" if r["nom_top1_lat"] is not None else "",
                f"{r['nom_top1_lon']:.7f}" if r["nom_top1_lon"] is not None else "",
            ]) + "\n")

    # Summary JSON
    per_cat = {}
    for cat in n_per_cat:
        per_cat[cat] = {
            "n": n_per_cat[cat],
            "bf_top1_ok": successes_by_cat[cat],
            "bf_top1_recall": successes_by_cat[cat] / n_per_cat[cat],
            "n_failed": n_per_cat[cat] - successes_by_cat[cat],
        }
    overall_n = sum(n_per_cat.values())
    overall_ok = sum(successes_by_cat.values())
    summary = {
        "n_total": overall_n,
        "n_passed": overall_ok,
        "n_failed": overall_n - overall_ok,
        "top1_recall": overall_ok / overall_n if overall_n else 0.0,
        "per_category": per_cat,
        "nom_wins_bf_loses": nom_wins_when_bf_loses,
        "bf_wins_nom_loses": bf_wins_when_nom_loses,
        "both_lose": both_lose,
        "top_failure_facets": dict(Counter({
            f"{c}/{f}": n for (c, f), n in facets_in_failures.items()
        }).most_common(20)),
    }
    with open(out_dir / "failure-summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"failures-categorized.tsv: {len(failures)} rows -> {tsv_path}")
    print(f"failure-summary.json -> {out_dir/'failure-summary.json'}")
    print()
    print(f"overall recall@1: {summary['top1_recall']:.3f} ({overall_ok}/{overall_n})")
    print(f"nom wins / bf loses: {nom_wins_when_bf_loses}")
    print(f"bf wins / nom loses: {bf_wins_when_nom_loses}")
    print(f"both lose:           {both_lose}")
    print()
    for cat, c in sorted(per_cat.items()):
        print(f"  {cat}: recall={c['bf_top1_recall']:.3f} ({c['bf_top1_ok']}/{c['n']}, failed={c['n_failed']})")
    print()
    print("top failure facets (cat/facet -> count):")
    for k, v in summary["top_failure_facets"].items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
