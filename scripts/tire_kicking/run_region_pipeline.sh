#!/bin/bash
# Build a region's CCH from PBF — full pipeline (steps 1-8) using subset modes.
# Usage: run_region_pipeline.sh <region_name> <pbf_path>
set -euo pipefail
cd /home/snape/projects/butterfly-osm
REGION=$1
PBF=$2
BIN=./target/release/butterfly-route
DATA=data/$REGION
MODES_DIR=models
MODES="car bike foot"

mkdir -p $DATA/step1 $DATA/step2 $DATA/step3 $DATA/step4 $DATA/step5 $DATA/step6 $DATA/step7 $DATA/step8

echo "=== step1 ==="
time $BIN step1-ingest --input $PBF --outdir $DATA/step1

echo "=== step2 ==="
time $BIN step2-profile \
  --ways $DATA/step1/ways.raw \
  --relations $DATA/step1/relations.raw \
  --models-dir $MODES_DIR \
  --outdir $DATA/step2

echo "=== step3 ==="
time $BIN step3-nbg \
  --nodes $DATA/step1/nodes.sa \
  --ways $DATA/step1/ways.raw \
  $(for m in $MODES; do echo --way-attrs $m=$DATA/step2/way_attrs.$m.bin; done) \
  --outdir $DATA/step3

echo "=== step4 ==="
time $BIN step4-ebg \
  --nbg-csr $DATA/step3/nbg.csr \
  --nbg-geo $DATA/step3/nbg.geo \
  --nbg-node-map $DATA/step3/nbg.node_map \
  --node-signals $DATA/step1/node_signals.bin \
  $(for m in $MODES; do echo --way-attrs $m=$DATA/step2/way_attrs.$m.bin; done) \
  $(for m in $MODES; do echo --turn-rules $m=$DATA/step2/turn_rules.$m.bin; done) \
  --outdir $DATA/step4

echo "=== step5 ==="
time $BIN step5-weights \
  --ebg-nodes $DATA/step4/ebg.nodes \
  --ebg-csr $DATA/step4/ebg.csr \
  --turn-table $DATA/step4/ebg.turn_table \
  --nbg-geo $DATA/step3/nbg.geo \
  $(for m in $MODES; do echo --way-attrs $m=$DATA/step2/way_attrs.$m.bin; done) \
  --outdir $DATA/step5

echo "=== step6 ==="
for m in $MODES; do
  time $BIN step6-order \
    --filtered-ebg $DATA/step5/filtered.$m.ebg \
    --ebg-nodes $DATA/step4/ebg.nodes \
    --nbg-geo $DATA/step3/nbg.geo \
    --mode $m --outdir $DATA/step6
done

echo "=== step7 ==="
for m in $MODES; do
  time $BIN step7-contract \
    --filtered-ebg $DATA/step5/filtered.$m.ebg \
    --order $DATA/step6/order.$m.ebg \
    --weights $DATA/step5/w.$m.u32 --turns $DATA/step5/t.$m.u32 \
    --mode $m --outdir $DATA/step7
done

echo "=== step8 ==="
for m in $MODES; do
  time $BIN step8-customize \
    --cch-topo $DATA/step7/cch.$m.topo \
    --filtered-ebg $DATA/step5/filtered.$m.ebg \
    --order $DATA/step6/order.$m.ebg \
    --weights $DATA/step5/w.$m.u32 --turns $DATA/step5/t.$m.u32 \
    --ebg-nodes $DATA/step4/ebg.nodes \
    --mode $m --outdir $DATA/step8
done

echo "DONE $REGION"
