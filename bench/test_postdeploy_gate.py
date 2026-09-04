#!/usr/bin/env python3
"""Offline unit checks for `bench/postdeploy_gate.py`.

The gate itself can only be judged against a live server. Everything it does
BEFORE talking to one — threshold derivation, reference-directory resolution,
the geometry primitives, the memoised fetchers and the registry/probe tables —
is pure Python and is checked here, with NO environment variables set and no
network:

    python3 bench/test_postdeploy_gate.py
    python3 -m unittest bench/test_postdeploy_gate.py

Two of these are parity checks that would otherwise need a deploy to notice:
`rest_probes()` against the router's own `MOUNTED_PATHS` (#572 drift alarm),
and `TICKET_GATES` against the gate registry (#572 fold guard).
"""

import json
import os
import re
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import postdeploy_gate as g  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ThresholdDerivation(unittest.TestCase):
    """#589: ONE stored tolerance, every level bound derived from it."""

    def setUp(self):
        self.saved = dict(g.THRESHOLDS)

    def tearDown(self):
        g.THRESHOLDS.clear()
        g.THRESHOLDS.update(self.saved)

    def test_bounds_derive_from_tol_and_slack(self):
        t = g.derive_level_bounds()
        lo, tol = t["never_fast"], t["tol"]
        self.assertEqual(t["band_level"], (lo, round(1.0 + tol + t["slack_level"], 3)))
        self.assertEqual(t["band_regional"], (lo, round(1.0 + tol + t["slack_regional"], 3)))
        self.assertEqual(t["dur_p50"], t["band_regional"])

    def test_changing_tol_moves_every_bound(self):
        before = dict(g.derive_level_bounds())
        g.THRESHOLDS["tol"] = before["tol"] + 0.01
        after = g.derive_level_bounds()
        for key in ("band_level", "band_regional", "dur_p50"):
            self.assertAlmostEqual(after[key][1], before[key][1] + 0.01, places=6, msg=key)
            self.assertEqual(after[key][0], before[key][0], msg=f"{key} lower bound must not move")

    def test_asymmetric_never_fast(self):
        """Pierre 2026-09-03: never more than 2 % fast, several % slow allowed."""
        t = g.derive_level_bounds()
        self.assertLessEqual(t["never_fast"], 1.0)
        self.assertGreaterEqual(t["never_fast"], 0.97)
        self.assertGreater(t["band_level"][1], 1.0)
        self.assertGreater(1.0 - t["never_fast"], 0.0)
        self.assertGreater(t["band_level"][1] - 1.0, 1.0 - t["never_fast"])

    def test_windows_json_overrides_and_rederives(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "windows.json"), "w") as f:
                json.dump({"never_fast": 0.99, "tol": 0.05, "slack_level": 0.02,
                           "slack_regional": 0.04, "match_tol": 0.15}, f)
            t = g._apply_windows_config(d)
        self.assertEqual(t["never_fast"], 0.99)
        self.assertEqual(t["tol"], 0.05)
        self.assertEqual(t["band_level"], (0.99, 1.07))
        self.assertEqual(t["band_regional"], (0.99, 1.09))
        self.assertEqual(t["dur_p50"], (0.99, 1.09))
        self.assertEqual(t["like_for_like_km_tol"], 0.15)

    def test_missing_windows_json_is_not_an_error(self):
        with tempfile.TemporaryDirectory() as d:
            t = g._apply_windows_config(d)
        self.assertEqual(t["band_level"][1], round(1.0 + t["tol"] + t["slack_level"], 3))

    def test_broken_windows_json_fails_loud(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "windows.json"), "w") as f:
                f.write("{not json")
            with self.assertRaises(SystemExit):
                g._apply_windows_config(d)

    def test_no_absolute_count_thresholds(self):
        """#589: outlier tolerance is a SHARE of the trip set, not a count."""
        self.assertNotIn("dist_outliers_max", g.THRESHOLDS)
        self.assertIn("dist_outliers_frac", g.THRESHOLDS)
        self.assertLessEqual(g.THRESHOLDS["dist_outliers_frac"], 0.08)
        self.assertNotIn("lopsided_scaling_max", g.THRESHOLDS)
        self.assertIn("lopsided_scaling_warn", g.THRESHOLDS)


class RefsResolution(unittest.TestCase):
    """#589: no default path, no crash at argparse time, a clear failure."""

    def setUp(self):
        self.saved = g.REFS_DIR

    def tearDown(self):
        g.REFS_DIR = self.saved

    def test_unset_raises_named_error(self):
        g.REFS_DIR = None
        with self.assertRaises(g.RefsUnavailable) as ctx:
            g.refs_path(g.DEFAULT_TRIPS)
        self.assertIn("BUTTERFLY_REFS_DIR", str(ctx.exception))

    def test_missing_directory_raises_named_error(self):
        g.REFS_DIR = "/nonexistent/reference-trips-for-test"
        with self.assertRaises(g.RefsUnavailable) as ctx:
            g.refs_path(g.DEFAULT_TRIPS)
        self.assertIn("BUTTERFLY_REFS_DIR", str(ctx.exception))

    def test_refs_unavailable_is_catchable_not_systemexit(self):
        """main() turns it into ONE gate's FAIL line; SystemExit would kill the run."""
        self.assertTrue(issubclass(g.RefsUnavailable, Exception))
        self.assertFalse(issubclass(g.RefsUnavailable, SystemExit))

    def test_override_wins_without_touching_the_env(self):
        g.REFS_DIR = None
        self.assertEqual(g.refs_path(g.DEFAULT_TRIPS, "/tmp/mine.csv"), "/tmp/mine.csv")

    def test_join_under_refs_dir(self):
        with tempfile.TemporaryDirectory() as d:
            g.REFS_DIR = d
            self.assertEqual(g.refs_path("od_typical.csv"), os.path.join(d, "od_typical.csv"))

    def test_no_module_level_default_path(self):
        for value in (g.DEFAULT_TRIPS, g.LEGACY_TRIPS_DISTANCE, g.REFS_PREFIX):
            self.assertFalse(os.path.isabs(value), f"{value} must be a bare name")


class Geometry(unittest.TestCase):
    SQUARE_CCW = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]

    def test_ring_area2_orientation(self):
        self.assertGreater(g.ring_area2(self.SQUARE_CCW), 0)
        self.assertLess(g.ring_area2(list(reversed(self.SQUARE_CCW))), 0)

    def test_point_in_ring_is_closure_tolerant(self):
        """#589: the docstring used to claim the ring must be OPEN."""
        closed = self.SQUARE_CCW + [self.SQUARE_CCW[0]]
        for pt in ((0.5, 0.5), (1.5, 0.5), (-0.1, 0.5), (0.5, 2.0)):
            self.assertEqual(g.point_in_ring(pt, self.SQUARE_CCW),
                             g.point_in_ring(pt, closed), f"pt={pt}")
        self.assertTrue(g.point_in_ring((0.5, 0.5), closed))
        self.assertFalse(g.point_in_ring((1.5, 0.5), closed))

    def test_polyline6_ring_contract_helpers(self):
        """The exact expression gate_isochrone_topology asserts (#589)."""
        r = self.SQUARE_CCW + [self.SQUARE_CCW[0]]
        self.assertEqual(r[0], r[-1])
        self.assertGreater(g.ring_area2(r[:-1]), 0)
        cw = list(reversed(self.SQUARE_CCW))
        self.assertLess(g.ring_area2(cw), 0)

    def test_outlier_frac(self):
        n, frac = g.outlier_frac([1.0, 1.0, 0.5, 1.3])
        self.assertEqual(n, 2)
        self.assertAlmostEqual(frac, 0.5)
        self.assertEqual(g.outlier_frac([]), (0, 0.0))


class Registry(unittest.TestCase):
    def test_gate_names_needs_no_env_and_no_server(self):
        names = g.gate_names()
        self.assertGreater(len(names), 20)
        self.assertEqual(len(names), len(set(names)), "duplicate gate name")

    def test_every_ticket_delegates_to_a_registered_gate(self):
        """#572 fold guard — the same assertion gate_ticket_invariants makes."""
        names = set(g.gate_names())
        for ticket, gates in g.TICKET_GATES.items():
            self.assertIn(ticket, g.TICKET_NOTES, f"{ticket} has no note")
            for name in gates:
                self.assertIn(name, names, f"{ticket} delegates to unregistered gate {name}")

    def test_folded_gates_are_gone(self):
        """#572: isochrone containment and matrix completeness were absorbed."""
        names = set(g.gate_names())
        self.assertNotIn("isochrone_containment", names)
        self.assertNotIn("matrix_completeness", names)
        self.assertIn("isochrone_topology", names)
        self.assertIn("flight_completeness", names)

    def test_quick_drops_only_the_reference_trip_gates(self):
        import argparse
        full = set(g.gate_names())
        quick_args = argparse.Namespace(**dict(g.GATE_ARGS_DEFAULTS, quick=True))
        quick = set(g.gate_names(quick_args))
        self.assertEqual(full - quick, {"ground_truth_duration", "ground_truth_distance"})


class RestProbeParity(unittest.TestCase):
    """#572: the probe table must cover exactly the router's mounted paths.
    Offline mirror of the live `openapi paths == probe table` drift alarm."""

    def mounted_paths(self):
        src = os.path.join(REPO, "route", "src", "server", "api.rs")
        if not os.path.exists(src):
            self.skipTest(f"{src} not present")
        with open(src) as f:
            text = f.read()
        m = re.search(r"MOUNTED_PATHS:\s*&\[&str\]\s*=\s*&\[(.*?)\];", text, re.S)
        self.assertIsNotNone(m, "MOUNTED_PATHS not found in api.rs")
        return set(re.findall(r'"([^"]+)"', m.group(1)))

    def test_probe_table_matches_mounted_paths(self):
        self.assertEqual(set(g.rest_probes()), self.mounted_paths())

    def test_probe_specs_are_well_formed(self):
        for path, (method, target, body) in g.rest_probes().items():
            self.assertIn(method, ("GET", "POST"), path)
            self.assertTrue(target.startswith(path), f"{path}: probe targets {target}")
            if method == "GET":
                self.assertIsNone(body, f"{path}: GET probe must not carry a body")
            else:
                self.assertIsInstance(body, dict, f"{path}: POST probe needs a JSON body")
                json.dumps(body)  # must be serialisable

    def test_only_documented_optional_surfaces_may_skip(self):
        probes = g.rest_probes()
        for path, skips in g.REST_PROBE_SKIPS.items():
            self.assertIn(path, probes, f"skip rule for unprobed path {path}")
            for status, reason in skips.items():
                self.assertIn(status, (404, 503), f"{path}: {status} is not an optional-surface status")
                self.assertTrue(reason.strip(), f"{path}: skip {status} needs a reason")


class FakeBatch:
    def __init__(self, cols):
        self.cols = cols
        self.num_rows = len(next(iter(cols.values()))) if cols else 0

    def column(self, name):
        class _C:
            def __init__(self, v):
                self.v = v

            def to_pylist(self):
                return list(self.v)
        return _C(self.cols[name])


class FakeChunk:
    def __init__(self, data=None, app_metadata=None):
        self.data = data
        self.app_metadata = app_metadata


class StreamedMatrixMemo(unittest.TestCase):
    """#572: ONE decoded pass per (base, mode, params); a FAILURE is memoised
    too and re-raised, so the second consumer still fails."""

    def setUp(self):
        self.saved = g.flight_reader
        g._STREAMED_MATRIX.clear()
        self.calls = []

    def tearDown(self):
        g.flight_reader = self.saved
        g._STREAMED_MATRIX.clear()

    def params(self):
        pts = [[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]]
        return {"origins": pts, "destinations": pts, "radius_km": 6, "sparse": True}

    def test_single_pass_shared_by_consumers(self):
        batch = FakeBatch({"duration_ms": [100, g.MAX_U32, 300],
                           "distance_m": [10, 20, g.MAX_U32],
                           "source_idx": [0, 1, 2], "target_idx": [1, 2, 0]})
        trailer = json.dumps({"complete": True, "total_rows": 3, "contract": "sparse"}).encode()

        def fake(base, action, mode, params):
            self.calls.append((base, action, mode))
            return [FakeChunk(batch), FakeChunk(FakeBatch({"duration_ms": [], "distance_m": [],
                                                           "source_idx": [], "target_idx": []})),
                    FakeChunk(None, trailer)]

        g.flight_reader = fake
        rec = g.streamed_matrix("http://x", "car", self.params())
        again = g.streamed_matrix("http://x", "car", self.params())
        self.assertIs(rec, again)
        self.assertEqual(len(self.calls), 1, "the 1.1M-cell pass must run ONCE")
        self.assertEqual(rec.batches, 2)
        self.assertEqual(rec.empties, 1)
        self.assertEqual(rec.rows, 3)
        self.assertEqual(rec.sentinels, 1)
        self.assertEqual(rec.dist_max, 1)
        self.assertEqual(rec.dist_sample, (2, 0))
        self.assertEqual(rec.cells_total, 9)
        self.assertEqual(rec.trailer["contract"], "sparse")

    def test_other_mode_is_a_separate_pass(self):
        def fake(base, action, mode, params):
            self.calls.append(mode)
            return [FakeChunk(FakeBatch({"duration_ms": [1], "distance_m": [1],
                                         "source_idx": [0], "target_idx": [0]}))]

        g.flight_reader = fake
        g.streamed_matrix("http://x", "car", self.params())
        g.streamed_matrix("http://x", "foot", self.params())
        g.streamed_matrix("http://x", "car", self.params())
        self.assertEqual(self.calls, ["car", "foot"])

    def test_failure_is_memoised_and_reraised_to_every_consumer(self):
        def boom(base, action, mode, params):
            self.calls.append(mode)
            raise RuntimeError("flight down")

        g.flight_reader = boom
        for _ in range(3):
            with self.assertRaises(RuntimeError):
                g.streamed_matrix("http://x", "car", self.params())
        self.assertEqual(len(self.calls), 1, "a memoised failure must not be retried")


class StreamingShape(unittest.TestCase):
    def test_grid_exceeds_the_bucket_threshold(self):
        grid = g._streaming_grid()
        self.assertEqual(len(grid), 1054)
        self.assertGreater(len(grid) ** 2, 1_000_000)

    def test_streaming_params_are_stable_and_json_keyable(self):
        a, b = g.streaming_params(), g.streaming_params()
        self.assertEqual(json.dumps(a, sort_keys=True), json.dumps(b, sort_keys=True))
        self.assertTrue(a["sparse"])
        self.assertEqual(len(a["origins"]), len(a["destinations"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
