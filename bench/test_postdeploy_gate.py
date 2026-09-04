#!/usr/bin/env python3
"""Offline unit checks for `bench/postdeploy_gate.py`.

The gate itself can only be judged against a live server. Everything it does
BEFORE talking to one — threshold derivation, reference-directory resolution,
the geometry primitives — is pure Python and is checked here, with NO
environment variables set and no network:

    python3 bench/test_postdeploy_gate.py
    python3 -m unittest bench/test_postdeploy_gate.py

Everything here runs in milliseconds and is the guard the ticket asks for:
change a threshold literal, a derivation or the reference-directory contract
and one of these fails before a deploy ever sees it.
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import postdeploy_gate as g  # noqa: E402


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



if __name__ == "__main__":
    unittest.main(verbosity=2)
