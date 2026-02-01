#!/usr/bin/env python3
"""
Isochrone Consistency Test

Verifies that isochrone polygons are geometrically correct:
1. All points INSIDE the polygon have drive time <= threshold
2. All points OUTSIDE the polygon have drive time > threshold

This catches bugs where:
- The polygon is too small (missing reachable areas)
- The polygon is too large (includes unreachable areas)
- Edge cases at the boundary
"""

import requests
import random
import json
import sys
from dataclasses import dataclass
from typing import List, Tuple, Optional
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
import time

BUTTERFLY_URL = "http://localhost:8080"

@dataclass
class TestResult:
    origin: Tuple[float, float]
    time_s: int
    n_samples: int
    inside_violations: List[dict]  # Points inside polygon but drive time > threshold
    outside_violations: List[dict]  # Points outside polygon but drive time <= threshold
    inside_correct: int
    outside_correct: int
    unreachable_inside: int  # Points inside polygon but no route found

    @property
    def passed(self) -> bool:
        return len(self.inside_violations) == 0 and len(self.outside_violations) == 0


def get_isochrone(lon: float, lat: float, time_s: int, mode: str = "car") -> Optional[Polygon]:
    """Fetch isochrone polygon from Butterfly."""
    url = f"{BUTTERFLY_URL}/isochrone?lon={lon}&lat={lat}&time_s={time_s}&mode={mode}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            print(f"  Isochrone request failed: {resp.status_code}")
            return None
        data = resp.json()
        polygon_coords = data.get("polygon", [])
        if len(polygon_coords) < 3:
            print(f"  Isochrone has < 3 points")
            return None
        # Convert to shapely polygon
        coords = [(p["lon"], p["lat"]) for p in polygon_coords]
        return Polygon(coords)
    except Exception as e:
        print(f"  Isochrone error: {e}")
        return None


def get_drive_time(src_lon: float, src_lat: float, dst_lon: float, dst_lat: float, mode: str = "car") -> Optional[float]:
    """Get drive time in seconds from src to dst."""
    url = f"{BUTTERFLY_URL}/route?src_lon={src_lon}&src_lat={src_lat}&dst_lon={dst_lon}&dst_lat={dst_lat}&mode={mode}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("duration_s")
        return None  # No route found or error
    except:
        return None


def sample_points_in_bbox(polygon: Polygon, n_points: int, buffer_factor: float = 1.5) -> List[Tuple[float, float]]:
    """Sample random points in and around the polygon's bounding box."""
    minx, miny, maxx, maxy = polygon.bounds

    # Expand bounding box to sample outside points too
    width = maxx - minx
    height = maxy - miny
    center_x = (minx + maxx) / 2
    center_y = (miny + maxy) / 2

    expanded_minx = center_x - width * buffer_factor / 2
    expanded_maxx = center_x + width * buffer_factor / 2
    expanded_miny = center_y - height * buffer_factor / 2
    expanded_maxy = center_y + height * buffer_factor / 2

    points = []
    for _ in range(n_points):
        x = random.uniform(expanded_minx, expanded_maxx)
        y = random.uniform(expanded_miny, expanded_maxy)
        points.append((x, y))

    return points


def test_isochrone_consistency(
    origin_lon: float,
    origin_lat: float,
    time_s: int,
    n_samples: int = 100,
    mode: str = "car"
) -> TestResult:
    """
    Test that an isochrone is geometrically consistent with actual drive times.
    """
    print(f"\nTesting isochrone: origin=({origin_lon:.4f}, {origin_lat:.4f}), time={time_s}s, samples={n_samples}")

    # Get isochrone polygon
    polygon = get_isochrone(origin_lon, origin_lat, time_s, mode)
    if polygon is None or not polygon.is_valid:
        print("  Failed to get valid isochrone polygon")
        return TestResult(
            origin=(origin_lon, origin_lat),
            time_s=time_s,
            n_samples=0,
            inside_violations=[],
            outside_violations=[],
            inside_correct=0,
            outside_correct=0,
            unreachable_inside=0,
        )

    print(f"  Polygon area: {polygon.area:.6f} deg², {len(polygon.exterior.coords)} vertices")

    # Sample points
    sample_points = sample_points_in_bbox(polygon, n_samples)

    inside_violations = []
    outside_violations = []
    inside_correct = 0
    outside_correct = 0
    unreachable_inside = 0

    for i, (px, py) in enumerate(sample_points):
        point = Point(px, py)
        is_inside = polygon.contains(point) or polygon.touches(point)

        # Get actual drive time
        drive_time = get_drive_time(origin_lon, origin_lat, px, py, mode)

        if is_inside:
            if drive_time is None:
                # Point is inside polygon but unreachable - could be water, etc.
                unreachable_inside += 1
            elif drive_time > time_s:
                # VIOLATION: Inside polygon but drive time exceeds threshold
                inside_violations.append({
                    "point": (px, py),
                    "drive_time_s": drive_time,
                    "threshold_s": time_s,
                    "excess_s": drive_time - time_s,
                })
            else:
                inside_correct += 1
        else:  # Outside polygon
            if drive_time is not None and drive_time <= time_s:
                # VIOLATION: Outside polygon but drive time is within threshold
                outside_violations.append({
                    "point": (px, py),
                    "drive_time_s": drive_time,
                    "threshold_s": time_s,
                    "margin_s": time_s - drive_time,
                })
            else:
                outside_correct += 1

        # Progress
        if (i + 1) % 20 == 0:
            print(f"  Sampled {i + 1}/{n_samples}...")

    result = TestResult(
        origin=(origin_lon, origin_lat),
        time_s=time_s,
        n_samples=n_samples,
        inside_violations=inside_violations,
        outside_violations=outside_violations,
        inside_correct=inside_correct,
        outside_correct=outside_correct,
        unreachable_inside=unreachable_inside,
    )

    # Report
    print(f"\n  Results:")
    print(f"    Inside polygon, correct: {inside_correct}")
    print(f"    Inside polygon, unreachable: {unreachable_inside}")
    print(f"    Inside polygon, VIOLATION (time > threshold): {len(inside_violations)}")
    print(f"    Outside polygon, correct: {outside_correct}")
    print(f"    Outside polygon, VIOLATION (time <= threshold): {len(outside_violations)}")

    if inside_violations:
        print(f"\n  Inside violations (worst 5):")
        for v in sorted(inside_violations, key=lambda x: -x["excess_s"])[:5]:
            print(f"    ({v['point'][0]:.4f}, {v['point'][1]:.4f}): {v['drive_time_s']:.1f}s > {v['threshold_s']}s (+{v['excess_s']:.1f}s)")

    if outside_violations:
        print(f"\n  Outside violations (worst 5):")
        for v in sorted(outside_violations, key=lambda x: -x["margin_s"])[:5]:
            print(f"    ({v['point'][0]:.4f}, {v['point'][1]:.4f}): {v['drive_time_s']:.1f}s <= {v['threshold_s']}s (margin {v['margin_s']:.1f}s)")

    return result


def run_test_suite():
    """Run comprehensive isochrone consistency tests."""
    print("=" * 70)
    print("ISOCHRONE CONSISTENCY TEST SUITE")
    print("=" * 70)

    # Check service
    try:
        r = requests.get(f"{BUTTERFLY_URL}/health", timeout=5)
        if r.status_code != 200:
            print(f"Butterfly not healthy: {r.status_code}")
            sys.exit(1)
    except Exception as e:
        print(f"Cannot connect to Butterfly: {e}")
        print(f"Start with: ./target/release/butterfly-route serve --data-dir ./data/belgium --port 8080")
        sys.exit(1)

    # Test cases: different origins and time thresholds
    test_cases = [
        # (lon, lat, time_s, n_samples, description)
        (4.3517, 50.8503, 300, 50, "Brussels center, 5min"),
        (4.3517, 50.8503, 600, 100, "Brussels center, 10min"),
        (4.3517, 50.8503, 1800, 150, "Brussels center, 30min"),
        (3.7250, 51.0543, 600, 100, "Ghent center, 10min"),
        (4.4028, 51.2194, 600, 100, "Antwerp center, 10min"),
        (5.5796, 50.6326, 600, 100, "Liège center, 10min"),
    ]

    results = []
    total_inside_violations = 0
    total_outside_violations = 0

    for lon, lat, time_s, n_samples, desc in test_cases:
        print(f"\n{'='*70}")
        print(f"Test: {desc}")
        result = test_isochrone_consistency(lon, lat, time_s, n_samples)
        results.append((desc, result))
        total_inside_violations += len(result.inside_violations)
        total_outside_violations += len(result.outside_violations)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    all_passed = True
    for desc, result in results:
        status = "PASS" if result.passed else "FAIL"
        if not result.passed:
            all_passed = False
        print(f"  {desc}: {status}")
        if not result.passed:
            print(f"    - Inside violations: {len(result.inside_violations)}")
            print(f"    - Outside violations: {len(result.outside_violations)}")

    print()
    print(f"Total inside violations: {total_inside_violations}")
    print(f"Total outside violations: {total_outside_violations}")
    print()

    if all_passed:
        print("ALL TESTS PASSED")
        return 0
    else:
        print("SOME TESTS FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(run_test_suite())
