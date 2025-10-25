use geo::HaversineDistance;
use geo::Point;
use std::collections::HashMap;

pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let p1 = Point::new(lon1, lat1);
    let p2 = Point::new(lon2, lat2);
    p1.haversine_distance(&p2)
}

pub fn nearest_node(
    target: (f64, f64),
    nodes: &HashMap<i64, (f64, f64)>,
) -> Option<i64> {
    nodes
        .iter()
        .min_by(|(_, coord1), (_, coord2)| {
            let dist1 = haversine_distance(target.0, target.1, coord1.0, coord1.1);
            let dist2 = haversine_distance(target.0, target.1, coord2.0, coord2.1);
            dist1.partial_cmp(&dist2).unwrap()
        })
        .map(|(id, _)| *id)
}
