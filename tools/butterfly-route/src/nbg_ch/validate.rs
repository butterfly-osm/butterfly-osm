//! Validation of NBG CH correctness
//!
//! Compares NBG CH distances against Dijkstra on the original graph.

use std::collections::BinaryHeap;
use std::cmp::Reverse;

use crate::formats::{NbgCsr, NbgGeo};
use super::{NbgChTopo, NbgBucketM2M};

/// Run Dijkstra on original NBG graph (ground truth)
pub fn dijkstra_nbg(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    source: u32,
    target: u32,
) -> u32 {
    let n_nodes = nbg_csr.n_nodes as usize;
    let mut dist = vec![u32::MAX; n_nodes];
    let mut heap: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();

    dist[source as usize] = 0;
    heap.push(Reverse((0, source)));

    while let Some(Reverse((d, u))) = heap.pop() {
        if u == target {
            return d;
        }

        if d > dist[u as usize] {
            continue;
        }

        let start = nbg_csr.offsets[u as usize] as usize;
        let end = nbg_csr.offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = nbg_csr.heads[i];
            let edge_idx = nbg_csr.edge_idx[i] as usize;
            let w = nbg_geo.edges[edge_idx].length_mm;

            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                heap.push(Reverse((new_dist, v)));
            }
        }
    }

    dist[target as usize]
}

/// Validate NBG CH against Dijkstra
pub fn validate_nbg_ch(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    topo: &NbgChTopo,
    n_tests: usize,
    seed: u64,
) -> ValidationResult {
    use rand::prelude::*;
    use rand::SeedableRng;

    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let n_nodes = topo.n_nodes;

    let engine = NbgBucketM2M::new(topo);

    let mut correct = 0;
    let mut incorrect = 0;
    let mut unreachable_both = 0;
    let mut errors: Vec<ValidationError> = Vec::new();

    println!("Validating {} random queries...", n_tests);

    for i in 0..n_tests {
        let source = rng.random_range(0..n_nodes);
        let target = rng.random_range(0..n_nodes);

        // Ground truth: Dijkstra
        let dijkstra_dist = dijkstra_nbg(nbg_csr, nbg_geo, source, target);

        // NBG CH: bucket M2M with single source/target
        let (matrix, _) = engine.compute(&[source], &[target]);
        let ch_dist = matrix[0];

        if dijkstra_dist == u32::MAX && ch_dist == u32::MAX {
            unreachable_both += 1;
            correct += 1;
        } else if dijkstra_dist == ch_dist {
            correct += 1;
        } else {
            incorrect += 1;
            if errors.len() < 10 {
                errors.push(ValidationError {
                    source,
                    target,
                    dijkstra_dist,
                    ch_dist,
                });
            }
        }

        if (i + 1) % 100 == 0 {
            println!("  {}/{} queries, {} correct, {} incorrect",
                     i + 1, n_tests, correct, incorrect);
        }
    }

    ValidationResult {
        n_tests,
        correct,
        incorrect,
        unreachable_both,
        errors,
    }
}

/// Validate matrix computation
pub fn validate_matrix(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    topo: &NbgChTopo,
    matrix_size: usize,
    seed: u64,
) -> ValidationResult {
    use rand::prelude::*;
    use rand::SeedableRng;

    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let n_nodes = topo.n_nodes;

    // Generate random sources and targets
    let sources: Vec<u32> = (0..matrix_size).map(|_| rng.random_range(0..n_nodes)).collect();
    let targets: Vec<u32> = (0..matrix_size).map(|_| rng.random_range(0..n_nodes)).collect();

    println!("Validating {}x{} matrix...", matrix_size, matrix_size);

    // Compute with NBG CH
    let engine = NbgBucketM2M::new(topo);
    let (ch_matrix, _) = engine.compute(&sources, &targets);

    // Verify each entry against Dijkstra
    let mut correct = 0;
    let mut incorrect = 0;
    let mut unreachable_both = 0;
    let mut errors: Vec<ValidationError> = Vec::new();

    for (i, &source) in sources.iter().enumerate() {
        for (j, &target) in targets.iter().enumerate() {
            let dijkstra_dist = dijkstra_nbg(nbg_csr, nbg_geo, source, target);
            let ch_dist = ch_matrix[i * matrix_size + j];

            if dijkstra_dist == u32::MAX && ch_dist == u32::MAX {
                unreachable_both += 1;
                correct += 1;
            } else if dijkstra_dist == ch_dist {
                correct += 1;
            } else {
                incorrect += 1;
                if errors.len() < 10 {
                    errors.push(ValidationError {
                        source,
                        target,
                        dijkstra_dist,
                        ch_dist,
                    });
                }
            }
        }

        if (i + 1) % 10 == 0 {
            println!("  Row {}/{} complete, {} correct, {} incorrect",
                     i + 1, matrix_size, correct, incorrect);
        }
    }

    ValidationResult {
        n_tests: matrix_size * matrix_size,
        correct,
        incorrect,
        unreachable_both,
        errors,
    }
}

#[derive(Debug)]
pub struct ValidationResult {
    pub n_tests: usize,
    pub correct: usize,
    pub incorrect: usize,
    pub unreachable_both: usize,
    pub errors: Vec<ValidationError>,
}

impl ValidationResult {
    pub fn print(&self) {
        println!("\n=== VALIDATION RESULTS ===");
        println!("  Total tests:     {}", self.n_tests);
        println!("  Correct:         {} ({:.2}%)",
                 self.correct, self.correct as f64 * 100.0 / self.n_tests as f64);
        println!("  Incorrect:       {} ({:.2}%)",
                 self.incorrect, self.incorrect as f64 * 100.0 / self.n_tests as f64);
        println!("  Unreachable:     {}", self.unreachable_both);

        if !self.errors.is_empty() {
            println!("\n  Sample errors:");
            for err in &self.errors {
                println!("    {} → {}: Dijkstra={}, CH={}",
                         err.source, err.target, err.dijkstra_dist, err.ch_dist);
            }
        }

        if self.incorrect == 0 {
            println!("\n  ✅ ALL TESTS PASSED!");
        } else {
            println!("\n  ❌ VALIDATION FAILED!");
        }
    }

    pub fn is_valid(&self) -> bool {
        self.incorrect == 0
    }
}

#[derive(Debug)]
pub struct ValidationError {
    pub source: u32,
    pub target: u32,
    pub dijkstra_dist: u32,
    pub ch_dist: u32,
}
