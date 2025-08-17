//! Loser-tree implementation for efficient k-way merge with low branch misprediction
//!
//! Robust index-only design with:
//! - next power-of-two `base` for the implicit tree,
//! - sentinel index S (= k) that behaves as +∞,
//! - bottom-up initialization that fills losers correctly,
//! - O(log k) adjust for updates,
//! - stable tie-breaking by run index.

use std::cmp::Ordering;

/// Entry containing a value and its source run index
#[derive(Debug, Clone)]
pub struct LoserTreeEntry<T> {
    pub value: T,
    pub run_index: usize,
}

impl<T: Ord> PartialEq for LoserTreeEntry<T> {
    fn eq(&self, other: &Self) -> bool { self.value == other.value }
}
impl<T: Ord> Eq for LoserTreeEntry<T> {}
impl<T: Ord> PartialOrd for LoserTreeEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl<T: Ord> Ord for LoserTreeEntry<T> {
    fn cmp(&self, other: &Self) -> Ordering { self.value.cmp(&other.value) }
}

pub struct LoserTree<T> {
    /// Current head values: keys[0..k] are runs, keys[k] is the +∞ sentinel (None)
    keys: Vec<Option<LoserTreeEntry<T>>>,
    /// Loser indices at internal nodes; 1-based nodes map into [1..base-1]
    loser: Vec<usize>,
    /// Current global winner index (S if empty)
    winner: usize,
    /// Real number of runs
    k: usize,
    /// Next power-of-two ≥ k (leaf-base in implicit tree)
    base: usize,
    /// Sentinel index (always equals k)
    sentinel: usize,
}

impl<T: Ord> LoserTree<T> {
    #[inline]
    fn next_pow2(mut x: usize) -> usize {
        if x <= 1 { return 1; }
        x -= 1;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        #[cfg(target_pointer_width = "64")]
        { x |= x >> 32; }
        x + 1
    }

    /// Create a new loser tree for `k` runs.
    pub fn new(k: usize) -> Self {
        assert!(k > 0, "loser tree requires k >= 1");
        let base = Self::next_pow2(k);
        let sentinel = k;

        let mut keys = Vec::with_capacity(k + 1);
        for _ in 0..k { keys.push(None); }
        keys.push(None); // keys[sentinel] = None → +∞

        // loser is indexed by 1..base-1 (we'll size it to base and ignore index 0)
        let mut loser = vec![sentinel; base];
        loser[1..base].fill(sentinel);

        Self { keys, loser, winner: sentinel, k, base, sentinel }
    }

    /// Comparator: true if i < j (min tournament), stable on ties via index
    #[inline]
    fn less(&self, i: usize, j: usize) -> bool {
        match (&self.keys[i], &self.keys[j]) {
            (Some(_), None)    => true,   // real < INF
            (None, Some(_))    => false,  // INF !< real
            (None, None)       => false,  // INF !< INF
            (Some(a), Some(b)) => a.value < b.value || (a.value == b.value && i < j),
        }
    }

    /// Bottom-up build of the tournament (correct for non power-of-two k)
    pub fn initialize(&mut self, initial: Vec<Option<LoserTreeEntry<T>>>) {
        assert_eq!(initial.len(), self.k);
        for (i, v) in initial.into_iter().enumerate() { self.keys[i] = v; }
        self.keys[self.sentinel] = None; // +∞

        // Temporary array to propagate winners bottom-up; leaves at [base .. base+base-1]
        let total_nodes = self.base * 2;
        let mut node = vec![self.sentinel; total_nodes];

        // Place leaves: real runs in [base .. base+k-1], padded leaves as sentinel
        for i in 0..self.base {
            node[self.base + i] = if i < self.k { i } else { self.sentinel };
        }

        // Build internal nodes: pick winner (min), record loser at this node
        for p in (1..self.base).rev() {
            let left = node[p << 1];
            let right = node[(p << 1) + 1];
            if self.less(left, right) {
                // left wins up, right loses here
                self.loser[p] = right;
                node[p] = left;
            } else {
                // right wins up, left loses here
                self.loser[p] = left;
                node[p] = right;
            }
        }

        // Global winner at the root
        self.winner = node[1];
    }

    /// Replay along the path from leaf `s` using Knuth's loser-tree adjust.
    #[inline]
    fn adjust(&mut self, mut s: usize) {
        // climb using the padded base
        let mut p = (s + self.base) >> 1;
        while p > 0 {
            let j = self.loser[p];
            // if j < s, j wins upward; store s as the loser at this node
            if self.less(j, s) {
                self.loser[p] = s;
                s = j;
            }
            // else s wins upward; loser[p] stays j
            p >>= 1;
        }
        self.winner = s;
    }

    /// Peek current minimum
    #[inline]
    pub fn peek_min(&self) -> Option<&LoserTreeEntry<T>> {
        self.keys.get(self.winner)?.as_ref()
    }

    /// Pop current minimum (winner index + entry)
    #[inline]
    pub fn pop_min(&mut self) -> Option<(usize, LoserTreeEntry<T>)> {
        let w = self.winner;
        let v = self.keys[w].take()?; // None => empty
        Some((w, v))
    }

    /// Replace the head value for a specific run and replay.
    #[inline]
    pub fn replace(&mut self, run_index: usize, new_entry: Option<LoserTreeEntry<T>>) {
        debug_assert!(run_index < self.k);
        self.keys[run_index] = new_entry;
        self.adjust(run_index);
    }

    /// Extract the minimum and replace it at the **winner** run.
    #[inline]
    pub fn extract_min_and_replace_winner(&mut self, new_entry: Option<LoserTreeEntry<T>>) -> Option<LoserTreeEntry<T>> {
        let w = self.winner;
        let old = self.keys[w].take()?;   // take winner
        self.keys[w] = new_entry;         // install replacement at same run
        self.adjust(w);                   // replay
        Some(old)
    }

    /// Back-compat helper if caller knows the run equals the winner (debug-checked).
    #[inline]
    pub fn extract_min_and_replace(&mut self, run_index: usize, new_entry: Option<LoserTreeEntry<T>>) -> Option<LoserTreeEntry<T>> {
        debug_assert_eq!(run_index, self.winner, "extract_min_and_replace: run_index must equal current winner");
        self.extract_min_and_replace_winner(new_entry)
    }

    #[inline]
    pub fn is_empty(&self) -> bool { self.peek_min().is_none() }

    #[inline]
    pub fn active_runs(&self) -> usize { self.keys[..self.k].iter().filter(|x| x.is_some()).count() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loser_tree_basic() {
        let mut tree = LoserTree::new(3);
        tree.initialize(vec![
            Some(LoserTreeEntry { value: 5, run_index: 0 }),
            Some(LoserTreeEntry { value: 3, run_index: 1 }),
            Some(LoserTreeEntry { value: 7, run_index: 2 }),
        ]);
        let min = tree.peek_min().unwrap();
        assert_eq!(min.value, 3);
        assert_eq!(min.run_index, 1);
    }

    #[test]
    fn test_loser_tree_extract_and_replace() {
        let mut tree = LoserTree::new(3);
        tree.initialize(vec![
            Some(LoserTreeEntry { value: 5, run_index: 0 }),
            Some(LoserTreeEntry { value: 3, run_index: 1 }),
            Some(LoserTreeEntry { value: 7, run_index: 2 }),
        ]);
        let extracted = tree.extract_min_and_replace(1, Some(LoserTreeEntry { value: 4, run_index: 1 }));
        assert_eq!(extracted.unwrap().value, 3);
        let min = tree.peek_min().unwrap();
        assert_eq!(min.value, 4);
        assert_eq!(min.run_index, 1);
    }

    #[test]
    fn test_loser_tree_empty_runs() {
        let mut tree = LoserTree::new(3);
        tree.initialize(vec![
            Some(LoserTreeEntry { value: 5, run_index: 0 }),
            None,
            Some(LoserTreeEntry { value: 7, run_index: 2 }),
        ]);
        let min = tree.peek_min().unwrap();
        assert_eq!(min.value, 5);

        let _ = tree.extract_min_and_replace(0, None).unwrap();
        let min = tree.peek_min().unwrap();
        assert_eq!(min.value, 7);
        assert_eq!(min.run_index, 2);
    }

    #[test]
    fn test_loser_tree_all_empty() {
        let mut tree: LoserTree<i32> = LoserTree::new(2);
        tree.initialize(vec![None, None]);
        assert!(tree.is_empty());
        assert_eq!(tree.active_runs(), 0);
    }

    #[test]
    fn test_loser_tree_sequence() {
        let mut tree = LoserTree::new(2);
        tree.initialize(vec![
            Some(LoserTreeEntry { value: 1, run_index: 0 }),
            Some(LoserTreeEntry { value: 2, run_index: 1 }),
        ]);

        assert_eq!(tree.extract_min_and_replace(0, Some(LoserTreeEntry { value: 3, run_index: 0 })).unwrap().value, 1);
        assert_eq!(tree.extract_min_and_replace(1, Some(LoserTreeEntry { value: 4, run_index: 1 })).unwrap().value, 2);
        assert_eq!(tree.extract_min_and_replace(0, None).unwrap().value, 3);
        assert_eq!(tree.extract_min_and_replace(1, None).unwrap().value, 4);
        assert!(tree.is_empty());
    }

    #[test]
    fn test_stable_tie_breaking() {
        let mut tree = LoserTree::new(3);
        tree.initialize(vec![
            Some(LoserTreeEntry { value: 5, run_index: 0 }),
            Some(LoserTreeEntry { value: 5, run_index: 1 }),
            Some(LoserTreeEntry { value: 5, run_index: 2 }),
        ]);
        let min = tree.peek_min().unwrap();
        assert_eq!(min.value, 5);
        assert_eq!(min.run_index, 0); // lowest run index wins the tie
    }

    #[test]
    fn test_pop_replace_semantics() {
        let mut tree = LoserTree::new(2);
        tree.initialize(vec![
            Some(LoserTreeEntry { value: 1, run_index: 0 }),
            Some(LoserTreeEntry { value: 2, run_index: 1 }),
        ]);

        let (run_idx, entry) = tree.pop_min().unwrap();
        assert_eq!(entry.value, 1);
        assert_eq!(run_idx, 0);

        tree.replace(run_idx, Some(LoserTreeEntry { value: 3, run_index: run_idx }));
        let min = tree.peek_min().unwrap();
        assert_eq!(min.value, 2);
        assert_eq!(min.run_index, 1);
    }
}

