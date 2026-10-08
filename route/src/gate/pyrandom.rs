//! CPython's `random.Random`, bit for bit: MT19937 seeded with
//! `init_by_array`, `random()` from two draws (53 bits), `getrandbits`,
//! `_randbelow` with rejection, `uniform`, `shuffle` and `sample`.
//!
//! The gate's sampling checks seed `random.Random(n)` so that a run is
//! reproducible; the Rust gate draws the SAME points so its PASS/FAIL lines
//! (counts, worst ratios, sampled cells) are comparable one to one with the
//! Python gate's during the parity phase (#646). Only the pieces the gate
//! uses are implemented; `sample` covers the set-based branch CPython takes
//! for the gate's sizes (k ≤ setsize) and the pool branch for the rest.

const N: usize = 624;
const M: usize = 397;
const MATRIX_A: u32 = 0x9908_b0df;
const UPPER_MASK: u32 = 0x8000_0000;
const LOWER_MASK: u32 = 0x7fff_ffff;

pub struct PyRandom {
    mt: [u32; N],
    index: usize,
}

impl PyRandom {
    /// `random.Random(seed)` for a non-negative integer seed that fits in
    /// 32 bits (every seed the gate uses).
    pub fn new(seed: u32) -> Self {
        let mut r = PyRandom {
            mt: [0; N],
            index: N,
        };
        r.init_genrand(19_650_218);
        r.init_by_array(&[seed]);
        r
    }

    fn init_genrand(&mut self, s: u32) {
        self.mt[0] = s;
        for i in 1..N {
            self.mt[i] = 1_812_433_253u32
                .wrapping_mul(self.mt[i - 1] ^ (self.mt[i - 1] >> 30))
                .wrapping_add(i as u32);
        }
        self.index = N;
    }

    fn init_by_array(&mut self, key: &[u32]) {
        let mut i = 1usize;
        let mut j = 0usize;
        let k = N.max(key.len());
        for _ in 0..k {
            self.mt[i] = (self.mt[i]
                ^ ((self.mt[i - 1] ^ (self.mt[i - 1] >> 30)).wrapping_mul(1_664_525)))
            .wrapping_add(key[j])
            .wrapping_add(j as u32);
            i += 1;
            j += 1;
            if i >= N {
                self.mt[0] = self.mt[N - 1];
                i = 1;
            }
            if j >= key.len() {
                j = 0;
            }
        }
        for _ in 0..N - 1 {
            self.mt[i] = (self.mt[i]
                ^ ((self.mt[i - 1] ^ (self.mt[i - 1] >> 30)).wrapping_mul(1_566_083_941)))
            .wrapping_sub(i as u32);
            i += 1;
            if i >= N {
                self.mt[0] = self.mt[N - 1];
                i = 1;
            }
        }
        self.mt[0] = 0x8000_0000;
        self.index = N;
    }

    fn generate(&mut self) {
        for kk in 0..N {
            let y = (self.mt[kk] & UPPER_MASK) | (self.mt[(kk + 1) % N] & LOWER_MASK);
            let mut v = self.mt[(kk + M) % N] ^ (y >> 1);
            if y & 1 == 1 {
                v ^= MATRIX_A;
            }
            self.mt[kk] = v;
        }
        self.index = 0;
    }

    /// `genrand_uint32`.
    pub fn next_u32(&mut self) -> u32 {
        if self.index >= N {
            self.generate();
        }
        let mut y = self.mt[self.index];
        self.index += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^= y >> 18;
        y
    }

    /// `random()`: 53-bit float in [0, 1).
    pub fn random(&mut self) -> f64 {
        let a = (self.next_u32() >> 5) as f64;
        let b = (self.next_u32() >> 6) as f64;
        (a * 67_108_864.0 + b) * (1.0 / 9_007_199_254_740_992.0)
    }

    /// `uniform(a, b)` = `a + (b - a) * random()`.
    pub fn uniform(&mut self, a: f64, b: f64) -> f64 {
        a + (b - a) * self.random()
    }

    /// `getrandbits(k)` for 0 < k ≤ 32.
    fn getrandbits(&mut self, k: u32) -> u32 {
        debug_assert!(k > 0 && k <= 32);
        self.next_u32() >> (32 - k)
    }

    /// `_randbelow(n)` (the `getrandbits` rejection loop CPython uses).
    pub fn randbelow(&mut self, n: usize) -> usize {
        debug_assert!(n > 0 && n <= u32::MAX as usize);
        let k = usize::BITS - n.leading_zeros();
        loop {
            let r = self.getrandbits(k) as usize;
            if r < n {
                return r;
            }
        }
    }

    /// `shuffle(x)`: Fisher–Yates from the end, `randbelow(i + 1)`.
    pub fn shuffle<T>(&mut self, x: &mut [T]) {
        for i in (1..x.len()).rev() {
            let j = self.randbelow(i + 1);
            x.swap(i, j);
        }
    }

    /// `sample(range(n), k)`: the indices CPython returns.
    pub fn sample_range(&mut self, n: usize, k: usize) -> Vec<usize> {
        assert!(k <= n, "sample larger than population");
        let mut setsize = 21usize;
        if k > 5 {
            // setsize += 4 ** _ceil(_log(k * 3, 4))
            let p = ((k * 3) as f64).ln() / 4f64.ln();
            setsize += 4usize.pow(p.ceil() as u32);
        }
        let mut result = Vec::with_capacity(k);
        if n <= setsize {
            let mut pool: Vec<usize> = (0..n).collect();
            for i in 0..k {
                let j = self.randbelow(n - i);
                result.push(pool[j]);
                pool[j] = pool[n - i - 1];
            }
        } else {
            let mut selected = std::collections::HashSet::with_capacity(k);
            for _ in 0..k {
                let mut j = self.randbelow(n);
                while selected.contains(&j) {
                    j = self.randbelow(n);
                }
                selected.insert(j);
                result.push(j);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference values from CPython 3.14 on this box:
    /// `random.Random(99).random()`, `random.Random(7).uniform(3.5, 5.8)`,
    /// `random.Random(31).sample(range(800), 25)`, a seeded `shuffle`, and
    /// `round(random.Random(602).uniform(3.4, 5.4), 6)`.
    #[test]
    fn matches_cpython_streams() {
        let mut r = PyRandom::new(99);
        assert_eq!(r.random(), 0.403_978_074_943_666_33);
        let mut r7 = PyRandom::new(7);
        assert_eq!(r7.uniform(3.5, 5.8), 4.244_815_359_116_274);
        let mut r602 = PyRandom::new(602);
        assert_eq!(
            super::super::geom::round_to(r602.uniform(3.4, 5.4), 6),
            4.202_557
        );
    }

    #[test]
    fn randbelow_sample_and_shuffle_match_cpython() {
        let mut r = PyRandom::new(31);
        assert_eq!(
            r.sample_range(800, 25),
            vec![
                12, 480, 115, 782, 402, 144, 700, 44, 142, 548, 237, 728, 775, 150, 758, 33, 678,
                62, 139, 236, 749, 458, 538, 422, 209
            ]
        );
        let mut r7 = PyRandom::new(7);
        let mut x: Vec<usize> = (0..10).collect();
        r7.shuffle(&mut x);
        assert_eq!(x, vec![8, 3, 1, 4, 7, 0, 9, 6, 2, 5]);
    }
}
