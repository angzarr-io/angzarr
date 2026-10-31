//! A seeded, platform-stable random stream for the property tests: the
//! round-trip and edit tests draw their cases from it, so a failure names
//! its seed and replays exactly.

/// SplitMix64: small, fast, dependency-free, and identical on every
/// platform.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// The stream `owner` draws from under `seed`. The owner is mixed in so
    /// streams stay well separated even for small seeds and ids.
    pub fn new(seed: u64, owner: u64) -> Rng {
        let s = seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(owner.wrapping_mul(0xBF58_476D_1CE4_E5B9));
        Rng { state: s }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`; 0 when `n` is 0 or 1. Rejection-sampled, so it is
    /// unbiased and integer.
    pub fn below(&mut self, n: u32) -> u32 {
        if n <= 1 {
            return 0;
        }
        let n64 = n as u64;
        let limit = u64::MAX - (u64::MAX % n64);
        loop {
            let v = self.next_u64();
            if v < limit {
                return (v % n64) as u32;
            }
        }
    }
}
