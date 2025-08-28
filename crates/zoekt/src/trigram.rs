//! Trigram extraction utilities (Zoekt-style)
//! In Zoekt, queries are compiled to trigrams to prefilter candidate files.

/// Extract lowercase ASCII trigrams from a haystack. Non-ASCII and whitespace
/// boundaries reset the window.
pub fn trigrams(hay: &str) -> impl Iterator<Item = [u8; 3]> + '_ {
    TrigramIter {
        bytes: hay.as_bytes(),
        i: 0,
        w: [0; 3],
        n: 0,
    }
}

struct TrigramIter<'a> {
    bytes: &'a [u8],
    i: usize,
    w: [u8; 3],
    n: usize,
}

impl<'a> Iterator for TrigramIter<'a> {
    type Item = [u8; 3];
    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.bytes.len() {
            let mut b = self.bytes[self.i];
            self.i += 1;
            // normalize: lowercase A-Z
            if b.is_ascii_uppercase() {
                b += 32;
            }
            // treat non-ascii alphanum as boundary
            let is_word = b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_';
            if !is_word {
                self.n = 0;
                continue;
            }
            // shift window
            if self.n < 3 {
                self.w[self.n] = b;
                self.n += 1;
                if self.n < 3 {
                    continue;
                }
            } else {
                self.w = [self.w[1], self.w[2], b];
            }
            return Some(self.w);
        }
        None
    }
}

/// Extract trigrams plus starting byte offset of each 3-byte window in the original string.
pub fn trigrams_with_pos(hay: &str) -> impl Iterator<Item = ([u8; 3], u32)> + '_ {
    TriPosIter {
        bytes: hay.as_bytes(),
        i: 0,
        w: [0; 3],
        posw: [0u32; 3],
        n: 0,
    }
}

struct TriPosIter<'a> {
    bytes: &'a [u8],
    i: usize,
    w: [u8; 3],
    posw: [u32; 3],
    n: usize,
}

impl<'a> Iterator for TriPosIter<'a> {
    type Item = ([u8; 3], u32);
    fn next(&mut self) -> Option<Self::Item> {
        while self.i < self.bytes.len() {
            let mut b = self.bytes[self.i];
            let pos = self.i as u32;
            self.i += 1;
            if b.is_ascii_uppercase() {
                b += 32;
            }
            let is_word = b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_';
            if !is_word {
                self.n = 0;
                continue;
            }
            if self.n < 3 {
                self.w[self.n] = b;
                self.posw[self.n] = pos;
                self.n += 1;
                if self.n < 3 {
                    continue;
                }
            } else {
                self.w = [self.w[1], self.w[2], b];
                self.posw = [self.posw[1], self.posw[2], pos];
            }
            return Some((self.w, self.posw[0]));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn basic_trigrams() {
        let v: Vec<[u8; 3]> = trigrams("Hello_Zoekt1").collect();
        // Expect some trigrams like "hel", "ell", ...
        assert!(v.contains(&*b"hel"))
    }
}
