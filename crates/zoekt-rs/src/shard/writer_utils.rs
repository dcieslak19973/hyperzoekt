use std::io::{Read, Write};

// Varint helpers: simple LEB128-style unsigned varint for u32. Kept as
// `pub(crate)` so the `writer` submodule can call them after the split.
pub(crate) fn write_var_u32<W: Write>(w: &mut W, mut v: u32) -> anyhow::Result<()> {
    let mut buf = [0u8; 5];
    let mut i = 0;
    while v >= 0x80 {
        buf[i] = (v as u8 & 0x7F) | 0x80;
        v >>= 7;
        i += 1;
    }
    buf[i] = v as u8;
    i += 1;
    w.write_all(&buf[..i])?;
    Ok(())
}

pub(crate) fn read_var_u32<R: Read>(r: &mut R) -> anyhow::Result<u32> {
    let mut shift = 0u32;
    let mut val: u32 = 0;
    loop {
        let mut buf = [0u8; 1];
        r.read_exact(&mut buf)?;
        let b = buf[0];
        val |= ((b & 0x7F) as u32) << shift;
        if (b & 0x80) == 0 {
            break;
        }
        shift += 7;
    }
    Ok(val)
}

// Radix sort for u128 optimized for our 88-bit encoded keys (tri24|doc32|pos32).
pub(crate) fn radix_sort_u128(buf: &mut [u128]) {
    if buf.len() <= 1 {
        return;
    }
    let mut tmp: Vec<u128> = vec![0u128; buf.len()];
    const RADIX_BITS: usize = 16;
    const RADIX: usize = 1 << RADIX_BITS;
    let passes = 128 / RADIX_BITS;
    let n = buf.len();
    for pass in 0..passes {
        let shift = pass * RADIX_BITS;
        let mut counts = vec![0usize; RADIX];
        for &k in buf.iter().take(n) {
            let bucket = ((k >> shift) & ((1u128 << RADIX_BITS) - 1)) as usize;
            counts[bucket] += 1;
        }
        let mut sum = 0usize;
        for c in counts.iter_mut() {
            let v = *c;
            *c = sum;
            sum += v;
        }
        for &k in buf.iter().take(n) {
            let bucket = ((k >> shift) & ((1u128 << RADIX_BITS) - 1)) as usize;
            tmp[counts[bucket]] = k;
            counts[bucket] += 1;
        }
        // copy tmp back into buf for next pass
        buf.copy_from_slice(&tmp[..n]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_write_var_u32_roundtrip() {
        let mut buf = vec![];
        // write a few values
        write_var_u32(&mut buf, 0).unwrap();
        write_var_u32(&mut buf, 1).unwrap();
        write_var_u32(&mut buf, 300).unwrap();
        write_var_u32(&mut buf, u32::MAX).unwrap();
        // now read back
        let mut cursor = Cursor::new(buf);
        assert_eq!(read_var_u32(&mut cursor).unwrap(), 0);
        assert_eq!(read_var_u32(&mut cursor).unwrap(), 1);
        assert_eq!(read_var_u32(&mut cursor).unwrap(), 300);
        assert_eq!(read_var_u32(&mut cursor).unwrap(), u32::MAX);
    }

    #[test]
    fn test_radix_sort_u128() {
        let mut v = vec![3u128, 1u128, 2u128, 0u128, u128::MAX];
        radix_sort_u128(&mut v);
        assert_eq!(v, vec![0u128, 1u128, 2u128, 3u128, u128::MAX]);
    }
}
