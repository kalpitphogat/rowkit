//! Line tokenizer: AVX2 masks over a 128-byte window from the line start give
//! the newline, the commas and the quotes at once. Lines that do not fit the
//! window (or sit near the end of the file) go through the scalar splitter.

use std::arch::x86_64::*;

use crate::text::split;

#[inline(always)]
unsafe fn masks(p: *const u8) -> (u128, u128, u128) {
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let mut m_nl = 0u128;
    let mut m_cm = 0u128;
    let mut m_qt = 0u128;
    for k in 0..4 {
        let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
        m_nl |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nl)) as u32 as u128) << (32 * k);
        m_cm |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, cm)) as u32 as u128) << (32 * k);
        m_qt |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, qt)) as u32 as u128) << (32 * k);
    }
    (m_nl, m_cm, m_qt)
}

/// Calls `f(fields)` for each well-formed data line whose start lies in
/// `data[start..end)`; the file's first line (the header) is skipped.
#[inline(always)]
pub fn lines<const N: usize>(data: &[u8], start: usize, end: usize, mut f: impl FnMut(&[&[u8]; N])) {
    let n = data.len();
    let mut p = if start == 0 {
        match memchr::memchr(b'\n', data) {
            Some(i) => i + 1,
            None => return,
        }
    } else {
        match memchr::memchr(b'\n', &data[start - 1..]) {
            Some(i) => start - 1 + i + 1,
            None => return,
        }
    };
    let base = data.as_ptr();
    let mut fl: [&[u8]; N] = [&[]; N];
    while p < end && p < n {
        if p + 128 <= n {
            let (m_nl, m_cm, m_qt) = unsafe { masks(base.add(p)) };
            if m_nl != 0 {
                let len = m_nl.trailing_zeros() as usize;
                let lm = (1u128 << len) - 1;
                let mut cm = m_cm & lm;
                let next = p + len + 1;
                if len == 0 || cm.count_ones() as usize != N - 1 {
                    p = next;
                    continue;
                }
                let line = unsafe { std::slice::from_raw_parts(base.add(p), len) };
                let mut s = 0usize;
                for k in 0..N - 1 {
                    let c = cm.trailing_zeros() as usize;
                    cm &= cm - 1;
                    fl[k] = unsafe { line.get_unchecked(s..c) };
                    s = c + 1;
                }
                fl[N - 1] = unsafe { line.get_unchecked(s..len) };
                let qm = m_qt & lm;
                if qm != 0 {
                    let q = qm.count_ones();
                    let mut k2 = 0u32;
                    for fld in fl.iter_mut() {
                        let l = fld.len();
                        if l >= 2 && fld[0] == b'"' && fld[l - 1] == b'"' {
                            *fld = &fld[1..l - 1];
                            k2 += 2;
                        }
                    }
                    if k2 != q {
                        p = next;
                        continue;
                    }
                }
                f(&fl);
                p = next;
                continue;
            }
        }
        // scalar path: long line or near the end of the file
        let e = match memchr::memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => n,
        };
        if split::<N>(&data[p..e], &mut fl) {
            f(&fl);
        }
        p = e + 1;
    }
}
