//! Line tokenizer. One pass over the data in 64-byte blocks: AVX2 compares give
//! the comma, newline and quote masks of each block once; the separators are
//! then walked in order. Quotes are checked without branches: a line is valid
//! when its quote count equals twice the number of fields that are wrapped
//! in quotes (first and last byte `"`, length at least 2).

use std::arch::x86_64::*;

#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64, u64) {
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let cm = _mm256_set1_epi8(b',' as i8);
    let qt = _mm256_set1_epi8(b'"' as i8);
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |x: __m256i, c: __m256i| -> u32 { _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, c)) as u32 };
    let n = (m(a, nl) as u64) | ((m(b, nl) as u64) << 32);
    let c = (m(a, cm) as u64) | ((m(b, cm) as u64) << 32);
    let q = (m(a, qt) as u64) | ((m(b, qt) as u64) << 32);
    (n, c, q)
}

#[inline(always)]
fn low(b: usize) -> u64 {
    if b >= 64 {
        u64::MAX
    } else {
        (1u64 << b) - 1
    }
}

/// Calls `f(fields)` for each well-formed data line whose start lies in
/// `data[start..end)`; the file's first line (the header) is skipped.
#[inline(always)]
pub fn lines<const N: usize>(data: &[u8], start: usize, end: usize, mut f: impl FnMut(&[&[u8]; N])) {
    let n = data.len();
    let p0 = if start == 0 {
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
    if p0 >= end || p0 >= n {
        return;
    }
    let ptr = data.as_ptr();
    let mut seps = [0usize; N];
    let mut k = 0usize; // commas seen in the current line
    let mut ls = p0; // current line start
    let mut qcount: u32 = 0; // quotes of the current line in earlier blocks
    let mut base = p0;
    let mut tmp = [0u8; 64];
    let mut fl: [&[u8]; N] = [&[]; N];

    let mut finish = |ls: usize, le: usize, k: usize, q: u32, seps: &[usize; N], fl: &mut [&[u8]; N]| {
        if k != N - 1 || le == ls {
            return;
        }
        if q == 0 {
            // no quote anywhere on the line: fields are the raw spans
            for i in 0..N {
                let s = if i == 0 { ls } else { seps[i - 1] + 1 };
                let e = if i == N - 1 { le } else { seps[i] };
                fl[i] = unsafe { std::slice::from_raw_parts(ptr.add(s), e - s) };
            }
            f(fl);
            return;
        }
        let mut expect = 0u32;
        for i in 0..N {
            let s = if i == 0 { ls } else { seps[i - 1] + 1 };
            let e = if i == N - 1 { le } else { seps[i] };
            let len = e - s;
            // reading data[s] and data[e-1] is safe: both lie inside the line or on its separators
            let first = unsafe { *ptr.add(s.min(n - 1)) };
            let last = unsafe { *ptr.add(if e > 0 { e - 1 } else { 0 }) };
            let qf = ((len >= 2) & (first == b'"') & (last == b'"')) as usize;
            expect += 2 * qf as u32;
            fl[i] = unsafe { std::slice::from_raw_parts(ptr.add(s + qf), len - 2 * qf) };
        }
        if expect == q {
            f(fl);
        }
    };

    loop {
        let (nl, cm, qt) = if base + 64 <= n {
            unsafe { masks64(ptr.add(base)) }
        } else {
            let r = n - base;
            tmp[..r].copy_from_slice(&data[base..n]);
            for x in tmp[r..].iter_mut() {
                *x = 0;
            }
            unsafe { masks64(tmp.as_ptr()) }
        };
        let mut sep = cm | nl;
        let mut lsb = if ls > base { ls - base } else { 0 }; // line start bit in this block
        while sep != 0 {
            let j = sep.trailing_zeros() as usize;
            sep &= sep - 1;
            if (nl >> j) & 1 == 0 {
                if k < N - 1 {
                    seps[k] = base + j;
                }
                k += 1;
            } else {
                let q = qcount + (qt & low(j) & !low(lsb)).count_ones();
                finish(ls, base + j, k, q, &seps, &mut fl);
                k = 0;
                qcount = 0;
                ls = base + j + 1;
                lsb = j + 1;
                if ls >= end {
                    return;
                }
            }
        }
        qcount += (qt & !low(lsb)).count_ones();
        base += 64;
        if base >= n {
            // a last line with no newline
            if ls < n && ls < end {
                finish(ls, n, k, qcount, &seps, &mut fl);
            }
            return;
        }
    }
}

/// Per-line variant: masks of a 128-byte window from the line start, comma
/// positions picked with `pdep` (no data-dependent loop exits). Lines longer
/// than 127 bytes, and lines near the end of the data, use the scalar splitter.
#[inline(always)]
pub fn lines_pdep<const N: usize>(data: &[u8], start: usize, end: usize, mut f: impl FnMut(&[&[u8]; N])) {
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
    let ptr = data.as_ptr();
    let mut fl: [&[u8]; N] = [&[]; N];
    while p < end && p < n {
        if p + 128 <= n {
            let (nl0, cm0, qt0) = unsafe { masks64(ptr.add(p)) };
            let (nl1, cm1, qt1) = unsafe { masks64(ptr.add(p + 64)) };
            let t0 = nl0.trailing_zeros() as usize;
            let t1 = 64 + nl1.trailing_zeros() as usize;
            let len = if nl0 != 0 { t0 } else { t1 };
            if len < 128 {
                let next = p + len + 1;
                let (lm0, lm1) = unsafe { (_bzhi_u64(u64::MAX, len.min(64) as u32), _bzhi_u64(u64::MAX, len.saturating_sub(64) as u32)) };
                let c0 = cm0 & lm0;
                let c1 = cm1 & lm1;
                let k0 = c0.count_ones() as usize;
                if len == 0 || k0 + c1.count_ones() as usize != N - 1 {
                    p = next;
                    continue;
                }
                let q = (qt0 & lm0).count_ones() + (qt1 & lm1).count_ones();
                let mut expect = 0u32;
                let mut s = 0usize;
                for i in 0..N {
                    let e = if i == N - 1 {
                        len
                    } else {
                        let a = unsafe { _pdep_u64(1u64 << (i & 63), c0) }.trailing_zeros() as usize;
                        let b = 64 + unsafe { _pdep_u64(1u64 << (i.wrapping_sub(k0) & 63), c1) }.trailing_zeros() as usize;
                        if i < k0 { a } else { b }
                    };
                    let l = e - s;
                    let first = unsafe { *ptr.add(p + s) };
                    let last = unsafe { *ptr.add(p + if e > 0 { e - 1 } else { 0 }) };
                    let qf = ((l >= 2) & (first == b'"') & (last == b'"')) as usize;
                    expect += 2 * qf as u32;
                    fl[i] = unsafe { std::slice::from_raw_parts(ptr.add(p + s + qf), l - 2 * qf) };
                    s = e + 1;
                }
                if expect == q {
                    f(&fl);
                }
                p = next;
                continue;
            }
        }
        let e = match memchr::memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => n,
        };
        if crate::text::split::<N>(&data[p..e], &mut fl) {
            f(&fl);
        }
        p = e + 1;
    }
}
