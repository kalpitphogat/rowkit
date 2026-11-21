//! CSV dialect, timestamps, and canonical IDs.

pub const DAY: i64 = 86_400_000;

/// Splits one line (no `\n`) into exactly N fields under the dialect.
/// Returns false for a malformed line. Quotes around a whole field are removed.
#[inline(always)]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    if len == 0 {
        return false;
    }
    let mut start = 0usize;
    let mut k = 0usize;
    let mut i = 0usize;
    let mut has_q = false;
    while i < len {
        let c = unsafe { *line.get_unchecked(i) };
        if c == b',' {
            if k + 1 >= N {
                return false;
            }
            let f = unsafe { line.get_unchecked(start..i) };
            if has_q {
                match unquote(f) {
                    Some(v) => out[k] = v,
                    None => return false,
                }
            } else {
                out[k] = f;
            }
            k += 1;
            start = i + 1;
            has_q = false;
        } else if c == b'"' {
            has_q = true;
        }
        i += 1;
    }
    if k + 1 != N {
        return false;
    }
    let f = unsafe { line.get_unchecked(start..len) };
    if has_q {
        match unquote(f) {
            Some(v) => out[k] = v,
            None => return false,
        }
    } else {
        out[k] = f;
    }
    true
}

/// A field that holds a quote: valid only as `"..."` with no quote inside.
#[inline]
fn unquote(f: &[u8]) -> Option<&[u8]> {
    let n = f.len();
    if n < 2 || f[0] != b'"' || f[n - 1] != b'"' {
        return None;
    }
    let inner = &f[1..n - 1];
    if inner.iter().any(|&c| c == b'"') {
        return None;
    }
    Some(inner)
}

#[inline(always)]
fn d2(a: u8, b: u8) -> Option<i64> {
    let x = a.wrapping_sub(b'0');
    let y = b.wrapping_sub(b'0');
    if x > 9 || y > 9 {
        return None;
    }
    Some((x as i64) * 10 + y as i64)
}

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn month_days(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
    }
}

/// `YYYY-MM-DDTHH:MM:SS` (first 19 bytes) to epoch ms, or None.
#[inline(always)]
fn civil19(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s[0], s[1])? * 100 + d2(s[2], s[3])?;
    let mo = d2(s[5], s[6])?;
    let d = d2(s[8], s[9])?;
    let h = d2(s[11], s[12])?;
    let mi = d2(s[14], s[15])?;
    let se = d2(s[17], s[18])?;
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    if d > month_days(y, mo) {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    Some(days * DAY + ((h * 60 + mi) * 60 + se) * 1000)
}

/// Parses `ts` in forms F1, F2 or F3. None means a bad timestamp
/// (F3 values that do not fit an i64 are returned as None too: they are
/// outside every window anyway).
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 {
        if s[19] != b'Z' {
            return f3(s);
        }
        return civil19(s);
    }
    if n == 25 {
        let sign = s[19];
        if sign != b'+' && sign != b'-' {
            return f3(s);
        }
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s[20], s[21])?;
        let om = d2(s[23], s[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        let base = civil19(s)?;
        let off = (oh * 60 + om) * 60_000;
        return Some(if sign == b'+' { base - off } else { base + off });
    }
    f3(s)
}

#[inline]
fn f3(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    let rest = &s[i..];
    let mut v: u64 = 0;
    for &c in rest {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        // keep scanning to reject non-digits, but saturate the value
        v = v.saturating_mul(10).saturating_add(x as u64);
    }
    if v > i64::MAX as u64 {
        return Some(i64::MAX);
    }
    Some(v as i64)
}

/// F1 only (`ingest_ts` is always valid F1); falls back to the full parser.
#[inline(always)]
pub fn parse_ingest(s: &[u8]) -> Option<i64> {
    parse_ts(s)
}

/// Digits only, value 1..=10,000,000.
#[inline]
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        v = v.saturating_mul(10).saturating_add(x as u64);
    }
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

/// Canonical base-36 ID: `prefix` then lowercase base 36 with no leading zero
/// (or exactly "0"), value < 2^40. Returns the value; the mapping is a
/// bijection between such strings and values.
#[inline(always)]
pub fn canon36(s: &[u8], prefix: u8) -> Option<u64> {
    let n = s.len();
    if n < 2 || n > 9 || s[0] != prefix {
        return None;
    }
    if s[1] == b'0' && n > 2 {
        return None;
    }
    let mut v: u64 = 0;
    for &c in &s[1..] {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'z' => c - b'a' + 10,
            _ => return None,
        };
        v = v * 36 + d as u64;
    }
    if v >= 1 << 40 {
        return None;
    }
    Some(v)
}

/// Two base-36 digits per entry: PAIRS[v] for v < 1296 is digit(v / 36), digit(v % 36).
static PAIRS: [[u8; 2]; 1296] = {
    let d = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut t = [[0u8; 2]; 1296];
    let mut i = 0;
    while i < 1296 {
        t[i] = [d[i / 36], d[i % 36]];
        i += 1;
    }
    t
};

pub fn base36_string(prefix: u8, mut v: u64, out: &mut Vec<u8>) {
    let mut buf = [0u8; 16];
    let mut at = buf.len();
    // two digits per step, then drop a leading zero digit (none when v is 0)
    loop {
        let p = PAIRS[(v % 1296) as usize];
        at -= 2;
        buf[at] = p[0];
        buf[at + 1] = p[1];
        v /= 1296;
        if v == 0 {
            break;
        }
    }
    if buf[at] == b'0' && at + 1 < buf.len() {
        at += 1;
    }
    out.push(prefix);
    out.extend_from_slice(&buf[at..]);
}

/// Canonical decimal ID: `prefix` then digits with no leading zero.
#[inline(always)]
pub fn canon10(s: &[u8], prefix: &[u8]) -> Option<u32> {
    let p = prefix.len();
    let n = s.len();
    if n <= p || n > p + 9 || &s[..p] != prefix {
        return None;
    }
    if s[p] == b'0' && n > p + 1 {
        return None;
    }
    let mut v: u32 = 0;
    for &c in &s[p..] {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        v = v * 10 + x as u32;
    }
    Some(v)
}

/// The ID mix and its inverse.
pub const MASK40: u64 = (1 << 40) - 1;
pub const M1: u64 = 173_961_102_589_771;
const M2: u64 = 236_296_194_254_425;

fn inv40(m: u64) -> u64 {
    let mut x: u64 = m; // Newton: x = x*(2 - m*x)
    for _ in 0..6 {
        x = x.wrapping_mul(2u64.wrapping_sub(m.wrapping_mul(x)));
    }
    x & MASK40
}

#[derive(Clone, Copy)]
pub struct Mix {
    pub m1i: u64,
    pub m2i: u64,
}

impl Mix {
    pub fn new() -> Self {
        Mix { m1i: inv40(M1), m2i: inv40(M2) }
    }
    /// out -> x where out = scramble(v, key) and x = (v*M1 ^ key) & MASK
    #[inline(always)]
    pub fn unmix(&self, out: u64) -> u64 {
        let y = out.wrapping_mul(self.m2i) & MASK40;
        (y ^ (y >> 17) ^ (y >> 34)) & MASK40
    }
    #[inline(always)]
    pub fn decode(&self, out: u64, key: u64) -> u64 {
        ((self.unmix(out) ^ key) & MASK40).wrapping_mul(self.m1i) & MASK40
    }
    #[inline(always)]
    pub fn key_for(&self, out: u64, v: u64) -> u64 {
        (self.unmix(out) ^ v.wrapping_mul(M1)) & MASK40
    }
}

pub fn scramble(value: u64, key: u64) -> u64 {
    let mut mixed = value & MASK40;
    mixed = (mixed.wrapping_mul(M1) ^ key) & MASK40;
    mixed ^= mixed >> 17;
    mixed.wrapping_mul(M2) & MASK40
}

#[cfg(test)]
mod b36_tests {
    #[test]
    fn pairs_match_simple() {
        let d = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let mut x: u64 = 88172645463325252;
        for i in 0..2_000_000u64 {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            let v = if i < 5000 { i } else { (x % (1u64 << 41)) >> (i % 41) };
            let mut want = vec![b'u'];
            let mut tmp = Vec::new();
            let mut w = v;
            loop { tmp.push(d[(w % 36) as usize]); w /= 36; if w == 0 { break; } }
            tmp.reverse();
            want.extend_from_slice(&tmp);
            let mut got = Vec::new();
            super::base36_string(b'u', v, &mut got);
            assert_eq!(got, want, "v={v}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mix_roundtrip() {
        let m = Mix::new();
        for key in [0u64, 12345, 0xFFFF_FFFF_FF, 0x1234_5678_9A] {
            for v in [0u64, 1, 2, 999, 123_456_789, (1 << 40) - 1] {
                let s = scramble(v, key);
                assert_eq!(m.decode(s, key), v);
                assert_eq!(m.key_for(s, v), key & MASK40);
            }
        }
    }
    #[test]
    fn ts() {
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), parse_ts(b"2026-10-03T19:58:10-04:00"));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"2026-02-29T00:00:00Z"), None);
        assert_eq!(parse_ts(b"2028-02-29T00:00:00Z").is_some(), true);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+0200"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10+24:00"), None);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), parse_ts(b"2026-10-03T23:58:10Z"));
        assert_eq!(parse_ts(b"1.79e12"), None);
        assert_eq!(parse_ts(b""), None);
    }
}
