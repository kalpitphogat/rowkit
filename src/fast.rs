//! Fast field parsers. They read up to 16 bytes *before* the end of a field.
//! Every field handed to them lies after the file's header line inside the
//! mapping, so those reads stay in bounds.

use crate::text::{days_from_civil, DAY};

#[inline(always)]
unsafe fn load8(p: *const u8) -> u64 {
    std::ptr::read_unaligned(p as *const u64)
}

const ONES: u64 = 0x0101_0101_0101_0101;
const HIGHS: u64 = 0x8080_8080_8080_8080;

#[inline(always)]
fn d2(a: u8, b: u8) -> Option<i64> {
    let x = a.wrapping_sub(b'0');
    let y = b.wrapping_sub(b'0');
    if x > 9 || y > 9 {
        return None;
    }
    Some((x as i64) * 10 + y as i64)
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

/// All 8 bytes are ASCII digits.
#[inline(always)]
fn all_digits(v: u64) -> bool {
    // high nibble 3 and low nibble <= 9 in every byte
    let hi_ok = (v & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030;
    let lo = v & 0x0F0F_0F0F_0F0F_0F0F;
    let le9 = (lo.wrapping_add(0x7676_7676_7676_7676) & HIGHS) == 0;
    hi_ok && le9
}

/// 8 ASCII digits (first char in the lowest byte) to their value.
#[inline(always)]
fn parse8(v: u64) -> u64 {
    let mut val = v.wrapping_sub(0x3030_3030_3030_3030);
    val = val.wrapping_mul(10).wrapping_add(val >> 8);
    ((val & 0x0000_00FF_0000_00FF)
        .wrapping_mul(100 + (1_000_000u64 << 32))
        .wrapping_add(((val >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32
}

/// F3 (digits only); None if any byte is not an ASCII digit.
#[inline(always)]
pub fn f3_fast(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if n == 0 || n > 16 {
        return f3_slow(f);
    }
    unsafe {
        let end = f.as_ptr().add(n);
        let mut lo = load8(end.sub(8));
        let mut hi = load8(end.sub(16));
        if n <= 8 {
            if n < 8 {
                let m = (1u64 << ((8 - n) * 8)) - 1;
                lo = (lo & !m) | (0x3030_3030_3030_3030 & m);
            }
            hi = 0x3030_3030_3030_3030;
        } else if n < 16 {
            let m = (1u64 << ((16 - n) * 8)) - 1;
            hi = (hi & !m) | (0x3030_3030_3030_3030 & m);
        }
        if !all_digits(lo) || !all_digits(hi) {
            return None;
        }
        Some((parse8(hi) * 100_000_000 + parse8(lo)) as i64)
    }
}

#[inline(never)]
fn f3_slow(s: &[u8]) -> Option<i64> {
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
    Some(v.min(i64::MAX as u64) as i64)
}

/// Per-thread cache of valid dates: `YYYY-MM-DD` -> epoch ms of midnight,
/// direct-mapped by a hash of the ten bytes. Only valid dates are cached.
pub struct DateCache {
    keys: [(u64, u16); 128],
    ms: [i64; 128],
}

impl DateCache {
    pub fn new() -> Self {
        DateCache { keys: [(0, 0); 128], ms: [0; 128] }
    }
    #[inline(always)]
    fn date(&mut self, s: &[u8]) -> Option<i64> {
        unsafe {
            let k8 = load8(s.as_ptr());
            let k2 = std::ptr::read_unaligned(s.as_ptr().add(8) as *const u16);
            let h = ((k8 ^ (k2 as u64).wrapping_mul(0x9E37_79B9)).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 57) as usize;
            let e = self.keys.get_unchecked(h);
            if e.0 == k8 && e.1 == k2 {
                return Some(*self.ms.get_unchecked(h));
            }
            self.date_slow(s, k8, k2, h)
        }
    }
    #[inline(never)]
    fn date_slow(&mut self, s: &[u8], k8: u64, k2: u16, h: usize) -> Option<i64> {
        if s[4] != b'-' || s[7] != b'-' {
            return None;
        }
        let y = d2(s[0], s[1])? * 100 + d2(s[2], s[3])?;
        let mo = d2(s[5], s[6])?;
        let d = d2(s[8], s[9])?;
        if !(1..=12).contains(&mo) || d < 1 || d > month_days(y, mo) {
            return None;
        }
        let ms = days_from_civil(y, mo, d) * DAY;
        self.keys[h] = (k8, k2);
        self.ms[h] = ms;
        Some(ms)
    }
    /// `THH:MM:SS` at s[10..19] -> ms of the day.
    #[inline(always)]
    fn time(s: &[u8]) -> Option<i64> {
        unsafe {
            if *s.get_unchecked(10) != b'T' {
                return None;
            }
            let v = load8(s.as_ptr().add(11));
            // "HH:MM:SS": digits everywhere except ':' at bytes 2 and 5
            let expect_colon: u64 = 0x0000_3A00_003A_0000;
            let colon_mask: u64 = 0x0000_FF00_00FF_0000;
            if v & colon_mask != expect_colon {
                return None;
            }
            let d = (v & !colon_mask) | (0x3030_3030_3030_3030 & colon_mask);
            if !all_digits(d) {
                return None;
            }
            let b = d.wrapping_sub(0x3030_3030_3030_3030).to_le_bytes();
            let h = b[0] as i64 * 10 + b[1] as i64;
            let m = b[3] as i64 * 10 + b[4] as i64;
            let sec = b[6] as i64 * 10 + b[7] as i64;
            if h > 23 || m > 59 || sec > 59 {
                return None;
            }
            Some(((h * 60 + m) * 60 + sec) * 1000)
        }
    }
    /// `ts` in forms F1, F2 or F3, with the cache; None for a bad timestamp.
    #[inline(always)]
    pub fn ts(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 20 && s[19] == b'Z' {
            let t = Self::time(s)?;
            return Some(self.date(s)? + t);
        }
        if n == 25 && (s[19] == b'+' || s[19] == b'-') {
            if s[22] != b':' {
                return None;
            }
            let oh = d2(s[20], s[21])?;
            let om = d2(s[23], s[24])?;
            if oh > 23 || om > 59 {
                return None;
            }
            let t = Self::time(s)?;
            let base = self.date(s)? + t;
            let off = (oh * 60 + om) * 60_000;
            return Some(if s[19] == b'+' { base - off } else { base + off });
        }
        f3_fast(s)
    }
}

/// Canonical base-36 ID (same contract as `text::canon36`), eight characters at a time.
#[inline(always)]
pub fn canon36_fast(s: &[u8], prefix: u8) -> Option<u64> {
    let n = s.len();
    if n < 2 || n > 9 || s[0] != prefix || (s[1] == b'0' && n > 2) {
        return None;
    }
    let d = n - 1;
    unsafe {
        let mut v = load8(s.as_ptr().add(n - 8));
        if d < 8 {
            let m = (1u64 << ((8 - d) * 8)) - 1;
            v = (v & !m) | (0x3030_3030_3030_3030 & m);
        }
        if v & HIGHS != 0 {
            return None;
        }
        // per byte (all < 0x80): digit 0x30..=0x39, letter 0x61..=0x7a
        let ge30 = v.wrapping_add(ONES * (0x80 - 0x30)) & HIGHS;
        let gt39 = v.wrapping_add(ONES * (0x80 - 0x3A)) & HIGHS;
        let ge61 = v.wrapping_add(ONES * (0x80 - 0x61)) & HIGHS;
        let gt7a = v.wrapping_add(ONES * (0x80 - 0x7B)) & HIGHS;
        let digit = ge30 & !gt39;
        let letter = ge61 & !gt7a;
        if (digit | letter) != HIGHS {
            return None;
        }
        let lmask = (letter >> 7) * 0x27;
        let x = v.wrapping_sub(0x3030_3030_3030_3030).wrapping_sub(lmask);
        let t1 = (x & 0x00FF_00FF_00FF_00FF).wrapping_mul(36).wrapping_add((x >> 8) & 0x00FF_00FF_00FF_00FF);
        let t2 = (t1 & 0x0000_FFFF_0000_FFFF).wrapping_mul(1296).wrapping_add((t1 >> 16) & 0x0000_FFFF_0000_FFFF);
        let val = (t2 & 0xFFFF_FFFF).wrapping_mul(1_679_616).wrapping_add(t2 >> 32);
        if val >= 1 << 40 {
            return None;
        }
        Some(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::text::{canon36, parse_ts};
    #[test]
    fn fast_matches_slow() {
        let mut buf = vec![b'x'; 96];
        let cases: &[&[u8]] = &[
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"2026-02-29T00:00:00Z", b"2028-02-29T00:00:00Z", b"2026-10-03T24:00:00Z", b"2026-10-03T23:60:00Z",
            b"2026-10-03T23:58:60Z", b"2026-13-03T23:58:10Z", b"2026-10-03t23:58:10Z", b"2026-10-03T23:58:10z",
            b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10+0200", b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10-00:00", b"1.79e12", b"-5", b"86400000",
            b"1791071890000.0", b"7", b"12345678", b"123456789", b"1234567890123456", b"12345678901234567",
            b"2026-10-03T23:5a:10Z", b"2026-10-03T2:358:10Z", b"0", b"00000000000000000000001",
            "\u{662}\u{665}".as_bytes(), "\u{ff12}\u{ff15}".as_bytes(), b"2026-10-03T23:58:10.000Z",
        ];
        for c in cases {
            let at = 40;
            buf[at..at + c.len()].copy_from_slice(c);
            let f = &buf[at..at + c.len()];
            let mut dc = DateCache::new();
            assert_eq!(dc.ts(f), parse_ts(c), "{:?}", String::from_utf8_lossy(c));
            assert_eq!(dc.ts(f), parse_ts(c), "cached {:?}", String::from_utf8_lossy(c));
        }
        let ids: &[&[u8]] = &[
            b"e1", b"e0", b"e01", b"eabcdefgh", b"ezzzzzzzz", b"eABC", b"e12345678", b"x12", b"e", b"e1234567",
            b"ea-b", b"e{z", b"e`a", b"e/0", b"e:0", b"e@", b"e[", b"ezzzzzzz", b"e1z", b"ea",
        ];
        for c in ids {
            let at = 40;
            buf[at..at + c.len()].copy_from_slice(c);
            assert_eq!(canon36_fast(&buf[at..at + c.len()], b'e'), canon36(c, b'e'), "{:?}", String::from_utf8_lossy(c));
        }
        // exhaustive-ish: random strings
        let mut r: u64 = 12345;
        for _ in 0..200_000 {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            let len = 1 + (r % 10) as usize;
            let alpha = b"0123456789abcdefxyzA:/`{e";
            let mut s = vec![b'e'];
            let mut q = r;
            for _ in 1..len {
                s.push(alpha[(q % alpha.len() as u64) as usize]);
                q /= alpha.len() as u64;
            }
            let at = 40;
            buf[at..at + s.len()].copy_from_slice(&s);
            assert_eq!(canon36_fast(&buf[at..at + s.len()], b'e'), canon36(&s, b'e'), "{:?}", String::from_utf8_lossy(&s));
            let digits = b"0123456789x.";
            let mut t = Vec::new();
            let mut q = r >> 5;
            for _ in 0..len + 5 {
                t.push(digits[(q % digits.len() as u64) as usize]);
                q /= digits.len() as u64;
            }
            buf[at..at + t.len()].copy_from_slice(&t);
            assert_eq!(f3_fast(&buf[at..at + t.len()]), parse_ts(&t), "{:?}", String::from_utf8_lossy(&t));
        }
    }
}
