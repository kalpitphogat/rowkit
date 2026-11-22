//! Batch job: CSV inputs to two Parquet tables.
//!
//! Inputs are memory-mapped and cut into chunks that workers take from a
//! shared counter. Rows whose IDs have a canonical spelling are placed by their
//! decoded number into a dense table, deduplicated in place; other rows take a
//! generic path (hash dedup, sort). Both paths feed the same per-key code, so
//! the output never depends on which path a row took.

mod fast;
mod names;
mod parquet;
mod probe;
mod scan;
mod text;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Instant;

use fast::*;
use text::*;

const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;
const NB: usize = 512; // person buckets
const LINK_TAG: u64 = 1 << 42;
const GEN_TAG: u64 = 1 << 40;

// ---------------------------------------------------------------- inputs

fn map_file(path: &Path) -> &'static [u8] {
    use std::os::unix::io::AsRawFd;
    let f = std::fs::File::open(path).expect("open");
    let len = f.metadata().expect("stat").len() as usize;
    if len == 0 {
        return &[];
    }
    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0);
        if p == libc::MAP_FAILED {
            panic!("mmap failed");
        }
        libc::madvise(p, len, libc::MADV_SEQUENTIAL);
        std::slice::from_raw_parts(p as *const u8, len)
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

/// Iterates the data lines of `data[start..end)` (lines that start in the range; the header,
/// the first line of the file, is skipped).
#[inline]
fn for_lines(data: &[u8], start: usize, end: usize, mut f: impl FnMut(&[u8])) {
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
    let n = data.len();
    while p < end && p < n {
        let e = match memchr::memchr(b'\n', &data[p..]) {
            Some(i) => p + i,
            None => n,
        };
        f(&data[p..e]);
        p = e + 1;
    }
}

const HEAD: usize = 64;
const PAD: usize = 256;

/// Loads the chunk `[a, b)` of a file: the bytes from `a - 1` up to the end of
/// the last line that starts before `b`. Returns (data, start, end) for `scan::lines`.
fn load_chunk<'a>(buf: &'a mut Vec<u8>, f: &std::fs::File, flen: usize, a: usize, b: usize) -> (&'a [u8], usize, usize) {
    use std::os::unix::fs::FileExt;
    let from = if a == 0 { 0 } else { a - 1 };
    let mut tail = 4096;
    loop {
        let to = (b + tail).min(flen);
        let m = to - from;
        if buf.len() < HEAD + m + PAD {
            buf.resize(HEAD + m + PAD, 0);
        }
        f.read_exact_at(&mut buf[HEAD..HEAD + m], from as u64).expect("read");
        let rel_b = b - from;
        let ok = to == flen || memchr::memchr(b'\n', &buf[HEAD + rel_b - 1..HEAD + m]).is_some();
        if ok {
            return (&buf[HEAD..HEAD + m], a - from, b - from);
        }
        tail *= 4;
    }
}

fn read_small_csv(path: &Path, mut f: impl FnMut(&[u8], &[u8])) {
    let data = std::fs::read(path).unwrap_or_default();
    for_lines(&data, 0, data.len(), |line| {
        let mut fl: [&[u8]; 2] = [&[]; 2];
        if split::<2>(line, &mut fl) {
            f(fl[0], fl[1]);
        }
    });
}

fn json_field(text: &str, key: &str) -> String {
    let at = text.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &text[at + key.len() + 2..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    rest[..q2].to_string()
}

// ---------------------------------------------------------------- shared context

struct Ctx {
    ws: i64,
    we: i64,
    day0: i64,
    ndays: usize,
    cats: Vec<Vec<u8>>,
    unknown_cat: u16,
    prod_dense: Vec<u16>, // number -> cat+1
    prod_map: HashMap<Vec<u8>, u16>,
    camp_names: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
    camp_dense: Vec<u16>, // number -> idx+1
    camp_map: HashMap<Vec<u8>, u16>,
    mix: Mix,
    pos: bool,
    ekey: u64,
    ckey: u64,
    ukey: u64,
    cap_e: u64,
    cap_c: u64,
    ingest_base_s: i64,
    shift: usize,
    nparts: usize,
    scatter: bool,
    mail: bool,
    owner_mul: u64,
    tok_pdep: bool,
    part_cap: usize,
    exp: u32,
    ge_cap: u64,
    gc_cap: u64,
}

impl Ctx {
    #[inline]
    fn product_cat(&self, s: &[u8]) -> u16 {
        if let Some(n) = canon10_fast(s, b"p") {
            if (n as usize) < self.prod_dense.len() {
                let c = self.prod_dense[n as usize];
                if c != 0 {
                    return c - 1;
                }
                return self.unknown_cat;
            }
        }
        match self.prod_map.get(s) {
            Some(&c) => c,
            None => self.unknown_cat,
        }
    }
    #[inline]
    fn campaign(&self, s: &[u8]) -> Option<u16> {
        if let Some(n) = canon10_fast(s, b"c_") {
            if (n as usize) < self.camp_dense.len() {
                let c = self.camp_dense[n as usize];
                if c != 0 {
                    return Some(c - 1);
                }
                return None;
            }
        }
        self.camp_map.get(s).copied()
    }
}

// ---------------------------------------------------------------- user ids

struct Interner {
    map: HashMap<Vec<u8>, u32>,
    strs: Vec<Vec<u8>>,
}

// Interned (non-canonical) IDs live in 64 independently locked shards, so threads
// rarely wait on each other. Index = shard << 24 | position in the shard.
const ISH: usize = 64;
#[allow(clippy::declare_interior_mutable_const)]
const ISH_INIT: Mutex<Option<Interner>> = Mutex::new(None);
static INTERN: [Mutex<Option<Interner>>; ISH] = [ISH_INIT; ISH];

#[inline]
fn bytes_hash(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325 ^ s.len() as u64;
    let mut i = 0;
    while i + 8 <= s.len() {
        let v = u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
        h = (h ^ v).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= h >> 29;
        i += 8;
    }
    let mut t = 0u64;
    for (k, &b) in s[i..].iter().enumerate() {
        t |= (b as u64) << (8 * k);
    }
    h = (h ^ t).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 31)
}

// Per-thread cache in front of the shards: (hash, enc, len, bytes), checked byte for byte.
const ICACHE: usize = 4096;
thread_local! {
    static ICACHE_T: std::cell::RefCell<Vec<(u64, u64, u8, [u8; 47])>> = std::cell::RefCell::new(Vec::new());
}

fn intern(s: &[u8]) -> u64 {
    let h = bytes_hash(s);
    let small = s.len() <= 47;
    if small {
        let hit = ICACHE_T.with(|c| {
            let c = c.borrow();
            if c.is_empty() {
                return None;
            }
            let e = &c[(h as usize) & (ICACHE - 1)];
            if e.0 == h && e.2 as usize == s.len() && &e.3[..s.len()] == s && e.1 != 0 {
                Some(e.1)
            } else {
                None
            }
        });
        if let Some(v) = hit {
            return v;
        }
    }
    let sh = (h >> 58) as usize;
    let enc = {
        let mut g = INTERN[sh].lock().unwrap();
        let it = g.get_or_insert_with(|| Interner { map: HashMap::new(), strs: Vec::new() });
        if let Some(&i) = it.map.get(s) {
            GEN_TAG | ((sh as u64) << 24) | i as u64
        } else {
            let i = it.strs.len() as u32;
            assert!(i < (1 << 24), "interner shard full");
            it.strs.push(s.to_vec());
            it.map.insert(s.to_vec(), i);
            GEN_TAG | ((sh as u64) << 24) | i as u64
        }
    };
    if small {
        ICACHE_T.with(|c| {
            let mut c = c.borrow_mut();
            if c.is_empty() {
                c.resize(ICACHE, (0, 0, 0, [0u8; 47]));
            }
            let e = &mut c[(h as usize) & (ICACHE - 1)];
            e.0 = h;
            e.1 = enc;
            e.2 = s.len() as u8;
            e.3[..s.len()].copy_from_slice(s);
        });
    }
    enc
}

#[inline(always)]
fn uid_enc(s: &[u8]) -> u64 {
    match canon36_fast(s, b'u') {
        Some(n) => n,
        None => intern(s),
    }
}

fn uid_bytes(enc: u64, out: &mut Vec<u8>) {
    out.clear();
    if enc & GEN_TAG != 0 {
        let sh = ((enc >> 24) & (ISH as u64 - 1)) as usize;
        let g = INTERN[sh].lock().unwrap();
        out.extend_from_slice(&g.as_ref().unwrap().strs[(enc & 0xFF_FFFF) as usize]);
    } else {
        base36_string(b'u', enc, out);
    }
}

const P36: [u64; 9] = [1, 36, 1296, 46_656, 1_679_616, 60_466_176, 2_176_782_336, 78_364_164_096, 2_821_109_907_456];

/// Order key of the canonical ID with value v (< 2^40): its digits left-aligned to
/// eight places, then its length, so that comparing keys compares the strings.
#[inline]
fn name_key(v: u64) -> u64 {
    let mut len = 1;
    while len < 8 && v >= P36[len] {
        len += 1;
    }
    ((v * P36[8 - len]) << 4) | len as u64
}

/// Order key for canonical IDs: base-36 digits as ASCII, left-aligned big-endian.
#[inline]
fn canon_key(mut v: u64) -> u64 {
    const D: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = [0u8; 8];
    let mut n = 0;
    let mut tmp = [0u8; 8];
    loop {
        tmp[n] = D[(v % 36) as usize];
        n += 1;
        v /= 36;
        if v == 0 {
            break;
        }
    }
    for i in 0..n {
        buf[i] = tmp[n - 1 - i];
    }
    u64::from_be_bytes(buf)
}



/// Sorts (thread, index) references into memory order.
fn sort_refs(v: &mut Vec<(u16, u32)>) {
    if v.iter().all(|r| r.1 < (1 << 24) && r.0 < 256) {
        radix_sort(v, |r| ((r.0 as u64) << 24) | r.1 as u64, 32);
    } else {
        radix_sort(v, |r| ((r.0 as u64) << 32) | r.1 as u64, 48);
    }
}

/// Stable LSD radix sort on the low `bits` bits of `key`, 16 bits per pass.
fn radix_sort<T: Copy>(v: &mut Vec<T>, key: impl Fn(&T) -> u64, bits: u32) {
    let n = v.len();
    if n < 256 {
        v.sort_by_key(|x| key(x) & if bits >= 64 { u64::MAX } else { (1u64 << bits) - 1 });
        return;
    }
    let mut tmp: Vec<T> = Vec::with_capacity(n);
    unsafe { tmp.set_len(n) };
    let mut cnt = vec![0usize; 1 << 16];
    let mut shift = 0;
    while shift < bits {
        for c in cnt.iter_mut() {
            *c = 0;
        }
        for x in v.iter() {
            cnt[((key(x) >> shift) & 0xFFFF) as usize] += 1;
        }
        let mut sum = 0;
        for c in cnt.iter_mut() {
            let t = *c;
            *c = sum;
            sum += t;
        }
        for x in v.iter() {
            let d = ((key(x) >> shift) & 0xFFFF) as usize;
            tmp[cnt[d]] = *x;
            cnt[d] += 1;
        }
        std::mem::swap(v, &mut tmp);
        shift += 16;
    }
}

// ---------------------------------------------------------------- cpu pinning

/// Allowed CPUs, hyperthread siblings next to each other, so threads 2k and 2k+1 share a core.
fn cpu_order() -> &'static Vec<usize> {
    static O: std::sync::OnceLock<Vec<usize>> = std::sync::OnceLock::new();
    O.get_or_init(|| {
        if std::env::var("SS_PIN").map(|v| v == "0").unwrap_or(false) {
            return Vec::new();
        }
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        if unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) } != 0 {
            return Vec::new();
        }
        let allowed: Vec<usize> = (0..libc::CPU_SETSIZE as usize).filter(|&c| unsafe { libc::CPU_ISSET(c, &set) }).collect();
        let mut seen = std::collections::HashSet::new();
        let mut out = Vec::new();
        for &c in &allowed {
            if seen.contains(&c) {
                continue;
            }
            let sib = std::fs::read_to_string(format!("/sys/devices/system/cpu/cpu{c}/topology/thread_siblings_list")).unwrap_or_default();
            let mut grp = vec![c];
            for part in sib.trim().split(',') {
                let mut it = part.split('-').filter_map(|x| x.parse::<usize>().ok());
                if let Some(a) = it.next() {
                    for x in a..=it.next().unwrap_or(a).min(4095) {
                        if x != c {
                            grp.push(x);
                        }
                    }
                }
            }
            for x in grp {
                if allowed.contains(&x) && seen.insert(x) {
                    out.push(x);
                }
            }
        }
        out
    })
}

static PIN_NEXT: AtomicUsize = AtomicUsize::new(0);

/// Pins the calling worker to the next CPU in `cpu_order` (round robin).
fn pin_self() {
    let o = cpu_order();
    if o.len() < 2 {
        return;
    }
    let c = o[PIN_NEXT.fetch_add(1, Relaxed) % o.len()];
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(c, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}

// ---------------------------------------------------------------- union-find over linked ids

#[derive(Clone, Copy)]
struct PInfo {
    comp: u64,
    mask: u8,
    amb: bool,      // aliases of this person in more than one set
    nonlocal: bool, // its set has IDs of another person, or IDs this table does not cover
}

struct Links {
    // Sharded open addressing. Entry = (key+1) << IDX_BITS | index; 0 = empty.
    tab: Vec<u64>,
    shard_shift: u32, // shard = hash >> shard_shift
    shard_bits: u32,  // log2 of the entries per shard
    // Per generator person p (canonical IDs decoded with the user key): which of its
    // aliases 0..8 are linked, and the name of their set. Read in person order, so
    // mostly sequential. Anything it cannot answer exactly goes to the hash table.
    pinfo: Vec<PInfo>,
    pcap: u64, // persons below this are covered: an alias < 8 missing from the mask is unlinked
    pkey: Option<(Mix, u64)>,
    name: Vec<u64>, // per linked ID: uid enc of the smallest ID of its set
}

const IDX_BITS: u32 = 23;

#[inline(always)]
fn hash64(x: u64) -> u64 {
    let x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
}

/// A big zeroed array on huge pages (falls back to normal pages silently).
fn huge_zeroed(n: usize) -> Vec<u64> {
    // calloc-backed: big allocations are fresh zero pages; advise huge pages before first touch
    let mut v: Vec<u64> = vec![0u64; n];
    let bytes = n * 8;
    if bytes >= (4 << 20) {
        let p = v.as_mut_ptr() as usize;
        let a = (p + (2 << 20) - 1) & !((2 << 20) - 1);
        let e = (p + bytes) & !((2 << 20) - 1);
        if e > a {
            unsafe { libc::madvise(a as *mut libc::c_void, e - a, libc::MADV_HUGEPAGE) };
        }
    }
    v
}

impl Links {
    fn build(pairs: Vec<Vec<(u64, u64)>>, pkey: Option<(Mix, u64)>) -> Links {
        let show = std::env::var("SS_TIMING").is_ok();
        let cpu = || -> f64 {
            let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
            ts.tv_sec as f64 * 1e3 + ts.tv_nsec as f64 / 1e6
        };
        let c0 = cpu();
        let stamp = |what: &str| {
            if show {
                eprintln!("      uf cpu {:>7.1} ms {}", cpu() - c0, what);
            }
        };
        let total: usize = pairs.iter().map(|p| p.len()).sum();
        let occ = total * 2;
        // shards of about 8K occurrences: each shard's table fits in L2 while it is built
        let shards = (occ / 8192).next_power_of_two().max(1);
        let sb = shards.trailing_zeros();
        let shard_shift = 64 - sb;
        let mut counts = vec![0usize; shards];
        let shard_of = |k: u64| -> usize { if sb == 0 { 0 } else { (hash64(k) >> shard_shift) as usize } };
        for v in &pairs {
            for &(a, b) in v {
                counts[shard_of(a)] += 1;
                counts[shard_of(b)] += 1;
            }
        }
        let maxc = counts.iter().copied().max().unwrap_or(0);
        let shard_bits = ((maxc * 11 / 10).max(16)).next_power_of_two().trailing_zeros();
        let per = 1usize << shard_bits;
        let mut start = vec![0usize; shards + 1];
        for i in 0..shards {
            start[i + 1] = start[i] + counts[i];
        }
        // ids grouped by shard, with the position of each occurrence
        let mut by: Vec<(u64, u32)> = vec![(0, 0); occ];
        let mut fill = start.clone();
        let mut pos = 0u32;
        for v in &pairs {
            for &(a, b) in v {
                for k in [a, b] {
                    let sh = shard_of(k);
                    by[fill[sh]] = (k, pos);
                    fill[sh] += 1;
                    pos += 1;
                }
            }
        }
        drop(pairs);
        stamp("scatter");
        let mut tab: Vec<u64> = huge_zeroed(shards * per);
        let mut ids: Vec<u64> = Vec::with_capacity(occ);
        let mut keys: Vec<u64> = Vec::with_capacity(occ);
        let mut idx: Vec<u32> = vec![0; occ];
        let lmask = per - 1;
        for sh in 0..shards {
            let base = sh * per;
            for &(k, at) in &by[start[sh]..start[sh + 1]] {
                let mut h = hash64(k) as usize & lmask;
                let id = loop {
                    let e = tab[base + h];
                    if e == 0 {
                        let n = ids.len() as u64;
                        assert!(n < (1 << IDX_BITS), "too many linked IDs");
                        tab[base + h] = ((k + 1) << IDX_BITS) | n;
                        ids.push(k);
                        keys.push(if k & GEN_TAG == 0 { name_key(k) } else { u64::MAX });
                        break n as u32;
                    }
                    if e >> IDX_BITS == k + 1 {
                        break (e & ((1 << IDX_BITS) - 1)) as u32;
                    }
                    h = (h + 1) & lmask;
                };
                idx[at as usize] = id;
            }
        }
        drop(by);
        stamp("insert");
        // node = (parent, name key) in one 16-byte entry
        let mut node: Vec<[u64; 2]> = keys.iter().enumerate().map(|(i, &k)| [i as u64, k]).collect();
        drop(keys);
        fn find(node: &mut [[u64; 2]], mut x: u32) -> u32 {
            loop {
                let p = node[x as usize][0] as u32;
                if p == x {
                    return x;
                }
                let gp = node[p as usize][0];
                node[x as usize][0] = gp;
                x = gp as u32;
            }
        }
        // union by name: the root of every set is its byte-wise smallest ID
        let mut sa = Vec::new();
        let mut sb = Vec::new();
        let less = |node: &[[u64; 2]], x: u32, y: u32, sa: &mut Vec<u8>, sb: &mut Vec<u8>| -> bool {
            let (kx, ky) = (node[x as usize][1], node[y as usize][1]);
            if kx != u64::MAX && ky != u64::MAX {
                return kx < ky;
            }
            uid_bytes(ids[x as usize], sa);
            uid_bytes(ids[y as usize], sb);
            sa < sb
        };
        for pr in idx.chunks_exact(2) {
            let ra = find(&mut node, pr[0]);
            let rb = find(&mut node, pr[1]);
            if ra != rb {
                if less(&node, ra, rb, &mut sa, &mut sb) {
                    node[rb as usize][0] = ra as u64;
                } else {
                    node[ra as usize][0] = rb as u64;
                }
            }
        }
        stamp("union");
        let n = node.len();
        let mut root = vec![0u32; n];
        for i in 0..n {
            root[i] = find(&mut node, i as u32);
        }
        let name: Vec<u64> = root.iter().map(|&r| ids[r as usize]).collect();
        let name_ids = ids;
        stamp(&format!("names ({} ids, {} occurrences)", n, occ));
        let mut pinfo: Vec<PInfo> = Vec::new();
        let pcap: u64 = 1 << 24;
        if let Some((mix, ukey)) = pkey {
            // covered person and alias of each ID, decoded once: pp << 3 | alias, or u32::MAX
            let cov: Vec<u32> = name_ids
                .iter()
                .map(|&id| {
                    if id & GEN_TAG != 0 {
                        return u32::MAX;
                    }
                    let nn = mix.decode(id, ukey);
                    if nn >= (1 << 39) || (nn & 511) >= 8 || (nn >> 9) >= pcap {
                        return u32::MAX;
                    }
                    (((nn >> 9) as u32) << 3) | (nn & 7) as u32
                })
                .collect();
            for (i, &cv) in cov.iter().enumerate() {
                if cv == u32::MAX {
                    continue;
                }
                let (pp, alias) = ((cv >> 3) as usize, (cv & 7) as u64);
                if pinfo.len() <= pp {
                    pinfo.resize(pp + 1, PInfo { comp: 0, mask: 0, amb: false, nonlocal: false });
                }
                let c = LINK_TAG | name[i];
                let e = &mut pinfo[pp];
                if e.mask == 0 {
                    e.comp = c;
                } else if e.comp != c {
                    e.amb = true;
                }
                e.mask |= 1 << alias;
            }
            // a set is local when all its IDs are covered aliases of one person
            let mut first_p: Vec<u32> = vec![u32::MAX; n];
            const NONLOCAL: u32 = u32::MAX - 1;
            let cover = |i: usize| -> Option<u32> {
                let cv = cov[i];
                if cv == u32::MAX {
                    None
                } else {
                    Some(cv >> 3)
                }
            };
            for i in 0..name_ids.len() {
                let r = root[i] as usize;
                first_p[r] = match (cover(i), first_p[r]) {
                    (None, _) => NONLOCAL,
                    (Some(_), NONLOCAL) => NONLOCAL,
                    (Some(pp), u32::MAX) => pp,
                    (Some(pp), q) => if q == pp { q } else { NONLOCAL },
                };
            }
            for i in 0..name_ids.len() {
                if let Some(pp) = cover(i) {
                    if first_p[root[i] as usize] != pp {
                        pinfo[pp as usize].nonlocal = true;
                    }
                }
            }
        }
        Links { tab, shard_shift, shard_bits, name, pinfo, pcap, pkey }
    }

    /// Same answer as `comp`, through the per-person table when it can answer exactly.
    #[inline(always)]
    fn comp_fast(&self, uid: u64) -> u64 {
        if let Some((mix, ukey)) = self.pkey {
            if uid & GEN_TAG == 0 {
                let n = mix.decode(uid, ukey);
                if n < (1 << 39) && (n & 511) < 8 && (n >> 9) < self.pcap {
                    let pp = (n >> 9) as usize;
                    let bit = 1u8 << (n & 511);
                    match self.pinfo.get(pp) {
                        None => return uid,
                        Some(e) => {
                            if e.mask & bit == 0 {
                                return uid;
                            }
                            if !e.amb {
                                return e.comp;
                            }
                        }
                    }
                }
            }
        }
        self.comp(uid)
    }

    /// The generator person p when the set of `uid` holds only covered aliases of p
    /// (so every row of that set has an ID that decodes to p), else None.
    #[inline(always)]
    fn local_person(&self, uid: u64) -> Option<usize> {
        let (mix, ukey) = self.pkey?;
        if uid & GEN_TAG != 0 {
            return None;
        }
        let n = mix.decode(uid, ukey);
        if n >= (1 << 39) || (n & 511) >= 8 || (n >> 9) >= self.pcap {
            return None;
        }
        let pp = (n >> 9) as usize;
        match self.pinfo.get(pp) {
            None => Some(pp),
            Some(e) => {
                if e.mask & (1u8 << (n & 511)) == 0 {
                    Some(pp) // unlinked: a set of its own
                } else if !e.amb && !e.nonlocal {
                    Some(pp)
                } else {
                    None
                }
            }
        }
    }

    /// Prefetches the table slot `comp(uid)` will read first.
    #[inline(always)]
    fn prefetch(&self, uid: u64) {
        let hh = hash64(uid);
        let sh = if self.shard_shift >= 64 { 0 } else { (hh >> self.shard_shift) as usize };
        let lmask = (1usize << self.shard_bits) - 1;
        let i = (sh << self.shard_bits) + (hh as usize & lmask);
        if i < self.tab.len() {
            unsafe { std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(self.tab.as_ptr().add(i) as *const i8) };
        }
    }

    /// The person of a user ID: LINK_TAG | root for a linked ID, the ID itself otherwise.
    #[inline(always)]
    fn comp(&self, uid: u64) -> u64 {
        let hh = hash64(uid);
        let sh = if self.shard_shift >= 64 { 0 } else { (hh >> self.shard_shift) as usize };
        let lmask = (1usize << self.shard_bits) - 1;
        let base = sh << self.shard_bits;
        let mut h = hh as usize & lmask;
        let want = uid + 1;
        loop {
            let e = unsafe { *self.tab.get_unchecked(base + h) };
            if e >> IDX_BITS == want {
                // a set is named by its smallest ID, which also serves as its identity
                return LINK_TAG | unsafe { *self.name.get_unchecked((e & ((1 << IDX_BITS) - 1)) as usize) };
            }
            if e == 0 {
                return uid;
            }
            h = (h + 1) & lmask;
        }
    }

    fn name_of(&self, comp: u64) -> u64 {
        comp & !LINK_TAG
    }
}

// ---------------------------------------------------------------- dense tables

struct Dense {
    ptr: *mut AtomicU64,
    len: usize, // slots
}
unsafe impl Sync for Dense {}
unsafe impl Send for Dense {}

impl Dense {
    fn new(slots: usize) -> Dense {
        let bytes = (slots.max(1) * 16 + (2 << 20) - 1) & !((2 << 20) - 1);
        unsafe {
            let p = libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            );
            if p == libc::MAP_FAILED {
                panic!("mmap dense");
            }
            libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
            Dense { ptr: p as *mut AtomicU64, len: slots }
        }
    }
    #[inline(always)]
    fn w0(&self, i: usize) -> &AtomicU64 {
        unsafe { &*self.ptr.add(2 * i) }
    }
    #[inline(always)]
    fn w1(&self, i: usize) -> &AtomicU64 {
        unsafe { &*self.ptr.add(2 * i + 1) }
    }
}

// event w0 (logical; stored inverted): ingest_rel+1 (22) << 42 | ts_off (31) << 11 | valid << 10 | purchase << 9 | cat(5) << 4 | uidgen << 3
// event w1: uid (40) | price (24) << 40
// click w0: 1<<63 | ts_off(31) << 20 | camp (12) << 8 | uidgen
// click w1: uid

#[derive(Clone, Copy)]
struct GenEvent {
    eid_off: u32,
    eid_len: u32,
    hash: u64,
    ingest: i64,
    ts: i64,
    uid: u64,
    price: u32,
    cat: u16,
    valid: bool,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct GenClick {
    cid_off: u32,
    cid_len: u32,
    hash: u64,
    ts: i64,
    uid: u64,
    camp: u16,
}

#[derive(Clone, Copy)]
#[repr(C, packed)]
struct Rec {
    w0s: u64, // inverted w0: larger = smaller ingest; 0 = empty
    w1: u64,
    local: u32,
}

/// Event records by slot range ("partition"): one preallocated region per
/// partition, filled in batches; records past a region's capacity spill to a list.
struct Scatter {
    buf: *mut Rec,
    cap: usize,
    fill: Vec<std::sync::atomic::AtomicU32>,
    spill: Mutex<Vec<(u32, Rec)>>,
}
unsafe impl Sync for Scatter {}
unsafe impl Send for Scatter {}

const STAGE: usize = 16; // 16 x 20 B = 320 B = five cache lines

impl Scatter {
    fn new(nparts: usize, cap: usize) -> Scatter {
        let n = nparts * cap;
        let bytes = (n * std::mem::size_of::<Rec>()).max(64);
        let p = unsafe {
            let p = libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            );
            if p == libc::MAP_FAILED {
                panic!("mmap scatter");
            }
            libc::madvise(p, bytes, libc::MADV_HUGEPAGE);
            p as *mut Rec
        };
        Scatter {
            buf: p,
            cap,
            fill: (0..nparts).map(|_| std::sync::atomic::AtomicU32::new(0)).collect(),
            spill: Mutex::new(Vec::new()),
        }
    }
    /// Copies `recs` (all for partition `part`) into its region.
    #[inline]
    fn push(&self, part: usize, recs: &[Rec]) {
        let n = recs.len();
        let at = self.fill[part].fetch_add(n as u32, Relaxed) as usize;
        if at + n <= self.cap {
            unsafe {
                let dst = self.buf.add(part * self.cap + at) as *mut u8;
                if n == STAGE && (dst as usize) & 31 == 0 {
                    // a whole batch to a 32-byte boundary: streaming stores, no read of the target lines
                    use std::arch::x86_64::*;
                    let src = recs.as_ptr() as *const u8;
                    for k in 0..(STAGE * 20 / 32) {
                        let v = _mm256_loadu_si256(src.add(32 * k) as *const __m256i);
                        _mm256_stream_si256(dst.add(32 * k) as *mut __m256i, v);
                    }
                } else {
                    std::ptr::copy_nonoverlapping(recs.as_ptr(), self.buf.add(part * self.cap + at), n);
                }
            }
        } else {
            let mut sp = self.spill.lock().unwrap();
            for (k, r) in recs.iter().enumerate() {
                if at + k < self.cap {
                    unsafe { *self.buf.add(part * self.cap + at + k) = *r };
                } else {
                    sp.push((part as u32, *r));
                }
            }
        }
    }
    fn region(&self, part: usize) -> &[Rec] {
        let n = (self.fill[part].load(Relaxed) as usize).min(self.cap);
        unsafe { std::slice::from_raw_parts(self.buf.add(part * self.cap), n) }
    }
}

/// Each thread owns a slot range of the event table. Records travel to their
/// owner in batches and only the owner stores them: plain stores, no races.
struct Mail {
    inbox: Vec<Mutex<Vec<Vec<Rec>>>>,
    published: AtomicUsize,
    owner_mul: u64, // owner = min(nt - 1, (slot * owner_mul) >> 32)
}

#[inline(always)]
fn owner_of(slot: usize, mul: u64, nt: usize) -> usize {
    (((slot as u64) * mul) >> 32).min(nt as u64 - 1) as usize
}

struct ParseOut {
    links: Vec<(u64, u64)>,
    gev: Vec<Vec<GenEvent>>, // by hash % T
    gcl: Vec<Vec<GenClick>>,
    arena: Vec<u8>,
    stage: Vec<Rec>,
    stage_n: Vec<u8>,
    outbox: Vec<Vec<Rec>>, // per owner
    pool: Vec<Vec<Rec>>,
    ring: [(usize, u64, u64); RING],
    ring_n: usize,
    cring: [(usize, u64, u64); RING],
    cring_n: usize,
    plain: bool,
    overflow: Vec<(u32, u64, u64)>,
    max_e: usize,
    max_c: usize,
    dc: DateCache,
}

/// Partition of a row hash (low 32 bits; the dedup sort uses the high 32).
#[inline(always)]
fn part_of(h: u64, nt: usize) -> usize {
    (((h & 0xFFFF_FFFF) * nt as u64) >> 32) as usize
}

#[allow(dead_code)]
fn fnv(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &c in s {
        h ^= c as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    hash64(h)
}

const RING: usize = 16;
const PREFETCHW: bool = true;

/// Stores records into the owner's range: the copy with the smallest ingest wins.
fn place(ev: &Dense, recs: &[Rec]) {
    for (k, r) in recs.iter().enumerate() {
        if let Some(nx) = recs.get(k + 16) {
            unsafe {
                let p = ev.ptr.add(2 * nx.local as usize) as *const u8;
                std::arch::asm!("prefetchw [{0}]", in(reg) p, options(nostack, preserves_flags, readonly));
            }
        }
        let i = r.local as usize;
        let (w0s, w1) = (r.w0s, r.w1);
        if w0s > ev.w0(i).load(Relaxed) {
            ev.w0(i).store(w0s, Relaxed);
            ev.w1(i).store(w1, Relaxed);
        }
    }
}

impl ParseOut {
    /// Hands the outgoing batches to their owners.
    fn publish(&mut self, mail: &Mail) {
        for o in 0..self.outbox.len() {
            if self.outbox[o].is_empty() {
                continue;
            }
            let fresh = self.pool.pop().unwrap_or_else(|| Vec::with_capacity(4096));
            let full = std::mem::replace(&mut self.outbox[o], fresh);
            mail.inbox[o].lock().unwrap().push(full);
        }
    }
    /// Stores every batch sent to thread `me` so far.
    fn drain(&mut self, mail: &Mail, me: usize, ev: &Dense) {
        let got = std::mem::take(&mut *mail.inbox[me].lock().unwrap());
        for mut b in got {
            place(ev, &b);
            b.clear();
            self.pool.push(b);
        }
    }
    #[inline(always)]
    fn stage(&mut self, sc: &Scatter, shift: usize, slot: usize, w0s: u64, w1: u64) {
        let part = slot >> shift;
        let k = self.stage_n[part] as usize;
        self.stage[part * STAGE + k] = Rec { w0s, w1, local: (slot & ((1 << shift) - 1)) as u32 };
        if k + 1 == STAGE {
            sc.push(part, &self.stage[part * STAGE..part * STAGE + STAGE]);
            self.stage_n[part] = 0;
        } else {
            self.stage_n[part] = (k + 1) as u8;
        }
    }
    fn stage_flush(&mut self, sc: &Scatter) {
        unsafe { std::arch::x86_64::_mm_sfence() };
        for part in 0..self.stage_n.len() {
            let k = self.stage_n[part] as usize;
            if k > 0 {
                sc.push(part, &self.stage[part * STAGE..part * STAGE + k]);
                self.stage_n[part] = 0;
            }
        }
    }
    /// Queues a record; the slot is prefetched now and stored RING records later.
    #[inline(always)]
    fn put(&mut self, ev: &Dense, slot: usize, w0s: u64, w1: u64) {
        unsafe {
            let p = ev.ptr.add(2 * slot) as *const u8;
            if PREFETCHW {
                std::arch::asm!("prefetchw [{0}]", in(reg) p, options(nostack, preserves_flags, readonly));
            } else {
                use std::arch::x86_64::*;
                _mm_prefetch::<_MM_HINT_T0>(p as *const i8);
            }
        }
        let k = self.ring_n % RING;
        if self.ring_n >= RING {
            let (s0, a, b) = self.ring[k];
            self.store(ev, s0, a, b);
        }
        self.ring[k] = (slot, w0s, w1);
        self.ring_n += 1;
    }
    #[inline(always)]
    fn store(&mut self, ev: &Dense, slot: usize, w0s: u64, w1: u64) {
        if self.plain {
            // measurement only (not race-free)
            let cur = ev.w0(slot).load(Relaxed);
            if w0s > cur {
                ev.w0(slot).store(w0s, Relaxed);
                ev.w1(slot).store(w1, Relaxed);
            }
            return;
        }
        let prev = ev.w0(slot).fetch_max(w0s, Relaxed);
        if prev == 0 {
            ev.w1(slot).store(w1, Relaxed);
        } else {
            self.overflow.push((slot as u32, w0s, w1));
            if w0s > prev {
                ev.w1(slot).store(w1, Relaxed);
            }
        }
    }
    /// Click rows: copies are identical, so plain stores; prefetched like events.
    #[inline(always)]
    fn put_click(&mut self, cl: &Dense, slot: usize, w0: u64, w1: u64) {
        unsafe {
            let p = cl.ptr.add(2 * slot) as *const u8;
            std::arch::asm!("prefetchw [{0}]", in(reg) p, options(nostack, preserves_flags, readonly));
        }
        let k = self.cring_n % RING;
        if self.cring_n >= RING {
            let (s0, a, b) = self.cring[k];
            cl.w0(s0).store(a, Relaxed);
            cl.w1(s0).store(b, Relaxed);
        }
        self.cring[k] = (slot, w0, w1);
        self.cring_n += 1;
    }
    fn flush_clicks(&mut self, cl: &Dense) {
        let n = self.cring_n.min(RING);
        let start = if self.cring_n >= RING { self.cring_n % RING } else { 0 };
        for j in 0..n {
            let (s0, a, b) = self.cring[(start + j) % RING];
            cl.w0(s0).store(a, Relaxed);
            cl.w1(s0).store(b, Relaxed);
        }
        self.cring_n = 0;
    }
    fn flush(&mut self, ev: &Dense) {
        let n = self.ring_n.min(RING);
        let start = if self.ring_n >= RING { self.ring_n % RING } else { 0 };
        for j in 0..n {
            let (s0, a, b) = self.ring[(start + j) % RING];
            self.store(ev, s0, a, b);
        }
        self.ring_n = 0;
    }
}

static DBG: [AtomicUsize; 6] = [AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0)];
static NOMARK: AtomicBool = AtomicBool::new(false);
static ANOMALY: AtomicBool = AtomicBool::new(false);

#[inline(always)]
fn parse_event(f: &[&[u8]; 7], ctx: &Ctx, ev: &Dense, sc: &Scatter, out: &mut ParseOut, nt: usize) {
    let eid = f[0];
    if eid.is_empty() {
        return;
    }
    let (ts, ingest) = out.dc.ts_pair(f[2], f[3]);
    let ingest = ingest.unwrap_or(i64::MAX);
    let mut valid = !f[1].is_empty() && !f[5].is_empty();
    let ts = match ts {
        Some(t) if t >= ctx.ws && t < ctx.we => t,
        _ => {
            valid = false;
            0
        }
    };
    if ctx.exp == 10 {
        std::hint::black_box((ingest, ts, valid));
        return;
    }
    let et = f[4];
    let mut purchase = false;
    let mut price = 0u32;
    if et.len() == 4 && (et == names::T_V || et == names::T_C) {
    } else if et.len() == 8 && et == names::T_P {
        purchase = true;
        match parse_price(f[6]) {
            Some(p) => price = p,
            None => valid = false,
        }
    } else {
        valid = false;
    }
    if valid && ingest.saturating_sub(ts) > LATE {
        valid = false;
    }
    if ctx.exp == 11 {
        std::hint::black_box((ingest, ts, valid, price));
        return;
    }
    let (uid, cat) = if valid { (uid_enc(f[1]), ctx.product_cat(f[5])) } else { (0, 0) };
    if !valid {
        purchase = false;
        price = 0;
    }
    if ctx.exp == 12 {
        std::hint::black_box((ingest, ts, valid, price, uid, cat, purchase));
        return;
    }
    if ctx.pos {
        if let Some(m) = canon36_fast(eid, b'e') {
            let slot = ctx.mix.decode(m, ctx.ekey);
            if slot < ctx.cap_e {
                let slot = slot as usize;
                let rel = if ingest % 1000 == 0 { ingest / 1000 - ctx.ingest_base_s } else { -1 };
                if !(0..(1 << 22) - 2).contains(&rel) {
                    if !ANOMALY.swap(true, Relaxed) {
                        eprintln!("anomaly: ingest={ingest} rel={rel}");
                    }
                    return;
                }
                let gen = (uid & GEN_TAG != 0) as u64;
                let w0 = ((rel as u64 + 1) << 42)
                    | (((ts - ctx.ws) as u64 * valid as u64) << 11)
                    | ((valid as u64) << 10)
                    | ((purchase as u64) << 9)
                    | ((cat as u64) << 4)
                    | (gen << 3);
                let w1 = (uid & (GEN_TAG - 1)) | ((price as u64) << 40);
                if ctx.exp == 2 {
                    std::hint::black_box(w0 ^ w1);
                    return;
                }
                if ctx.mail {
                    out.outbox[owner_of(slot, ctx.owner_mul, nt)].push(Rec { w0s: !w0, w1, local: slot as u32 });
                } else if ctx.scatter {
                    out.stage(sc, ctx.shift, slot, !w0, w1);
                } else {
                    out.put(ev, slot, !w0, w1);
                }
                if slot > out.max_e {
                    out.max_e = slot;
                }
                return;
            }
        }
    }
    let h = bytes_hash(eid);
    let off = out.arena.len() as u32;
    out.arena.extend_from_slice(eid);
    out.gev[part_of(h, nt)].push(GenEvent {
        eid_off: off,
        eid_len: eid.len() as u32,
        hash: h,
        ingest,
        ts,
        uid,
        price,
        cat,
        valid,
        purchase,
    });
}

#[inline(always)]
fn parse_click(f: &[&[u8]; 4], ctx: &Ctx, cl: &Dense, out: &mut ParseOut, nt: usize) {
    let cid = f[0];
    if cid.is_empty() || f[1].is_empty() {
        return;
    }
    let ts = match out.dc.ts(f[3]) {
        Some(t) if t >= ctx.ws - WEEK && t < ctx.we => t,
        _ => return,
    };
    let camp = match ctx.campaign(f[2]) {
        Some(c) => c,
        None => return,
    };
    let uid = uid_enc(f[1]);
    if ctx.pos {
        if let Some(m) = canon36_fast(cid, b'k') {
            let slot = ctx.mix.decode(m, ctx.ckey);
            if slot < ctx.cap_c {
                let slot = slot as usize;
                let gen = (uid & GEN_TAG != 0) as u64;
                let w0 = (1u64 << 63) | (((ts - (ctx.ws - WEEK)) as u64) << 20) | ((camp as u64) << 8) | gen;
                out.put_click(cl, slot, w0, uid & (GEN_TAG - 1));
                if slot > out.max_c {
                    out.max_c = slot;
                }
                return;
            }
        }
    }
    let h = bytes_hash(cid);
    let off = out.arena.len() as u32;
    out.arena.extend_from_slice(cid);
    out.gcl[part_of(h, nt)].push(GenClick { cid_off: off, cid_len: cid.len() as u32, hash: h, ts, uid, camp });
}

// ---------------------------------------------------------------- key discovery

fn sample_lines(files: &[(PathBuf, &'static [u8])], per_file: usize, nfields: usize) -> (Vec<Vec<Vec<u8>>>, f64) {
    let mut idx: Vec<usize> = (0..files.len()).collect();
    idx.sort_by_key(|&i| std::cmp::Reverse(files[i].1.len()));
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    let mut lines = 0usize;
    for &i in idx.iter().take(4) {
        let d = files[i].1;
        let end = d.len().min(per_file);
        for_lines(d, 0, end, |line| {
            let mut f: [&[u8]; 7] = [&[]; 7];
            let ok = match nfields {
                7 => split::<7>(line, &mut f),
                _ => {
                    let mut g: [&[u8]; 4] = [&[]; 4];
                    let ok = split::<4>(line, &mut g);
                    f[..4].copy_from_slice(&g);
                    ok
                }
            };
            bytes += line.len() + 1;
            lines += 1;
            if ok {
                rows.push(vec![f[0].to_vec(), f[1].to_vec()]);
            }
        });
    }
    let avg = if lines == 0 { 60.0 } else { bytes as f64 / lines as f64 };
    (rows, avg)
}

/// Finds the ID keys from a sample, or None. Never affects correctness, only speed.
fn find_keys(mix: &Mix, ev_rows: &[Vec<Vec<u8>>], cl_rows: &[Vec<Vec<u8>>], est_e: u64, est_c: u64) -> Option<(u64, u64, u64)> {
    // user key: the most common user ID is a whale's, numbered person*512+alias with person < 5
    let mut freq: HashMap<u64, usize> = HashMap::new();
    for r in ev_rows {
        if let Some(u) = canon36(&r[1], b'u') {
            *freq.entry(u).or_default() += 1;
        }
    }
    let top = freq.iter().max_by_key(|(_, &c)| c).map(|(&u, _)| u)?;
    let distinct: Vec<u64> = freq.keys().copied().take(4000).collect();
    let max_users = est_e / 10 + 10_000;
    let mut ukey = None;
    for cand in (0..5u64).flat_map(|p| (0..8u64).map(move |a| p * 512 + a)) {
        let k = mix.key_for(top, cand);
        let ok = distinct.iter().filter(|&&u| {
            let n = mix.decode(u, k);
            n < (1 << 39) && (n >> 9) < max_users
        });
        let c = ok.count();
        if c * 10 >= distinct.len() * 9 {
            ukey = Some(k);
            break;
        }
    }
    let ukey = ukey?;
    let person = |u: &[u8]| -> Option<u64> {
        let n = mix.decode(canon36(u, b'u')?, ukey);
        if n < (1 << 39) {
            Some(n >> 9)
        } else {
            None
        }
    };
    let solve = |rows: &[Vec<Vec<u8>>], idf: usize, uidf: usize, prefix: u8, est: u64, share: f64| -> Option<u64> {
        let mut first: Vec<u64> = Vec::new();
        for r in rows {
            if person(&r[uidf]) == Some(0) {
                if let Some(m) = canon36(&r[idf], prefix) {
                    if !first.contains(&m) {
                        first.push(m);
                    }
                }
            }
        }
        if first.len() < 3 {
            return None;
        }
        let all: Vec<u64> = rows.iter().filter_map(|r| canon36(&r[idf], prefix)).collect();
        let bound = ((est as f64) * share) as u64 + 1000;
        let x1 = mix.unmix(first[0]);
        for g in 0..bound {
            let k = (x1 ^ g.wrapping_mul(M1)) & MASK40;
            if first[1..first.len().min(5)].iter().all(|&m| mix.decode(m, k) < bound) {
                let good = all.iter().filter(|&&m| mix.decode(m, k) < est * 3 / 2 + 1000).count();
                if good * 100 >= all.len() * 97 {
                    return Some(k);
                }
            }
        }
        None
    };
    let ekey = solve(ev_rows, 0, 1, b'e', est_e, 0.04)?;
    let ckey = solve(cl_rows, 0, 1, b'k', est_c, 0.04)?;
    Some((ekey, ckey, ukey))
}

// ---------------------------------------------------------------- per-person processing

#[derive(Clone, Copy)]
struct Ev {
    ts: i64,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Cl {
    ts: i64,
    camp: u16,
    // id for tie-breaks: canonical value (with slot) or generic arena ref
    key: ClKey,
}

#[derive(Clone, Copy)]
enum ClKey {
    Canon(u64), // base-36 value m of "k" + base36(m)
    Gen(u32, u32, u32), // thread, off, len into the generic-click arena
}

struct UserRow {
    comp: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u16,
}

struct Out {
    users: Vec<UserRow>,
    camp: Vec<[u64; 4]>, // (camp, day): sessions, purchasing, revenue, assisted
    cat_rev: Vec<u64>,
    cat_touched: Vec<u16>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u16>,
    neg: bool, // subtract instead of add, and emit no rows (undo of an optimistic person)
}

fn process_person(ctx: &Ctx, comp: u64, evs: &[Ev], cls: &[Cl], out: &mut Out) {
    let ncamp = ctx.camp_names.len();
    let ndays = ctx.ndays;
    let organic = ncamp;
    let mut i = 0;
    let n = evs.len();
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut cur_day: i64 = -1;
    let mut row = UserRow { comp, day: 0, sessions: 0, events: 0, revenue: 0, top: u16::MAX };
    while i < n {
        let start = evs[i].ts;
        let day = start.div_euclid(DAY) - ctx.day0;
        if day != cur_day {
            if cur_day >= 0 {
                flush_row(&mut row, out);
            }
            cur_day = day;
            row.day = (ctx.day0 + day) as i32;
        }
        let mut last = start;
        let mut j = i;
        let mut rev = 0u64;
        let mut purch = false;
        while j < n && evs[j].ts - last <= GAP {
            let e = evs[j];
            last = e.ts;
            if e.purchase {
                purch = true;
                rev += e.price as u64;
                let c = e.cat as usize;
                if out.cat_rev[c] == 0 {
                    out.cat_touched.push(c as u16);
                }
                out.cat_rev[c] += e.price as u64;
            }
            j += 1;
        }
        row.sessions += 1;
        row.events += (j - i) as u32;
        row.revenue += rev;
        // attribution
        // sliding window [start - WEEK, start] with per-campaign counts and the distinct set
        while hi < cls.len() && cls[hi].ts <= start {
            let c = cls[hi].camp as usize;
            if out.cnt[c] == 0 {
                out.pos[c] = out.active.len() as u32;
                out.active.push(c as u16);
            }
            out.cnt[c] += 1;
            hi += 1;
        }
        while lo < hi && cls[lo].ts < start - WEEK {
            let c = cls[lo].camp as usize;
            out.cnt[c] -= 1;
            if out.cnt[c] == 0 {
                let p = out.pos[c] as usize;
                let last = out.active.pop().unwrap();
                if last as usize != c {
                    out.active[p] = last;
                    out.pos[last as usize] = p as u32;
                }
            }
            lo += 1;
        }
        let d = day as usize;
        let credited = if hi > lo {
            let t = cls[hi - 1].ts;
            let mut k = hi - 1;
            while k > lo && cls[k - 1].ts == t {
                k -= 1;
            }
            cls[k].camp as usize
        } else {
            organic
        };
        let sg: u64 = if out.neg { u64::MAX } else { 1 };
        let a = &mut out.camp[credited * ndays + d];
        a[0] = a[0].wrapping_add(sg);
        a[1] = a[1].wrapping_add(sg.wrapping_mul(purch as u64));
        a[2] = a[2].wrapping_add(sg.wrapping_mul(rev));
        for &c in &out.active {
            let x = &mut out.camp[c as usize * ndays + d][3];
            *x = x.wrapping_add(sg);
        }
        i = j;
    }
    for &c in &out.active {
        out.cnt[c as usize] = 0;
    }
    out.active.clear();
    if cur_day >= 0 {
        flush_row(&mut row, out);
    }
}

#[inline]
fn flush_row(row: &mut UserRow, out: &mut Out) {
    let mut best = u16::MAX;
    let mut best_v = 0u64;
    for &c in &out.cat_touched {
        let v = out.cat_rev[c as usize];
        if v > best_v || (v == best_v && c < best) {
            best_v = v;
            best = c;
        }
        out.cat_rev[c as usize] = 0;
    }
    out.cat_touched.clear();
    if out.neg {
        row.sessions = 0;
        row.events = 0;
        row.revenue = 0;
        return;
    }
    out.users.push(UserRow { comp: row.comp, day: row.day, sessions: row.sessions, events: row.events, revenue: row.revenue, top: best });
    row.sessions = 0;
    row.events = 0;
    row.revenue = 0;
}

// ---------------------------------------------------------------- runs and occupancy bits

#[derive(Clone, Copy)]
struct Run {
    comp: u64,
    loc: u32, // local person index (u32::MAX if not local)
    p: u64, // generator person of the first row's ID (ordering hint only)
    a: u32,
    b: u32,
    sorted: bool,
}

const EV_HAVE: u64 = 1;
const EV_MULTI: u64 = 2;
const CL_HAVE: u64 = 4;
const CL_MULTI: u64 = 8;

/// Four bits per hashed person: has event runs / more than one / has click runs / more than one.
/// Collisions only make a person look complex (slower path), never simple.
struct Bits {
    w: Vec<AtomicU64>,
    mask: u64,
}

impl Bits {
    /// Indexed by the key itself (keys below `entries`), no hashing.
    fn direct(entries: usize) -> Bits {
        let mut b = Bits::new(entries);
        b.mask = u64::MAX; // marks "direct"
        b
    }
    fn new(entries: usize) -> Bits {
        let words = (entries / 16).max(1);
        // zeroed by the allocator (fresh pages), not by a serial loop
        let v: Vec<u64> = huge_zeroed(words);
        let mut v = std::mem::ManuallyDrop::new(v);
        let w = unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU64, v.len(), v.capacity()) };
        Bits { w, mask: (entries as u64) - 1 }
    }
    #[inline(always)]
    fn at(&self, comp: u64) -> (usize, u32) {
        if self.mask == u64::MAX {
            return ((comp >> 4) as usize, ((comp & 15) * 4) as u32);
        }
        let h = (hash64(comp ^ 0x2545_F491_4F6C_DD1D) >> 20) & self.mask;
        ((h >> 4) as usize, ((h & 15) * 4) as u32)
    }
    /// Sets `have`; if it was already set, sets `multi` too and returns true.
    #[inline(always)]
    fn mark(&self, comp: u64, have: u64, multi: u64) -> bool {
        let (i, sh) = self.at(comp);
        let old = self.w[i].fetch_or(have << sh, Relaxed);
        if old & (have << sh) != 0 {
            self.w[i].fetch_or(multi << sh, Relaxed);
            return true;
        }
        false
    }
    #[inline(always)]
    fn force(&self, comp: u64, bits: u64) {
        let (i, sh) = self.at(comp);
        self.w[i].fetch_or(bits << sh, Relaxed);
    }
    #[inline(always)]
    fn get(&self, comp: u64) -> u64 {
        let (i, sh) = self.at(comp);
        (self.w[i].load(Relaxed) >> sh) & 15
    }
}

#[inline(always)]
fn person_hint(ctx: &Ctx, uid: u64) -> u64 {
    if uid & GEN_TAG != 0 {
        return u64::MAX;
    }
    let n = ctx.mix.decode(uid, ctx.ukey);
    if n < (1 << 39) {
        n >> 9
    } else {
        u64::MAX - 1
    }
}

fn decode_events(ev: &Dense, a: usize, b: usize, ctx: &Ctx, evbuf: &mut Vec<Ev>) {
    for i in a..b {
        let s0 = ev.w0(i).load(Relaxed);
        if s0 == 0 {
            continue;
        }
        let w0 = !s0;
        if w0 & (1 << 10) == 0 {
            continue;
        }
        let w1 = ev.w1(i).load(Relaxed);
        evbuf.push(Ev {
            ts: ctx.ws + ((w0 >> 11) & 0x7FFF_FFFF) as i64,
            price: (w1 >> 40) as u32,
            cat: ((w0 >> 4) & 31) as u16,
            purchase: w0 & (1 << 9) != 0,
        });
    }
}

fn decode_clicks(cl: &Dense, a: usize, b: usize, ctx: &Ctx, clbuf: &mut Vec<Cl>) {
    for i in a..b {
        let w0 = cl.w0(i).load(Relaxed);
        if w0 == 0 {
            continue;
        }
        clbuf.push(Cl {
            ts: ctx.ws - WEEK + ((w0 >> 20) & 0x7FFF_FFFF) as i64,
            camp: ((w0 >> 8) & 0xFFF) as u16,
            key: ClKey::Canon(scramble(i as u64, ctx.ckey)),
        });
    }
}

/// Direct-mapped cache in front of the link table.
struct CompCache {
    k: [u64; 64],
    v: [u64; 64],
}

impl CompCache {
    fn new() -> Self {
        CompCache { k: [u64::MAX; 64], v: [0; 64] }
    }
    #[inline(always)]
    fn get(&mut self, links: &Links, uid: u64) -> u64 {
        let h = (hash64(uid) >> 58) as usize;
        if self.k[h] == uid {
            return self.v[h];
        }
        let c = links.comp_fast(uid);
        self.k[h] = uid;
        self.v[h] = c;
        c
    }
}

fn new_out(ctx: &Ctx) -> Out {
    let ncamp = ctx.camp_names.len();
    Out {
        users: Vec::new(),
        camp: vec![[0u64; 4]; (ncamp + 1) * ctx.ndays],
        cat_rev: vec![0; ctx.cats.len()],
        cat_touched: Vec::new(),
        cnt: vec![0; ncamp + 1],
        pos: vec![0; ncamp + 1],
        active: Vec::new(),
        neg: false,
    }
}

fn write_users(path: &Path, users: &[UserRow], ctx: &Ctx, links: &Links) {
    let n = users.iter().filter(|r| r.sessions != 0).count();
    let mut c_user = parquet::Column::new(names::A_ID, parquet::Kind::Str, n);
    let mut c_day = parquet::Column::new(names::DAY, parquet::Kind::Date, n);
    let mut c_s = parquet::Column::new(names::N_S, parquet::Kind::I32, n);
    let mut c_e = parquet::Column::new(names::N_E, parquet::Kind::I32, n);
    // revenue as INT32 when every value of this file fits (any integer width is accepted)
    let rev32 = users.iter().all(|r| r.revenue <= i32::MAX as u64);
    let mut c_r = parquet::Column::new(names::REV, if rev32 { parquet::Kind::I32 } else { parquet::Kind::I64 }, n);
    let mut c_t = parquet::Column::new(names::TOP, parquet::Kind::OptStr, n);
    let mut buf = Vec::new();
    let mut last = u64::MAX;
    for r in users {
        if r.sessions == 0 {
            continue; // removed by an undo
        }
        // a person's rows are consecutive: its name is built once
        if r.comp != last || last == u64::MAX {
            uid_bytes(links.name_of(r.comp), &mut buf);
            last = r.comp;
        }
        c_user.push_str(&buf);
        c_day.push_i32(r.day);
        c_s.push_i32(r.sessions as i32);
        c_e.push_i32(r.events as i32);
        if rev32 {
            c_r.push_i32(r.revenue as i32);
        } else {
            c_r.push_i64(r.revenue as i64);
        }
        c_t.push_opt(if r.top == u16::MAX { None } else { Some(&ctx.cats[r.top as usize]) });
    }
    let tb = Instant::now();
    parquet::write(path, &[c_user, c_day, c_s, c_e, c_r, c_t]).expect("write");
    if std::env::var("SS_TIMING2").is_ok() {
        eprintln!("  file {:.1} ms", tb.elapsed().as_secs_f64() * 1e3);
    }
}

// ---------------------------------------------------------------- person buckets

#[derive(Clone, Copy)]
struct Entry {
    comp: u64,
    kind: u8, // 0 event run, 1 click run, 2 generic event, 3 generic click
    sorted: bool,
    t: u16,
    a: u32,
    b: u32,
    c: u32, // kind 2: price (with a, b = ts, t = cat, sorted = purchase)
}

#[inline(always)]
fn bucket_of(comp: u64) -> usize {
    (hash64(comp ^ 0x5555) >> 40) as usize % NB
}

// ---------------------------------------------------------------- main

fn main() {
    if std::env::var("SS_PROBE").is_ok() {
        probe::run();
    }
    // Fewer, larger heap regions: big buffers come from one padded arena instead of
    // fresh mappings, so threads fault less and contend less on the address-space lock.
    if std::env::var("SS_NOMALLOPT").is_err() {
        unsafe {
            libc::mallopt(-8, 1); // M_ARENA_MAX
            libc::mallopt(-3, 64 << 20); // M_MMAP_THRESHOLD
            libc::mallopt(-1, 256 << 20); // M_TRIM_THRESHOLD
            libc::mallopt(-2, 64 << 20); // M_TOP_PAD
        }
    }
    let t0 = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nt: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or_else(|| {
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)
    });
    let force_generic = std::env::var("SS_GENERIC").is_ok();
    let timing = std::env::var("SS_TIMING").is_ok();
    let lap = |what: &str| {
        if timing {
            eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, what);
        }
    };

    let manifest = std::fs::read_to_string(data_dir.join(names::META)).expect("meta");
    let ws = parse_ts(json_field(&manifest, names::META_S).as_bytes()).expect("start");
    let we = parse_ts(json_field(&manifest, names::META_E).as_bytes()).expect("end");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;

    // products and campaigns load on a helper thread while the inputs are mapped and sampled
    let lookups = std::thread::spawn({
        let data_dir = data_dir.clone();
        move || {
        // products and categories: canonical IDs ("p" + number) go to a dense table,
        // any other spelling to a map; categories are numbered after sorting
        let mut cat_names: Vec<Vec<u8>> = Vec::new();
        let mut dense_tmp: Vec<u16> = Vec::new();
        let mut odd_tmp: Vec<(Vec<u8>, u16)> = Vec::new();
        read_small_csv(&data_dir.join(names::F_P), |p, c| {
            let ci = match cat_names.iter().position(|x| x.as_slice() == c) {
                Some(i) => i,
                None => {
                    cat_names.push(c.to_vec());
                    cat_names.len() - 1
                }
            } as u16;
            match canon10(p, b"p") {
                Some(n) if n < 10_000_000 => {
                    if dense_tmp.len() <= n as usize {
                        dense_tmp.resize(n as usize + 1, 0);
                    }
                    dense_tmp[n as usize] = ci + 1;
                }
                _ => odd_tmp.push((p.to_vec(), ci)),
            }
        });
        let mut cats: Vec<Vec<u8>> = cat_names.clone();
        cats.push(names::UNK.to_vec());
        cats.sort();
        cats.dedup();
        let remap: Vec<u16> = cat_names.iter().map(|c| cats.binary_search(c).unwrap() as u16).collect();
        let unknown_cat = cats.binary_search(&names::UNK.to_vec()).unwrap() as u16;
        let prod_dense: Vec<u16> = dense_tmp.iter().map(|&v| if v == 0 { 0 } else { remap[v as usize - 1] + 1 }).collect();
        let prod_map: HashMap<Vec<u8>, u16> = odd_tmp.into_iter().map(|(p, ci)| (p, remap[ci as usize])).collect();
        // campaigns
        let mut camp_names = Vec::new();
        let mut camp_channels = Vec::new();
        read_small_csv(&data_dir.join(names::F_C), |a, b| {
            camp_names.push(a.to_vec());
            camp_channels.push(b.to_vec());
        });
        let mut camp_dense: Vec<u16> = Vec::new();
        let mut camp_map: HashMap<Vec<u8>, u16> = HashMap::new();
        for (i, c) in camp_names.iter().enumerate() {
            match canon10(c, b"c_") {
                Some(n) if n < 10_000_000 => {
                    if camp_dense.len() <= n as usize {
                        camp_dense.resize(n as usize + 1, 0);
                    }
                    camp_dense[n as usize] = i as u16 + 1;
                }
                _ => {
                    camp_map.insert(c.clone(), i as u16);
                }
            }
        }
            (cats, unknown_cat, prod_dense, prod_map, camp_names, camp_channels, camp_dense, camp_map)
        }
    });

    let map_all = |sub: &str| -> Vec<(PathBuf, &'static [u8])> {
        list_csv(&data_dir.join(sub)).into_iter().map(|p| {
            let d = map_file(&p);
            (p, d)
        }).collect()
    };
    let link_files = map_all(names::D_L);
    let event_files = map_all(names::D_E);
    let click_files = map_all(names::D_C);
    lap("inputs");

    let mix = Mix::new();
    let ev_bytes: usize = event_files.iter().map(|f| f.1.len()).sum();
    let cl_bytes: usize = click_files.iter().map(|f| f.1.len()).sum();
    lap("mapped");
    let (ev_sample, ev_avg) = sample_lines(&event_files, 128 << 10, 7);
    let (cl_sample, cl_avg) = sample_lines(&click_files, 128 << 10, 4);
    let est_e = (ev_bytes as f64 / ev_avg) as u64;
    let est_c = (cl_bytes as f64 / cl_avg) as u64;
    let mut pos = false;
    let (mut ekey, mut ckey, mut ukey) = (0, 0, 0);
    lap("sampled");
    let (cats, unknown_cat, prod_dense, prod_map, camp_names, camp_channels, camp_dense, camp_map) = lookups.join().unwrap();
    lap("lookups joined");
    let ncamp = camp_names.len();
    if !force_generic && (we - ws) <= 17 * DAY && cats.len() <= 31 && ncamp < 4095 {
        if let Some((a, b, c)) = find_keys(&mix, &ev_sample, &cl_sample, est_e, est_c) {
            pos = true;
            ekey = a;
            ckey = b;
            ukey = c;
        }
    }
    lap(&format!("keys pos={pos} est_e={est_e} est_c={est_c}"));

    let ctx = Ctx {
        ws,
        we,
        day0,
        ndays,
        cats,
        unknown_cat,
        prod_dense,
        prod_map,
        camp_names,
        camp_channels,
        camp_dense,
        camp_map,
        mix,
        pos,
        ekey,
        ckey,
        ukey,
        cap_e: if pos { est_e * 2 + 4096 } else { 0 },
        // reservations for the generic lists: the estimate plus 10%, capped against bad estimates
        ge_cap: (est_e + est_e / 10 + 1024).min(400_000_000),
        gc_cap: (est_c + est_c / 10 + 1024).min(200_000_000),
        cap_c: if pos { est_c * 2 + 4096 } else { 0 },
        ingest_base_s: ws / 1000 - 15 * 86_400,
        shift: 15,
        nparts: 0,
        scatter: false,
        mail: false,
        owner_mul: 0,
        tok_pdep: std::env::var("SS_TOK").map(|v| v == "pdep").unwrap_or(true),
        part_cap: 0,
        exp: std::env::var("SS_EXP").ok().and_then(|v| v.parse().ok()).unwrap_or(0),
    };
    run(ctx, nt, &link_files, &event_files, &click_files, &out_dir, t0, timing);
}

#[allow(clippy::too_many_arguments)]
fn run(
    mut ctx: Ctx,
    nt: usize,
    link_files: &[(PathBuf, &'static [u8])],
    event_files: &[(PathBuf, &'static [u8])],
    click_files: &[(PathBuf, &'static [u8])],
    out_dir: &Path,
    t0: Instant,
    timing: bool,
) {
    let lap = |what: &str| {
        if timing {
            eprintln!("{:>8.1} ms  {}", t0.elapsed().as_secs_f64() * 1e3, what);
        }
    };
    // chunks: (kind, file, start, end)
    let ch: usize = std::env::var("SS_CH").ok().and_then(|v| v.parse().ok()).unwrap_or(1 << 20);
    let mut chunks: Vec<(u8, usize, usize, usize)> = Vec::new();
    let mut add = |kind: u8, files: &[(PathBuf, &'static [u8])], chunks: &mut Vec<(u8, usize, usize, usize)>| {
        for (i, f) in files.iter().enumerate() {
            let len = f.1.len();
            let mut s = 0;
            while s < len {
                let e = (s + ch).min(len);
                chunks.push((kind, i, s, e));
                s = e;
            }
        }
    };
    add(0, link_files, &mut chunks);
    let nlink = chunks.len();
    let mut rest = Vec::new();
    add(1, event_files, &mut rest);
    add(2, click_files, &mut rest);
    rest.sort_by_key(|c| std::cmp::Reverse(c.3 - c.2));
    chunks.extend(rest);

    ctx.scatter = ctx.pos && std::env::var("SS_SCATTER").map(|v| v != "0").unwrap_or(true);
    if ctx.scatter {
        // partitions of 16K to 64K slots (256 KB to 1 MB of table): placed in cache later
        let span = std::env::var("SS_SPAN").ok().and_then(|v| v.parse().ok()).unwrap_or(((ctx.cap_e / 2) as usize / 1024).next_power_of_two().clamp(1 << 14, 1 << 16));
        ctx.shift = span.trailing_zeros() as usize;
        ctx.nparts = ((ctx.cap_e as usize) >> ctx.shift) + 1;
        ctx.part_cap = (span + span / 4 + 64 + 15) & !15; // multiple of 16 records keeps regions 320-byte aligned
    } else if ctx.pos {
        // slot ranges for the run pass
        let target = ((ctx.cap_e / 2) as usize / (4 * nt)).max(1 << 12);
        ctx.shift = (usize::BITS - 1 - target.leading_zeros()) as usize;
        ctx.nparts = ((ctx.cap_e as usize) >> ctx.shift) + 1;
    } else {
        ctx.nparts = 1;
    }
    ctx.mail = ctx.pos && !ctx.scatter && std::env::var("SS_MAIL").is_ok();
    if ctx.mail {
        // equal slot ranges over the expected numbers (cap_e is twice the estimate)
        let span = (ctx.cap_e / 2).max(1) as f64 * 1.02;
        ctx.owner_mul = ((nt as f64) * (1u64 << 32) as f64 / span) as u64;
    }
    let mail = Mail { inbox: (0..nt).map(|_| Mutex::new(Vec::new())).collect(), published: AtomicUsize::new(0), owner_mul: ctx.owner_mul };
    let sc = Scatter::new(if ctx.scatter { ctx.nparts } else { 1 }, if ctx.scatter { ctx.part_cap } else { 1 });
    let ev = Dense::new(if ctx.pos { ctx.cap_e as usize } else { 1 });
    let cl = Dense::new(if ctx.pos { ctx.cap_c as usize } else { 1 });

    let use_mmap = std::env::var("SS_MMAP").is_ok();
    let open_all = |files: &[(PathBuf, &'static [u8])]| -> Vec<std::fs::File> {
        files.iter().map(|f| std::fs::File::open(&f.0).expect("open")).collect()
    };
    let (lfd, efd, cfd) = (open_all(link_files), open_all(event_files), open_all(click_files));
    let remaining: Vec<Vec<AtomicUsize>> = [link_files, event_files, click_files]
        .iter()
        .enumerate()
        .map(|(k, fs)| {
            (0..fs.len())
                .map(|fi| AtomicUsize::new(chunks.iter().filter(|c| c.0 as usize == k && c.1 == fi).count()))
                .collect()
        })
        .collect();
    let next = AtomicUsize::new(0);
    let links_done = AtomicUsize::new(0);
    let link_pairs: Mutex<Vec<Vec<(u64, u64)>>> = Mutex::new(Vec::new());
    let ctxr = &ctx;

    let uf_slot: Mutex<Option<Links>> = Mutex::new(None);
    let build_uf = || {
        let pairs = std::mem::take(&mut *link_pairs.lock().unwrap());
        let tb = Instant::now();
        let l = Links::build(pairs, if ctx.pos { Some((ctx.mix, ctx.ukey)) } else { None });
        if timing {
            eprintln!("   union-find {:.1} ms", tb.elapsed().as_secs_f64() * 1e3);
        }
        *uf_slot.lock().unwrap() = Some(l);
    };
    let (outs, links): (Vec<ParseOut>, Links) = std::thread::scope(|s| {
        let mut hs = Vec::new();
        for me in 0..nt {
            let mail = &mail;
            let (ev, cl, sc) = (&ev, &cl, &sc);
            let (next, chunks, link_pairs, links_done, remaining, lfd, efd, cfd, build_uf) = (&next, &chunks, &link_pairs, &links_done, &remaining, &lfd, &efd, &cfd, &build_uf);
            hs.push(s.spawn(move || {
                pin_self();
                let mut out = ParseOut {
                    links: Vec::new(),
                    // generic mode: room for this thread's share up front (no regrowth copies)
                    gev: (0..nt).map(|_| Vec::with_capacity(if ctxr.pos { 0 } else { (ctxr.ge_cap as usize) / (nt * nt) })).collect(),
                    gcl: (0..nt).map(|_| Vec::with_capacity(if ctxr.pos { 0 } else { (ctxr.gc_cap as usize) / (nt * nt) })).collect(),
                    arena: Vec::with_capacity(if ctxr.pos { 0 } else { ((ctxr.ge_cap + ctxr.gc_cap) as usize / nt) * 14 }),
                    outbox: (0..nt).map(|_| Vec::with_capacity(4096)).collect(),
                    pool: Vec::new(),
                    stage: if ctxr.scatter { vec![Rec { w0s: 0, w1: 0, local: 0 }; ctxr.nparts * STAGE] } else { Vec::new() },
                    stage_n: if ctxr.scatter { vec![0u8; ctxr.nparts] } else { Vec::new() },
                    ring: [(0, 0, 0); RING],
                    ring_n: 0,
                    cring: [(0, 0, 0); RING],
                    cring_n: 0,
                    plain: ctxr.exp == 6,
                    overflow: Vec::new(),
                    max_e: 0,
                    max_c: 0,
                    dc: DateCache::new(),
                };
                let mut did_links = false;
                let mut rbuf: Vec<u8> = Vec::new();
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    if i >= chunks.len() {
                        break;
                    }
                    if i >= nlink && !did_links {
                        did_links = true;
                        link_pairs.lock().unwrap().push(std::mem::take(&mut out.links));
                        if links_done.fetch_add(1, Relaxed) + 1 == nt {
                            build_uf();
                        }
                    }
                    let (kind, fi, a0, b0) = chunks[i];
                    let (fset, fds) = match kind {
                        0 => (link_files, &lfd),
                        1 => (event_files, &efd),
                        _ => (click_files, &cfd),
                    };
                    let (d, a, b) = if use_mmap {
                        (fset[fi].1, a0, b0)
                    } else {
                        load_chunk(&mut rbuf, &fds[fi], fset[fi].1.len(), a0, b0)
                    };
                    match kind {
                        0 => {
                            scan::lines::<2>(d, a, b, |f| {
                                if !f[0].is_empty() && !f[1].is_empty() {
                                    out.links.push((uid_enc(f[0]), uid_enc(f[1])));
                                }
                            });
                        }
                        1 => {
                            match ctxr.exp {
                                3 => {
                                    std::hint::black_box(memchr::memchr_iter(b'\n', &d[a..b]).count());
                                }
                                1 => {
                                    let mut c = 0usize;
                                    if ctxr.tok_pdep {
                                        scan::lines_pdep::<7>(d, a, b, |f| c += f[0].len() + f[6].len());
                                    } else {
                                        scan::lines::<7>(d, a, b, |f| c += f[0].len() + f[6].len());
                                    }
                                    std::hint::black_box(c);
                                }
                                _ => {
                                    if ctxr.tok_pdep {
                                        scan::lines_pdep::<7>(d, a, b, |f| parse_event(f, ctxr, &ev, &sc, &mut out, nt))
                                    } else {
                                        scan::lines::<7>(d, a, b, |f| parse_event(f, ctxr, &ev, &sc, &mut out, nt))
                                    }
                                }
                            }
                        }
                        _ => {
                            scan::lines::<4>(d, a, b, |f| parse_click(f, ctxr, &cl, &mut out, nt));
                        }
                    }
                    if ctxr.mail && kind == 1 {
                        out.publish(mail);
                        out.drain(mail, me, ev);
                    }
                    // the last worker done with a file unmaps it, so exit has less to tear down
                    if remaining[kind as usize][fi].fetch_sub(1, Relaxed) == 1 && use_mmap {
                        let m = fset[fi].1;
                        if !m.is_empty() {
                            unsafe { libc::munmap(m.as_ptr() as *mut libc::c_void, m.len()) };
                        }
                    }
                }
                out.flush(&ev);
                if ctxr.mail {
                    // last batches out, wait for every thread to do the same, then store what is ours
                    out.publish(mail);
                    mail.published.fetch_add(1, Relaxed);
                    while mail.published.load(Relaxed) < nt {
                        out.drain(mail, me, ev);
                        std::thread::yield_now();
                    }
                    out.drain(mail, me, ev);
                }
                if ctxr.scatter {
                    out.stage_flush(&sc);
                }
                out.flush_clicks(&cl);
                if !did_links {
                    link_pairs.lock().unwrap().push(std::mem::take(&mut out.links));
                    if links_done.fetch_add(1, Relaxed) + 1 == nt {
                        build_uf();
                    }
                }
                out
            }));
        }
        let outs = hs.into_iter().map(|h| h.join().unwrap()).collect();
        (outs, uf_slot.lock().unwrap().take().expect("links"))
    });
    lap("parse + links");

    if ANOMALY.load(Relaxed) || (std::env::var("SS_FORCE_ANOMALY").is_ok() && ctx.pos) {
        // Rare: an input the dense layout cannot hold. Redo everything on the generic path.
        // The inputs may already be unmapped: start over as a fresh process on the generic path.
        eprintln!("anomaly: falling back to the generic path");
        let exe = std::env::current_exe().expect("exe");
        let st = std::process::Command::new(exe).env("SS_GENERIC", "1").status().expect("rerun");
        unsafe { libc::_exit(st.code().unwrap_or(1)) };
    }
    let ctx = &ctx;

    let max_e = outs.iter().map(|o| o.max_e).max().unwrap_or(0) + 1;
    let max_c = outs.iter().map(|o| o.max_c).max().unwrap_or(0) + 1;
    lap("max");

    // generic dedup (events: smallest ingest; clicks: any copy), per hash partition
    let arenas: Vec<&Vec<u8>> = outs.iter().map(|o| &o.arena).collect();
    // kept rows are referenced as (thread, index) into that thread's list for this partition
    let (gev_kept, gcl_kept): (Vec<Vec<(u16, u32)>>, Vec<Vec<(u16, u32)>>) = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|p| {
                let outs = &outs;
                let arenas = &arenas;
                s.spawn(move || {
                pin_self();
                    // events: group by hash (radix sort); per distinct ID the copy with the
                    // smallest ingest is kept, then dropped if invalid
                    // Compact form first: 16-byte records (hash, ingest rel 32 | valid 1 | thread 4 | index 27).
                    // Anything that does not fit (ingest far outside the window, huge lists) takes the exact
                    // 24-byte form below; both keep, per distinct ID, the copy with the smallest ingest.
                    let ibase = ctx.ws - 15 * DAY;
                    let fits = outs.len() <= 16 && outs.iter().all(|o| o.gev[p].len() < (1 << 27));
                    let mut compact: Option<Vec<(u16, u32)>> = None;
                    if fits {
                        let mut r16: Vec<(u64, u64)> = Vec::with_capacity(outs.iter().map(|o| o.gev[p].len()).sum());
                        let mut ok = true;
                        'build: for (t, o) in outs.iter().enumerate() {
                            for (i, g) in o.gev[p].iter().enumerate() {
                                let rel: u64 = if g.ingest == i64::MAX {
                                    0xFFFF_FFFF
                                } else {
                                    let d = g.ingest.wrapping_sub(ibase);
                                    if g.ingest < ibase || d >= 0xFFFF_FFFF {
                                        ok = false;
                                        break 'build;
                                    }
                                    d as u64
                                };
                                r16.push((g.hash, (rel << 32) | ((g.valid as u64) << 31) | ((t as u64) << 27) | i as u64));
                            }
                        }
                        if ok {
                            radix_sort(&mut r16, |r| r.0 >> 32, 32);
                            let key16 = |w: u64| -> &[u8] {
                                let (t, i) = (((w >> 27) & 15) as usize, (w & ((1 << 27) - 1)) as usize);
                                let g = &outs[t].gev[p][i];
                                &arenas[t][g.eid_off as usize..(g.eid_off + g.eid_len) as usize]
                            };
                            let tid = |w: u64| -> (u16, u32) { (((w >> 27) & 15) as u16, (w & ((1 << 27) - 1)) as u32) };
                            let mut evs: Vec<(u16, u32)> = Vec::new();
                            let mut taken: Vec<bool> = Vec::new();
                            let mut g0 = 0;
                            while g0 < r16.len() {
                                let mut g1 = g0 + 1;
                                while g1 < r16.len() && r16[g1].0 >> 32 == r16[g0].0 >> 32 {
                                    g1 += 1;
                                }
                                if g1 == g0 + 1 {
                                    let w = r16[g0].1;
                                    if (w >> 31) & 1 != 0 {
                                        evs.push(tid(w));
                                    }
                                } else {
                                    taken.clear();
                                    taken.resize(g1 - g0, false);
                                    for j in g0..g1 {
                                        if taken[j - g0] {
                                            continue;
                                        }
                                        let kj = key16(r16[j].1);
                                        let hj = r16[j].0;
                                        let mut best = j;
                                        for k in j..g1 {
                                            if !taken[k - g0] && r16[k].0 == hj && key16(r16[k].1) == kj {
                                                taken[k - g0] = true;
                                                if (r16[k].1 >> 32) < (r16[best].1 >> 32) {
                                                    best = k;
                                                }
                                            }
                                        }
                                        let w = r16[best].1;
                                        if (w >> 31) & 1 != 0 {
                                            evs.push(tid(w));
                                        }
                                    }
                                }
                                g0 = g1;
                            }
                            compact = Some(evs);
                        }
                    }
                    let mut refs: Vec<(u64, i64, u16, u32, bool)> = Vec::with_capacity(if compact.is_some() { 0 } else { outs.iter().map(|o| o.gev[p].len()).sum() });
                    for (t, o) in outs.iter().enumerate().filter(|_| compact.is_none()) {
                        for (i, g) in o.gev[p].iter().enumerate() {
                            refs.push((g.hash, g.ingest, t as u16, i as u32, g.valid));
                        }
                    }
                    radix_sort(&mut refs, |r| r.0 >> 32, 32);
                    let key_e = |t: u16, i: u32| -> &[u8] {
                        let g = &outs[t as usize].gev[p][i as usize];
                        &arenas[t as usize][g.eid_off as usize..(g.eid_off + g.eid_len) as usize]
                    };
                    let mut evs: Vec<(u16, u32)> = Vec::new();
                    let mut taken: Vec<bool> = Vec::new();
                    let mut g0 = 0;
                    while g0 < refs.len() {
                        let mut g1 = g0 + 1;
                        while g1 < refs.len() && refs[g1].0 >> 32 == refs[g0].0 >> 32 {
                            g1 += 1;
                        }
                        if g1 == g0 + 1 {
                            let r = refs[g0];
                            if r.4 {
                                evs.push((r.2, r.3));
                            }
                        } else {
                            taken.clear();
                            taken.resize(g1 - g0, false);
                            for j in g0..g1 {
                                if taken[j - g0] {
                                    continue;
                                }
                                let kj = key_e(refs[j].2, refs[j].3);
                                let hj = refs[j].0;
                                let mut best = j;
                                for k in j..g1 {
                                    if !taken[k - g0] && refs[k].0 == hj && key_e(refs[k].2, refs[k].3) == kj {
                                        taken[k - g0] = true;
                                        if refs[k].1 < refs[best].1 {
                                            best = k;
                                        }
                                    }
                                }
                                if refs[best].4 {
                                    evs.push((refs[best].2, refs[best].3));
                                }
                            }
                        }
                        g0 = g1;
                    }
                    drop(refs);
                    if let Some(c) = compact {
                        evs = c;
                    }
                    // kept rows back in memory order, so later passes read them sequentially
                    sort_refs(&mut evs);
                    // clicks: copies are identical, keep one of each ID
                    let mut refs: Vec<(u64, u16, u32)> = Vec::with_capacity(outs.iter().map(|o| o.gcl[p].len()).sum());
                    for (t, o) in outs.iter().enumerate() {
                        for (i, g) in o.gcl[p].iter().enumerate() {
                            refs.push((g.hash, t as u16, i as u32));
                        }
                    }
                    radix_sort(&mut refs, |r| r.0 >> 32, 32);
                    let key_c = |t: u16, i: u32| -> &[u8] {
                        let g = &outs[t as usize].gcl[p][i as usize];
                        &arenas[t as usize][g.cid_off as usize..(g.cid_off + g.cid_len) as usize]
                    };
                    let mut cls: Vec<(u16, u32)> = Vec::new();
                    let mut g0 = 0;
                    while g0 < refs.len() {
                        let mut g1 = g0 + 1;
                        while g1 < refs.len() && refs[g1].0 >> 32 == refs[g0].0 >> 32 {
                            g1 += 1;
                        }
                        for j in g0..g1 {
                            let kj = key_c(refs[j].1, refs[j].2);
                            if (g0..j).any(|k| refs[k].0 == refs[j].0 && key_c(refs[k].1, refs[k].2) == kj) {
                                continue;
                            }
                            cls.push((refs[j].1, refs[j].2));
                        }
                        g0 = g1;
                    }
                    drop(refs);
                    sort_refs(&mut cls);
                    (evs, cls)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).unzip()
    });
    lap("generic dedup");

    // dedup fix-up: the copy with the smallest ingest keeps its w1
    std::thread::scope(|s| {
        for o in &outs {
            let ev = &ev;
            s.spawn(move || {
                pin_self();
                for &(slot, w0s, w1) in &o.overflow {
                    if ev.w0(slot as usize).load(Relaxed) == w0s {
                        ev.w1(slot as usize).store(w1, Relaxed);
                    }
                }
            });
        }
    });
    lap("overflow");

    // records that did not fit their partition's region, grouped by partition
    let mut spill: Vec<(u32, Rec)> = std::mem::take(&mut *sc.spill.lock().unwrap());
    spill.sort_by_key(|x| x.0);
    let mut spill_range = vec![(0usize, 0usize); ctx.nparts.max(1)];
    {
        let mut i = 0;
        while i < spill.len() {
            let p = spill[i].0 as usize;
            let mut j = i;
            while j < spill.len() && spill[j].0 as usize == p {
                j += 1;
            }
            spill_range[p] = (i, j);
            i = j;
        }
    }

    // R1: click runs and generic rows; their bits are final before any event run is judged
    let bits = Bits::new(if ctx.pos { ((ctx.cap_e as usize / 2) / 25 * 32).next_power_of_two().clamp(1 << 20, 1 << 28) } else { 16 });
    // for local persons: flags and click-run counts indexed by the person number itself
    let pn = if ctx.pos { links.pcap as usize } else { 16 };
    let pbits = Bits::direct(pn);
    let clcnt: Vec<std::sync::atomic::AtomicU32> = {
        let v: Vec<u64> = huge_zeroed(pn / 2);
        let mut v = std::mem::ManuallyDrop::new(v);
        unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut std::sync::atomic::AtomicU32, pn, pn) }
    };
    let r1: Vec<(Vec<Run>, Vec<Vec<Entry>>)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (cl, links, gev_kept, gcl_kept, bits, outs, pbits, clcnt) = (&cl, &links, &gev_kept, &gcl_kept, &bits, &outs, &pbits, &clcnt);
                s.spawn(move || {
                pin_self();
                    let per = (gev_kept[t].len() + gcl_kept[t].len()) / NB;
                    let mut b: Vec<Vec<Entry>> = (0..NB).map(|_| Vec::with_capacity(per + per / 4 + 16)).collect();
                    let mut cr: Vec<Run> = Vec::new();
                    let mut cache = CompCache::new();
                    if ctx.pos {
                        let lo = max_c * t / nt;
                        let hi = max_c * (t + 1) / nt;
                        let mut cur = Run { comp: u64::MAX, loc: u32::MAX, p: 0, a: 0, b: 0, sorted: true };
                        for i in lo..hi {
                            let w0 = cl.w0(i).load(Relaxed);
                            if w0 == 0 {
                                continue;
                            }
                            let w1 = cl.w1(i).load(Relaxed);
                            let uid = w1 | if w0 & 1 != 0 { GEN_TAG } else { 0 };
                            let comp = cache.get(links, uid);
                            if comp == cur.comp {
                                cur.b = i as u32 + 1;
                            } else {
                                if cur.comp != u64::MAX {
                                    if cur.loc != u32::MAX {
                                        clcnt[cur.loc as usize].fetch_add(1, Relaxed);
                                    } else {
                                        bits.mark(cur.comp, CL_HAVE, CL_MULTI);
                                    }
                                    cr.push(cur);
                                }
                                cur = Run { comp, loc: links.local_person(uid).map(|x| x as u32).unwrap_or(u32::MAX), p: person_hint(ctx, uid), a: i as u32, b: i as u32 + 1, sorted: true };
                            }
                        }
                        if cur.comp != u64::MAX {
                            if cur.loc != u32::MAX {
                                clcnt[cur.loc as usize].fetch_add(1, Relaxed);
                            } else {
                                bits.mark(cur.comp, CL_HAVE, CL_MULTI);
                            }
                            cr.push(cur);
                        }
                    }
                    const AHEAD: usize = 8;
                    let gk = &gev_kept[t];
                    for (k, &(tt, ii)) in gk.iter().enumerate() {
                        if !ctx.pos && k + AHEAD < gk.len() {
                            let (t2, i2) = gk[k + AHEAD];
                            links.prefetch(outs[t2 as usize].gev[t][i2 as usize].uid);
                        }
                        let g = &outs[tt as usize].gev[t][ii as usize];
                        let comp = cache.get(links, g.uid);
                        if ctx.pos {
                            // occupancy bits are read only by the dense-table passes
                            bits.force(comp, EV_HAVE | EV_MULTI);
                            if let Some(pp) = links.local_person(g.uid) {
                                pbits.force(pp as u64, EV_HAVE | EV_MULTI);
                            }
                        }
                        // the event itself rides in the entry: ts in a/b, cat in t, purchase in sorted, price in c
                        let _ = k;
                        b[bucket_of(comp)].push(Entry { comp, kind: 2, sorted: g.purchase, t: g.cat, a: g.ts as u64 as u32, b: (g.ts as u64 >> 32) as u32, c: g.price });
                    }
                    let ck = &gcl_kept[t];
                    for (k, &(tt, ii)) in ck.iter().enumerate() {
                        if !ctx.pos && k + AHEAD < ck.len() {
                            let (t2, i2) = ck[k + AHEAD];
                            links.prefetch(outs[t2 as usize].gcl[t][i2 as usize].uid);
                        }
                        let g = &outs[tt as usize].gcl[t][ii as usize];
                        let comp = cache.get(links, g.uid);
                        if ctx.pos {
                            bits.force(comp, CL_HAVE | CL_MULTI);
                            if let Some(pp) = links.local_person(g.uid) {
                                // counted but never in the run list: such a person is never taken as simple
                                clcnt[pp].fetch_add(1, Relaxed);
                            }
                        }
                        b[bucket_of(comp)].push(Entry { comp, kind: 3, sorted: true, t: t as u16, a: k as u32, b: 0, c: 0 });
                    }
                    (cr, b)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut entries: Vec<Vec<Vec<Entry>>> = Vec::new();
    let mut cr_parts: Vec<Vec<Run>> = Vec::new();
    for (cr, b) in r1 {
        cr_parts.push(cr);
        entries.push(b);
    }
    let cr_all: Vec<Run> = cr_parts.concat();
    drop(cr_parts);
    lap("click runs");

    // R2: one pass over the event table. Each run is processed as soon as it ends,
    // unless it is clearly not simple; a person later found to have another run is undone below.
    let consumed: Vec<std::sync::atomic::AtomicU8> = (0..cr_all.len()).map(|_| std::sync::atomic::AtomicU8::new(0)).collect();
    let nparts = if ctx.pos { ctx.nparts } else { 0 };
    let next_t = AtomicUsize::new(0);
    struct Proc {
        comp: u64,
        loc: u32,
        p: u64,
        a: u32,
        b: u32,
        win: (u32, u32), // click runs consumed: those in this range that are this person's
        rows: (u32, u32),
    }
    let r2: Vec<(Out, Vec<Vec<Entry>>, Vec<Proc>)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                let (ev, cl, links, bits, cr_all, consumed, next_t, sc, spill, spill_range, pbits, clcnt) =
                    (&ev, &cl, &links, &bits, &cr_all, &consumed, &next_t, &sc, &spill, &spill_range, &pbits, &clcnt);
                s.spawn(move || {
                pin_self();
                    let mut out = new_out(ctx);
                    let mut b: Vec<Vec<Entry>> = (0..NB).map(|_| Vec::new()).collect();
                    let mut procs: Vec<Proc> = Vec::new();
                    let mut evbuf: Vec<Ev> = Vec::new();
                    let mut clbuf: Vec<Cl> = Vec::new();
                    let mut cache = CompCache::new();
                    let mut local: Vec<u64> = if ctx.scatter { vec![0u64; 2 << ctx.shift] } else { Vec::new() };
                    loop {
                        let task = next_t.fetch_add(1, Relaxed);
                        if task >= nparts {
                            break;
                        }
                        let base = task << ctx.shift;
                        let evl;
                        let ev: &Dense = if ctx.scatter {
                            // place this partition into a local, cache-sized table
                            unsafe { std::ptr::write_bytes(local.as_mut_ptr(), 0, local.len()) };
                            let lp = local.as_mut_ptr();
                            let (sa, sb) = spill_range[task];
                            let reg = sc.region(task);
                            for r in reg.iter().chain(spill[sa..sb].iter().map(|x| &x.1)) {
                                let j = 2 * r.local as usize;
                                let (w0s, w1) = (r.w0s, r.w1);
                                unsafe {
                                    if w0s > *lp.add(j) {
                                        *lp.add(j) = w0s;
                                        *lp.add(j + 1) = w1;
                                    }
                                }
                            }
                            // the global table gets a streamed copy (for the undo and slow paths)
                            let lo = base.min(max_e).min(ev.len);
                            let hi = (base + (1 << ctx.shift)).min(max_e).min(ev.len);
                            if hi > lo {
                                unsafe {
                                    use std::arch::x86_64::*;
                                    let src = lp as *const u8;
                                    let dst = ev.ptr.add(2 * lo) as *mut u8;
                                    let bytes = (hi - lo) * 16;
                                    let mut k = 0;
                                    while k + 32 <= bytes {
                                        _mm256_stream_si256(dst.add(k) as *mut __m256i, _mm256_loadu_si256(src.add(k) as *const __m256i));
                                        k += 32;
                                    }
                                    if k < bytes {
                                        std::ptr::copy_nonoverlapping(src.add(k), dst.add(k), bytes - k);
                                    }
                                    _mm_sfence();
                                }
                            }
                            evl = Dense { ptr: (lp as *mut AtomicU64).wrapping_sub(2 * base), len: ev.len };
                            &evl
                        } else {
                            ev
                        };
                        let lo = base.min(max_e);
                        let hi = (base + (1 << ctx.shift)).min(max_e);
                        let mut ci = usize::MAX; // click cursor, placed at the first run
                        let mut cur = Run { comp: u64::MAX, loc: u32::MAX, p: 0, a: 0, b: 0, sorted: true };
                        let mut last_ts = 0u64;
                        let mut i = lo;
                        loop {
                            let at_end = i >= hi;
                            let mut next_comp = u64::MAX;
                            let mut uid = 0u64;
                            let mut ts = 0u64;
                            if !at_end {
                                let s0 = ev.w0(i).load(Relaxed);
                                if s0 == 0 {
                                    i += 1;
                                    continue;
                                }
                                let w0 = !s0;
                                if w0 & (1 << 10) == 0 {
                                    i += 1;
                                    continue;
                                }
                                let w1 = ev.w1(i).load(Relaxed);
                                uid = (w1 & (GEN_TAG - 1)) | if w0 & 8 != 0 { GEN_TAG } else { 0 };
                                next_comp = cache.get(links, uid);
                                ts = (w0 >> 11) & 0x7FFF_FFFF;
                                if next_comp == cur.comp {
                                    cur.b = i as u32 + 1;
                                    if ts < last_ts {
                                        cur.sorted = false;
                                    }
                                    last_ts = ts;
                                    i += 1;
                                    continue;
                                }
                            }
                            // the current run (if any) has ended: judge and process it
                            if cur.comp != u64::MAX {
                                let r = cur;
                                let local = r.loc != u32::MAX;
                                let had = if local { pbits.mark(r.loc as u64, EV_HAVE, EV_MULTI) } else { bits.mark(r.comp, EV_HAVE, EV_MULTI) };
                                let bt = if local { 0 } else { bits.get(r.comp) };
                                let mut simple = r.sorted && !had;
                                let mut found = usize::MAX;
                                let mut win = (0u32, 0u32);
                                if simple && local {
                                    let total = clcnt[r.loc as usize].load(Relaxed);
                                    if total > 0 {
                                        if ci == usize::MAX {
                                            ci = cr_all.partition_point(|c| c.p < r.p);
                                        }
                                        while ci < cr_all.len() && (cr_all[ci].p < r.p || cr_all[ci].p >= u64::MAX - 1) {
                                            ci += 1;
                                        }
                                        let mut j = ci;
                                        let mut seen = 0u32;
                                        while j < cr_all.len() && (cr_all[j].p == r.p || cr_all[j].p >= u64::MAX - 1) {
                                            if cr_all[j].p == r.p && cr_all[j].loc == r.loc {
                                                seen += 1;
                                            }
                                            j += 1;
                                        }
                                        if seen != total {
                                            simple = false;
                                        } else {
                                            win = (ci as u32, j as u32);
                                        }
                                    }
                                } else if simple && bt & CL_HAVE != 0 {
                                    if bt & CL_MULTI != 0 || r.p >= u64::MAX - 1 {
                                        simple = false;
                                    } else {
                                        if ci == usize::MAX {
                                            ci = cr_all.partition_point(|c| c.p < r.p);
                                        }
                                        while ci < cr_all.len() && (cr_all[ci].p < r.p || cr_all[ci].p >= u64::MAX - 1) {
                                            ci += 1;
                                        }
                                        let mut j = ci;
                                        while j < cr_all.len() && (cr_all[j].p == r.p || cr_all[j].p >= u64::MAX - 1) {
                                            if cr_all[j].comp == r.comp {
                                                found = j;
                                                break;
                                            }
                                            j += 1;
                                        }
                                        if found == usize::MAX {
                                            simple = false;
                                        }
                                    }
                                }
                                if !simple {
                                    b[bucket_of(r.comp)].push(Entry { comp: r.comp, kind: 0, sorted: r.sorted, t: 0, a: r.a, b: r.b, c: 0 });
                                } else {
                                    evbuf.clear();
                                    decode_events(ev, r.a as usize, r.b as usize, ctx, &mut evbuf);
                                    clbuf.clear();
                                    if found != usize::MAX {
                                        consumed[found].store(1, Relaxed);
                                        let c = cr_all[found];
                                        decode_clicks(cl, c.a as usize, c.b as usize, ctx, &mut clbuf);
                                        win = (found as u32, found as u32 + 1);
                                    } else if win.1 > win.0 {
                                        for j in win.0 as usize..win.1 as usize {
                                            let c = cr_all[j];
                                            if c.comp == r.comp && c.p == r.p {
                                                consumed[j].store(1, Relaxed);
                                                decode_clicks(cl, c.a as usize, c.b as usize, ctx, &mut clbuf);
                                            }
                                        }
                                    }
                                    if clbuf.len() > 1 {
                                        clbuf.sort_unstable_by_key(|c| (c.ts, match c.key { ClKey::Canon(m) => name_key(m), ClKey::Gen(..) => 0 }));
                                    }
                                    let row0 = out.users.len() as u32;
                                    if ctx.exp != 5 {
                                        process_person(ctx, r.comp, &evbuf, &clbuf, &mut out);
                                    }
                                    procs.push(Proc { comp: r.comp, loc: r.loc, p: r.p, a: r.a, b: r.b, win, rows: (row0, out.users.len() as u32) });
                                }
                            }
                            if at_end {
                                break;
                            }
                            cur = Run { comp: next_comp, loc: links.local_person(uid).map(|x| x as u32).unwrap_or(u32::MAX), p: person_hint(ctx, uid), a: i as u32, b: i as u32 + 1, sorted: true };
                            last_ts = ts;
                            i += 1;
                        }
                    }
                    (out, b, procs)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("event pass");

    // Undo: a person processed above whose bits now say "more than one run" goes to the buckets
    let r2u: Vec<(Out, Vec<Vec<Entry>>)> = std::thread::scope(|s| {
        let hs: Vec<_> = r2
            .into_iter()
            .map(|(mut out, mut b, procs)| {
                let (ev, cl, bits, cr_all, consumed, pbits) = (&ev, &cl, &bits, &cr_all, &consumed, &pbits);
                s.spawn(move || {
                pin_self();
                    let mut evbuf: Vec<Ev> = Vec::new();
                    let mut clbuf: Vec<Cl> = Vec::new();
                    for p in &procs {
                        let multi = if p.loc != u32::MAX { pbits.get(p.loc as u64) & EV_MULTI != 0 } else { bits.get(p.comp) & EV_MULTI != 0 };
                        if !multi {
                            continue;
                        }
                        for r in &mut out.users[p.rows.0 as usize..p.rows.1 as usize] {
                            r.sessions = 0;
                        }
                        evbuf.clear();
                        decode_events(ev, p.a as usize, p.b as usize, ctx, &mut evbuf);
                        clbuf.clear();
                        for j in p.win.0 as usize..p.win.1 as usize {
                            let c = cr_all[j];
                            if c.comp == p.comp && (p.win.1 - p.win.0 == 1 || c.p == p.p) {
                                consumed[j].store(0, Relaxed);
                                decode_clicks(cl, c.a as usize, c.b as usize, ctx, &mut clbuf);
                            }
                        }
                        if clbuf.len() > 1 {
                            clbuf.sort_unstable_by_key(|c| (c.ts, match c.key { ClKey::Canon(m) => name_key(m), ClKey::Gen(..) => 0 }));
                        }
                        out.neg = true;
                        process_person(ctx, p.comp, &evbuf, &clbuf, &mut out);
                        out.neg = false;
                        b[bucket_of(p.comp)].push(Entry { comp: p.comp, kind: 0, sorted: true, t: 0, a: p.a, b: p.b, c: 0 });
                    }
                    (out, b)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut fast_outs: Vec<Out> = Vec::new();
    for (o, b) in r2u {
        fast_outs.push(o);
        entries.push(b);
    }
    // R3: click runs not used above go to the buckets
    let r3: Vec<Vec<Vec<Entry>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (cr_all, consumed) = (&cr_all, &consumed);
                s.spawn(move || {
                pin_self();
                    let mut b: Vec<Vec<Entry>> = (0..NB).map(|_| Vec::new()).collect();
                    let lo = cr_all.len() * t / nt;
                    let hi = cr_all.len() * (t + 1) / nt;
                    for j in lo..hi {
                        if consumed[j].load(Relaxed) == 0 {
                            let c = cr_all[j];
                            b[bucket_of(c.comp)].push(Entry { comp: c.comp, kind: 1, sorted: true, t: 0, a: c.a, b: c.b, c: 0 });
                        }
                    }
                    b
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    entries.extend(r3);
    lap("simple persons");

    let ud = out_dir.join(names::OUT_A);
    let cd = out_dir.join(names::OUT_B);
    let _ = std::fs::create_dir_all(&ud);
    let _ = std::fs::create_dir_all(&cd);

    // per-bucket person processing
    let ncamp = ctx.camp_names.len();
    let ndays = ctx.ndays;
    let next_b = AtomicUsize::new(0);
    let outs2: Vec<Out> = std::thread::scope(|s| {
        let hs: Vec<_> = fast_outs
            .into_iter()
            .enumerate()
            .map(|(t, mut out)| {
                let (ev, cl, entries, gev_kept, gcl_kept, next_b, arenas, ud, links, outs) =
                    (&ev, &cl, &entries, &gev_kept, &gcl_kept, &next_b, &arenas, &ud, &links, &outs);
                s.spawn(move || {
                pin_self();
                    let mut list: Vec<Entry> = Vec::new();
                    let mut evbuf: Vec<Ev> = Vec::new();
                    let mut clbuf: Vec<Cl> = Vec::new();
                    loop {
                        let bi = next_b.fetch_add(1, Relaxed);
                        if bi >= NB {
                            break;
                        }
                        list.clear();
                        for e in entries.iter() {
                            list.extend_from_slice(&e[bi]);
                        }
                        // only grouping by person matters: events and clicks are ordered per person below
                        list.sort_unstable_by_key(|x| x.comp);
                        let mut g = 0;
                        while g < list.len() {
                            let comp = list[g].comp;
                            let mut h = g;
                            while h < list.len() && list[h].comp == comp {
                                h += 1;
                            }
                            let grp = &list[g..h];
                            g = h;
                            if !grp.iter().any(|e| e.kind == 0 || e.kind == 2) {
                                continue;
                            }
                            evbuf.clear();
                            let mut nruns = 0;
                            let mut sorted = true;
                            for e in grp {
                                match e.kind {
                                    0 => {
                                        nruns += 1;
                                        sorted &= e.sorted;
                                        decode_events(ev, e.a as usize, e.b as usize, ctx, &mut evbuf);
                                    }
                                    2 => {
                                        nruns += 2;
                                        let ts = (e.a as u64 | ((e.b as u64) << 32)) as i64;
                                        evbuf.push(Ev { ts, price: e.c, cat: e.t, purchase: e.sorted });
                                    }
                                    _ => {}
                                }
                            }
                            if nruns != 1 || !sorted {
                                evbuf.sort_by_key(|e| e.ts);
                            }
                            clbuf.clear();
                            for e in grp {
                                match e.kind {
                                    1 => {
                                        decode_clicks(cl, e.a as usize, e.b as usize, ctx, &mut clbuf);
                                    }
                                    3 => {
                                        let (t, ii) = gcl_kept[e.t as usize][e.a as usize];
                                        let gc = &outs[t as usize].gcl[e.t as usize][ii as usize];
                                        clbuf.push(Cl { ts: gc.ts, camp: gc.camp, key: ClKey::Gen(t as u32, gc.cid_off, gc.cid_len) });
                                    }
                                    _ => {}
                                }
                            }
                            if clbuf.len() > 1 {
                                clbuf.sort_by(|a, b| click_cmp_refs(a, b, arenas));
                            }
                            if ctx.exp != 5 {
                                process_person(ctx, comp, &evbuf, &clbuf, &mut out);
                            }
                        }
                    }
                    // this thread's rows go straight to their own file
                    let tw = Instant::now();
                    write_users(&ud.join(format!("part-{t}.parquet")), &out.users, ctx, links);
                    if std::env::var("SS_TIMING2").is_ok() {
                        eprintln!("w{t} write {:.1} ms rows {}", tw.elapsed().as_secs_f64() * 1e3, out.users.len());
                    }
                    out.users = Vec::new();
                    out
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("sessions");

    // second table
    {
        // second table on this thread
        let mut tot = vec![[0u64; 4]; (ncamp + 1) * ndays];
        for o in &outs2 {
            for (a, b) in tot.iter_mut().zip(&o.camp) {
                for k in 0..4 {
                    a[k] += b[k];
                }
            }
        }
        let mut c_id = parquet::Column::new(names::B_ID, parquet::Kind::Str, 0);
        let mut c_ch = parquet::Column::new(names::CHAN, parquet::Kind::Str, 0);
        let mut c_day = parquet::Column::new(names::DAY, parquet::Kind::Date, 0);
        let mut c_s = parquet::Column::new(names::N_S, parquet::Kind::I64, 0);
        let mut c_p = parquet::Column::new(names::N_P, parquet::Kind::I64, 0);
        let mut c_r = parquet::Column::new(names::REV, parquet::Kind::I64, 0);
        let mut c_a = parquet::Column::new(names::N_A, parquet::Kind::I64, 0);
        for c in 0..=ncamp {
            for d in 0..ndays {
                let a = tot[c * ndays + d];
                if a[0] == 0 && a[3] == 0 {
                    continue;
                }
                if c == ncamp {
                    c_id.push_str(names::NONE.as_bytes());
                    c_ch.push_str(names::NONE.as_bytes());
                } else {
                    c_id.push_str(&ctx.camp_names[c]);
                    c_ch.push_str(&ctx.camp_channels[c]);
                }
                c_day.push_i32((ctx.day0 + d as i64) as i32);
                c_s.push_i64(a[0] as i64);
                c_p.push_i64(a[1] as i64);
                c_r.push_i64(a[2] as i64);
                c_a.push_i64(a[3] as i64);
            }
        }
        parquet::write(&cd.join("part-0.parquet"), &[c_id, c_ch, c_day, c_s, c_p, c_r, c_a]).expect("write");
    }
    lap("output");
    unsafe { libc::_exit(0) };
}

fn click_cmp_refs(a: &Cl, b: &Cl, arenas: &[&Vec<u8>]) -> std::cmp::Ordering {
    a.ts.cmp(&b.ts).then_with(|| {
        if let (ClKey::Canon(x), ClKey::Canon(y)) = (a.key, b.key) {
            return name_key(x).cmp(&name_key(y));
        }
        let mut sa = Vec::new();
        let mut sb = Vec::new();
        let bytes = |k: &ClKey, s: &mut Vec<u8>| match *k {
            ClKey::Canon(m) => base36_string(b'k', m, s),
            ClKey::Gen(t, o, l) => s.extend_from_slice(&arenas[t as usize][o as usize..(o + l) as usize]),
        };
        bytes(&a.key, &mut sa);
        bytes(&b.key, &mut sb);
        sa.cmp(&sb)
    })
}
