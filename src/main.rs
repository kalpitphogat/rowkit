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
    cap_e: u64,
    cap_c: u64,
    ingest_base_s: i64,
    shift: usize,
    nparts: usize,
    exp: u32,
}

impl Ctx {
    #[inline]
    fn product_cat(&self, s: &[u8]) -> u16 {
        if let Some(n) = canon10(s, b"p") {
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
        if let Some(n) = canon10(s, b"c_") {
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

static INTERN: Mutex<Option<Interner>> = Mutex::new(None);

fn intern(s: &[u8]) -> u64 {
    let mut g = INTERN.lock().unwrap();
    let it = g.get_or_insert_with(|| Interner { map: HashMap::new(), strs: Vec::new() });
    if let Some(&i) = it.map.get(s) {
        return GEN_TAG | i as u64;
    }
    let i = it.strs.len() as u32;
    it.strs.push(s.to_vec());
    it.map.insert(s.to_vec(), i);
    GEN_TAG | i as u64
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
        let g = INTERN.lock().unwrap();
        out.extend_from_slice(&g.as_ref().unwrap().strs[(enc & 0xFFFF_FFFF) as usize]);
    } else {
        base36_string(b'u', enc, out);
    }
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

// ---------------------------------------------------------------- union-find over linked ids

struct Links {
    keys: Vec<u64>, // open addressing, key+1, 0 = empty
    vals: Vec<u32>,
    mask: usize,
    root: Vec<u32>,
    name: Vec<u64>, // per root: uid enc of the smallest ID
}

#[inline(always)]
fn hash64(x: u64) -> u64 {
    let x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
}

impl Links {
    fn build(pairs: Vec<Vec<(u64, u64)>>) -> Links {
        let total: usize = pairs.iter().map(|p| p.len()).sum();
        let cap = (total * 2 * 2).next_power_of_two().max(1024);
        let mut keys = vec![0u64; cap];
        let mut vals = vec![0u32; cap];
        let mask = cap - 1;
        let mut parent: Vec<u32> = Vec::with_capacity(total * 2);
        let mut ids: Vec<u64> = Vec::with_capacity(total * 2);
        let mut get = |k: u64, keys: &mut Vec<u64>, vals: &mut Vec<u32>, parent: &mut Vec<u32>, ids: &mut Vec<u64>| -> u32 {
            let mut h = hash64(k) as usize & mask;
            loop {
                let kk = keys[h];
                if kk == k + 1 {
                    return vals[h];
                }
                if kk == 0 {
                    keys[h] = k + 1;
                    let i = parent.len() as u32;
                    vals[h] = i;
                    parent.push(i);
                    ids.push(k);
                    return i;
                }
                h = (h + 1) & mask;
            }
        };
        fn find(parent: &mut [u32], mut x: u32) -> u32 {
            while parent[x as usize] != x {
                let p = parent[x as usize];
                parent[x as usize] = parent[p as usize];
                x = parent[x as usize];
            }
            x
        }
        for v in &pairs {
            for &(a, b) in v {
                let ia = get(a, &mut keys, &mut vals, &mut parent, &mut ids);
                let ib = get(b, &mut keys, &mut vals, &mut parent, &mut ids);
                let ra = find(&mut parent, ia);
                let rb = find(&mut parent, ib);
                if ra != rb {
                    if ra < rb {
                        parent[rb as usize] = ra;
                    } else {
                        parent[ra as usize] = rb;
                    }
                }
            }
        }
        let n = parent.len();
        let mut root = vec![0u32; n];
        for i in 0..n {
            root[i] = find(&mut parent, i as u32);
        }
        // smallest ID per root
        let mut name: Vec<u64> = vec![u64::MAX; n];
        let mut best_key: Vec<u64> = vec![u64::MAX; n];
        let mut sa = Vec::new();
        let mut sb = Vec::new();
        for i in 0..n {
            let r = root[i] as usize;
            let id = ids[i];
            if name[r] == u64::MAX {
                name[r] = id;
                best_key[r] = if id & GEN_TAG == 0 { canon_key(id) } else { 0 };
                continue;
            }
            let cur = name[r];
            let smaller = if id & GEN_TAG == 0 && cur & GEN_TAG == 0 {
                let k = canon_key(id);
                if k < best_key[r] {
                    best_key[r] = k;
                    true
                } else {
                    false
                }
            } else {
                uid_bytes(id, &mut sa);
                uid_bytes(cur, &mut sb);
                if sa < sb {
                    best_key[r] = if id & GEN_TAG == 0 { canon_key(id) } else { 0 };
                    true
                } else {
                    false
                }
            };
            if smaller {
                name[r] = id;
            }
        }
        Links { keys, vals, mask, root, name }
    }

    /// The person of a user ID: LINK_TAG | root for a linked ID, the ID itself otherwise.
    #[inline(always)]
    fn comp(&self, uid: u64) -> u64 {
        let mut h = hash64(uid) as usize & self.mask;
        loop {
            let kk = unsafe { *self.keys.get_unchecked(h) };
            if kk == uid + 1 {
                return LINK_TAG | self.root[unsafe { *self.vals.get_unchecked(h) } as usize] as u64;
            }
            if kk == 0 {
                return uid;
            }
            h = (h + 1) & self.mask;
        }
    }

    fn name_of(&self, comp: u64) -> u64 {
        if comp & LINK_TAG != 0 {
            self.name[(comp & 0xFFFF_FFFF) as usize]
        } else {
            comp
        }
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

struct ParseOut {
    links: Vec<(u64, u64)>,
    gev: Vec<Vec<GenEvent>>, // by hash % T
    gcl: Vec<Vec<GenClick>>,
    arena: Vec<u8>,
    ring: [(usize, u64, u64); RING],
    ring_n: usize,
    overflow: Vec<(u32, u64, u64)>,
    max_e: usize,
    max_c: usize,
    dc: DateCache,
}

fn fnv(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &c in s {
        h ^= c as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    hash64(h)
}

const RING: usize = 16;

impl ParseOut {
    /// Queues a record; the slot is prefetched now and stored RING records later.
    #[inline(always)]
    fn put(&mut self, ev: &Dense, slot: usize, w0s: u64, w1: u64) {
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch::<_MM_HINT_T0>(ev.ptr.add(2 * slot) as *const i8);
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

static ANOMALY: AtomicBool = AtomicBool::new(false);

#[inline(always)]
fn parse_event(f: &[&[u8]; 7], ctx: &Ctx, ev: &Dense, out: &mut ParseOut, nt: usize) {
    let eid = f[0];
    if eid.is_empty() {
        return;
    }
    let ingest = out.dc.ts(f[3]).unwrap_or(i64::MAX);
    let mut valid = !f[1].is_empty() && !f[5].is_empty();
    let ts = out.dc.ts(f[2]);
    let ts = match ts {
        Some(t) if t >= ctx.ws && t < ctx.we => t,
        _ => {
            valid = false;
            0
        }
    };
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
    let (uid, cat) = if valid { (uid_enc(f[1]), ctx.product_cat(f[5])) } else { (0, 0) };
    if !valid {
        purchase = false;
        price = 0;
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
                out.put(ev, slot, !w0, w1);
                if slot > out.max_e {
                    out.max_e = slot;
                }
                return;
            }
        }
    }
    let h = fnv(eid);
    let off = out.arena.len() as u32;
    out.arena.extend_from_slice(eid);
    out.gev[(h % nt as u64) as usize].push(GenEvent {
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
                cl.w0(slot).store(w0, Relaxed);
                cl.w1(slot).store(uid & (GEN_TAG - 1), Relaxed);
                if slot > out.max_c {
                    out.max_c = slot;
                }
                return;
            }
        }
    }
    let h = fnv(cid);
    let off = out.arena.len() as u32;
    out.arena.extend_from_slice(cid);
    out.gcl[(h % nt as u64) as usize].push(GenClick { cid_off: off, cid_len: cid.len() as u32, hash: h, ts, uid, camp });
}

// ---------------------------------------------------------------- key discovery

fn sample_lines(files: &[(PathBuf, &'static [u8])], per_file: usize, nfields: usize) -> Vec<Vec<Vec<u8>>> {
    let mut idx: Vec<usize> = (0..files.len()).collect();
    idx.sort_by_key(|&i| std::cmp::Reverse(files[i].1.len()));
    let mut rows = Vec::new();
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
            if ok {
                rows.push(f[..nfields].iter().map(|x| x.to_vec()).collect());
            }
        });
    }
    rows
}

/// Finds the ID keys from a sample, or None. Never affects correctness, only speed.
fn find_keys(mix: &Mix, ev_rows: &[Vec<Vec<u8>>], cl_rows: &[Vec<Vec<u8>>], est_e: u64, est_c: u64) -> Option<(u64, u64)> {
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
            let k = (x1 ^ g.wrapping_mul(0x9E37_79B9_7F4B)) & MASK40;
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
    Some((ekey, ckey))
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
    stamp: Vec<u32>,
    stamp_n: u32,
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
        while hi < cls.len() && cls[hi].ts <= start {
            hi += 1;
        }
        while lo < hi && cls[lo].ts < start - WEEK {
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
        let a = &mut out.camp[credited * ndays + d];
        a[0] += 1;
        a[1] += purch as u64;
        a[2] += rev;
        if hi > lo {
            out.stamp_n += 1;
            let sn = out.stamp_n;
            for c in &cls[lo..hi] {
                let ci = c.camp as usize;
                if out.stamp[ci] != sn {
                    out.stamp[ci] = sn;
                    out.camp[ci * ndays + d][3] += 1;
                }
            }
        }
        i = j;
    }
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
    out.users.push(UserRow { comp: row.comp, day: row.day, sessions: row.sessions, events: row.events, revenue: row.revenue, top: best });
    row.sessions = 0;
    row.events = 0;
    row.revenue = 0;
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
}

#[inline(always)]
fn bucket_of(comp: u64) -> usize {
    (hash64(comp ^ 0x5555) >> 40) as usize % NB
}

// ---------------------------------------------------------------- main

fn main() {
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

    // products and categories
    let mut prod_rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    read_small_csv(&data_dir.join(names::F_P), |a, b| prod_rows.push((a.to_vec(), b.to_vec())));
    let mut cats: Vec<Vec<u8>> = prod_rows.iter().map(|r| r.1.clone()).collect();
    cats.push(names::UNK.to_vec());
    cats.sort();
    cats.dedup();
    let cat_idx: HashMap<Vec<u8>, u16> = cats.iter().enumerate().map(|(i, c)| (c.clone(), i as u16)).collect();
    let unknown_cat = cat_idx[names::UNK];
    let mut prod_dense: Vec<u16> = Vec::new();
    let mut prod_map: HashMap<Vec<u8>, u16> = HashMap::new();
    for (p, c) in &prod_rows {
        let ci = cat_idx[c];
        prod_map.insert(p.clone(), ci);
        if let Some(n) = canon10(p, b"p") {
            if n < 10_000_000 {
                if prod_dense.len() <= n as usize {
                    prod_dense.resize(n as usize + 1, 0);
                }
                prod_dense[n as usize] = ci + 1;
            }
        }
    }
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
        camp_map.insert(c.clone(), i as u16);
        if let Some(n) = canon10(c, b"c_") {
            if n < 10_000_000 {
                if camp_dense.len() <= n as usize {
                    camp_dense.resize(n as usize + 1, 0);
                }
                camp_dense[n as usize] = i as u16 + 1;
            }
        }
    }

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
    let ev_sample = sample_lines(&event_files, 256 << 10, 7);
    let cl_sample = sample_lines(&click_files, 256 << 10, 4);
    let avg = |rows: &Vec<Vec<Vec<u8>>>, n: usize| -> f64 {
        if rows.is_empty() {
            return 60.0;
        }
        let tot: usize = rows.iter().map(|r| r.iter().map(|f| f.len()).sum::<usize>() + n).sum();
        tot as f64 / rows.len() as f64
    };
    let est_e = (ev_bytes as f64 / avg(&ev_sample, 7)) as u64;
    let est_c = (cl_bytes as f64 / avg(&cl_sample, 4)) as u64;
    let mut pos = false;
    let (mut ekey, mut ckey) = (0, 0);
    let ncamp = camp_names.len();
    if !force_generic && (we - ws) <= 17 * DAY && cats.len() <= 31 && ncamp < 4095 {
        if let Some((a, b)) = find_keys(&mix, &ev_sample, &cl_sample, est_e, est_c) {
            pos = true;
            ekey = a;
            ckey = b;
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
        cap_e: if pos { est_e * 2 + 4096 } else { 0 },
        cap_c: if pos { est_c * 2 + 4096 } else { 0 },
        ingest_base_s: ws / 1000 - 15 * 86_400,
        shift: 15,
        nparts: 0,
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
    const CH: usize = 4 << 20;
    let mut chunks: Vec<(u8, usize, usize, usize)> = Vec::new();
    let mut add = |kind: u8, files: &[(PathBuf, &'static [u8])], chunks: &mut Vec<(u8, usize, usize, usize)>| {
        for (i, f) in files.iter().enumerate() {
            let len = f.1.len();
            let mut s = 0;
            while s < len {
                let e = (s + CH).min(len);
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

    if ctx.pos {
        // partitions of ~16K slots (256 KB of table): placed in cache, no atomics
        ctx.shift = 14;
        ctx.nparts = ((ctx.cap_e as usize) >> ctx.shift) + 1;
    } else {
        ctx.nparts = 1;
    }
    let ev = Dense::new(if ctx.pos { ctx.cap_e as usize } else { 1 });
    let cl = Dense::new(if ctx.pos { ctx.cap_c as usize } else { 1 });

    let next = AtomicUsize::new(0);
    let links_done = AtomicUsize::new(0);
    let link_pairs: Mutex<Vec<Vec<(u64, u64)>>> = Mutex::new(Vec::new());
    let ctxr = &ctx;

    let (outs, links): (Vec<ParseOut>, Links) = std::thread::scope(|s| {
        let mut hs = Vec::new();
        for _ in 0..nt {
            hs.push(s.spawn(|| {
                let mut out = ParseOut {
                    links: Vec::new(),
                    gev: (0..nt).map(|_| Vec::new()).collect(),
                    gcl: (0..nt).map(|_| Vec::new()).collect(),
                    arena: Vec::new(),
                    ring: [(0, 0, 0); RING],
                    ring_n: 0,
                    overflow: Vec::new(),
                    max_e: 0,
                    max_c: 0,
                    dc: DateCache::new(),
                };
                let mut did_links = false;
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    if i >= chunks.len() {
                        break;
                    }
                    if i >= nlink && !did_links {
                        did_links = true;
                        link_pairs.lock().unwrap().push(std::mem::take(&mut out.links));
                        links_done.fetch_add(1, Relaxed);
                    }
                    let (kind, fi, a, b) = chunks[i];
                    match kind {
                        0 => {
                            let d = link_files[fi].1;
                            scan::lines::<2>(d, a, b, |f| {
                                if !f[0].is_empty() && !f[1].is_empty() {
                                    out.links.push((uid_enc(f[0]), uid_enc(f[1])));
                                }
                            });
                        }
                        1 => {
                            let d = event_files[fi].1;
                            match ctxr.exp {
                                3 => {
                                    std::hint::black_box(memchr::memchr_iter(b'\n', &d[a..b]).count());
                                }
                                1 => {
                                    let mut c = 0usize;
                                    scan::lines::<7>(d, a, b, |f| c += f[0].len() + f[6].len());
                                    std::hint::black_box(c);
                                }
                                _ => scan::lines::<7>(d, a, b, |f| parse_event(f, ctxr, &ev, &mut out, nt)),
                            }
                        }
                        _ => {
                            let d = click_files[fi].1;
                            scan::lines::<4>(d, a, b, |f| parse_click(f, ctxr, &cl, &mut out, nt));
                        }
                    }
                }
                out.flush(&ev);
                if !did_links {
                    link_pairs.lock().unwrap().push(std::mem::take(&mut out.links));
                    links_done.fetch_add(1, Relaxed);
                }
                out
            }));
        }
        // union-find on this thread once every worker has handed over its links
        let uf = s.spawn(|| {
            while links_done.load(Relaxed) < nt {
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
            let pairs = std::mem::take(&mut *link_pairs.lock().unwrap());
            Links::build(pairs)
        });
        let outs = hs.into_iter().map(|h| h.join().unwrap()).collect();
        (outs, uf.join().unwrap())
    });
    lap("parse + links");

    if ANOMALY.load(Relaxed) {
        // Rare: an input the dense layout cannot hold. Redo everything on the generic path.
        ANOMALY.store(false, Relaxed);
        ctx.pos = false;
        eprintln!("anomaly: falling back to the generic path");
        return run(ctx, nt, link_files, event_files, click_files, out_dir, t0, timing);
    }
    let ctx = &ctx;

    let max_e = outs.iter().map(|o| o.max_e).max().unwrap_or(0) + 1;
    let max_c = outs.iter().map(|o| o.max_c).max().unwrap_or(0) + 1;
    lap("max");

    // generic dedup (events: smallest ingest; clicks: any copy), per hash partition
    let arenas: Vec<&Vec<u8>> = outs.iter().map(|o| &o.arena).collect();
    let (gev_kept, gcl_kept): (Vec<Vec<(GenEvent, u16)>>, Vec<Vec<(GenClick, u16)>>) = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|p| {
                let outs = &outs;
                let arenas = &arenas;
                s.spawn(move || {
                    let mut best: HashMap<(u64, &[u8]), (GenEvent, u16)> = HashMap::new();
                    for (t, o) in outs.iter().enumerate() {
                        for g in &o.gev[p] {
                            let key = &arenas[t][g.eid_off as usize..(g.eid_off + g.eid_len) as usize];
                            match best.get_mut(&(g.hash, key)) {
                                Some(cur) => {
                                    if g.ingest < cur.0.ingest {
                                        *cur = (*g, t as u16);
                                    }
                                }
                                None => {
                                    best.insert((g.hash, key), (*g, t as u16));
                                }
                            }
                        }
                    }
                    let evs: Vec<(GenEvent, u16)> = best.into_values().filter(|(g, _)| g.valid).collect();
                    let mut seen: HashMap<(u64, &[u8]), ()> = HashMap::new();
                    let mut cls = Vec::new();
                    for (t, o) in outs.iter().enumerate() {
                        for g in &o.gcl[p] {
                            let key = &arenas[t][g.cid_off as usize..(g.cid_off + g.cid_len) as usize];
                            if seen.insert((g.hash, key), ()).is_none() {
                                cls.push((*g, t as u16));
                            }
                        }
                    }
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
                for &(slot, w0s, w1) in &o.overflow {
                    if ev.w0(slot as usize).load(Relaxed) == w0s {
                        ev.w1(slot as usize).store(w1, Relaxed);
                    }
                }
            });
        }
    });
    lap("overflow");

    // event runs over slot ranges
    let mut entries: Vec<Vec<Vec<Entry>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (ev, links) = (&ev, &links);
                s.spawn(move || {
                    let mut b: Vec<Vec<Entry>> = (0..NB).map(|_| Vec::new()).collect();
                    if !ctx.pos {
                        return b;
                    }
                    let mut cache_k = [u64::MAX; 64];
                    let mut cache_v = [0u64; 64];
                    let lo = max_e * t / nt;
                    let hi = max_e * (t + 1) / nt;
                    let mut cur = Entry { comp: u64::MAX, kind: 0, sorted: true, t: 0, a: 0, b: 0 };
                    let mut last_ts = 0u64;
                    for i in lo..hi {
                        let s0 = ev.w0(i).load(Relaxed);
                        if s0 == 0 {
                            continue;
                        }
                        let w0 = !s0;
                        if w0 & (1 << 10) == 0 {
                            continue;
                        }
                        let w1 = ev.w1(i).load(Relaxed);
                        let uid = (w1 & (GEN_TAG - 1)) | if w0 & 8 != 0 { GEN_TAG } else { 0 };
                        let h = (hash64(uid) >> 58) as usize;
                        let comp = if cache_k[h] == uid {
                            cache_v[h]
                        } else {
                            let c = links.comp(uid);
                            cache_k[h] = uid;
                            cache_v[h] = c;
                            c
                        };
                        let ts = (w0 >> 11) & 0x7FFF_FFFF;
                        if comp == cur.comp {
                            cur.b = i as u32 + 1;
                            if ts < last_ts {
                                cur.sorted = false;
                            }
                        } else {
                            if cur.comp != u64::MAX {
                                b[bucket_of(cur.comp)].push(cur);
                            }
                            cur = Entry { comp, kind: 0, sorted: true, t: 0, a: i as u32, b: i as u32 + 1 };
                        }
                        last_ts = ts;
                    }
                    if cur.comp != u64::MAX {
                        b[bucket_of(cur.comp)].push(cur);
                    }
                    b
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("event runs");

    // click runs and generic entries, bucketed by person
    let entries2: Vec<Vec<Vec<Entry>>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|t| {
                let (ev, cl, links, gev_kept, gcl_kept) = (&ev, &cl, &links, &gev_kept, &gcl_kept);
                s.spawn(move || {
                    let mut b: Vec<Vec<Entry>> = (0..NB).map(|_| Vec::new()).collect();
                    let mut cache_k = [u64::MAX; 64];
                    let mut cache_v = [0u64; 64];
                    let mut lookup = |uid: u64| -> u64 {
                        let h = (hash64(uid) >> 58) as usize;
                        if cache_k[h] == uid {
                            return cache_v[h];
                        }
                        let c = links.comp(uid);
                        cache_k[h] = uid;
                        cache_v[h] = c;
                        c
                    };
                    let mut push = |b: &mut Vec<Vec<Entry>>, e: Entry| b[bucket_of(e.comp)].push(e);
                    if ctx.pos {
                        // click runs
                        let lo = max_c * t / nt;
                        let hi = max_c * (t + 1) / nt;
                        let mut cur = Entry { comp: u64::MAX, kind: 1, sorted: true, t: 0, a: 0, b: 0 };
                        for i in lo..hi {
                            let w0 = cl.w0(i).load(Relaxed);
                            if w0 == 0 {
                                continue;
                            }
                            let w1 = cl.w1(i).load(Relaxed);
                            let uid = w1 | if w0 & 1 != 0 { GEN_TAG } else { 0 };
                            let comp = lookup(uid);
                            if comp == cur.comp {
                                cur.b = i as u32 + 1;
                            } else {
                                if cur.comp != u64::MAX {
                                    push(&mut b, cur);
                                }
                                cur = Entry { comp, kind: 1, sorted: true, t: 0, a: i as u32, b: i as u32 + 1 };
                            }
                        }
                        if cur.comp != u64::MAX {
                            push(&mut b, cur);
                        }
                    }
                    for (k, (g, _)) in gev_kept[t].iter().enumerate() {
                        push(&mut b, Entry { comp: lookup(g.uid), kind: 2, sorted: true, t: t as u16, a: k as u32, b: 0 });
                    }
                    for (k, (g, _)) in gcl_kept[t].iter().enumerate() {
                        push(&mut b, Entry { comp: lookup(g.uid), kind: 3, sorted: true, t: t as u16, a: k as u32, b: 0 });
                    }
                    b
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    entries.extend(entries2);
    lap("runs");

    // per-bucket person processing
    let ncamp = ctx.camp_names.len();
    let ndays = ctx.ndays;
    let next_b = AtomicUsize::new(0);
    let outs2: Vec<Out> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                let (ev, cl, entries, gev_kept, gcl_kept, next_b, arenas) =
                    (&ev, &cl, &entries, &gev_kept, &gcl_kept, &next_b, &arenas);
                s.spawn(move || {
                    let mut out = Out {
                        users: Vec::new(),
                        camp: vec![[0u64; 4]; (ncamp + 1) * ndays],
                        cat_rev: vec![0; ctx.cats.len()],
                        cat_touched: Vec::new(),
                        stamp: vec![0; ncamp + 1],
                        stamp_n: 0,
                    };
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
                        list.sort_unstable_by(|x, y| x.comp.cmp(&y.comp).then(x.kind.cmp(&y.kind)).then(x.a.cmp(&y.a)));
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
                                        for i in e.a as usize..e.b as usize {
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
                                    2 => {
                                        nruns += 2;
                                        let (ge, _) = gev_kept[e.t as usize][e.a as usize];
                                        evbuf.push(Ev { ts: ge.ts, price: ge.price, cat: ge.cat, purchase: ge.purchase });
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
                                        for i in e.a as usize..e.b as usize {
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
                                    3 => {
                                        let (gc, t) = gcl_kept[e.t as usize][e.a as usize];
                                        clbuf.push(Cl { ts: gc.ts, camp: gc.camp, key: ClKey::Gen(t as u32, gc.cid_off, gc.cid_len) });
                                    }
                                    _ => {}
                                }
                            }
                            if clbuf.len() > 1 {
                                clbuf.sort_by(|a, b| click_cmp_refs(a, b, arenas));
                            }
                            process_person(ctx, comp, &evbuf, &clbuf, &mut out);
                        }
                    }
                    out
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    lap("sessions");

    // outputs
    let ud = out_dir.join(names::OUT_A);
    let cd = out_dir.join(names::OUT_B);
    let _ = std::fs::create_dir_all(&ud);
    let _ = std::fs::create_dir_all(&cd);
    std::thread::scope(|s| {
        for (t, o) in outs2.iter().enumerate() {
            let ud = &ud;
            let links = &links;
            s.spawn(move || {
                let n = o.users.len();
                let mut c_user = parquet::Column::new(names::A_ID, parquet::Kind::Str, n);
                let mut c_day = parquet::Column::new(names::DAY, parquet::Kind::Date, n);
                let mut c_s = parquet::Column::new(names::N_S, parquet::Kind::I64, n);
                let mut c_e = parquet::Column::new(names::N_E, parquet::Kind::I64, n);
                let mut c_r = parquet::Column::new(names::REV, parquet::Kind::I64, n);
                let mut c_t = parquet::Column::new(names::TOP, parquet::Kind::OptStr, n);
                let mut buf = Vec::new();
                for r in &o.users {
                    uid_bytes(links.name_of(r.comp), &mut buf);
                    c_user.push_str(&buf);
                    c_day.push_i32(r.day);
                    c_s.push_i64(r.sessions as i64);
                    c_e.push_i64(r.events as i64);
                    c_r.push_i64(r.revenue as i64);
                    c_t.push_opt(if r.top == u16::MAX { None } else { Some(&ctx.cats[r.top as usize]) });
                }
                parquet::write(&ud.join(format!("part-{t}.parquet")), &[c_user, c_day, c_s, c_e, c_r, c_t]).expect("write");
            });
        }
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
    });
    lap("output");
    unsafe { libc::_exit(0) };
}

fn click_cmp_refs(a: &Cl, b: &Cl, arenas: &[&Vec<u8>]) -> std::cmp::Ordering {
    a.ts.cmp(&b.ts).then_with(|| {
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
