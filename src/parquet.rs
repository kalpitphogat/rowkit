//! A minimal Parquet writer: one row group, one PLAIN data page per column,
//! no compression. Enough for strings, DATE (INT32) and INT64 columns, with
//! an optional (nullable) string column.

pub enum Kind {
    Str,
    OptStr,
    Date,
    I32,
    I64,
}

pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    /// PLAIN-encoded values (non-null only for OptStr).
    pub data: Vec<u8>,
    /// Definition levels for OptStr: one bit per row (1 = present), LSB first.
    pub defs: Vec<u8>,
    pub rows: usize,
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, cap_rows: usize) -> Self {
        let per = match kind {
            Kind::Str | Kind::OptStr => 14,
            Kind::Date | Kind::I32 => 4,
            Kind::I64 => 8,
        };
        Column { name, kind, data: Vec::with_capacity(cap_rows * per), defs: Vec::new(), rows: 0 }
    }
    #[inline]
    pub fn push_str(&mut self, s: &[u8]) {
        self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.data.extend_from_slice(s);
        self.rows += 1;
    }
    #[inline]
    pub fn push_opt(&mut self, s: Option<&[u8]>) {
        let r = self.rows;
        if r % 8 == 0 {
            self.defs.push(0);
        }
        if let Some(s) = s {
            *self.defs.last_mut().unwrap() |= 1 << (r % 8);
            self.data.extend_from_slice(&(s.len() as u32).to_le_bytes());
            self.data.extend_from_slice(s);
        }
        self.rows += 1;
    }
    #[inline]
    pub fn push_i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_le_bytes());
        self.rows += 1;
    }
    #[inline]
    pub fn push_i64(&mut self, v: i64) {
        self.data.extend_from_slice(&v.to_le_bytes());
        self.rows += 1;
    }
}

struct Tw {
    b: Vec<u8>,
    last: Vec<i16>,
    cur: i16,
}

impl Tw {
    fn new() -> Self {
        Tw { b: Vec::with_capacity(1024), last: Vec::new(), cur: 0 }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }
    fn field(&mut self, id: i16, ty: u8) {
        let d = id - self.cur;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.varint(Self::zz(id as i64));
        }
        self.cur = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.varint(Self::zz(v as i64));
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.varint(Self::zz(v));
    }
    fn bin(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.push_scope();
    }
    fn push_scope(&mut self) {
        self.last.push(self.cur);
        self.cur = 0;
    }
    fn end(&mut self) {
        self.b.push(0);
        self.cur = self.last.pop().unwrap();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

fn rle_defs(col: &Column) -> Vec<u8> {
    // Bit-packed run covering all rows: header (groups << 1) | 1, then one byte per 8 values.
    let groups = col.rows.div_ceil(8);
    if groups == 0 {
        return vec![0, 0, 0, 0];
    }
    let mut run = Tw::new();
    run.varint(((groups as u64) << 1) | 1);
    let mut body = run.b;
    body.extend_from_slice(&col.defs[..groups]);
    let mut out = Vec::with_capacity(body.len() + 4);
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Writes the columns (all with the same row count) to `path` as one Parquet file.
pub fn write(path: &std::path::Path, cols: &[Column]) -> std::io::Result<()> {
    let rows = cols.first().map(|c| c.rows).unwrap_or(0);
    let mut file: Vec<u8> = Vec::with_capacity(cols.iter().map(|c| c.data.len() + 64).sum::<usize>() + 4096);
    file.extend_from_slice(b"PAR1");
    let mut metas: Vec<(usize, usize)> = Vec::new(); // (offset, size)
    for c in cols {
        let defs = if let Kind::OptStr = c.kind { rle_defs(c) } else { Vec::new() };
        let page_len = defs.len() + c.data.len();
        let mut h = Tw::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page_len as i32);
        h.i32(3, page_len as i32);
        h.begin(5);
        h.i32(1, rows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3); // RLE
        h.end();
        h.b.push(0);
        let off = file.len();
        file.extend_from_slice(&h.b);
        file.extend_from_slice(&defs);
        file.extend_from_slice(&c.data);
        metas.push((off, file.len() - off));
    }
    let mut t = Tw::new();
    t.i32(1, 1);
    t.list(2, 12, cols.len() + 1);
    // root
    t.push_scope();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.push_scope();
        let (ty, rep, conv): (i32, i32, i32) = match c.kind {
            Kind::Str => (6, 0, 0),
            Kind::OptStr => (6, 1, 0),
            Kind::Date => (1, 0, 6),
            Kind::I32 => (1, 0, -1),
            Kind::I64 => (2, 0, -1),
        };
        t.i32(1, ty);
        t.i32(3, rep);
        t.bin(4, c.name.as_bytes());
        if conv >= 0 {
            t.i32(6, conv);
            t.begin(10);
            if conv == 0 {
                t.begin(1);
                t.end();
            } else {
                t.begin(6);
                t.end();
            }
            t.end();
        }
        t.end();
    }
    t.i64(3, rows as i64);
    t.list(4, 12, 1);
    t.push_scope();
    t.list(1, 12, cols.len());
    let mut total = 0i64;
    for (c, &(off, size)) in cols.iter().zip(&metas) {
        t.push_scope();
        t.i64(2, off as i64);
        t.begin(3);
        let ty = match c.kind {
            Kind::Str | Kind::OptStr => 6,
            Kind::Date | Kind::I32 => 1,
            Kind::I64 => 2,
        };
        t.i32(1, ty);
        t.list(2, 5, 2);
        t.varint(Tw::zz(0));
        t.varint(Tw::zz(3));
        t.list(3, 8, 1);
        t.varint(c.name.len() as u64);
        t.b.extend_from_slice(c.name.as_bytes());
        t.i32(4, 0);
        t.i64(5, rows as i64);
        t.i64(6, size as i64);
        t.i64(7, size as i64);
        t.i64(9, off as i64);
        t.end();
        t.end();
        total += size as i64;
    }
    t.i64(2, total);
    t.i64(3, rows as i64);
    t.end();
    t.b.push(0);
    file.extend_from_slice(&t.b);
    file.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    std::fs::write(path, &file)
}
