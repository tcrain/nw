use std::{
    cmp, fmt,
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
};

#[inline(always)]
fn is_pow2(x: usize) -> bool {
    x != 0 && x & (x - 1) == 0
}

#[inline(always)]
fn modulo_pow2(x: usize, d: usize) -> usize {
    debug_assert!(is_pow2(d));
    debug_assert!(x & (d - 1) == x % d);
    x & (d - 1)
}

pub trait SeekRelative {
    fn seek_relative(&mut self, pos: i64) -> io::Result<()>;
}

pub trait RWS: Read + Write + Seek + SeekRelative {}
impl<T: Read + Write + Seek + SeekRelative> RWS for T {}

use crate::config::DEFAULT_BUF_SIZE;

struct WriteLocations {
    w: WriteRange, // total start and end positions of the writes (can be larger than b_len)
    pos: usize,    // total position in the buffer (can be larger than b_len)
    end_w: usize,  // maximum write location (in the buffer, i.e. 0 <= end_w < b_len)
    at_end: bool,
}

impl<W: Read + Write + Seek> Write for RWBuf<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let b_len = self.buf.len();
        // if we are writing a larger amount than our buffer, just write it directly
        let buf_len = buf.len();
        if buf_len == 0 {
            return Ok(0);
        }
        if buf_len >= b_len {
            self.flush_inner()?;
            self.clear_buf(true)?;
            return self.inner.write(buf);
        }
        if let Some((l, _)) = self.write_range.get_l_r() {
            if self.pos == l && self.increased_pos {
                // the buffer is full, we must flush it before writing
                self.flush_inner()?;
            }
        }
        // find the end of where we can write
        let loc = self.compute_write_locations();
        // let loc = self.compute_locations();
        let w_len = cmp::min(buf_len, loc.end_w - self.pos);
        debug_assert!(w_len > 0);
        let w_end = self.pos + w_len;
        self.buf[self.pos..w_end].copy_from_slice(&buf[..w_len]);
        let (new_l, new_r) = {
            if self.write_range.is_none() {
                (self.pos, w_end)
            } else {
                let (l, r) = loc.w.get_l_r().unwrap();
                let new_l = {
                    if loc.pos < l {
                        // we have a smaller write start
                        loc.pos
                    } else {
                        // we use the previous write start
                        l
                    }
                };
                let w_end = loc.pos + w_len;
                let new_r = {
                    if w_end > r {
                        // we have a larger write end
                        w_end
                    } else {
                        // we use the previous write end
                        r
                    }
                };
                (new_l, new_r)
            }
        };
        match self.write_range {
            WriteRange::None => {
                if loc.at_end {
                    //new_l == self.pos && (new_l == self.cap.get_cap()) && self.increased_pos {
                    // the write started at the end of the buffer
                    self.write_range = WriteRange::AtEnd(new_l, new_r);
                } else {
                    // the write did not start at the end of the buffer
                    self.write_range = WriteRange::AtStart(new_l, new_r)
                }
            }
            _ => self.write_range.update(new_l, new_r),
        }
        self.pos = modulo_pow2(self.pos + w_len, b_len);
        self.check_overflow(true, b_len);
        Ok(w_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_inner()?;
        self.inner.flush()
    }
}

impl<W: Read + Write + Seek> RWBuf<W> {
    /// empties the buffer, if seek_to_pos is true, then we will also seek in the inner
    /// until the actual position
    fn clear_buf(&mut self, seek_to_pos: bool) -> io::Result<()> {
        debug_assert!(self.write_range.is_none());
        if seek_to_pos {
            // we need to seek the uderlying buffer from self.cap to self.pos
            let loc = self.compute_seek_locations();
            // let loc = self.compute_locations();
            // compute how far ahead of the inner buffer we are
            let ahead = loc.pos as i64 - self.get_seek_cap() as i64;
            if ahead != 0 {
                self.inner.seek(SeekFrom::Current(ahead))?;
            }
        }
        self.pos = 0;
        self.increased_pos = false;
        self.cap = Capacity::Partial(0);
        Ok(())
    }

    // gets the offset in the inner buffer
    fn get_offset(&mut self) -> io::Result<u64> {
        let loc = self.compute_locations();
        Ok((self.inner.seek(SeekFrom::Current(0))? as i64
            + (loc.pos as i64 - self.get_seek_cap() as i64)) as u64)
    }

    fn flush_inner(&mut self) -> io::Result<()> {
        if !self.write_range.is_none() {
            let cap = self.get_seek_cap();
            // compute how much we seek before writing
            // and compute how much we should seek after writing
            // if r ovewrote cap, then we do not need to seek
            // othewise we need to seek back to cap

            let wr = self.compute_write_range();
            // let wr = self.compute_locations().w;
            // take out the locations
            let (l, r) = self.write_range.take().get_l_r().unwrap();
            // seek from cap to l
            let (abs_l, abs_r) = wr.get_l_r().unwrap();

            let dist = abs_l as i64 - cap as i64;
            if dist != 0 {
                self.inner.seek(SeekFrom::Current(dist))?;
            }
            if r == 0 {
                // we write until the end of the buffer
                self.inner.write_all(&self.buf[l..])?;
            } else if r > l {
                // we write until r
                self.inner.write_all(&self.buf[l..r])?;
            } else {
                // r overlaps so we write both sections
                // TODO better to call write twice?
                self.inner
                    .write_all(&[&self.buf[l..], &self.buf[..r]].concat())?;
            }
            if abs_r >= cap {
                self.cap.update(r);
            } else {
                // we have to seek forward to cap
                let dist = cap as i64 - abs_r as i64;
                self.inner.seek(SeekFrom::Current(dist))?;
            }
        }
        // TODO should call inner flush here?
        Ok(())
    }
}

impl<W: Read + Write + Seek> SeekRelative for RWBuf<W> {
    /// Seeks relative to the current position.
    fn seek_relative(&mut self, offset: i64) -> io::Result<()> {
        match offset.cmp(&0) {
            cmp::Ordering::Equal => (), // no seek needed
            cmp::Ordering::Less => {
                let loc = self.compute_seek_locations();
                // let loc = self.compute_locations();
                let b_len = self.buf.len();
                let off = offset.abs() as usize;
                // we can seek back either until cap or r, if r writes past cap
                if off <= loc.pos - loc.seek_min {
                    self.pos = modulo_pow2(loc.pos - off, b_len);
                    self.check_overflow(false, b_len)
                } else {
                    self.seek(SeekFrom::Current(offset))?;
                }
            }
            cmp::Ordering::Greater => {
                let loc = self.compute_seek_locations();
                // let loc = self.compute_locations();
                let b_len = self.buf.len();
                let off = offset as usize;
                // we can seek to read end
                if off <= loc.seek_max - loc.pos {
                    self.pos = modulo_pow2(self.pos + off, b_len);
                    self.check_overflow(true, b_len)
                } else {
                    self.seek(SeekFrom::Current(offset))?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum WriteRange {
    None,
    AtStart(usize, usize),
    AtEnd(usize, usize),
}

impl Default for WriteRange {
    #[inline(always)]
    fn default() -> Self {
        WriteRange::None
    }
}

impl WriteRange {
    #[inline(always)]
    pub fn take(&mut self) -> WriteRange {
        mem::take(self)
    }

    #[inline(always)]
    fn is_none(&self) -> bool {
        matches!(*self, WriteRange::None)
    }

    #[inline(always)]
    fn is_at_start(&self) -> bool {
        matches!(*self, WriteRange::AtStart(_, _))
    }

    #[inline(always)]
    fn is_at_end(&self) -> bool {
        matches!(*self, WriteRange::AtEnd(_, _))
    }

    fn update_copy_or_else<F>(&self, l: usize, r: usize, f: F) -> WriteRange
    where
        F: FnOnce() -> WriteRange,
    {
        match self {
            WriteRange::None => f(),
            _ => self.update_copy(l, r),
        }
    }

    #[inline(always)]
    fn perform_mod(&mut self, b_len: usize) {
        let (l, r) = match self {
            WriteRange::None => return,
            WriteRange::AtStart(l, r) => (l, r),
            WriteRange::AtEnd(l, r) => (l, r),
        };
        if *l >= b_len {
            *l = modulo_pow2(*l, b_len);
        }
        if *r >= b_len {
            *r = modulo_pow2(*r, b_len);
        }
    }

    #[inline(always)]
    fn update(&mut self, l: usize, r: usize) {
        if let WriteRange::AtStart(old_l, old_r) = self {
            *old_l = l;
            *old_r = r;
        } else if let WriteRange::AtEnd(old_l, old_r) = self {
            *old_l = l;
            *old_r = r;
        }
    }

    #[inline(always)]
    fn update_copy(self, l: usize, r: usize) -> WriteRange {
        match self {
            WriteRange::None => panic!("should not be called on none"),
            WriteRange::AtStart(_, _) => WriteRange::AtStart(l, r),
            WriteRange::AtEnd(_, _) => WriteRange::AtEnd(l, r),
        }
    }

    #[inline(always)]
    fn get_l_r(&self) -> Option<(usize, usize)> {
        match self {
            WriteRange::None => None,
            WriteRange::AtStart(l, r) => Some((*l, *r)),
            WriteRange::AtEnd(l, r) => Some((*l, *r)),
        }
    }
}

pub struct RWBuf<R: Read + Write + Seek> {
    inner: R,
    buf: Box<[u8]>,
    pos: usize,              // position of the reader in the buffer
    increased_pos: bool,     // true if the last operation moved forward in the buffer
    cap: Capacity,           // position of inner reader in the buffer
    write_range: WriteRange, // if the write range goes past cap
}

#[derive(Debug)]
enum Capacity {
    Partial(usize), // we have not yet filled the entire buffer
    Full(usize),    // we have filled the entire buffer
}

impl Capacity {
    fn get_cap(&self) -> usize {
        match self {
            Capacity::Partial(s) => *s,
            Capacity::Full(s) => *s,
        }
    }

    fn is_partial(&self) -> bool {
        matches!(*self, Capacity::Partial(_))
    }

    fn update(&mut self, s: usize) {
        *self = match self {
            Capacity::Full(_) => Capacity::Full(s),
            Capacity::Partial(_) => Capacity::Partial(s),
        }
    }
}

#[derive(Debug)]
struct Locations {
    w: WriteRange, // total start and end positions of the writes (can be larger than b_len)
    pos: usize,    // total position in the buffer (can be larger than b_len)
    end_w: usize,  // maximum write location (in the buffer, i.e. 0 <= end_w < b_len)
    end_r: usize,  // maximum read location (in the buffer, i.e. 0 <= end_r < b_len)
    // minimum seek position, will either be 0 if the buf is not full, or cap if it is full and r does not overwrite cap, or self.w.1 - b_len (b_len less than the actual position of the end of the write)
    seek_min: usize,
    // maximum seek position
    seek_max: usize,
    at_end: bool, // true if we are at the end of the buffer
}

struct ReadLocations {
    end_r: usize, // maximum read location (in the buffer, i.e. 0 <= end_r < b_len)
    at_end: bool, // true if we are at the end of the buffer
}

struct SeekLocations {
    pos: usize, // total position in the buffer (can be larger than b_len)
    seek_min: usize,
    // maximum seek position
    seek_max: usize,
}

impl<R: Read + Write + Seek> RWBuf<R> {
    pub fn new(inner: R) -> RWBuf<R> {
        RWBuf::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    /// Creates a new `BufReader<R>` with the specified buffer capacity.
    pub fn with_capacity(capacity: usize, inner: R) -> RWBuf<R> {
        assert!(is_pow2(capacity));
        let buffer = vec![0; capacity];
        RWBuf {
            inner,
            buf: buffer.into_boxed_slice(),
            pos: 0,
            increased_pos: false,
            cap: Capacity::Partial(0),
            write_range: WriteRange::None,
        }
    }

    /// Returns the number of bytes the internal buffer can hold at once.
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    fn check_overflow(&mut self, increased_pos: bool, b_len: usize) {
        // if we have loaded or written to the end of the buffer, then we are now full
        if let Capacity::Partial(cap) = self.cap {
            if cap == b_len
                || self
                    .write_range
                    .get_l_r()
                    .map_or(false, |(_, r)| r == b_len)
            {
                self.cap = Capacity::Full(cap)
            }
        }
        if self.pos == b_len {
            self.pos = 0;
        }
        let cap = self.cap.get_cap();
        if cap == b_len {
            self.cap.update(0);
        }
        self.increased_pos = increased_pos;
        self.write_range.perform_mod(b_len);
    }

    /// returns the cap, when used for seeking
    #[inline(always)]
    fn get_seek_cap(&self) -> usize {
        let cap = self.cap.get_cap();
        if self.cap.is_partial() {
            return cap;
        }
        if let Some((l, _)) = self.write_range.get_l_r() {
            if l == cap {
                // the write starts at cap
                return cap + self.buf.len();
            }
        }
        // cap overlaps the buffer
        cap + self.buf.len()
    }

    /// compute the write locations, and the buffer position, based on the current value of self.cap,
    /// (without taking the modulos when they would overlap the buffer)
    #[inline(always)]
    fn compute_locations(&mut self) -> Locations {
        let b_len = self.buf.len();
        let cap = match self.cap {
            Capacity::Partial(cap) => {
                let end_r = self
                    .write_range
                    .get_l_r()
                    .map_or(cap, |(_, r)| cmp::max(cap, r));
                return Locations {
                    w: self.write_range,
                    pos: self.pos,
                    end_w: b_len,
                    end_r,
                    seek_min: 0,
                    seek_max: end_r,
                    at_end: end_r == self.pos,
                    // seek_cap: cap,
                };
            }
            Capacity::Full(cap) => cap,
        };
        match self.write_range.get_l_r() {
            None => {
                let (pos, end_r, at_end) = {
                    let at_cap = self.pos == cap;
                    if self.pos < cap || (at_cap && self.increased_pos) {
                        (self.pos + b_len, cap, at_cap)
                    } else {
                        (self.pos, b_len, false)
                    }
                };
                Locations {
                    w: WriteRange::None,
                    pos,
                    end_w: b_len,
                    end_r,
                    seek_max: cap + b_len,
                    seek_min: cap, // we can seek back to cap
                    at_end,
                    // seek_cap: cap + b_len,
                }
            }
            Some((l, r)) => {
                if l < cap {
                    if r > l {
                        // the start of the write overlaps the buffer
                        let m = cmp::max(cap, r);
                        let (pos, end_r, at_end) = {
                            let at_m = self.pos == m;
                            if self.pos < m || (at_m && self.increased_pos) {
                                (self.pos + b_len, m, at_m)
                            } else {
                                (self.pos, b_len, false)
                            }
                        };
                        let seek_min = {
                            if r > cap {
                                r
                            } else {
                                cap
                            }
                        };
                        Locations {
                            w: self.write_range.update_copy(l + b_len, r + b_len),
                            pos,
                            end_w: b_len,
                            end_r,
                            seek_max: seek_min + b_len,
                            seek_min,
                            at_end,
                        }
                    } else {
                        // the end of the write buffer overlaps twice
                        let (pos, end_w, end_r, at_end) = {
                            let at_r = self.pos == r;
                            if self.pos < r || (at_r && self.increased_pos) {
                                // we cannot write past l, since then we would overwrite the written buffer
                                (self.pos + b_len + b_len, l, r, at_r)
                            } else {
                                (self.pos + b_len, b_len, b_len, false)
                            }
                        };
                        Locations {
                            w: self.write_range.update_copy(l + b_len, r + b_len + b_len),
                            pos,
                            end_w,
                            end_r,
                            seek_max: r + b_len + b_len,
                            seek_min: r + b_len,
                            at_end,
                        }
                    }
                } else if l == cap && self.write_range.is_at_end() {
                    // the end of the buffer is r
                    let (pos, end_w, end_r, at_end) = {
                        let at_cap = self.pos == r;
                        if self.pos < r || (at_cap && self.increased_pos) {
                            // pos overlaps the buffer
                            if r <= l {
                                // pos overlaps twice
                                (self.pos + b_len + b_len, l, r, at_cap)
                            } else {
                                (self.pos + b_len, b_len, r, at_cap)
                            }
                        } else if r <= l {
                            (self.pos + b_len, b_len, b_len, false)
                        } else {
                            (self.pos, b_len, b_len, false)
                        }
                    };
                    let (new_l, new_r) = (l + b_len, {
                        if r <= l {
                            r + b_len + b_len
                        } else {
                            r + b_len
                        }
                    });
                    Locations {
                        w: self.write_range.update_copy(new_l, new_r),
                        pos,
                        end_w,
                        end_r,
                        seek_max: new_r,
                        seek_min: new_r - b_len,
                        at_end,
                    }
                } else if r > l {
                    // the write does not overlap the buffer
                    let (pos, end_r, at_end) = {
                        let at_cap = self.pos == cap;
                        if self.pos < cap || (at_cap && self.increased_pos) {
                            (self.pos + b_len, cap, at_cap)
                        } else {
                            (self.pos, b_len, false)
                        }
                    };
                    Locations {
                        w: self.write_range,
                        pos,
                        end_w: {
                            if self.pos < l {
                                l
                            } else {
                                b_len
                            }
                        },
                        end_r,
                        seek_max: cap + b_len,
                        seek_min: cap,
                        at_end,
                    }
                } else {
                    // just the end of the write overlaps the buffer
                    let m = cmp::max(cap, r);
                    let (pos, end_w, end_r, at_end) = {
                        let at_m = self.pos == m;
                        if self.pos < m || (at_m && self.increased_pos) {
                            // we cannot write past l, since then we would overwrite the written buffer
                            (self.pos + b_len, l, m, at_m)
                        } else {
                            (self.pos, b_len, b_len, false)
                        }
                    };
                    let seek_min = {
                        if r > cap {
                            r
                        } else {
                            cap
                        }
                    };
                    Locations {
                        w: self.write_range.update_copy(l, r + b_len),
                        pos,
                        seek_max: seek_min + b_len,
                        seek_min,
                        end_w,
                        end_r,
                        at_end,
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn compute_write_locations(&mut self) -> WriteLocations {
        let b_len = self.buf.len();
        let cap = match self.cap {
            Capacity::Partial(cap) => {
                let end_r = self
                    .write_range
                    .get_l_r()
                    .map_or(cap, |(_, r)| cmp::max(cap, r));
                return WriteLocations {
                    w: self.write_range,
                    pos: self.pos,
                    end_w: b_len,
                    at_end: end_r == self.pos,
                    // seek_cap: cap,
                };
            }
            Capacity::Full(cap) => cap,
        };
        match self.write_range.get_l_r() {
            None => {
                let (pos, at_end) = {
                    let at_cap = self.pos == cap;
                    if self.pos < cap || (at_cap && self.increased_pos) {
                        (self.pos + b_len, at_cap)
                    } else {
                        (self.pos, false)
                    }
                };
                WriteLocations {
                    w: WriteRange::None,
                    pos,
                    end_w: b_len,
                    at_end,
                }
            }
            Some((l, r)) => {
                if l < cap {
                    if r > l {
                        // the start of the write overlaps the buffer
                        let m = cmp::max(cap, r);
                        let (pos, at_end) = {
                            let at_m = self.pos == m;
                            if self.pos < m || (at_m && self.increased_pos) {
                                (self.pos + b_len, at_m)
                            } else {
                                (self.pos, false)
                            }
                        };
                        WriteLocations {
                            w: self.write_range.update_copy(l + b_len, r + b_len),
                            pos,
                            end_w: b_len,
                            at_end,
                        }
                    } else {
                        // the end of the write buffer overlaps twice
                        let (pos, end_w, at_end) = {
                            let at_r = self.pos == r;
                            if self.pos < r || (at_r && self.increased_pos) {
                                // we cannot write past l, since then we would overwrite the written buffer
                                (self.pos + b_len + b_len, l, at_r)
                            } else {
                                (self.pos + b_len, b_len, false)
                            }
                        };
                        WriteLocations {
                            w: self.write_range.update_copy(l + b_len, r + b_len + b_len),
                            pos,
                            end_w,
                            at_end,
                        }
                    }
                } else if l == cap && self.write_range.is_at_end() {
                    // the end of the buffer is r
                    let (pos, end_w, at_end) = {
                        let at_cap = self.pos == r;
                        if self.pos < r || (at_cap && self.increased_pos) {
                            // pos overlaps the buffer
                            if r <= l {
                                // pos overlaps twice
                                (self.pos + b_len + b_len, l, at_cap)
                            } else {
                                (self.pos + b_len, b_len, at_cap)
                            }
                        } else if r <= l {
                            (self.pos + b_len, b_len, false)
                        } else {
                            (self.pos, b_len, false)
                        }
                    };
                    let (new_l, new_r) = (l + b_len, {
                        if r <= l {
                            r + b_len + b_len
                        } else {
                            r + b_len
                        }
                    });
                    WriteLocations {
                        w: self.write_range.update_copy(new_l, new_r),
                        pos,
                        end_w,
                        at_end,
                    }
                } else if r > l {
                    // the write does not overlap the buffer
                    let (pos, at_end) = {
                        let at_cap = self.pos == cap;
                        if self.pos < cap || (at_cap && self.increased_pos) {
                            (self.pos + b_len, at_cap)
                        } else {
                            (self.pos, false)
                        }
                    };
                    WriteLocations {
                        w: self.write_range,
                        pos,
                        end_w: {
                            if self.pos < l {
                                l
                            } else {
                                b_len
                            }
                        },
                        at_end,
                    }
                } else {
                    // just the end of the write overlaps the buffer
                    let m = cmp::max(cap, r);
                    let (pos, end_w, at_end) = {
                        let at_m = self.pos == m;
                        if self.pos < m || (at_m && self.increased_pos) {
                            // we cannot write past l, since then we would overwrite the written buffer
                            (self.pos + b_len, l, at_m)
                        } else {
                            (self.pos, b_len, false)
                        }
                    };
                    WriteLocations {
                        w: self.write_range.update_copy(l, r + b_len),
                        pos,
                        end_w,
                        at_end,
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn compute_write_range(&mut self) -> WriteRange {
        let b_len = self.buf.len();
        let cap = match self.cap {
            Capacity::Partial(_) => return self.write_range,
            Capacity::Full(cap) => cap,
        };
        match self.write_range.get_l_r() {
            None => WriteRange::None,
            Some((l, r)) => {
                if l < cap {
                    if r > l {
                        self.write_range.update_copy(l + b_len, r + b_len)
                    } else {
                        self.write_range.update_copy(l + b_len, r + b_len + b_len)
                    }
                } else if l == cap && self.write_range.is_at_end() {
                    // the end of the buffer is r
                    let (new_l, new_r) = (l + b_len, {
                        if r <= l {
                            r + b_len + b_len
                        } else {
                            r + b_len
                        }
                    });
                    self.write_range.update_copy(new_l, new_r)
                } else if r > l {
                    // the write does not overlap the buffer
                    self.write_range
                } else {
                    // just the end of the write overlaps the buffer
                    self.write_range.update_copy(l, r + b_len)
                }
            }
        }
    }

    #[inline(always)]
    fn compute_read_locations(&mut self) -> ReadLocations {
        let b_len = self.buf.len();
        let cap = match self.cap {
            Capacity::Partial(cap) => {
                let end_r = self
                    .write_range
                    .get_l_r()
                    .map_or(cap, |(_, r)| cmp::max(cap, r));
                return ReadLocations {
                    end_r,
                    at_end: end_r == self.pos,
                };
            }
            Capacity::Full(cap) => cap,
        };
        match self.write_range.get_l_r() {
            None => {
                let (end_r, at_end) = {
                    let at_cap = self.pos == cap;
                    if self.pos < cap || (at_cap && self.increased_pos) {
                        (cap, at_cap)
                    } else {
                        (b_len, false)
                    }
                };
                ReadLocations {
                    end_r,
                    at_end,
                    // seek_cap: cap + b_len,
                }
            }
            Some((l, r)) => {
                if l < cap {
                    if r > l {
                        // the start of the write overlaps the buffer
                        let m = cmp::max(cap, r);
                        let (end_r, at_end) = {
                            let at_m = self.pos == m;
                            if self.pos < m || (at_m && self.increased_pos) {
                                (m, at_m)
                            } else {
                                (b_len, false)
                            }
                        };
                        ReadLocations { end_r, at_end }
                    } else {
                        // the end of the write buffer overlaps twice
                        let (end_r, at_end) = {
                            let at_r = self.pos == r;
                            if self.pos < r || (at_r && self.increased_pos) {
                                // we cannot write past l, since then we would overwrite the written buffer
                                (r, at_r)
                            } else {
                                (b_len, false)
                            }
                        };
                        ReadLocations { end_r, at_end }
                    }
                } else if l == cap && self.write_range.is_at_end() {
                    // the end of the buffer is r
                    let (end_r, at_end) = {
                        let at_cap = self.pos == r;
                        if self.pos < r || (at_cap && self.increased_pos) {
                            // pos overlaps the buffer
                            (r, at_cap)
                        } else {
                            (b_len, false)
                        }
                    };
                    ReadLocations { end_r, at_end }
                } else if r > l {
                    // the write does not overlap the buffer
                    let (end_r, at_end) = {
                        let at_cap = self.pos == cap;
                        if self.pos < cap || (at_cap && self.increased_pos) {
                            (cap, at_cap)
                        } else {
                            (b_len, false)
                        }
                    };
                    ReadLocations { end_r, at_end }
                } else {
                    // just the end of the write overlaps the buffer
                    let m = cmp::max(cap, r);
                    let (end_r, at_end) = {
                        let at_m = self.pos == m;
                        if self.pos < m || (at_m && self.increased_pos) {
                            // we cannot write past l, since then we would overwrite the written buffer
                            (m, at_m)
                        } else {
                            (b_len, false)
                        }
                    };
                    ReadLocations { end_r, at_end }
                }
            }
        }
    }

    #[inline(always)]
    fn compute_seek_locations(&mut self) -> SeekLocations {
        let b_len = self.buf.len();
        let cap = match self.cap {
            Capacity::Partial(cap) => {
                let end_r = self
                    .write_range
                    .get_l_r()
                    .map_or(cap, |(_, r)| cmp::max(cap, r));
                return SeekLocations {
                    pos: self.pos,
                    seek_min: 0,
                    seek_max: end_r,
                };
            }
            Capacity::Full(cap) => cap,
        };
        match self.write_range.get_l_r() {
            None => {
                let pos = {
                    let at_cap = self.pos == cap;
                    if self.pos < cap || (at_cap && self.increased_pos) {
                        self.pos + b_len
                    } else {
                        self.pos
                    }
                };
                SeekLocations {
                    pos,
                    seek_max: cap + b_len,
                    seek_min: cap, // we can seek back to cap
                }
            }
            Some((l, r)) => {
                if l < cap {
                    if r > l {
                        // the start of the write overlaps the buffer
                        let m = cmp::max(cap, r);
                        let pos = {
                            let at_m = self.pos == m;
                            if self.pos < m || (at_m && self.increased_pos) {
                                self.pos + b_len
                            } else {
                                self.pos
                            }
                        };
                        let seek_min = {
                            if r > cap {
                                r
                            } else {
                                cap
                            }
                        };
                        SeekLocations {
                            pos,
                            seek_max: seek_min + b_len,
                            seek_min,
                        }
                    } else {
                        // the end of the write buffer overlaps twice
                        let pos = {
                            let at_r = self.pos == r;
                            if self.pos < r || (at_r && self.increased_pos) {
                                // we cannot write past l, since then we would overwrite the written buffer
                                self.pos + b_len + b_len
                            } else {
                                self.pos + b_len
                            }
                        };
                        SeekLocations {
                            pos,
                            seek_max: r + b_len + b_len,
                            seek_min: r + b_len,
                        }
                    }
                } else if l == cap && self.write_range.is_at_end() {
                    // the end of the buffer is r
                    let pos = {
                        let at_cap = self.pos == r;
                        if self.pos < r || (at_cap && self.increased_pos) {
                            // pos overlaps the buffer
                            if r <= l {
                                // pos overlaps twice
                                self.pos + b_len + b_len
                            } else {
                                self.pos + b_len
                            }
                        } else if r <= l {
                            self.pos + b_len
                        } else {
                            self.pos
                        }
                    };
                    let new_r = {
                        if r <= l {
                            r + b_len + b_len
                        } else {
                            r + b_len
                        }
                    };
                    SeekLocations {
                        pos,
                        seek_max: new_r,
                        seek_min: new_r - b_len,
                    }
                } else if r > l {
                    // the write does not overlap the buffer
                    let pos = {
                        let at_cap = self.pos == cap;
                        if self.pos < cap || (at_cap && self.increased_pos) {
                            self.pos + b_len
                        } else {
                            self.pos
                        }
                    };
                    SeekLocations {
                        pos,
                        seek_max: cap + b_len,
                        seek_min: cap,
                    }
                } else {
                    // just the end of the write overlaps the buffer
                    let m = cmp::max(cap, r);
                    let pos = {
                        let at_m = self.pos == m;
                        if self.pos < m || (at_m && self.increased_pos) {
                            // we cannot write past l, since then we would overwrite the written buffer
                            self.pos + b_len
                        } else {
                            self.pos
                        }
                    };
                    let seek_min = {
                        if r > cap {
                            r
                        } else {
                            cap
                        }
                    };
                    SeekLocations {
                        pos,
                        seek_max: seek_min + b_len,
                        seek_min,
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn compute_end_w(&mut self) -> usize {
        let b_len = self.buf.len();
        let cap = match self.cap {
            Capacity::Partial(_) => return b_len,
            Capacity::Full(cap) => cap,
        };
        match self.write_range.get_l_r() {
            None => b_len,
            Some((l, r)) => {
                if l < cap {
                    if r > l {
                        // the start of the write overlaps the buffer
                        b_len
                    } else {
                        let at_r = self.pos == r;
                        if self.pos < r || (at_r && self.increased_pos) {
                            // we cannot write past l, since then we would overwrite the written buffer
                            l
                        } else {
                            b_len
                        }
                    }
                } else if l == cap && self.write_range.is_at_end() {
                    // the end of the buffer is r
                    let at_cap = self.pos == r;
                    if self.pos < r || (at_cap && self.increased_pos) {
                        // pos overlaps the buffer
                        if r <= l {
                            // pos overlaps twice
                            l
                        } else {
                            b_len
                        }
                    } else {
                        b_len
                    }
                } else if r > l {
                    // the write does not overlap the buffer
                    if self.pos < l {
                        l
                    } else {
                        b_len
                    }
                } else {
                    // just the end of the write overlaps the buffer
                    let m = cmp::max(cap, r);
                    let at_m = self.pos == m;
                    if self.pos < m || (at_m && self.increased_pos) {
                        // we cannot write past l, since then we would overwrite the written buffer
                        l
                    } else {
                        b_len
                    }
                }
            }
        }
    }
}

impl<R: Read + Write + Seek> Read for RWBuf<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let into_len = buf.len();
        if into_len == 0 {
            return Ok(0);
        }
        let b_len = self.buf.len();
        let loc = self.compute_read_locations();
        // let loc = self.compute_locations();
        let end_r = {
            if loc.at_end {
                // we have consumed the full buffer and need to read new values
                // first flush the writes so we can read as much as we want
                self.flush_inner()?;
                // If we don't have any buffered data and we're doing a massive read
                // (larger than our internal buffer), bypass our internal buffer
                // entirely.
                if buf.len() >= b_len {
                    self.clear_buf(true)?;
                    return self.inner.read(buf);
                }
                // we need to fill the buffer
                debug_assert!(self.pos == loc.end_r);
                // we only want to read half the buffer at most, so we can still seek backwards
                let end_pos = {
                    if self.cap.is_partial() {
                        // we should fill the buffer
                        b_len
                    } else {
                        cmp::min(b_len / 2, b_len - self.pos) + self.pos
                    }
                };
                let n = self.inner.read(&mut self.buf[self.pos..end_pos])?;
                if n == 0 {
                    // the inner buffer has no new value to read
                    return Ok(0);
                }
                let end_r = loc.end_r + n;
                self.cap.update(end_r);
                end_r
            } else {
                loc.end_r
            }
        };
        let nread = cmp::min(into_len, end_r - self.pos);
        debug_assert!(nread > 0);
        let new_pos = self.pos + nread;
        debug_assert!(new_pos <= b_len);
        buf[..nread].copy_from_slice(&self.buf[self.pos..new_pos]);
        if new_pos > b_len {
            panic!();
        }
        self.pos = new_pos;
        self.check_overflow(true, b_len);
        Ok(nread)
    }
}

impl<R: Read + Write + Seek> fmt::Debug for RWBuf<R>
where
    R: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("RWBuf")
            .field("reader", &self.inner)
            .field("buffer", &format_args!("{}/{}", self.pos, self.buf.len()))
            .finish()
    }
}

impl<W: Read + Write + Seek> Drop for RWBuf<W> {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

impl<R: Read + Write + Seek> Seek for RWBuf<R> {
    /// Seek to an offset, in bytes, in the underlying reader.
    ///
    /// The position used for seeking with [`SeekFrom::Current`]`(_)` is the
    /// position the underlying reader would be at if the `BufReader<R>` had no
    /// internal buffer.
    ///
    /// Seeking always discards the internal buffer, even if the seek position
    /// would otherwise fall within it.
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // first flush any pending writes
        self.flush_inner()?;
        let result: u64;
        if let SeekFrom::Current(n) = pos {
            let loc = self.compute_seek_locations();
            // let loc = self.compute_locations();
            // compute how far ahead of the inner buffer we are
            let ahead = self.get_seek_cap() as i64 - loc.pos as i64;
            result = self.inner.seek(SeekFrom::Current(n - ahead))?;
        } else {
            // Seeking with Start/End doesn't care about our buffer length.
            result = self.inner.seek(pos)?;
        }
        self.clear_buf(false).unwrap();
        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use std::{
        cmp,
        fmt::Debug,
        fs::{File, OpenOptions},
        io::{Read, Result, Seek, SeekFrom, Write},
        path::Path,
    };

    use log::debug;
    use rand::{prelude::StdRng, Rng, SeedableRng};

    use crate::utils::gen_rand;

    use super::{RWBuf, SeekRelative};

    struct MultiRW {
        rw_buf: RWBuf<File>,
        rw: File,
    }

    #[derive(Debug)]
    struct Info {
        size: u64,
        offset: u64,
    }

    impl MultiRW {
        fn open_files(path_string: &str) -> (File, File) {
            let s = format!("{}1.log", path_string);
            let path = Path::new(&s);
            let f1 = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .unwrap();
            let s = format!("{}2.log", path_string);
            let path = Path::new(&s);
            let f2 = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .unwrap();
            (f1, f2)
        }

        fn new(buf_size: usize, path_string: &str) -> MultiRW {
            let (f1, f2) = MultiRW::open_files(path_string);
            MultiRW {
                rw_buf: RWBuf::with_capacity(buf_size, f1),
                rw: f2,
            }
        }

        fn check_offset(&mut self) {
            let v1 = self.rw_buf.get_offset();
            let v2 = self.rw.seek(SeekFrom::Current(0));
            check_equal(&v1, &v2, "offset");
        }

        fn with_capacity(buf_size: usize, path_string: &str, v: Vec<u8>) -> MultiRW {
            let (mut f1, mut f2) = MultiRW::open_files(path_string);
            f1.write_all(&v).unwrap();
            f1.seek(SeekFrom::Start(0)).unwrap();
            f2.write_all(&v).unwrap();
            f2.seek(SeekFrom::Start(0)).unwrap();
            MultiRW {
                rw_buf: RWBuf::with_capacity(buf_size, f1),
                rw: f2,
            }
        }

        fn seek_start(&mut self) {
            self.seek(SeekFrom::Start(0)).unwrap();
        }

        fn seek_end(&mut self) {
            self.seek(SeekFrom::End(0)).unwrap();
        }

        fn check_buf(&mut self) {
            self.check_offset();
            self.seek_start();

            let mut buf1 = vec![];
            let v1 = self.rw.read_to_end(&mut buf1);

            let mut buf2 = vec![];
            let v2 = self.rw_buf.read_to_end(&mut buf2);
            check_equal(&v1, &v2, "read size");

            debug!("check buf sizes {}, {}", buf1.len(), buf2.len());

            assert_eq!(buf1, buf2);
        }

        fn get_info(&mut self) -> Info {
            let offset = self.rw.seek(SeekFrom::Current(0)).unwrap();
            let size = self.rw.seek(SeekFrom::End(0)).unwrap();
            self.rw.seek(SeekFrom::Start(offset)).unwrap();
            Info { size, offset }
        }
    }

    impl SeekRelative for MultiRW {
        fn seek_relative(&mut self, pos: i64) -> Result<()> {
            let v1 = self.rw_buf.seek_relative(pos);
            let v2 = self.rw.seek(SeekFrom::Current(pos));
            check_equal_err(&v2, &v1);
            self.check_offset();
            v1
        }
    }

    impl Read for MultiRW {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            let mut buf1 = buf.to_vec();
            let v1 = self.rw_buf.read(buf);
            let end = *v1.as_ref().unwrap_or(&buf1.len());
            let v2 = self.rw.read_exact(&mut buf1[..end]);
            check_equal_err(&v1, &v2);
            self.check_offset();
            v1
        }
    }

    fn check_equal<T: Eq + Debug>(v1: &Result<T>, v2: &Result<T>, name: &str) {
        match v1 {
            Ok(n) => {
                debug!("{} check equal {:?}, {:?}", name, n, v2.as_ref().unwrap());
                assert_eq!(n, v2.as_ref().unwrap());
            }
            Err(e) => assert_eq!(e.kind(), v2.as_ref().unwrap_err().kind()),
        }
    }

    fn check_equal_err<T, D: Debug>(v1: &Result<T>, v2: &Result<D>) {
        match v1 {
            Ok(_) => (),
            Err(e) => assert_eq!(e.kind(), v2.as_ref().unwrap_err().kind()),
        }
    }

    impl Seek for MultiRW {
        fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
            let v1 = self.rw_buf.seek(pos);
            let v2 = self.rw.seek(pos);
            check_equal(&v1, &v2, "seek position");
            self.check_offset();
            v1
        }
    }

    impl Write for MultiRW {
        fn write(&mut self, buf: &[u8]) -> Result<usize> {
            let v1 = self.rw_buf.write(buf);
            let end = *v1.as_ref().unwrap_or(&buf.len());
            let v2 = self.rw.write_all(&buf[..end]);
            check_equal_err(&v1, &v2);
            self.check_offset();
            v1
        }

        fn flush(&mut self) -> Result<()> {
            let v1 = self.rw_buf.flush();
            let v2 = self.rw.flush();
            check_equal(&v1, &v2, "nil");
            self.check_offset();
            v1
        }
    }

    #[test]
    fn write_buf() {
        let mut rng = StdRng::seed_from_u64(100);
        let b_len = 128;
        let path_string = "log_files/rw_buff1";
        let mut rw = MultiRW::new(b_len, path_string);
        rw.check_buf();
        // write until the exact end of the buffer
        for _ in 0..10 {
            rw.write_all(&gen_rand(10, &mut rng)).unwrap();
            rw.check_buf();
        }
        // write past the end
        for _ in 0..9 {
            rw.write_all(&gen_rand(10, &mut rng)).unwrap();
            rw.check_buf();
        }
        // overlap the end in a write
        rw.write_all(&gen_rand(20, &mut rng)).unwrap();
        rw.check_buf();
        // write more than the entire buffer
        rw.write_all(&gen_rand(200, &mut rng)).unwrap();
        rw.check_buf();

        let mut rw = MultiRW::new(b_len, path_string);
        // write and only check at the end
        for _i in 0..10 {
            rw.write_all(&gen_rand(11, &mut rng)).unwrap();
        }
        rw.check_buf();
    }

    #[test]
    fn read() {
        let mut rng = StdRng::seed_from_u64(101);
        // start with a small initial value smaller than the buffer
        let v = gen_rand(10, &mut rng);
        let v_orign = v.clone();
        let path_string = "log_files/rw_buff2";
        let mut rw = MultiRW::with_capacity(128, path_string, v);
        rw.check_buf();

        // we should read no bytes
        let mut buf = vec![0; 100];
        assert_eq!(0, rw.read(&mut buf).unwrap());

        rw.seek_start();
        // try to read into an empty buf
        assert_eq!(0, rw.read(&mut []).unwrap());
        // read into a buf with enough room
        assert_eq!(10, rw.read(&mut buf).unwrap());
        assert_eq!(v_orign, &buf[..v_orign.len()]);
        // we should read no bytes
        assert_eq!(0, rw.read(&mut buf).unwrap());
        // read half at a time
        rw.seek_start();
        let mut buf = vec![0; 100];
        assert_eq!(5, rw.read(&mut buf[..5]).unwrap());
        assert_eq!(v_orign[..5], buf[..5]);
        assert_eq!(5, rw.read(&mut buf[5..10]).unwrap());
        assert_eq!(v_orign[..10], buf[..10]);

        // try with a bigger buffer
        let v = gen_rand(200, &mut rng);
        let v_orign = v.clone();
        let mut rw = MultiRW::with_capacity(128, path_string, v);
        rw.check_buf();

        rw.seek_start();
        let mut buf = vec![0; 200];
        rw.read_exact(&mut buf).unwrap();
        assert_eq!(v_orign, buf);
    }

    #[test]
    fn read_seek() {
        let b_len = 128;
        let mut rng = StdRng::seed_from_u64(101);
        let v = gen_rand(1000, &mut rng);
        let mut rw = MultiRW::with_capacity(b_len, "log_files/rw_buff3", v);

        // fill the buffer
        rw.read_exact(&mut [0; 10]).unwrap();
        // seek forward
        rw.seek_relative(50).unwrap();
        // read around buffer
        rw.read_exact(&mut [0; 80]).unwrap();
        // seek backwards
        rw.seek_relative(-20).unwrap();
        rw.read_exact(&mut [0; 30]).unwrap();
        rw.check_buf();
    }

    #[test]
    fn read_write_seek() {
        let b_len = 128;
        let mut rng = StdRng::seed_from_u64(101);
        let v = gen_rand(1000, &mut rng);
        let mut rw = MultiRW::with_capacity(b_len, "log_files/rw_buff4", v);

        // start with a write
        rw.write_all(&gen_rand(10, &mut rng)).unwrap();
        // seek forward
        rw.seek_relative(50).unwrap();
        // write around buffer
        rw.write_all(&gen_rand(80, &mut rng)).unwrap();

        // seek backwards
        rw.seek_relative(-20).unwrap();
        rw.read_exact(&mut [0; 30]).unwrap();
        rw.check_buf();

        let v = gen_rand(1000, &mut rng);
        let mut rw = MultiRW::with_capacity(b_len, "log_files/rw_buff4", v);
        // read until the middle of the buffer
        rw.read_exact(&mut [0; 50]).unwrap();
        // write
        rw.write_all(&gen_rand(10, &mut rng)).unwrap();
        // seek back
        rw.seek_relative(-20).unwrap();
        // write
        rw.write_all(&gen_rand(30, &mut rng)).unwrap();
        // seek forward
        rw.seek_relative(10).unwrap();
        // write around the buffer
        rw.write_all(&gen_rand(50, &mut rng)).unwrap();
        // write enough to flush
        rw.write_all(&gen_rand(200, &mut rng)).unwrap();
        rw.check_buf();

        let v = gen_rand(1000, &mut rng);
        let mut rw = MultiRW::with_capacity(b_len, "log_files/rw_buff4", v);
        // read until the middle of the buffer
        rw.read_exact(&mut [0; 50]).unwrap();
        // write
        rw.write_all(&gen_rand(10, &mut rng)).unwrap();
        // read around so we update cap
        rw.read_exact(&mut [0; 80]).unwrap();
        // write so we flush the old write
        rw.write_all(&gen_rand(90, &mut rng)).unwrap();
        // seek back
        rw.seek_relative(-90).unwrap();
        rw.read_exact(&mut [0; 90]).unwrap();
        rw.check_buf();
    }

    #[test]
    fn rand_op_initial_large() {
        let b_len = 128;
        let mut rng = StdRng::seed_from_u64(101);
        // start with a large initial value smaller than the buffer
        let v = gen_rand(1000, &mut rng);
        let rw = MultiRW::with_capacity(b_len, "log_files/rw_buff5", v);
        run_rand_op(rw, b_len, rng);
    }

    #[test]
    fn rand_op_initial_small() {
        let b_len = 128;
        let mut rng = StdRng::seed_from_u64(102);
        // start with a small initial value smaller than the buffer
        let v = gen_rand(10, &mut rng);
        let rw = MultiRW::with_capacity(b_len, "log_files/rw_buff6", v);
        run_rand_op(rw, b_len, rng);
    }

    #[test]
    fn rand_op() {
        let b_len = 128;
        let rng = StdRng::seed_from_u64(103);
        // start with a large initial value smaller than the buffer
        let rw = MultiRW::new(b_len, "log_files/rw_buff7");
        run_rand_op(rw, b_len, rng);
    }

    fn run_rand_op(mut rw: MultiRW, b_len: usize, mut rng: StdRng) {
        let num_ops = 1000;

        // run 100 different iterations
        for j in 0..100 {
            debug!("perform iteration {}", j);
            rw.seek_start();
            for i in 0..num_ops {
                match rng.gen_range(0..100) {
                    0..=59 => {
                        // read
                        let mut v = vec![0; rng.gen_range(0..(2 * b_len))];
                        debug!("perform op {}, read {}", i, v.len());
                        let _ = rw.read(&mut v).unwrap();
                    }
                    60..=89 => {
                        // write
                        let v = gen_rand(rng.gen_range(0..(2 * b_len)), &mut rng);
                        debug!("perform op {}, write {}", i, v.len());
                        rw.write_all(&v).unwrap();
                    }
                    90..=99 => {
                        // seek
                        let inf = rw.get_info();
                        // compute something in range
                        let min_range = cmp::max(0 - (inf.offset as i64), -(3 * b_len as i64));
                        let max_range =
                            cmp::min(inf.size as i64 - inf.offset as i64, 3 * b_len as i64);
                        let seek_size = rng.gen_range(min_range..max_range);
                        debug!(
                            "perform op {}, seek {} ({}, {}), info: {:?}",
                            i, seek_size, min_range, max_range, inf
                        );
                        rw.seek_relative(seek_size).unwrap();
                    }
                    _ => {
                        panic!("invalid op")
                    }
                }
            }
            rw.check_buf();
        }
    }

    #[test]
    fn read_write() {
        let mut rng = StdRng::seed_from_u64(100);
        let b_len = 128;
        let path_string = "log_files/rw_buff8";

        // write, seek back, read part of it, overwrite
        let mut rw = MultiRW::new(b_len, path_string);
        let v = gen_rand(10, &mut rng);
        let v_clone = v.clone();
        assert_eq!(10, rw.write(&v).unwrap());
        rw.seek_relative(-5).unwrap();
        let mut buf = vec![0; 3];
        rw.read_exact(&mut buf).unwrap();
        assert_eq!(&v_clone[5..8], buf);
        let v = gen_rand(10, &mut rng);
        assert_eq!(10, rw.write(&v).unwrap());
        rw.seek_relative(-18).unwrap();
        let mut buf = vec![0; 18];
        rw.read_exact(&mut buf).unwrap();
        rw.check_buf();

        // write 150, seek back 100, read 10, write 10
        let mut rw = MultiRW::new(b_len, path_string);
        let v = gen_rand(150, &mut rng);
        rw.write_all(&v).unwrap();
        rw.seek_relative(-100).unwrap();
        rw.read_exact(&mut [0; 10]).unwrap();
        let v = gen_rand(10, &mut rng);
        rw.write_all(&v).unwrap();
        rw.check_buf();

        // write 150, seek back 100, read 10, write 10, read 20, write 100
        let mut rw = MultiRW::new(b_len, path_string);
        let v = gen_rand(150, &mut rng);
        rw.write_all(&v).unwrap();
        rw.seek_relative(-100).unwrap();
        rw.read_exact(&mut [0; 10]).unwrap();
        let v = gen_rand(10, &mut rng);
        rw.write_all(&v).unwrap();
        rw.read_exact(&mut [0; 20]).unwrap();
        let v = gen_rand(100, &mut rng);
        rw.write_all(&v).unwrap();
        rw.check_buf();

        // double overlap
        // start with a file of size 150
        // read 50, 2 times - then 40, so we are 10 before cap
        let mut rw = MultiRW::with_capacity(b_len, path_string, gen_rand(150, &mut rng));
        rw.read_exact(&mut [0; 50]).unwrap();
        rw.read_exact(&mut [0; 50]).unwrap();
        rw.read_exact(&mut [0; 40]).unwrap();
        // now write 50, 2 times so we overlap the buffer
        let v = gen_rand(50, &mut rng);
        rw.write_all(&v).unwrap();
        let v = gen_rand(50, &mut rng);
        rw.write_all(&v).unwrap();
        rw.check_buf();
    }
}
