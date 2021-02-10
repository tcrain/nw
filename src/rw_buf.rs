use std::{
    cmp, fmt,
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
};

use crate::config::DEFAULT_BUF_SIZE;

// when pos == cap, then we must increase cap by reading into the buffer
// when cap == write_range.0 - 1, then we must flush the buffer
// when flushing we always write all write buffer

// if write range intersects cap, then we wrote past cap
//   - in this case write_range.1 is the max end of the buffer
// otw, write range is contained inside cap and cap is the end of the buffer
// NOTE if cap == write_range.0 then we consider write_range to insersect cap

// to ensure this
// 1 - when cap reaches write_range.0 - 1 we flush the buffer
// 2 - after writing we set pos = write_range.1
// 3 - when write_range.1 reaches write_range.0, we must flush the buffer
// 4 - when write_range.1 reaches cap, we write past cap

// flushing the buffer -
// when?
// 1. on write when write_range.1 == write_range.0
// 2. on fill when cap == write_range.0 - 1
// how?
// 1. seek from cap to write_range.0
// - we always seek backward
// 2. write from write_range.0 to write_range.1
// 3. if write_range intersects cap (we are at new cap) set cap to write_range.1,
//  - otw seek forward to cap
// 4. set write_range to none

// filling the buffer
// when?
// 1. on read when pos == m (see below how to compute m)
// how?
// 1. (if cap == write_range.0 - 1, then flush)
// 2. let m = write_range.1 if write_range intersects cap, otherwise let m = cap
// 3. Seek to m if needed
// 4. Read upto half the size of the buffer from m until either
//   - the end of the buffer
//   - write_range.0 - 1
//   - pos - 1 (can this happen?)

// reading
// 1. compute m (as in fill)
// 2. if pos == m then fill the buff
// 3. consume until the input buff is full or
//    - m
//    - end of the stored buffer

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
        let loc = self.compute_locations();
        let w_len = cmp::min(buf_len, loc.end_w - self.pos);
        debug_assert!(w_len > 0);
        let w_end = self.pos + w_len;
        // println!(
        // "loc {:?}, buf_len {}, self pos {}, cap {:?}, w_len {}, w_end {}",
        //loc, buf_len, self.pos, self.cap, w_len, w_end
        //);
        self.buf[self.pos..w_end].copy_from_slice(&buf[..w_len]);
        let (new_l, new_r) = loc.w.get_l_r().map_or((self.pos, w_end), |(l, r)| {
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
        });
        match self.write_range {
            WriteRange::None => {
                if loc.at_end && new_l == self.pos {
                    // the write started at the end of the buffer
                    self.write_range = WriteRange::AtEnd(new_l, new_r);
                } else {
                    // the write did not start at the end of the buffer
                    self.write_range = WriteRange::AtStart(new_l, new_r)
                }
            }
            _ => self.write_range.update(new_l, new_r),
        }
        self.pos = (self.pos + w_len) % b_len;
        self.check_overflow(true, b_len);
        // let loc = self.compute_locations();
        // println!(
        //   "after write loc {:?}, buf_len {}, self pos {}, cap {:?}, w_len {}, w_end {}",
        // loc, buf_len, self.pos, self.cap, w_len, w_end
        //);
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
            let loc = self.compute_locations();
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
        // println!(
        // "loc: {:?}\n loc.pos {}, seek cap {}, off {}, my cap: {:?}",
        //loc,
        //loc.pos,
        // self.get_seek_cap(),
        // self.inner.seek(SeekFrom::Current(0))?,
        // self.cap
        // );
        Ok((self.inner.seek(SeekFrom::Current(0))? as i64
            + (loc.pos as i64 - self.get_seek_cap() as i64)) as u64)
    }

    // writing
    // 1. copy into stored buff until e, defined as
    //    - end of input buff
    //    - end of stored buff
    //    - write_range.0
    // 2. if write_range == None, then set write_range.0 = pos and write_range.1 = current index
    //    - otw compute m (as in fill) and
    //         a. if write_range intersects cap
    //          - if pos is inside the current write range, do nothing, otw. set write_range.0 = pos
    //          - if e is not between write_range.0 and write_range.1, then set write_range.1 to e
    //         b. otw (write range does not intersect cap)
    //           - if pos is not between write_range.0 and m, then set write_range.0 = pos
    //           - if e is NOT between write_range.0 and write_range.1, set write_range.1 = e
    // 3. update pos

    fn flush_inner(&mut self) -> io::Result<()> {
        if !self.write_range.is_none() {
            // let b_len = self.buf.len();
            let cap = self.get_seek_cap();
            // compute how much we seek before writing
            // and compute how much we should seek after writing
            // if r ovewrote cap, then we do not need to seek
            // othewise we need to seek back to cap

            let loc = self.compute_locations();
            // take out the locations
            let (l, r) = self.write_range.take().get_l_r().unwrap();
            // seek from cap to l
            let (abs_l, abs_r) = loc.w.get_l_r().unwrap();

            let dist = abs_l as i64 - cap as i64;
            // println!("seeking back {}", dist);
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
                // println!("seeking forward {}", dist);
                self.inner.seek(SeekFrom::Current(dist))?;
            }
        }
        // TODO should call inner flush here?
        Ok(())
    }

    /// Seeks relative to the current position.
    pub fn seek_relative(&mut self, offset: i64) -> io::Result<()> {
        // println!("seek cap before {:?}", self.cap);
        let loc = self.compute_locations();
        let b_len = self.buf.len();
        // println!(
        //     "off {} loc {:?}, my cap {:?}, my pos {}",
        //   offset, loc, self.cap, self.pos
        //);
        match offset.cmp(&0) {
            cmp::Ordering::Equal => (), // no seek needed
            cmp::Ordering::Less => {
                let off = offset.abs() as usize;
                // we can seek back either until cap or r, if r writes past cap
                // let min = loc.r_is_start.unwrap_or_else(|| self.cap.get_cap());
                // println!("seek cap {:?}", self.cap);
                if off <= loc.pos - loc.seek_min {
                    self.pos = (loc.pos - off) % b_len;
                    self.check_overflow(false, b_len)
                } else {
                    self.seek(SeekFrom::Current(offset))?;
                }
            }
            cmp::Ordering::Greater => {
                let off = offset as usize;
                // we can seek to read end
                if off < loc.end_r - self.pos {
                    self.pos += off % b_len;
                    self.check_overflow(true, b_len)
                } else {
                    self.seek(SeekFrom::Current(offset))?;
                }
            }
        }
        Ok(())
    }
}

// before we have read capacity bytes, some of the buffer is invalid
// for this we keep min_valid: Option<usize>.
// when this is not None, we cannot seek backwards to before this

// how to increase cap
//

#[derive(Debug, Clone, Copy)]
enum WriteRange {
    None,
    AtStart(usize, usize),
    AtEnd(usize, usize),
}

impl Default for WriteRange {
    fn default() -> Self {
        WriteRange::None
    }
}

impl WriteRange {
    pub fn take(&mut self) -> WriteRange {
        mem::take(self)
    }

    fn is_none(&self) -> bool {
        matches!(*self, WriteRange::None)
    }

    fn is_at_start(&self) -> bool {
        matches!(*self, WriteRange::AtStart(_, _))
    }

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

    fn update(&mut self, l: usize, r: usize) {
        match self {
            WriteRange::None => panic!("should not be called on none"),
            WriteRange::AtStart(_, _) => *self = WriteRange::AtStart(l, r),
            WriteRange::AtEnd(_, _) => *self = WriteRange::AtEnd(l, r),
        }
    }

    fn update_copy(self, l: usize, r: usize) -> WriteRange {
        match self {
            WriteRange::None => panic!("should not be called on none"),
            WriteRange::AtStart(_, _) => WriteRange::AtStart(l, r),
            WriteRange::AtEnd(_, _) => WriteRange::AtEnd(l, r),
        }
    }

    fn get_l_r(&self) -> Option<(usize, usize)> {
        match self {
            WriteRange::None => None,
            WriteRange::AtStart(l, r) => Some((*l, *r)),
            WriteRange::AtEnd(l, r) => Some((*l, *r)),
        }
    }
}

pub struct RWBuf<R> {
    inner: R,
    buf: Box<[u8]>,
    pos: usize,              // position of the reader in the buffer
    increased_pos: bool,     // true if the last operation moved forward in the buffer
    cap: Capacity,           // position of inner reader in the buffer
    write_range: WriteRange, // if the write range goes past cap
}

impl<R: Read> RWBuf<R> {
    pub fn new(inner: R) -> RWBuf<R> {
        RWBuf::with_capacity(DEFAULT_BUF_SIZE, inner)
    }

    /// Creates a new `BufReader<R>` with the specified buffer capacity.
    pub fn with_capacity(capacity: usize, inner: R) -> RWBuf<R> {
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
    at_end: bool, // true if we are at the end of the buffer
                  // cap: usize, // position of cap relative to the other values (this is either the original cap, or cap + b_len)
                  // seek_cap: usize, // seeking loc.pos as i64 - loc.seek_cap as i64 in the inner buffer puts the inner buffer at offset pos
}

impl<R> RWBuf<R> {
    /// Returns the number of bytes the internal buffer can hold at once.
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

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
        if let Some((l, r)) = self.write_range.get_l_r() {
            if r >= b_len || l >= b_len {
                self.write_range.update(l % b_len, r % b_len);
            }
        }
    }

    /// returns the cap, when used for seeking
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
    fn compute_locations(&mut self) -> Locations {
        let b_len = self.buf.len();
        let cap = match self.cap {
            Capacity::Partial(cap) => {
                let end_r = self
                    .write_range
                    .get_l_r()
                    .map_or(cap, |(_, r)| cmp::max(cap, r));
                // println!("cap {}, pos {}, end_r {}", cap, self.pos, end_r);
                return Locations {
                    w: self.write_range,
                    pos: self.pos,
                    end_w: b_len,
                    end_r,
                    seek_min: 0,
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
                        Locations {
                            w: self.write_range.update_copy(l + b_len, r + b_len),
                            pos,
                            end_w: b_len,
                            end_r,
                            seek_min: {
                                if r > cap {
                                    r
                                } else {
                                    cap
                                }
                            },
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
                            seek_min: r + b_len,
                            at_end,
                        }
                    }
                } else if l == cap && self.write_range.is_at_end() {
                    // the end of the buffer is r
                    let (pos, end_w, end_r, at_end) = {
                        let at_cap = self.pos == r;
                        // println!("pos {}, r {}", self.pos, r);
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
                        w: self.write_range.update_copy(l, r),
                        pos,
                        end_w: {
                            if self.pos < l {
                                l
                            } else {
                                b_len
                            }
                        },
                        end_r,
                        seek_min: cap,
                        at_end,
                    }
                } else {
                    // just the end of the write overlaps the buffer
                    let m = cmp::max(cap, r);
                    if l == 0 && r == 0 {
                        // println!("here");
                    }
                    let (pos, end_w, end_r, at_end) = {
                        let at_m = self.pos == m;
                        if self.pos < m || (at_m && self.increased_pos) {
                            // we cannot write past l, since then we would overwrite the written buffer
                            (self.pos + b_len, l, m, at_m)
                        } else {
                            (self.pos, b_len, b_len, false)
                        }
                    };
                    Locations {
                        w: self.write_range.update_copy(l, r + b_len),
                        pos,
                        end_w,
                        end_r,
                        seek_min: {
                            if r > cap {
                                r
                            } else {
                                cap
                            }
                        },
                        at_end,
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
        let loc = self.compute_locations();
        let end_r = {
            if loc.at_end {
                // println!(
                //      "pos {}, loc pos {}, loc {:#?}, cap {:?}",
                //    self.pos, loc.pos, loc, self.cap
                //);
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
                // TODO how much should we read
                // let cap = self.cap.get_cap();
                debug_assert!(self.pos == loc.end_r);
                let n = self.inner.read(&mut self.buf[self.pos..])?;
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
        self.pos = new_pos;
        self.check_overflow(true, b_len);
        Ok(nread)
    }
}

impl<R> fmt::Debug for RWBuf<R>
where
    R: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("BufReader")
            .field("reader", &self.inner)
            .field("buffer", &format_args!("{}/{}", self.pos, self.buf.len()))
            .finish()
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
            let loc = self.compute_locations();
            // compute how far ahead of the inner buffer we are
            // let ahead = loc.pos as i64 - self.get_seek_cap() as i64;
            let ahead = self.get_seek_cap() as i64 - loc.pos as i64;
            // println!(
            //    "loc.pos {}, seek cap {}, ahead {}, seek {}",
            //  loc.pos,
            //                self.get_seek_cap(),
            //              ahead,
            //            n - ahead
            //      );
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

    use log::info;
    use rand::{prelude::StdRng, Rng, SeedableRng};

    use crate::utils::gen_rand;

    use super::RWBuf;

    struct MultiRW {
        rw_buf: RWBuf<File>,
        rw: File,
    }

    #[derive(Debug)]
    struct Info {
        size: u64,
        offset: u64,
    }

    impl Drop for MultiRW {
        fn drop(&mut self) {
            self.flush()
                .unwrap_or_else(|e| info!("Error flushing buffer: {}", e))
        }
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

            // println!("check buf sizes {}, {}", buf1.len(), buf2.len());

            assert_eq!(buf1, buf2);
        }

        fn get_info(&mut self) -> Info {
            let offset = self.rw.seek(SeekFrom::Current(0)).unwrap();
            let size = self.rw.seek(SeekFrom::End(0)).unwrap();
            self.rw.seek(SeekFrom::Start(offset)).unwrap();
            Info { size, offset }
        }

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

    fn check_equal<T: Eq + Debug>(v1: &Result<T>, v2: &Result<T>, _name: &str) {
        match v1 {
            Ok(n) => {
                // println!("{} check equal {:?}, {:?}", name, n, v2.as_ref().unwrap());
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
    fn write() {
        let mut rng = StdRng::seed_from_u64(100);
        let b_len = 100;
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
        for i in 0..10 {
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
        let mut rw = MultiRW::with_capacity(100, path_string, v);
        rw.check_buf();

        // we should read no bytes
        let mut buf = vec![0; 100];
        assert_eq!(0, rw.read(&mut buf).unwrap());

        rw.seek_start();
        // try to read into an empty buf
        assert_eq!(0, rw.read(&mut []).unwrap());
        // read into a buf with enough room
        assert_eq!(10, rw.read(&mut buf).unwrap());
        assert_eq!(v_orign, buf[..v_orign.len()]);
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
        let mut rw = MultiRW::with_capacity(100, path_string, v);
        rw.check_buf();

        rw.seek_start();
        let mut buf = vec![0; 200];
        rw.read_exact(&mut buf).unwrap();
        assert_eq!(v_orign, buf);
    }

    #[test]
    fn read_seek() {
        let b_len = 100;
        let mut rw = MultiRW::new(b_len, "log_files/rw_buff3");
        rw.check_buf();
        rw.seek_relative(100).unwrap();
        rw.check_buf();
    }

    #[test]
    fn rand_op() {
        let mut rng = StdRng::seed_from_u64(100);
        let b_len = 100;
        let mut rw = MultiRW::new(b_len, "log_files/rw_buff4");
        let num_ops = 1000;

        // run 100 different iterations
        for _j in 0..100 {
            // println!("perform iteration {}", j);
            for _i in 0..num_ops {
                match rng.gen_range(0..100) {
                    0..=59 => {
                        // read
                        let mut v = vec![0; rng.gen_range(0..(2 * b_len))];
                        //println!("perform op {}, read {}", i, v.len());
                        let _ = rw.read(&mut v).unwrap();
                    }
                    60..=89 => {
                        // write
                        let v = gen_rand(rng.gen_range(0..(2 * b_len)), &mut rng);
                        //println!("perform op {}, write {}", i, v.len());
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
                        //println!(
                        //  "perform op {}, seek {} ({}, {}), info: {:?}",
                        //i, seek_size, min_range, max_range, inf
                        //);
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
        let b_len = 100;
        let path_string = "log_files/rw_buff5";

        // write, seek back, read part of it, overwrite
        let mut rw = MultiRW::new(b_len, path_string);
        let v = gen_rand(10, &mut rng);
        let v_clone = v.clone();
        assert_eq!(10, rw.write(&v).unwrap());
        rw.seek_relative(-5).unwrap();
        let mut buf = vec![0; 3];
        rw.read_exact(&mut buf).unwrap();
        assert_eq!(v_clone[5..8], buf);
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
        let mut rw = MultiRW::with_capacity(100, path_string, gen_rand(150, &mut rng));
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
