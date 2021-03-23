use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom, Write},
};

use crate::rw_buf::SeekRelative;

pub struct FileSR(File);

impl FileSR {
    pub fn new(f: File) -> FileSR {
        FileSR(f)
    }
}

impl Read for FileSR {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Seek for FileSR {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(pos)
    }
}

impl Write for FileSR {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl SeekRelative for FileSR {
    fn seek_relative(&mut self, pos: i64) -> std::io::Result<()> {
        self.0.seek(SeekFrom::Current(pos)).and(Ok(()))
    }
}

pub struct CursorSR(Cursor<Vec<u8>>);

impl CursorSR {
    pub fn new() -> CursorSR {
        CursorSR::default()
    }
}

impl Default for CursorSR {
    fn default() -> Self {
        CursorSR(Cursor::new(vec![]))
    }
}

impl Read for CursorSR {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Seek for CursorSR {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.0.seek(pos)
    }
}

impl Write for CursorSR {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl SeekRelative for CursorSR {
    fn seek_relative(&mut self, pos: i64) -> std::io::Result<()> {
        self.0.seek(SeekFrom::Current(pos)).and(Ok(()))
    }
}
