use std::{
    fs::{File, OpenOptions},
    io::{Read, SeekFrom, Write},
    path::Path,
};

use bincode::{options, DefaultOptions, Options};
use log::{error, info};

use super::log_error::LogError;
use super::log_error::Result;
use crate::errors::{EncodeError, LogIOError};
use crate::rw_buf::RWS;

struct FileWriter<T> {
    // written: Vec<u8>,
    bytes_written: usize,
    bytes_read: usize,
    // bytes_read: usize,
    file: T,
}

impl<T> FileWriter<T> {
    fn reset_bytes_written(&mut self) -> u64 {
        let ret = self.bytes_written as u64;
        self.bytes_written = 0;
        ret
    }

    fn reset_bytes_read(&mut self) -> u64 {
        let ret = self.bytes_read as u64;
        self.bytes_read = 0;
        ret
    }
}

impl<T: Read> Read for FileWriter<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let b = self.file.read(buf)?;
        self.bytes_read += b;
        Ok(b)
    }
}

impl<T: Read + Write> Write for FileWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let b = self.file.write(buf)?;
        self.bytes_written += b;
        Ok(b)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

pub struct LogFile<F> {
    file: FileWriter<F>,
    file_idx: u64,
    end_idx: u64,
    options: DefaultOptions,
    at_end: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct FileOpInfo {
    pub start_location: u64,
    pub bytes_consumed: u64,
    pub at_end: bool,
}

pub fn open_log_file<F: RWS, G: Fn(File) -> F>(
    path_string: &str,
    clear: bool,
    open_fn: G,
) -> Result<LogFile<F>> {
    let path = Path::new(path_string);
    info!("Opening log file {}", path.display());
    let file = open_file(path, clear)?;
    // let file = Cursor::new(Vec::new());
    let options = options();
    let mut l = LogFile {
        file_idx: 0,
        end_idx: 0,
        options,
        at_end: true,
        file: FileWriter {
            bytes_written: 0,
            bytes_read: 0,
            file: open_fn(file),
        },
    };
    // find the end of the file
    l.end_idx = l
        .file
        .file
        .seek(SeekFrom::End(0))
        .map_err(|err| LogError::IOError(LogIOError(err)))?;
    l.seek_to(0)?; // go back to the start
    Ok(l)
}

fn open_file(path: &Path, truncate: bool) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(truncate)
        .open(path)
        .map_err(|err| {
            error!("Error opening file: {}", err);
            LogError::IOError(LogIOError(err))
        })
}

impl<F: RWS> LogFile<F> {
    pub fn serialize_option(&self) -> DefaultOptions {
        self.options
    }

    pub fn get_end_index(&self) -> u64 {
        self.end_idx
    }

    // should be called after reading or writing to ensure the indicies are not broked
    fn check_file(&self) {
        if self.file_idx > self.end_idx {
            panic!("file idx past end idx");
        }
        if self.file_idx == self.end_idx && !self.at_end {
            panic!("at end shoud be true");
        }
        if self.file_idx != self.end_idx && self.at_end {
            panic!("at end should be false");
        }
    }

    pub fn read_u64(&mut self) -> Result<(u64, FileOpInfo)> {
        let mut buf: [u8; 8] = [0; 8];
        self.file
            .file
            .read_exact(&mut buf)
            .map_err(|err| LogError::IOError(LogIOError(err)))?;
        let start_location = self.file_idx;
        self.file_idx += 8;
        if self.file_idx == self.end_idx {
            self.at_end = true
        }
        self.check_file();
        Ok((
            u64::from_ne_bytes(buf),
            FileOpInfo {
                at_end: self.at_end,
                start_location,
                bytes_consumed: 8,
            },
        ))
    }

    // returns the current index and if it is at the end of the file
    pub fn check_index(&self) -> FileOpInfo {
        FileOpInfo {
            at_end: self.at_end,
            start_location: self.file_idx,
            bytes_consumed: 0,
        }
    }

    pub fn write_u64(&mut self, v: u64) -> Result<FileOpInfo> {
        self.file
            .file
            .write_all(&v.to_ne_bytes())
            .map_err(|err| LogError::IOError(LogIOError(err)))?;
        if self.at_end {
            self.end_idx += 8;
        }
        let start_location = self.file_idx;
        self.file_idx += 8;
        if self.file_idx == self.end_idx {
            self.at_end = true;
        }
        self.check_file();
        Ok(FileOpInfo {
            at_end: self.at_end,
            start_location,
            bytes_consumed: 8,
        })
    }

    fn after_read(&mut self) -> FileOpInfo {
        let bytes_read = self.file.reset_bytes_read();
        let start_location = self.file_idx;
        self.file_idx += bytes_read as u64;
        if self.file_idx == self.end_idx {
            self.at_end = true
        } else {
            self.at_end = false
        }
        self.check_file();
        FileOpInfo {
            at_end: self.at_end,
            start_location,
            bytes_consumed: bytes_read,
        }
    }

    // returns the number of bytes written
    fn after_write(&mut self) -> FileOpInfo {
        let bytes_consumed = self.file.reset_bytes_written();
        let start_location = self.file_idx;
        self.file_idx += bytes_consumed;
        if self.at_end {
            self.end_idx += bytes_consumed
        }
        self.check_file();
        FileOpInfo {
            at_end: self.at_end,
            start_location,
            bytes_consumed,
        }
    }

    pub fn seek_to(&mut self, idx: u64) -> Result<()> {
        if idx == self.file_idx {
            // already at the index, just return
            return Ok(());
        }
        if idx > self.end_idx {
            return Err(LogError::FileSeekPastEndError);
        }
        if idx == self.end_idx {
            self.at_end = true;
        } else {
            self.at_end = false;
        }
        let seek_amount = idx as i64 - self.file_idx as i64;
        self.file.file.seek_relative(seek_amount).map_err(|err| {
            error!("Error seeking file: {}", err);
            LogError::IOError(LogIOError(err))
        })?;
        self.file_idx = idx;
        Ok(())
    }

    pub fn seek_to_end(&mut self) -> u64 {
        self.at_end = true;
        self.seek_to(self.end_idx)
            .expect("unable to seek to file end");
        self.file_idx = self.end_idx;
        self.file_idx
    }

    pub fn append_log<T>(&mut self, entry: &T) -> Result<FileOpInfo>
    where
        T: ?Sized + serde::Serialize,
    {
        if !self.at_end {
            self.seek_to_end();
        }
        self.options
            .serialize_into(&mut self.file, entry)
            .map_err(|err| LogError::EncodeError(EncodeError(err)))?;
        Ok(self.after_write())
    }

    pub fn read_log_at<T>(&mut self, idx: u64) -> Result<(FileOpInfo, T)>
    where
        T: serde::de::DeserializeOwned,
    {
        self.seek_to(idx)?;
        self.read_log()
    }

    pub fn read_log<T>(&mut self) -> Result<(FileOpInfo, T)>
    where
        T: serde::de::DeserializeOwned,
    {
        if self.at_end {
            return Err(LogError::EOFError);
        }
        let e = self
            .options
            .deserialize_from(&mut self.file)
            .map_err(|_err| LogError::DeserializeError)?;
        let info = self.after_read();
        Ok((info, e))
    }
}

#[cfg(test)]
mod tests {
    use super::open_log_file;
    use super::LogError;
    use crate::file_sr::FileSR;
    use bincode::Options;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct SomeSer {
        b: u64,
        c: Vec<u8>,
    }

    impl SomeSer {
        fn new(size: u64) -> SomeSer {
            SomeSer {
                b: size,
                c: vec![1; size as usize],
            }
        }
    }

    #[test]
    fn append_log() {
        let mut l = open_log_file("log_files/testfile-1.log", true, FileSR::new).unwrap();
        let mut entrys = vec![];
        let count: u64 = 5;
        // let mut prv_idx = 0;
        // write entries to log
        for i in 0..count {
            let e = SomeSer::new(i);
            let prv_idx = l.append_log(&e).unwrap();
            let ser = l.options.serialize(&e).unwrap();
            entrys.push((prv_idx, ser, e));
        }
        l.seek_to(0).unwrap();
        // read the entries
        for (i, (_, _e_bytes, e)) in entrys.iter().enumerate() {
            let (e2_info, e2): (_, SomeSer) = l.read_log().unwrap();
            assert_eq!(e, &e2);
            if i == entrys.len() - 1 {
                assert!(e2_info.at_end);
            } else {
                assert!(!e2_info.at_end);
            }
        }
        // see if we can read using the location entry
        for (i, (e_info, _, e)) in entrys.iter().enumerate() {
            let (e2_info, e2): (_, SomeSer) = l.read_log_at(e_info.start_location).unwrap();
            assert_eq!(e, &e2);
            if i == entrys.len() - 1 {
                assert!(e2_info.at_end);
            } else {
                assert!(!e2_info.at_end);
            }
        }
        l.seek_to_end();
        assert!(l.at_end);
        assert_eq!(LogError::EOFError, l.read_log::<SomeSer>().unwrap_err());
    }

    #[test]
    fn read_write_u64() {
        let mut l = open_log_file("log_files/testfile-2.log", true, FileSR::new).unwrap();
        let count = 10;
        for i in 0..count {
            let op_info = l.write_u64(i).unwrap();
            assert!(op_info.at_end);
            assert_eq!(8, op_info.bytes_consumed);
            assert_eq!(8 * i, op_info.start_location);
        }
        assert!(l.read_u64().unwrap_err().is_io_error());
        l.seek_to(0).unwrap();
        for i in 0..count {
            let (v, op_info) = l.read_u64().unwrap();
            assert_eq!(i, v);
            assert_eq!(8, op_info.bytes_consumed);
            assert_eq!(8 * i, op_info.start_location);
            if i < count - 1 {
                assert!(!op_info.at_end);
            } else {
                assert!(op_info.at_end);
            }
        }
        assert!(l.read_u64().unwrap_err().is_io_error());
    }
}
