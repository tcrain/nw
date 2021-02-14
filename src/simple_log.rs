use log::{Level, LevelFilter, Log, Metadata, Record, SetLoggerError};
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::Mutex,
};

struct SimpleLog(Mutex<BufWriter<File>>);

impl Default for SimpleLog {
    fn default() -> Self {
        let path = Path::new("log_files/debug.log");
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        SimpleLog(Mutex::new(BufWriter::with_capacity(8000, f)))
    }
}

impl Log for SimpleLog {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Debug
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let _ = self
                .0
                .lock()
                .unwrap()
                .write_all(format!("{} - {}\n", record.level(), record.args()).as_bytes());
        }
    }

    fn flush(&self) {
        let mut buf = self.0.lock().unwrap();
        // buf.flush();
        buf.flush().unwrap();
    }
}

impl Drop for SimpleLog {
    fn drop(&mut self) {
        self.flush()
    }
}

pub fn init() -> Result<(), SetLoggerError> {
    log::set_boxed_logger(Box::new(SimpleLog::default()))
        .map(|()| log::set_max_level(LevelFilter::Debug))
}
