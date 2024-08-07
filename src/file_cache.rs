use std::collections::HashMap;
use std::fs;
use std::io::BufRead;
use std::io::Cursor;
use std::io::Seek;
use std::io::SeekFrom;
use sysinfo::System;

pub struct FileCache {
    buffers: HashMap<String, Cursor<Vec<u8>>>,
    ttl: HashMap<String, i64>,
    sys: System,
}

impl FileCache {
    pub fn new() -> Self {
        FileCache {
            buffers: HashMap::new(),
            ttl: HashMap::new(),
            sys: System::new(),
        }
    }

    fn remove_oldest(&mut self) {
        let oldest = self.ttl.iter().min_by_key(|x| x.1).map(|x| x.0.clone());

        if let Some(oldest) = oldest {
            self.buffers.remove(&oldest);
            self.ttl.remove(&oldest);
        }
    }

    pub fn get(&mut self, file: &str, start: u64) -> String {
        if !self.buffers.contains_key(file) {
            self.sys.refresh_memory();
            let free_memory = self.sys.total_memory() - self.sys.used_memory();
            let file_size = fs::metadata(file).unwrap().len();

            if free_memory < (1.5 * file_size as f64) as u64 {
                self.remove_oldest();
            }

            let content = fs::read(file).unwrap();
            let cursor = Cursor::new(content);
            self.buffers.insert(file.to_string(), cursor);
            self.ttl.insert(file.to_string(), 0);
        }

        let cursor = self.buffers.get_mut(file).unwrap();
        let access_count = self.ttl.get_mut(file).unwrap();
        *access_count += 1;

        for (_, ttl) in self.ttl.iter_mut() {
            *ttl -= 1;
        }

        cursor.seek(SeekFrom::Start(start)).unwrap();

        let mut line = String::new();
        cursor.read_line(&mut line).unwrap();

        return line;
    }
}
