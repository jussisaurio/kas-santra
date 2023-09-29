use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Lines, Read, Result, Seek, SeekFrom, Write};

pub struct Wal {
    file: File,
    path: String,
}

impl Wal {
    pub fn new(path: &str) -> Wal {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        Wal {
            file,
            path: path.to_string(),
        }
    }

    pub fn from_file(path: &str) -> Wal {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(path)
            .unwrap();

        Wal {
            file,
            path: path.to_string(),
        }
    }

    pub fn path(&self) -> String {
        self.path.clone()
    }

    pub fn append(&mut self, line: &str) -> Result<()> {
        writeln!(self.file, "{}", line)
    }

    pub fn get_line_iterator(&mut self) -> Lines<BufReader<File>> {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let reader = BufReader::new(self.file.try_clone().unwrap());
        reader.lines()
    }

    pub fn read(&mut self, offset: u64, size: usize) -> Vec<u8> {
        let mut buf = vec![0; size];
        self.file.seek(SeekFrom::Start(offset)).unwrap();
        self.file.read_exact(&mut buf).unwrap();
        buf
    }

    pub fn clear(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}
