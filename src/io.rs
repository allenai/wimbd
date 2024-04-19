//! IO helpers.

use std::io::Cursor;
use std::{
    fs::File,
    io::{self, prelude::*},
    rc::Rc,
};
use anyhow::Result;
use flate2::read::MultiGzDecoder;
use zstd::stream::read::Decoder as ZstdDecoder;

use tokio;
use crate::s3::{is_s3, get_reader_from_s3};

trait ReadLine: BufRead {}

impl<R: BufRead> ReadLine for R {}

/// A buffered reader for gzip files.

enum GzReader {
    File(io::BufReader<MultiGzDecoder<File>>),
    ZstdFile(io::BufReader<ZstdDecoder<'static, io::BufReader<File>>>),
    Memory(io::BufReader<Cursor<Vec<u8>>>),
}

impl Read for GzReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            GzReader::File(reader) => reader.read(buf),
            GzReader::ZstdFile(reader) => reader.read(buf),
            GzReader::Memory(reader) => reader.read(buf),
        }
    }
}

impl BufRead for GzReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            GzReader::File(reader) => reader.fill_buf(),
            GzReader::ZstdFile(reader) => reader.fill_buf(),
            GzReader::Memory(reader) => reader.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            GzReader::File(reader) => reader.consume(amt),
            GzReader::ZstdFile(reader) => reader.consume(amt),
            GzReader::Memory(reader) => reader.consume(amt),
        }
    }
}



pub struct GzBufReader {
    reader: GzReader,
    buf: Rc<String>,
}
fn new_buf() -> Rc<String> {
    Rc::new(String::with_capacity(2048))
}


impl GzBufReader {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let buf = new_buf();
        //println!("MAKING READER {:?} {:?}", path.as_ref(),  );
        let reader = if is_s3(path.as_ref()) {
            // TODO: I want to define a reader of type BufReader<Cursor<Vec<u8>>> here
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();   
            let result = rt.block_on(get_reader_from_s3(path, None));
            GzReader::Memory(result.unwrap())
        }  else if path.as_ref().extension().unwrap() == "zstd" {

            let decoder = ZstdDecoder::new(File::open(path)?)?;
            //decoder.aonsetuhs();
            //let reader = io::BufReader::new(decoder);
            //reader.aosnetuh();
            GzReader::ZstdFile(io::BufReader::new(decoder))
            //GzReader::ZstdFile(io::BufReader::new(ZstdDecoder::with_buffer(File::open(path)?)))
        } else {
            GzReader::File(io::BufReader::new(MultiGzDecoder::new(File::open(path)?)))
        };

        Ok(Self { reader, buf })
    }
}

type DataIteratorItem = io::Result<Rc<String>>;

impl Iterator for GzBufReader {
    type Item = DataIteratorItem;

    fn next(&mut self) -> Option<Self::Item> {
        let buf = match Rc::get_mut(&mut self.buf) {
            Some(buf) => {
                buf.clear();
                buf
            }
            None => {
                self.buf = new_buf();
                Rc::make_mut(&mut self.buf)
            }
        };

        self.reader
            .read_line(buf)
            .map(|u| {
                if u == 0 {
                    None
                } else {
                    Some(Rc::clone(&self.buf))
                }
            })
            .transpose()
    }
}
