use std::io::{BufRead, Read};

/// Calls `callback` with the amount of bytes read in each operation. The callback closure is
/// responsible for keeping track of total amount read.
pub struct ReadReporter<R, F> {
    reader: R,
    callback: F,
}

impl<R, F> ReadReporter<R, F> {
    pub fn new(reader: R, callback: F) -> Self {
        Self { reader, callback }
    }
}

impl<R, F> Read for ReadReporter<R, F>
where
    R: Read,
    F: FnMut(u64),
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader
            .read(buf)
            .inspect(|read_amount| (self.callback)(*read_amount as u64))
    }

    fn read_vectored(&mut self, bufs: &mut [std::io::IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.reader
            .read_vectored(bufs)
            .inspect(|read_amount| (self.callback)(*read_amount as u64))
    }
}

impl<R, F> BufRead for ReadReporter<R, F>
where
    R: BufRead,
    F: FnMut(u64),
{
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.reader.fill_buf()
    }

    fn consume(&mut self, amount: usize) {
        self.reader.consume(amount);
        (self.callback)(amount as u64)
    }

    fn read_until(&mut self, byte: u8, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        self.reader
            .read_until(byte, buf)
            .inspect(|read_amount| (self.callback)(*read_amount as u64))
    }

    fn skip_until(&mut self, byte: u8) -> std::io::Result<usize> {
        self.reader
            .skip_until(byte)
            .inspect(|read_amount| (self.callback)(*read_amount as u64))
    }

    fn read_line(&mut self, buf: &mut String) -> std::io::Result<usize> {
        self.reader
            .read_line(buf)
            .inspect(|read_amount| (self.callback)(*read_amount as u64))
    }
}
