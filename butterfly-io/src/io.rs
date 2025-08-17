//! High-performance I/O operations with alignment and hints

use crate::error::IoResult;
use std::fs::File;
use std::io::{IoSlice, IoSliceMut, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;

/// Aligned I/O operations with madvise hints
pub struct AlignedIo {
    file: File,
}

impl AlignedIo {
    pub fn new(file: File) -> Self {
        Self { file }
    }

    /// Set sequential access hint
    pub fn hint_sequential(&self) -> IoResult<()> {
        #[cfg(unix)]
        unsafe {
            let fd = self.file.as_raw_fd();
            if libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_SEQUENTIAL) != 0 {
                return Err(crate::IoError::Io(std::io::Error::last_os_error()));
            }
        }
        Ok(())
    }

    /// Set random access hint  
    pub fn hint_random(&self) -> IoResult<()> {
        #[cfg(unix)]
        unsafe {
            let fd = self.file.as_raw_fd();
            if libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_RANDOM) != 0 {
                return Err(crate::IoError::Io(std::io::Error::last_os_error()));
            }
        }
        Ok(())
    }

    /// Vectored write with 4KB alignment
    pub fn write_vectored_aligned(&mut self, bufs: &[IoSlice<'_>]) -> IoResult<usize> {
        // Ensure we're at 4KB aligned position
        let pos = self.file.stream_position()?;
        let aligned_pos = align_to_4k(pos);
        if pos != aligned_pos {
            self.file.seek(SeekFrom::Start(aligned_pos))?;
        }

        // Use vectored write
        self.file.write_vectored(bufs).map_err(Into::into)
    }

    /// Vectored read with 4KB alignment
    pub fn read_vectored_aligned(&mut self, bufs: &mut [IoSliceMut<'_>]) -> IoResult<usize> {
        // Ensure we're at 4KB aligned position
        let pos = self.file.stream_position()?;
        let aligned_pos = align_to_4k(pos);
        if pos != aligned_pos {
            self.file.seek(SeekFrom::Start(aligned_pos))?;
        }

        // Use vectored read
        self.file.read_vectored(bufs).map_err(Into::into)
    }

    /// Write at specific offset using pwritev (atomic positioned write)
    pub fn pwrite_vectored(&self, bufs: &[IoSlice<'_>], offset: u64) -> IoResult<usize> {
        let aligned_offset = align_to_4k(offset);

        #[cfg(unix)]
        unsafe {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();

            // Convert IoSlice to iovec for pwritev
            let iovecs: Vec<libc::iovec> = bufs
                .iter()
                .map(|slice| libc::iovec {
                    iov_base: slice.as_ptr() as *mut libc::c_void,
                    iov_len: slice.len(),
                })
                .collect();

            let result = libc::pwritev(
                fd,
                iovecs.as_ptr(),
                iovecs.len() as libc::c_int,
                aligned_offset as libc::off_t,
            );

            if result < 0 {
                Err(crate::IoError::Io(std::io::Error::last_os_error()))
            } else {
                Ok(result as usize)
            }
        }

        #[cfg(not(unix))]
        {
            // Fallback for non-Unix systems
            self.file.seek(SeekFrom::Start(aligned_offset))?;
            self.file.write_vectored(bufs).map_err(Into::into)
        }
    }

    /// Read at specific offset using preadv (atomic positioned read)
    pub fn pread_vectored(&self, bufs: &mut [IoSliceMut<'_>], offset: u64) -> IoResult<usize> {
        let aligned_offset = align_to_4k(offset);

        #[cfg(unix)]
        unsafe {
            use std::os::unix::io::AsRawFd;
            let fd = self.file.as_raw_fd();

            // Convert IoSliceMut to iovec for preadv
            let iovecs: Vec<libc::iovec> = bufs
                .iter_mut()
                .map(|slice| libc::iovec {
                    iov_base: slice.as_mut_ptr() as *mut libc::c_void,
                    iov_len: slice.len(),
                })
                .collect();

            let result = libc::preadv(
                fd,
                iovecs.as_ptr(),
                iovecs.len() as libc::c_int,
                aligned_offset as libc::off_t,
            );

            if result < 0 {
                Err(crate::IoError::Io(std::io::Error::last_os_error()))
            } else {
                Ok(result as usize)
            }
        }

        #[cfg(not(unix))]
        {
            // Fallback for non-Unix systems
            self.file.seek(SeekFrom::Start(aligned_offset))?;
            self.file.read_vectored(bufs).map_err(Into::into)
        }
    }

    /// Write at specific offset (pwrite equivalent)
    pub fn write_at(&mut self, buf: &[u8], offset: u64) -> IoResult<usize> {
        let slice = IoSlice::new(buf);
        self.pwrite_vectored(&[slice], offset)
    }

    /// Read at specific offset (pread equivalent)
    pub fn read_at(&mut self, buf: &mut [u8], offset: u64) -> IoResult<usize> {
        let slice = IoSliceMut::new(buf);
        self.pread_vectored(&mut [slice], offset)
    }

    /// Consume AlignedIo and return the underlying File
    pub fn into_inner(self) -> File {
        self.file
    }
}

/// Align offset to 4KB boundary
fn align_to_4k(offset: u64) -> u64 {
    (offset + 4095) & !4095
}
