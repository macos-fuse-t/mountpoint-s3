use std::{fs::File, io, os::unix::prelude::AsRawFd, sync::Arc};

use libc::{c_int, c_void, size_t};
use std::io::{Error, ErrorKind, Result};
use log::warn;

use byteorder::{ByteOrder, LittleEndian};

use crate::reply::ReplySender;

use std::sync::Mutex;

static MY_MUTEX: Mutex<()> = Mutex::new(());

/// A raw communication channel to the FUSE kernel driver
#[derive(Debug)]
pub struct Channel(Arc<File>);

fn read_exact(fd: i32, buf: &mut [u8]) -> io::Result<usize>  {
    let mut total_read = 0;
    while total_read < buf.len() {
        let r = unsafe {
            libc::read(
                fd,
                buf[total_read..].as_mut_ptr() as *mut c_void,
                (buf.len() - total_read) as size_t
            )
        };
        
        if r == 0 {
            // EOF
            return Err(Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF while reading."));
        } else if r < 0 {
            // Handle read error
            return Err(Error::new(ErrorKind::Other, "Failed to read from the descriptor."));
        } else {
            total_read += r as usize;
        }
    }
    Ok(total_read)
}

 
pub fn receive_stream(fd: i32, buffer: &mut [u8]) -> io::Result<usize> {

    let _guard = MY_MUTEX.lock().unwrap();

    if let Err(e) = read_exact(fd, &mut buffer[0..4]) {
        return Err(e);
    }

    // Convert 4 bytes into usize
    let msg_len: usize = LittleEndian::read_u32(&buffer[0..4]) as usize;

    // Ensure your buffer is large enough for the message
    if buffer.len() < msg_len {
        // Handle error
        return Err(io::Error::from_raw_os_error(22));
    }

    if let Err(e) = read_exact(fd, &mut buffer[4..msg_len]) {
        return Err(e);
    }
    Ok(msg_len)
}

impl Channel {
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel.
    pub(crate) fn new(device: Arc<File>) -> Self {
        Self(device)
    }

    /// Receives data up to the capacity of the given buffer (can block).
    pub fn receive(&self, buffer: &mut [u8]) -> io::Result<usize> {
        if cfg!(subfeature = "fuse-t") {
            return receive_stream(self.0.as_raw_fd(), buffer);
        }
    
        let rc = unsafe {
            libc::read(
                self.0.as_raw_fd(),
                buffer.as_ptr() as *mut c_void,
                buffer.len() as size_t,
            )
        };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc as usize)
        }
    }

    /// Returns a sender object for this channel. The sender object can be
    /// used to send to the channel. Multiple sender objects can be used
    /// and they can safely be sent to other threads.
    pub fn sender(&self) -> ChannelSender {
        // Since write/writev syscalls are threadsafe, we can simply create
        // a sender by using the same file and use it in other threads.
        ChannelSender(self.0.clone())
    }
}

#[derive(Clone, Debug)]
pub struct ChannelSender(Arc<File>);

impl ReplySender for ChannelSender {
    fn send(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<()> {
        let rc = unsafe {
            libc::writev(
                self.0.as_raw_fd(),
                bufs.as_ptr() as *const libc::iovec,
                bufs.len() as c_int,
            )
        };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            debug_assert_eq!(bufs.iter().map(|b| b.len()).sum::<usize>(), rc as usize);
            Ok(())
        }
    }
}
