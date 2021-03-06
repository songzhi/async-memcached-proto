//! [Memcached Binary Protocol](https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped)
use crate::code::{Magic, Opcode, Status};
use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, Bytes, BytesMut};
use futures_lite::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use num_traits::FromPrimitive;
use std::io::{self, Read, Write};

pub trait SyncOps: Sized {
    /// Write synchronously without flush;
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()>;
    fn read_from<R: Read>(r: &mut R) -> io::Result<Self>;
}

#[async_trait]
pub trait AsyncOps: Sized {
    /// Write asynchronously without flush;
    async fn write_to<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> io::Result<()>;
    async fn read_from<R: AsyncRead + Unpin + Send>(r: &mut R) -> io::Result<Self>;
}

// Byte/     0       |       1       |       2       |       3       |
//    /              |               |               |               |
//   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
//   +---------------+---------------+---------------+---------------+
//  0| Magic         | Opcode        | Key length                    |
//   +---------------+---------------+---------------+---------------+
//  4| Extras length | Data type     | vbucket id or status          |
//   +---------------+---------------+---------------+---------------+
//  8| Total body length                                             |
//   +---------------+---------------+---------------+---------------+
// 12| Opaque                                                        |
//   +---------------+---------------+---------------+---------------+
// 16| CAS                                                           |
//   |                                                               |
//   +---------------+---------------+---------------+---------------+
//   Total 24 bytes
#[derive(Debug, PartialEq)]
pub struct PacketHeader {
    pub magic: Magic,
    pub opcode: Opcode,
    pub key_len: u16,
    pub extras_len: u8,
    /// raw bytes(0x00)
    pub data_type: u8,
    pub vbucket_id_or_status: u16,
    /// `key_len` + `extras_len` + `val_len`
    pub body_len: u32,
    pub opaque: u32,
    pub cas: u64,
}

impl PacketHeader {
    pub fn request_from_payload(
        opcode: Opcode,
        vbucket_id: u16,
        opaque: u32,
        cas: u64,
        extras: &Extras,
        key: &[u8],
        val: &[u8],
    ) -> Self {
        let key_len = key.len() as u16;
        let extras_len = extras.len() as u8;
        let body_len = (key.len() + extras.len() + val.len()) as u32;

        Self {
            magic: Magic::Request,
            opcode,
            key_len,
            extras_len,
            data_type: 0,
            vbucket_id_or_status: vbucket_id,
            body_len,
            opaque,
            cas,
        }
    }
    pub fn response_from_payload(
        opcode: Opcode,
        status: Status,
        opaque: u32,
        cas: u64,
        extras: &Extras,
        key: &[u8],
        val: &[u8],
    ) -> Self {
        let key_len = key.len() as u16;
        let extras_len = extras.len() as u8;
        let body_len = (key.len() + extras.len() + val.len()) as u32;

        Self {
            magic: Magic::Request,
            opcode,
            key_len,
            extras_len,
            data_type: 0,
            vbucket_id_or_status: status as u16,
            body_len,
            opaque,
            cas,
        }
    }
    /// Size of `PacketHeader` in bytes
    pub const fn size() -> usize {
        1 + 1 + 2 + 1 + 1 + 2 + 4 + 4 + 8
    }

    /// Parse from buffer;
    /// if anything incorrect such as wrong magic number returns `None`;
    /// # Panics
    /// This function panics if there is not enough remaining data in `buf`.
    pub fn parse(mut buf: &[u8]) -> io::Result<Self> {
        Ok(Self {
            magic: Magic::from_u8(buf.get_u8()).ok_or(io::ErrorKind::InvalidData)?,
            opcode: Opcode::from_u8(buf.get_u8()).ok_or(io::ErrorKind::InvalidData)?,
            key_len: buf.get_u16(),
            extras_len: buf.get_u8(),
            data_type: buf.get_u8(),
            vbucket_id_or_status: buf.get_u16(),
            body_len: buf.get_u32(),
            opaque: buf.get_u32(),
            cas: buf.get_u64(),
        })
    }
}

#[async_trait]
impl AsyncOps for PacketHeader {
    async fn write_to<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&(self.magic as u8).to_be_bytes()).await?;
        w.write_all(&(self.opcode as u8).to_be_bytes()).await?;
        w.write_all(&self.key_len.to_be_bytes()).await?;
        w.write_all(&self.extras_len.to_be_bytes()).await?;
        w.write_all(&self.data_type.to_be_bytes()).await?;
        w.write_all(&self.vbucket_id_or_status.to_be_bytes())
            .await?;
        w.write_all(&self.body_len.to_be_bytes()).await?;
        w.write_all(&self.opaque.to_be_bytes()).await?;
        w.write_all(&self.cas.to_be_bytes()).await?;
        Ok(())
    }

    async fn read_from<R: AsyncRead + Unpin + Send>(r: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; Self::size()];
        r.read_exact(&mut buf).await?;
        Self::parse(&buf)
    }
}

impl SyncOps for PacketHeader {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u8(self.magic as u8)?;
        w.write_u8(self.opcode as u8)?;
        w.write_u16::<BigEndian>(self.key_len)?;
        w.write_u8(self.extras_len)?;
        w.write_u8(self.data_type)?;
        w.write_u16::<BigEndian>(self.vbucket_id_or_status)?;
        w.write_u32::<BigEndian>(self.body_len)?;
        w.write_u32::<BigEndian>(self.opaque)?;
        w.write_u64::<BigEndian>(self.cas)?;
        Ok(())
    }

    fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; Self::size()];
        r.read_exact(&mut buf)?;
        Self::parse(&buf)
    }
}

#[derive(Debug, PartialEq)]
pub enum Extras {
    /// No Extra data
    None,
    /// Unknown extras data
    Unknown(Bytes),
    /// Extra data for set/add/replace
    Store { flags: u32, expiration: u32 },
    /// Extra data for incr/decr
    Counter {
        amount: u64,
        initial: u64,
        expiration: u32,
    },
    /// Extra data for flush
    Flush { expiration: u32 },
    /// Extra data for incr/decr
    Verbosity { verbosity: u32 },
    /// Extra data for touch/GAT/GATQ
    Touch { expiration: u32 },
    /// Extra data for the get commands in `Response`
    Get { flags: u32 },
}

impl Extras {
    /// Extra data length
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::None => 0,
            Self::Unknown(b) => b.len(),
            Self::Store { .. } => 4 + 4,
            Self::Counter { .. } => 8 + 8 + 4,
            Self::Flush { .. } => 4,
            Self::Verbosity { .. } => 4,
            Self::Touch { .. } => 4,
            Self::Get { .. } => 4,
        }
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Write asynchronously without flush;
    pub async fn write<W: AsyncWrite + Unpin>(&self, w: &mut W) -> io::Result<()> {
        match self {
            Self::None => {}
            Self::Unknown(b) => {
                w.write_all(b.bytes()).await?;
            }
            Self::Store { flags, expiration } => {
                w.write_all(&flags.to_be_bytes()).await?;
                w.write_all(&expiration.to_be_bytes()).await?;
            }
            Self::Counter {
                amount,
                initial,
                expiration,
            } => {
                w.write_all(&amount.to_be_bytes()).await?;
                w.write_all(&initial.to_be_bytes()).await?;
                w.write_all(&expiration.to_be_bytes()).await?;
            }
            Self::Flush { expiration } => {
                w.write_all(&expiration.to_be_bytes()).await?;
            }
            Self::Verbosity { verbosity } => {
                w.write_all(&verbosity.to_be_bytes()).await?;
            }
            Self::Touch { expiration } => {
                w.write_all(&expiration.to_be_bytes()).await?;
            }
            Self::Get { flags } => {
                w.write_all(&flags.to_be_bytes()).await?;
            }
        }
        Ok(())
    }
    /// Write asynchronously without flush;
    pub fn write_sync<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            Self::None => {}
            Self::Unknown(b) => {
                w.write_all(b.bytes())?;
            }
            Self::Store { flags, expiration } => {
                w.write_u32::<BigEndian>(*flags)?;
                w.write_u32::<BigEndian>(*expiration)?;
            }
            Self::Counter {
                amount,
                initial,
                expiration,
            } => {
                w.write_u64::<BigEndian>(*amount)?;
                w.write_u64::<BigEndian>(*initial)?;
                w.write_u32::<BigEndian>(*expiration)?;
            }
            Self::Flush { expiration } => {
                w.write_u32::<BigEndian>(*expiration)?;
            }
            Self::Verbosity { verbosity } => {
                w.write_u32::<BigEndian>(*verbosity)?;
            }
            Self::Touch { expiration } => {
                w.write_u32::<BigEndian>(*expiration)?;
            }
            Self::Get { flags } => {
                w.write_u32::<BigEndian>(*flags)?;
            }
        }
        Ok(())
    }
    /// Parse from buf based on `buf.len()` and `Opcode`
    pub fn parse(opcode: Opcode, mut buf: &[u8]) -> io::Result<Self> {
        if buf.is_empty() {
            return Ok(Self::None);
        }
        Ok(match opcode {
            Opcode::Set
            | Opcode::SetQ
            | Opcode::Add
            | Opcode::AddQ
            | Opcode::Replace
            | Opcode::ReplaceQ => Self::Store {
                flags: buf.read_u32::<BigEndian>()?,
                expiration: buf.read_u32::<BigEndian>()?,
            },
            Opcode::Get | Opcode::GetQ | Opcode::GetK | Opcode::GetKQ => Self::Get {
                flags: buf.read_u32::<BigEndian>()?,
            },
            Opcode::Increment | Opcode::IncrementQ | Opcode::Decrement | Opcode::DecrementQ => {
                Self::Counter {
                    amount: buf.read_u64::<BigEndian>()?,
                    initial: buf.read_u64::<BigEndian>()?,
                    expiration: buf.read_u32::<BigEndian>()?,
                }
            }
            Opcode::Verbosity => Self::Verbosity {
                verbosity: buf.read_u32::<BigEndian>()?,
            },
            Opcode::Touch | Opcode::GAT | Opcode::GATQ => Self::Touch {
                expiration: buf.read_u32::<BigEndian>()?,
            },
            Opcode::Flush => Self::Flush {
                expiration: buf.read_u32::<BigEndian>()?,
            },
            _ => Self::Unknown(buf.to_bytes()),
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct Packet {
    pub header: PacketHeader,
    pub extras: Extras,
    pub key: Bytes,
    pub val: Bytes,
}

impl Packet {
    /// Constructs new `Request`, just pass `Bytes::new()` to represents an empty key or value;
    ///
    /// # Examples
    /// ```rust
    /// use memcached_proto::{Packet, PacketHeader, Opcode, Extras};
    /// use bytes::Bytes;
    ///
    /// let p = Packet::request(Opcode::NoOp, 0, 0, 0, Extras::None, Bytes::new(), Bytes::new());
    /// assert_eq!(p.header.key_len + p.header.extras_len, p.header.body_len)
    /// ```
    pub fn request(
        opcode: Opcode,
        vbucket_id: u16,
        opaque: u32,
        cas: u64,
        extras: Extras,
        key: Bytes,
        val: Bytes,
    ) -> Self {
        let header = PacketHeader {
            magic: Magic::Request,
            opcode,
            key_len: key.len() as u16,
            extras_len: extras.len() as u8,
            data_type: 0,
            vbucket_id_or_status: vbucket_id,
            body_len: (key.len() + extras.len() + val.len()) as u32,
            opaque,
            cas,
        };
        Self {
            header,
            extras,
            key,
            val,
        }
    }
    /// Constructs new `Response`, just pass `Bytes::new()` to represents an empty key or value;
    ///
    /// # Examples
    /// ```rust
    /// use memcached_proto::{Packet, PacketHeader, Opcode, Extras, Status};
    /// use bytes::Bytes;
    ///
    /// let p = Packet::response(Opcode::NoOp, Status::NoError, 0, 0,
    ///     Extras::None, Bytes::new(), Bytes::new());
    /// assert_eq!(p.header.key_len + p.header.extras_len, p.header.body_len)
    /// ```
    pub fn response(
        opcode: Opcode,
        status: Status,
        opaque: u32,
        cas: u64,
        extras: Extras,
        key: Bytes,
        val: Bytes,
    ) -> Self {
        let header = PacketHeader {
            magic: Magic::Request,
            opcode,
            key_len: key.len() as u16,
            extras_len: extras.len() as u8,
            data_type: 0,
            vbucket_id_or_status: status as u16,
            body_len: (key.len() + extras.len() + val.len()) as u32,
            opaque,
            cas,
        };
        Self {
            header,
            extras,
            key,
            val,
        }
    }
    /// Constructs new `Packet`, just pass `Bytes::new()` to represents an empty key or value;
    ///
    pub fn new(header: PacketHeader, extras: Extras, key: Bytes, val: Bytes) -> Self {
        Self {
            header,
            extras,
            key,
            val,
        }
    }
    #[inline]
    pub fn is_request(&self) -> bool {
        matches!(self.header.magic, Magic::Request)
    }

    #[inline]
    pub fn is_response(&self) -> bool {
        matches!(self.header.magic, Magic::Response)
    }
    /// Response status;
    /// # Panics
    /// if data is incorrect or packet isn't response
    #[inline]
    pub fn status(&self) -> Status {
        debug_assert!(self.is_response());
        Status::from_u16(self.header.vbucket_id_or_status).unwrap()
    }
}

impl SyncOps for Packet {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        SyncOps::write_to(&self.header, w)?;
        self.extras.write_sync(w)?;
        w.write_all(self.key.bytes())?;
        w.write_all(self.val.bytes())?;
        w.flush()?;
        Ok(())
    }

    fn read_from<R: Read>(r: &mut R) -> io::Result<Self> {
        let header: PacketHeader = SyncOps::read_from(r)?;

        let body_len = header.body_len as usize;
        let mut buf = BytesMut::with_capacity(body_len);
        unsafe {
            buf.set_len(body_len);
        }
        r.read_exact(buf.as_mut())?;

        let extras = Extras::parse(
            header.opcode,
            buf.split_to(header.extras_len as usize).bytes(),
        )?;
        let key = buf.split_to(header.key_len as usize).freeze();
        let value = buf.freeze();

        Ok(Packet {
            header,
            extras,
            key,
            val: value,
        })
    }
}

#[async_trait]
impl AsyncOps for Packet {
    async fn write_to<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> io::Result<()> {
        AsyncOps::write_to(&self.header, w).await?;
        self.extras.write(w).await?;
        w.write_all(self.key.bytes()).await?;
        w.write_all(self.val.bytes()).await?;
        w.flush().await?;
        Ok(())
    }

    async fn read_from<R: AsyncRead + Unpin + Send>(r: &mut R) -> io::Result<Self> {
        let header: PacketHeader = AsyncOps::read_from(r).await?;

        let body_len = header.body_len as usize;
        let mut buf = BytesMut::with_capacity(body_len);
        unsafe {
            buf.set_len(body_len);
        }
        r.read_exact(buf.as_mut()).await?;

        let extras = Extras::parse(
            header.opcode,
            buf.split_to(header.extras_len as usize).bytes(),
        )?;
        let key = buf.split_to(header.key_len as usize).freeze();
        let value = buf.freeze();

        Ok(Packet {
            header,
            extras,
            key,
            val: value,
        })
    }
}

#[derive(Debug)]
pub struct PacketRef<'a> {
    pub header: &'a PacketHeader,
    pub extras: &'a Extras,
    pub key: &'a [u8],
    pub val: &'a [u8],
}

impl<'a> PacketRef<'a> {
    pub fn new(
        header: &'a PacketHeader,
        extras: &'a Extras,
        key: &'a [u8],
        val: &'a [u8],
    ) -> PacketRef<'a> {
        PacketRef {
            header,
            extras,
            key,
            val,
        }
    }
}

impl<'a> SyncOps for PacketRef<'a> {
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        SyncOps::write_to(self.header, w)?;
        self.extras.write_sync(w)?;
        w.write_all(self.key)?;
        w.write_all(self.val)?;

        Ok(())
    }
    /// unimplemented
    fn read_from<R: Read>(_r: &mut R) -> io::Result<Self> {
        unimplemented!()
    }
}

#[async_trait]
impl<'a> AsyncOps for PacketRef<'a> {
    async fn write_to<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> io::Result<()> {
        AsyncOps::write_to(self.header, w).await?;
        self.extras.write(w).await?;
        w.write_all(self.key).await?;
        w.write_all(self.val).await?;

        Ok(())
    }
    /// unimplemented
    async fn read_from<R: AsyncRead + Unpin + Send>(_r: &mut R) -> io::Result<Self> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::{Extras, Opcode, Packet, Status, SyncOps};

    use std::io::Write;
    use std::net::TcpStream;

    use bytes::{Buf, Bytes};

    fn test_stream() -> TcpStream {
        TcpStream::connect("127.0.0.1:11211").unwrap()
    }

    #[test]
    fn test_binary_protocol() {
        let mut stream = test_stream();

        {
            let req_packet = Packet::request(
                Opcode::Set,
                0,
                0,
                0,
                Extras::Store {
                    flags: 0,
                    expiration: 0,
                },
                b"test:binary_proto:hello".as_ref().into(),
                b"world".as_ref().into(),
            );

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = Packet::read_from(&mut stream).unwrap();

            assert_eq!(resp_packet.status(), Status::NoError);
        }

        {
            let req_packet = Packet::request(
                Opcode::Get,
                0,
                0,
                0,
                Extras::None,
                b"test:binary_proto:hello".as_ref().into(),
                Bytes::new(),
            );

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = Packet::read_from(&mut stream).unwrap();

            assert_eq!(resp_packet.status(), Status::NoError);
            assert_eq!(&resp_packet.val.bytes(), b"world");
        }

        {
            let req_packet = Packet::request(
                Opcode::Delete,
                0,
                0,
                0,
                Extras::None,
                b"test:binary_proto:hello".as_ref().into(),
                Bytes::new(),
            );

            req_packet.write_to(&mut stream).unwrap();
            stream.flush().unwrap();

            let resp_packet = Packet::read_from(&mut stream).unwrap();

            assert_eq!(resp_packet.status(), Status::NoError);
        }
    }
}
