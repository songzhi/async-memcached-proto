//! [Memcached Binary Protocol](https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped)
use super::code::{Magic, Opcode, Status};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, Bytes, BytesMut};
use futures_lite::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use num_traits::FromPrimitive;
use std::io::{self, Read, Write};

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
    /// Request header with default(zeroed) values
    #[inline]
    pub fn request(opcode: Opcode) -> Self {
        Self {
            magic: Magic::Request,
            opcode,
            key_len: 0,
            extras_len: 0,
            data_type: 0,
            vbucket_id_or_status: 0,
            body_len: 0,
            opaque: 0,
            cas: 0,
        }
    }
    /// Response header with default(zeroed) values
    #[inline]
    pub fn response(opcode: Opcode) -> Self {
        Self {
            magic: Magic::Response,
            opcode,
            key_len: 0,
            extras_len: 0,
            data_type: 0,
            vbucket_id_or_status: 0,
            body_len: 0,
            opaque: 0,
            cas: 0,
        }
    }
    /// Size of `PacketHeader` in bytes
    pub const fn size() -> usize {
        1 + 1 + 2 + 1 + 1 + 2 + 4 + 4 + 8
    }
    /// Write asynchronously without flush;
    pub async fn write<W: AsyncWrite + Unpin>(&self, w: &mut W) -> io::Result<()> {
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
    /// Write synchronously without flush;
    pub fn write_sync<W: Write>(&self, w: &mut W) -> io::Result<()> {
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
        initial_value: u64,
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
                initial_value,
                expiration,
            } => {
                w.write_all(&amount.to_be_bytes()).await?;
                w.write_all(&initial_value.to_be_bytes()).await?;
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
                initial_value,
                expiration,
            } => {
                w.write_u64::<BigEndian>(*amount)?;
                w.write_u64::<BigEndian>(*initial_value)?;
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
                    initial_value: buf.read_u64::<BigEndian>()?,
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
    /// use async_memcached_proto::binary::{Packet, PacketHeader, Opcode, Extras};
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
    /// use async_memcached_proto::binary::{Packet, PacketHeader, Opcode, Extras, Status};
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
    /// it will automatically calculate and set `key_length`, `extras_length` and `total_body_length`
    /// by checking args.
    /// # Examples
    /// ```rust
    /// use async_memcached_proto::binary::{Packet, PacketHeader, Opcode, Extras};
    /// use bytes::Bytes;
    ///
    /// let p = Packet::new(PacketHeader::request(Opcode::NoOp),
    ///     Extras::None, Bytes::from("Hello"), Bytes::new());
    /// assert_eq!(p.header.key_len + p.header.extras_len, p.header.body_len)
    /// ```
    pub fn new(mut header: PacketHeader, extras: Extras, key: Bytes, val: Bytes) -> Self {
        header.extras_len = extras.len() as u8;
        header.key_len = key.len() as u16;
        header.body_len = (key.len() + extras.len() + val.len()) as u32;
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
    /// Write asynchronously with flush;
    pub async fn write<W: AsyncWrite + Unpin>(&self, w: &mut W) -> io::Result<()> {
        self.header.write(w).await?;
        self.extras.write(w).await?;
        w.write_all(self.key.bytes()).await?;
        w.write_all(self.val.bytes()).await?;
        w.flush().await?;
        Ok(())
    }
    /// Write synchronously with flush;
    pub fn write_sync<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.header.write_sync(w)?;
        self.extras.write_sync(w)?;
        w.write_all(self.key.bytes())?;
        w.write_all(self.val.bytes())?;
        w.flush()?;
        Ok(())
    }
    /// Read asynchronously;
    pub async fn read<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Packet> {
        let mut buf = [0u8; PacketHeader::size()];
        r.read_exact(&mut buf).await?;
        let header = PacketHeader::parse(&buf)?;

        let body_len = header.body_len as usize;
        let mut buf = BytesMut::with_capacity(body_len);
        unsafe {
            buf.set_len(body_len);
        }

        let extras = {
            let mut extras = buf.split_to(header.extras_len as usize);
            r.read_exact(extras.as_mut()).await?;
            Extras::parse(header.opcode, extras.bytes())?
        };
        let key = {
            let mut key = buf.split_to(header.key_len as usize);
            r.read_exact(key.as_mut()).await?;
            key.freeze()
        };
        let value = {
            let mut value = buf;
            r.read_exact(value.as_mut()).await?;
            value.freeze()
        };

        Ok(Packet {
            header,
            extras,
            key,
            val: value,
        })
    }
    /// Read synchronously;
    pub fn read_sync<R: Read>(r: &mut R) -> io::Result<Packet> {
        let mut buf = [0u8; PacketHeader::size()];
        r.read_exact(&mut buf)?;
        let header = PacketHeader::parse(&buf)?;

        let body_len = header.body_len as usize;
        let mut buf = BytesMut::with_capacity(body_len);
        unsafe {
            buf.set_len(body_len);
        }

        let extras = {
            let mut extras = buf.split_to(header.extras_len as usize);
            r.read_exact(extras.as_mut())?;
            Extras::parse(header.opcode, extras.bytes())?
        };
        let key = {
            let mut key = buf.split_to(header.key_len as usize);
            r.read_exact(key.as_mut())?;
            key.freeze()
        };
        let value = {
            let mut value = buf;
            r.read_exact(value.as_mut())?;
            value.freeze()
        };

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

    #[inline]
    pub fn write_sync<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.header.write_sync(w)?;
        self.extras.write_sync(w)?;
        w.write_all(self.key)?;
        w.write_all(self.val)?;

        Ok(())
    }

    #[inline]
    pub async fn write<W: AsyncWrite + Unpin>(&self, w: &mut W) -> io::Result<()> {
        self.header.write(w).await?;
        self.extras.write(w).await?;
        w.write_all(self.key).await?;
        w.write_all(self.val).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basics() {
        let header = PacketHeader::request(Opcode::Flush);
        let extras = Extras::Flush { expiration: 1234 };
        let key = "test:binary_protocol:hello";
        let val = "world";
        let packet = Packet::new(header, extras, key.into(), val.into());
        let mut buf = vec![];
        packet.write_sync(&mut buf).unwrap();
        let mut r = buf.as_slice();
        let packet_read = Packet::read_sync(&mut r).unwrap();
        assert_eq!(packet, packet_read);
    }
}
