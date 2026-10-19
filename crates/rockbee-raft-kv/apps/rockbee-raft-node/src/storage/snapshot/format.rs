//! Version-one snapshot framing. Every length is checked before allocation.
//! CRC-32/ISO-HDLC covers all bytes except the final checksum, including the
//! record count. It detects accidental corruption; it is not authentication.
use crate::types::{LogId, StoredMembership};
use openraft::{BasicNode, CommittedLeaderId, Membership, SnapshotMeta};
use rockbee_raft_protocol::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
};

pub(super) type Meta = SnapshotMeta<u64, BasicNode>;
pub(super) const MAGIC: &[u8; 8] = b"RBRSNAP1";
pub(super) const IO_BUFFER: usize = 64 * 1024;

pub(super) fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Header {
    pub cluster: [u8; 16],
    pub group: u64,
    pub hash_version: u8,
    pub shards: u16,
    pub seed: u64,
    pub meta: Meta,
}

struct HeaderWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl HeaderWriter {
    fn put(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(invalid(
                "snapshot metadata exceeds configured header budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn count(&mut self, count: usize) -> io::Result<()> {
        self.put(&u32::try_from(count).map_err(invalid)?.to_be_bytes())
    }
    fn text(&mut self, value: &str) -> io::Result<()> {
        self.count(value.len())?;
        self.put(value.as_bytes())
    }
    fn log(&mut self, log: Option<LogId>) -> io::Result<()> {
        self.put(&[u8::from(log.is_some())])?;
        if let Some(log) = log {
            self.put(&log.leader_id.term.to_be_bytes())?;
            self.put(&log.leader_id.node_id.to_be_bytes())?;
            self.put(&log.index.to_be_bytes())?;
        }
        Ok(())
    }
}

pub(super) fn encode_header(header: &Header, limit: usize) -> io::Result<Vec<u8>> {
    let mut out = HeaderWriter {
        bytes: Vec::new(),
        limit,
    };
    out.put(&header.cluster)?;
    out.put(&header.group.to_be_bytes())?;
    out.put(&[header.hash_version])?;
    out.put(&header.shards.to_be_bytes())?;
    out.put(&header.seed.to_be_bytes())?;
    out.log(header.meta.last_log_id)?;
    out.log(*header.meta.last_membership.log_id())?;
    let membership = header.meta.last_membership.membership();
    out.count(membership.get_joint_config().len())?;
    for voters in membership.get_joint_config() {
        out.count(voters.len())?;
        for id in voters {
            out.put(&id.to_be_bytes())?;
        }
    }
    out.count(membership.nodes().count())?;
    for (id, node) in membership.nodes() {
        out.put(&id.to_be_bytes())?;
        out.text(&node.addr)?;
    }
    out.text(&header.meta.snapshot_id)?;
    Ok(out.bytes)
}

pub(super) struct Fields<'a> {
    remaining: &'a [u8],
}
impl<'a> Fields<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
    pub(super) fn take(&mut self, length: usize) -> io::Result<&'a [u8]> {
        if length > self.remaining.len() {
            return Err(invalid("truncated metadata field"));
        }
        let (value, rest) = self.remaining.split_at(length);
        self.remaining = rest;
        Ok(value)
    }
    pub(super) fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(invalid)?,
        ))
    }
    pub(super) fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(invalid)?,
        ))
    }
    fn count(&mut self, minimum_bytes: usize) -> io::Result<usize> {
        let count = usize::try_from(self.u32()?).map_err(invalid)?;
        if count > self.remaining.len() / minimum_bytes {
            return Err(invalid("metadata count exceeds remaining bounded header"));
        }
        Ok(count)
    }
    fn text(&mut self) -> io::Result<String> {
        let length = self.count(1)?;
        String::from_utf8(self.take(length)?.to_vec()).map_err(invalid)
    }
    fn log(&mut self) -> io::Result<Option<LogId>> {
        match self.take(1)?[0] {
            0 => Ok(None),
            1 => Ok(Some(LogId::new(
                CommittedLeaderId::new(self.u64()?, self.u64()?),
                self.u64()?,
            ))),
            _ => Err(invalid("invalid optional log marker")),
        }
    }
    pub(super) fn finish(self) -> io::Result<()> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(invalid("trailing metadata bytes"))
        }
    }
}

pub(super) fn decode_header(bytes: &[u8]) -> io::Result<Header> {
    let mut input = Fields::new(bytes);
    let cluster = input.take(16)?.try_into().map_err(invalid)?;
    let group = input.u64()?;
    let hash_version = input.take(1)?[0];
    let shards = u16::from_be_bytes(input.take(2)?.try_into().map_err(invalid)?);
    let seed = input.u64()?;
    let last_log_id = input.log()?;
    let membership_log = input.log()?;
    let joint_count = input.count(4)?;
    let mut configs = Vec::new();
    for _ in 0..joint_count {
        let count = input.count(8)?;
        let mut voters = BTreeSet::new();
        let mut prior = None;
        for _ in 0..count {
            let id = input.u64()?;
            if prior.is_some_and(|p| id <= p) {
                return Err(invalid("non-increasing voter IDs"));
            }
            voters.insert(id);
            prior = Some(id);
        }
        configs.push(voters);
    }
    let count = input.count(12)?;
    let mut nodes = BTreeMap::new();
    let mut prior = None;
    for _ in 0..count {
        let id = input.u64()?;
        if prior.is_some_and(|p| id <= p) {
            return Err(invalid("non-increasing member IDs"));
        }
        nodes.insert(
            id,
            BasicNode {
                addr: input.text()?,
            },
        );
        prior = Some(id);
    }
    if configs.iter().flatten().any(|id| !nodes.contains_key(id)) {
        return Err(invalid("membership voter has no node"));
    }
    let snapshot_id = input.text()?;
    input.finish()?;
    if membership_log.is_some_and(|id| {
        last_log_id.is_none_or(|last| {
            id.index > last.index
                || id.leader_id > last.leader_id
                || (id.index == last.index && id != last)
        })
    }) {
        return Err(invalid(
            "membership is incompatible with snapshot applied log",
        ));
    }
    Ok(Header {
        cluster,
        group,
        hash_version,
        shards,
        seed,
        meta: Meta {
            last_log_id,
            last_membership: StoredMembership::new(membership_log, Membership::new(configs, nodes)),
            snapshot_id,
        },
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Summary {
    pub length: u64,
    pub count: u64,
    pub checksum: u32,
}

pub(super) struct Crc32(u32);
impl Crc32 {
    pub(super) fn new() -> Self {
        Self(u32::MAX)
    }
    pub(super) fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u32::from(*byte);
            for _ in 0..8 {
                self.0 = (self.0 >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(self.0 & 1));
            }
        }
    }
    pub(super) fn finish(&self) -> u32 {
        !self.0
    }
}

pub(super) struct ImageWriter {
    output: BufWriter<File>,
    crc: Crc32,
    length: u64,
    count: u64,
}
impl ImageWriter {
    pub(super) fn new(file: File, header: &Header, limit: usize) -> io::Result<Self> {
        let mut out = Self {
            output: BufWriter::with_capacity(IO_BUFFER, file),
            crc: Crc32::new(),
            length: 0,
            count: 0,
        };
        let encoded = encode_header(header, limit)?;
        out.put(MAGIC)?;
        out.put(&u32::try_from(encoded.len()).map_err(invalid)?.to_be_bytes())?;
        out.put(&encoded)?;
        Ok(out)
    }
    fn put(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.output.write_all(bytes)?;
        self.crc.update(bytes);
        self.length = self
            .length
            .checked_add(u64::try_from(bytes.len()).map_err(invalid)?)
            .ok_or_else(|| invalid("image length overflow"))?;
        Ok(())
    }
    pub(super) fn record(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        check_record_lengths(key.len(), value.len())?;
        self.put(&u32::try_from(key.len()).map_err(invalid)?.to_be_bytes())?;
        self.put(&u32::try_from(value.len()).map_err(invalid)?.to_be_bytes())?;
        self.put(key)?;
        self.put(value)?;
        self.count = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("record count overflow"))?;
        Ok(())
    }
    pub(super) fn finish(mut self) -> io::Result<(File, Summary)> {
        self.put(&0u32.to_be_bytes())?;
        self.put(&self.count.to_be_bytes())?;
        let checksum = self.crc.finish();
        self.output.write_all(&checksum.to_be_bytes())?;
        self.output.flush()?;
        let length = self
            .length
            .checked_add(4)
            .ok_or_else(|| invalid("image length overflow"))?;
        let file = self.output.into_inner().map_err(|e| e.into_error())?;
        Ok((
            file,
            Summary {
                length,
                count: self.count,
                checksum,
            },
        ))
    }
}

fn check_record_lengths(key: usize, value: usize) -> io::Result<()> {
    if key == 0 || key > MAX_KEY_BYTES || value > MAX_VALUE_BYTES {
        return Err(invalid("invalid snapshot record lengths"));
    }
    Ok(())
}

/// Retains one record, its previous key, and two fixed-size I/O buffers. The
/// importer callback owns its separately bounded WriteBatch; no record queue.
pub(super) struct ImageReader {
    input: BufReader<File>,
    output: Option<BufWriter<File>>,
    crc: Crc32,
    length: u64,
    #[cfg(test)]
    peak_record_bytes: usize,
}
impl ImageReader {
    pub(super) fn new(input: File, copy: Option<File>) -> Self {
        Self {
            input: BufReader::with_capacity(IO_BUFFER, input),
            output: copy.map(|f| BufWriter::with_capacity(IO_BUFFER, f)),
            crc: Crc32::new(),
            length: 0,
            #[cfg(test)]
            peak_record_bytes: 0,
        }
    }
    fn take(&mut self, bytes: &mut [u8], checksummed: bool) -> io::Result<()> {
        self.input.read_exact(bytes)?;
        if let Some(output) = &mut self.output {
            output.write_all(bytes)?;
        }
        if checksummed {
            self.crc.update(bytes);
        }
        self.length = self
            .length
            .checked_add(u64::try_from(bytes.len()).map_err(invalid)?)
            .ok_or_else(|| invalid("image length overflow"))?;
        Ok(())
    }
    fn u32(&mut self) -> io::Result<u32> {
        let mut bytes = [0; 4];
        self.take(&mut bytes, true)?;
        Ok(u32::from_be_bytes(bytes))
    }
    pub(super) fn header(&mut self, limit: usize) -> io::Result<Header> {
        let mut magic = [0; 8];
        self.take(&mut magic, true)?;
        if &magic != MAGIC {
            return Err(invalid("unsupported snapshot magic or version"));
        }
        let length = usize::try_from(self.u32()?).map_err(invalid)?;
        if length > limit {
            return Err(invalid("snapshot header length exceeds budget"));
        }
        let mut bytes = vec![0; length];
        self.take(&mut bytes, true)?;
        decode_header(&bytes)
    }
    pub(super) fn records(
        &mut self,
        mut accept: impl FnMut(&[u8], &[u8]) -> io::Result<()>,
    ) -> io::Result<Summary> {
        let mut previous = Vec::new();
        let mut count = 0u64;
        loop {
            let key_len = usize::try_from(self.u32()?).map_err(invalid)?;
            if key_len == 0 {
                break;
            }
            let value_len = usize::try_from(self.u32()?).map_err(invalid)?;
            check_record_lengths(key_len, value_len)?;
            let mut key = vec![0; key_len];
            self.take(&mut key, true)?;
            if key <= previous {
                return Err(invalid("duplicate or non-increasing snapshot keys"));
            }
            let mut value = vec![0; value_len];
            self.take(&mut value, true)?;
            #[cfg(test)]
            {
                self.peak_record_bytes = self
                    .peak_record_bytes
                    .max(key.capacity() + value.capacity() + previous.capacity());
            }
            accept(&key, &value)?;
            previous = key;
            count = count
                .checked_add(1)
                .ok_or_else(|| invalid("record count overflow"))?;
        }
        let mut stored_count = [0; 8];
        self.take(&mut stored_count, true)?;
        if u64::from_be_bytes(stored_count) != count {
            return Err(invalid("snapshot record count mismatch"));
        }
        let checksum = self.crc.finish();
        let mut stored_crc = [0; 4];
        self.take(&mut stored_crc, false)?;
        if u32::from_be_bytes(stored_crc) != checksum {
            return Err(invalid("snapshot checksum mismatch"));
        }
        let mut trailing = [0; 1];
        if self.input.read(&mut trailing)? != 0 {
            return Err(invalid("trailing snapshot bytes"));
        }
        Ok(Summary {
            length: self.length,
            count,
            checksum,
        })
    }
    #[cfg(test)]
    pub(super) fn peak_record_bytes(&self) -> usize {
        self.peak_record_bytes
    }
    pub(super) fn finish_copy(self) -> io::Result<Option<File>> {
        self.output
            .map(|mut output| {
                output.flush()?;
                output.into_inner().map_err(|e| e.into_error())
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn crc32_iso_hdlc_known_vector() {
        let mut crc = Crc32::new();
        crc.update(b"1234");
        crc.update(b"56789");
        assert_eq!(crc.finish(), 0xcbf4_3926);
    }
}
