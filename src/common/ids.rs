//! Numeric identifiers for runtime entities (vertices and channels).
//!
//! Replaces string-based ids on the hot path. The layout encodes operator
//! semantics directly into the id so a `VertexId` is self-describing
//! (operator type, per-type instance, parallel index) without a side table.
//!
//! Architectural limits (enforced by construction):
//! - up to 255 distinct operator types in the system
//! - up to 255 instances of the same operator type within one pipeline
//! - up to 65535 parallel partitions per operator instance

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;

/// Stable u8 code per concrete operator variant.
///
/// New codes must be appended (never inserted in the middle, never renumbered)
/// so existing checkpoints and serialized vertex ids remain interpretable.
#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Serialize, Deserialize)]
pub enum OperatorTypeCode {
    Invalid = 0x00,

    SourceVector = 0x01,
    SourceWordCount = 0x02,
    SourceDatagen = 0x03,
    SourceHttpRequest = 0x04,
    SourceKafka = 0x05,
    SourceParquet = 0x06,

    SinkInMemoryStorageGrpc = 0x07,
    SinkRequest = 0x08,
    SinkParquet = 0x09,

    Map = 0x0A,
    KeyBy = 0x0B,
    Reduce = 0x0C,
    Aggregate = 0x0D,
    Window = 0x0E,
    WindowRequest = 0x0F,
    Join = 0x10,

    Chained = 0x11,
}

impl OperatorTypeCode {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0x00 => Some(Self::Invalid),
            0x01 => Some(Self::SourceVector),
            0x02 => Some(Self::SourceWordCount),
            0x03 => Some(Self::SourceDatagen),
            0x04 => Some(Self::SourceHttpRequest),
            0x05 => Some(Self::SourceKafka),
            0x06 => Some(Self::SourceParquet),
            0x07 => Some(Self::SinkInMemoryStorageGrpc),
            0x08 => Some(Self::SinkRequest),
            0x09 => Some(Self::SinkParquet),
            0x0A => Some(Self::Map),
            0x0B => Some(Self::KeyBy),
            0x0C => Some(Self::Reduce),
            0x0D => Some(Self::Aggregate),
            0x0E => Some(Self::Window),
            0x0F => Some(Self::WindowRequest),
            0x10 => Some(Self::Join),
            0x11 => Some(Self::Chained),
            _ => None,
        }
    }

    pub fn short_name(self) -> &'static str {
        match self {
            Self::Invalid => "Invalid",
            Self::SourceVector => "SourceVector",
            Self::SourceWordCount => "SourceWordCount",
            Self::SourceDatagen => "SourceDatagen",
            Self::SourceHttpRequest => "SourceHttpRequest",
            Self::SourceKafka => "SourceKafka",
            Self::SourceParquet => "SourceParquet",
            Self::SinkInMemoryStorageGrpc => "SinkInMemoryStorageGrpc",
            Self::SinkRequest => "SinkRequest",
            Self::SinkParquet => "SinkParquet",
            Self::Map => "Map",
            Self::KeyBy => "KeyBy",
            Self::Reduce => "Reduce",
            Self::Aggregate => "Aggregate",
            Self::Window => "Window",
            Self::WindowRequest => "WindowRequest",
            Self::Join => "Join",
            Self::Chained => "Chained",
        }
    }
}

impl fmt::Display for OperatorTypeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short_name())
    }
}

impl From<&OperatorConfig> for OperatorTypeCode {
    fn from(cfg: &OperatorConfig) -> Self {
        match cfg {
            OperatorConfig::MapConfig(_) => Self::Map,
            OperatorConfig::JoinConfig(_) => Self::Join,
            OperatorConfig::KeyByConfig(_) => Self::KeyBy,
            OperatorConfig::ReduceConfig(_, _) => Self::Reduce,
            OperatorConfig::AggregateConfig(_) => Self::Aggregate,
            OperatorConfig::WindowConfig(_) => Self::Window,
            OperatorConfig::WindowRequestConfig(_) => Self::WindowRequest,
            OperatorConfig::ChainedConfig(_) => Self::Chained,
            OperatorConfig::SourceConfig(src) => match src {
                SourceConfig::VectorSourceConfig(_) => Self::SourceVector,
                SourceConfig::WordCountSourceConfig(_) => Self::SourceWordCount,
                SourceConfig::DatagenSourceConfig(_) => Self::SourceDatagen,
                SourceConfig::HttpRequestSourceConfig(_) => Self::SourceHttpRequest,
                SourceConfig::KafkaSourceConfig(_) => Self::SourceKafka,
                SourceConfig::ParquetSourceConfig(_) => Self::SourceParquet,
            },
            OperatorConfig::SinkConfig(snk) => match snk {
                SinkConfig::InMemoryStorageGrpcSinkConfig(_) => Self::SinkInMemoryStorageGrpc,
                SinkConfig::RequestSinkConfig => Self::SinkRequest,
                SinkConfig::ParquetSinkConfig(_) => Self::SinkParquet,
            },
        }
    }
}

const VERTEX_OP_TYPE_SHIFT: u32 = 24;
const VERTEX_INSTANCE_SHIFT: u32 = 16;
const VERTEX_OP_TYPE_MASK: u32 = 0xFF00_0000;
const VERTEX_INSTANCE_MASK: u32 = 0x00FF_0000;
const VERTEX_PARALLEL_MASK: u32 = 0x0000_FFFF;

const OPERATOR_OP_TYPE_SHIFT: u16 = 8;
const OPERATOR_OP_TYPE_MASK: u16 = 0xFF00;
const OPERATOR_INSTANCE_MASK: u16 = 0x00FF;

/// Logical identifier for an operator instance (one node of the LogicalGraph).
///
/// Layout: `[op_type: u8 | instance: u8]`.
///
/// Identifies "this operator type + this instance among that type" — the upper
/// 16 bits of a `VertexId`. All parallel partitions of one operator share the
/// same `OperatorId`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Serialize, Deserialize)]
pub struct OperatorId(u16);

impl OperatorId {
    pub const INVALID: OperatorId = OperatorId(0);

    pub fn new(op: OperatorTypeCode, instance: u8) -> Self {
        debug_assert!(
            op != OperatorTypeCode::Invalid,
            "OperatorId constructed with Invalid op type"
        );
        Self::from_parts(op, instance)
    }

    pub const fn from_parts(op: OperatorTypeCode, instance: u8) -> Self {
        let raw = ((op.as_u8() as u16) << OPERATOR_OP_TYPE_SHIFT) | (instance as u16);
        OperatorId(raw)
    }

    pub const fn from_raw(raw: u16) -> Self {
        OperatorId(raw)
    }

    pub const fn raw(self) -> u16 {
        self.0
    }

    pub fn op_type(self) -> OperatorTypeCode {
        let byte = ((self.0 & OPERATOR_OP_TYPE_MASK) >> OPERATOR_OP_TYPE_SHIFT) as u8;
        OperatorTypeCode::from_u8(byte).unwrap_or(OperatorTypeCode::Invalid)
    }

    pub const fn instance(self) -> u8 {
        (self.0 & OPERATOR_INSTANCE_MASK) as u8
    }

    pub const fn is_invalid(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for OperatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.op_type(), self.instance())
    }
}

impl fmt::Debug for OperatorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (0x{:04X})", self, self.0)
    }
}

/// Runtime identifier for an execution vertex (one partition of an operator).
///
/// Layout: `[op_type: u8 | instance: u8 | parallel: u16]`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Serialize, Deserialize)]
pub struct VertexId(u32);

impl VertexId {
    pub const INVALID: VertexId = VertexId(0);

    pub fn new(op: OperatorTypeCode, instance: u8, parallel: u16) -> Self {
        debug_assert!(
            op != OperatorTypeCode::Invalid,
            "VertexId constructed with Invalid op type"
        );
        Self::from_parts(op, instance, parallel)
    }

    pub const fn from_parts(op: OperatorTypeCode, instance: u8, parallel: u16) -> Self {
        let raw = ((op.as_u8() as u32) << VERTEX_OP_TYPE_SHIFT)
            | ((instance as u32) << VERTEX_INSTANCE_SHIFT)
            | (parallel as u32);
        VertexId(raw)
    }

    pub const fn from_raw(raw: u32) -> Self {
        VertexId(raw)
    }

    pub const fn raw(self) -> u32 {
        self.0
    }

    pub fn op_type(self) -> OperatorTypeCode {
        let byte = ((self.0 & VERTEX_OP_TYPE_MASK) >> VERTEX_OP_TYPE_SHIFT) as u8;
        OperatorTypeCode::from_u8(byte).unwrap_or(OperatorTypeCode::Invalid)
    }

    pub const fn instance(self) -> u8 {
        ((self.0 & VERTEX_INSTANCE_MASK) >> VERTEX_INSTANCE_SHIFT) as u8
    }

    pub const fn task_index(self) -> u16 {
        (self.0 & VERTEX_PARALLEL_MASK) as u16
    }

    /// Derive the logical `OperatorId` for this vertex (upper 16 bits of the layout).
    /// All parallel partitions of the same operator instance share this id.
    pub const fn operator_id(self) -> OperatorId {
        OperatorId::from_raw((self.0 >> VERTEX_INSTANCE_SHIFT) as u16)
    }

    pub const fn is_invalid(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for VertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}#{}",
            self.op_type(),
            self.instance(),
            self.task_index()
        )
    }
}

impl fmt::Debug for VertexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (0x{:08X})", self, self.0)
    }
}

const CHANNEL_SOURCE_SHIFT: u64 = 32;
const CHANNEL_TARGET_MASK: u64 = 0x0000_0000_FFFF_FFFF;

/// Runtime identifier for a directed channel between two vertices.
///
/// Layout: `[source: VertexId | target: VertexId]`.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Serialize, Deserialize)]
pub struct ChannelId(u64);

impl ChannelId {
    pub const INVALID: ChannelId = ChannelId(0);

    pub const fn new(source: VertexId, target: VertexId) -> Self {
        let raw = ((source.raw() as u64) << CHANNEL_SOURCE_SHIFT) | (target.raw() as u64);
        ChannelId(raw)
    }

    pub const fn from_raw(raw: u64) -> Self {
        ChannelId(raw)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }

    pub const fn source(self) -> VertexId {
        VertexId::from_raw((self.0 >> CHANNEL_SOURCE_SHIFT) as u32)
    }

    pub const fn target(self) -> VertexId {
        VertexId::from_raw((self.0 & CHANNEL_TARGET_MASK) as u32)
    }
}

impl fmt::Display for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}->{}", self.source(), self.target())
    }
}

impl fmt::Debug for ChannelId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (0x{:016X})", self, self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_type_code_roundtrip_covers_all_variants() {
        // Exhaustively iterate via from_u8 across the assigned range.
        let all: Vec<_> = (0u8..=u8::MAX)
            .filter_map(OperatorTypeCode::from_u8)
            .collect();
        for code in all {
            let byte = code.as_u8();
            assert_eq!(OperatorTypeCode::from_u8(byte), Some(code), "code {byte:#x}");
        }
        // Unassigned bytes return None.
        assert_eq!(OperatorTypeCode::from_u8(0x12), None);
        assert_eq!(OperatorTypeCode::from_u8(0xFF), None);
    }

    #[test]
    fn vertex_id_layout_packs_and_unpacks() {
        let v = VertexId::new(OperatorTypeCode::Map, 7, 42);
        assert_eq!(v.op_type(), OperatorTypeCode::Map);
        assert_eq!(v.instance(), 7);
        assert_eq!(v.task_index(), 42);
        assert_eq!(v.raw(), 0x0A_07_00_2A);
    }

    #[test]
    fn vertex_id_max_values() {
        let v = VertexId::new(OperatorTypeCode::Join, u8::MAX, u16::MAX);
        assert_eq!(v.op_type(), OperatorTypeCode::Join);
        assert_eq!(v.instance(), u8::MAX);
        assert_eq!(v.task_index(), u16::MAX);
    }

    #[test]
    fn vertex_id_invalid_default() {
        assert!(VertexId::default().is_invalid());
        assert_eq!(VertexId::default().op_type(), OperatorTypeCode::Invalid);
    }

    #[test]
    fn vertex_id_display() {
        let v = VertexId::new(OperatorTypeCode::SourceKafka, 1, 0);
        assert_eq!(format!("{}", v), "SourceKafka.1#0");
        assert_eq!(format!("{:?}", v), "SourceKafka.1#0 (0x05010000)");
    }

    #[test]
    fn channel_id_layout_packs_and_unpacks() {
        let src = VertexId::new(OperatorTypeCode::SourceKafka, 1, 0);
        let tgt = VertexId::new(OperatorTypeCode::Map, 2, 7);
        let c = ChannelId::new(src, tgt);
        assert_eq!(c.source(), src);
        assert_eq!(c.target(), tgt);
    }

    #[test]
    fn channel_id_display() {
        let src = VertexId::new(OperatorTypeCode::SourceKafka, 1, 0);
        let tgt = VertexId::new(OperatorTypeCode::Map, 2, 7);
        let c = ChannelId::new(src, tgt);
        assert_eq!(format!("{}", c), "SourceKafka.1#0->Map.2#7");
    }

    #[test]
    fn operator_id_layout_packs_and_unpacks() {
        let o = OperatorId::new(OperatorTypeCode::Map, 7);
        assert_eq!(o.op_type(), OperatorTypeCode::Map);
        assert_eq!(o.instance(), 7);
        assert_eq!(o.raw(), 0x0A_07);
    }

    #[test]
    fn operator_id_max_values() {
        let o = OperatorId::new(OperatorTypeCode::Join, u8::MAX);
        assert_eq!(o.op_type(), OperatorTypeCode::Join);
        assert_eq!(o.instance(), u8::MAX);
    }

    #[test]
    fn operator_id_invalid_default() {
        assert!(OperatorId::default().is_invalid());
        assert_eq!(OperatorId::default().op_type(), OperatorTypeCode::Invalid);
    }

    #[test]
    fn operator_id_display() {
        let o = OperatorId::new(OperatorTypeCode::SourceKafka, 1);
        assert_eq!(format!("{}", o), "SourceKafka.1");
        assert_eq!(format!("{:?}", o), "SourceKafka.1 (0x0501)");
    }

    #[test]
    fn vertex_id_derives_operator_id() {
        // All parallel partitions of the same operator instance share the OperatorId.
        let v0 = VertexId::new(OperatorTypeCode::Window, 3, 0);
        let v1 = VertexId::new(OperatorTypeCode::Window, 3, 42);
        let v2 = VertexId::new(OperatorTypeCode::Window, 3, u16::MAX);
        let op = OperatorId::new(OperatorTypeCode::Window, 3);
        assert_eq!(v0.operator_id(), op);
        assert_eq!(v1.operator_id(), op);
        assert_eq!(v2.operator_id(), op);

        // Different instances of the same op type produce different OperatorIds.
        let other = VertexId::new(OperatorTypeCode::Window, 4, 0);
        assert_ne!(other.operator_id(), op);

        // Different op types with the same instance number also differ.
        let same_instance_diff_type = VertexId::new(OperatorTypeCode::Map, 3, 0);
        assert_ne!(same_instance_diff_type.operator_id(), op);
    }
}
