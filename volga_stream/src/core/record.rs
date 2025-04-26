use serde::{Serialize, Deserialize};
use std::fmt::Debug;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
    // Add more variants as needed
}

#[derive(Clone, Debug)]
pub struct BaseRecord {
    pub value: Value,
    pub event_time: Option<i64>,
    pub source_emit_ts: Option<i64>,
    pub stream_name: Option<String>,
}

impl BaseRecord {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            event_time: None,
            source_emit_ts: None,
            stream_name: None,
        }
    }

    pub fn value(&self) -> &Value { &self.value }
    pub fn event_time(&self) -> Option<i64> { self.event_time }
    pub fn source_emit_ts(&self) -> Option<i64> { self.source_emit_ts }
    pub fn stream_name(&self) -> Option<&str> { self.stream_name.as_deref() }

    pub fn with_new_value(&self, value: Value) -> Self {
        Self {
            value,
            event_time: self.event_time,
            source_emit_ts: self.source_emit_ts,
            stream_name: self.stream_name.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    pub base: BaseRecord,
}

impl Record {
    pub fn new(value: Value) -> Self {
        Self {
            base: BaseRecord::new(value),
        }
    }

    pub fn value(&self) -> &Value { self.base.value() }
    pub fn event_time(&self) -> Option<i64> { self.base.event_time() }
    pub fn source_emit_ts(&self) -> Option<i64> { self.base.source_emit_ts() }
    pub fn stream_name(&self) -> Option<&str> { self.base.stream_name() }

    pub fn with_new_value(&self, value: Value) -> Self {
        Self {
            base: self.base.with_new_value(value),
        }
    }
}

#[derive(Clone, Debug)]
pub struct KeyedRecord {
    pub key: Value,
    pub base: BaseRecord,
}

impl KeyedRecord {
    pub fn new(key: Value, value: Value) -> Self {
        Self {
            key,
            base: BaseRecord::new(value),
        }
    }

    pub fn key(&self) -> &Value { &self.key }
    pub fn value(&self) -> &Value { self.base.value() }
    pub fn event_time(&self) -> Option<i64> { self.base.event_time() }
    pub fn source_emit_ts(&self) -> Option<i64> { self.base.source_emit_ts() }
    pub fn stream_name(&self) -> Option<&str> { self.base.stream_name() }

    pub fn with_new_value(&self, value: Value) -> Self {
        Self {
            key: self.key.clone(),
            base: self.base.with_new_value(value),
        }
    }

    pub fn into_record(self) -> Record {
        Record { base: self.base }
    }
}

#[derive(Clone, Debug)]
pub enum StreamRecord {
    Record(Record),
    KeyedRecord(KeyedRecord),
}

impl StreamRecord {
    pub fn value(&self) -> &Value {
        match self {
            StreamRecord::Record(r) => r.value(),
            StreamRecord::KeyedRecord(kr) => kr.value(),
        }
    }

    pub fn key(&self) -> Option<&Value> {
        match self {
            StreamRecord::Record(_) => None,
            StreamRecord::KeyedRecord(kr) => Some(kr.key()),
        }
    }

    pub fn with_new_value(&self, value: Value) -> Self {
        match self {
            StreamRecord::Record(r) => StreamRecord::Record(r.with_new_value(value)),
            StreamRecord::KeyedRecord(kr) => StreamRecord::KeyedRecord(kr.with_new_value(value)),
        }
    }

    pub fn new_record(value: Value) -> Self {
        StreamRecord::Record(Record::new(value))
    }

    pub fn new_keyed_record(key: Value, value: Value) -> Self {
        StreamRecord::KeyedRecord(KeyedRecord::new(key, value))
    }
}