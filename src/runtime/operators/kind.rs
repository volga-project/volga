//! Operator kind (logical type) vs role (topology).
//!
//! - [`OperatorKind`]: discriminant of [`super::operator::OperatorConfig`] (Window, Join, …).
//! - [`super::operator::OperatorType`]: execution role (Source / Sink / Processor / …).

use crate::runtime::functions::map::MapFunction;
use crate::runtime::operators::operator::{OperatorConfig, OperatorType};

/// Logical operator kind — promote of planner `NodeType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorKind {
    Source,
    Sink,
    Projection,
    Filter,
    KeyBy,
    Aggregate,
    Join,
    Window,
    WindowRequest,
    Chained,
    Map,
}

impl OperatorKind {
    /// Topology role for this kind. Prefer [`OperatorConfig::role`] for chained configs.
    pub fn role(self) -> OperatorType {
        match self {
            Self::Source => OperatorType::Source,
            Self::Sink => OperatorType::Sink,
            Self::Chained => OperatorType::Processor,
            _ => OperatorType::Processor,
        }
    }

    /// Kinds that own a shared [`crate::runtime::state::OperatorStore`] on the worker.
    pub fn owns_operator_store(self) -> bool {
        matches!(self, Self::Window)
    }
}

impl OperatorConfig {
    pub fn kind(&self) -> OperatorKind {
        match self {
            Self::SourceConfig(_) => OperatorKind::Source,
            Self::SinkConfig(_) => OperatorKind::Sink,
            Self::MapConfig(MapFunction::Projection(_)) => OperatorKind::Projection,
            Self::MapConfig(MapFunction::Filter(_)) => OperatorKind::Filter,
            Self::MapConfig(_) => OperatorKind::Map,
            Self::KeyByConfig(_) => OperatorKind::KeyBy,
            Self::AggregateConfig(_) => OperatorKind::Aggregate,
            Self::JoinConfig(_) => OperatorKind::Join,
            Self::WindowConfig(_) => OperatorKind::Window,
            Self::WindowRequestConfig(_) => OperatorKind::WindowRequest,
            Self::ChainedConfig(_) => OperatorKind::Chained,
        }
    }

    /// Execution role (today’s [`OperatorType`]). Chained inspects nested configs.
    pub fn role(&self) -> OperatorType {
        super::operator::get_operator_type_from_config(self)
    }
}
