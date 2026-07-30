use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::spec::WindowSpec;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperatorTuningSpec {
    Window(WindowSpec),
}

