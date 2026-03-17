use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::window_tuning::WindowOperatorSpec;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperatorTuningSpec {
    Window(WindowOperatorSpec),
}

