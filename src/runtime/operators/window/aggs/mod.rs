mod accumulator;
mod registry;

pub(crate) use accumulator::{
    apply_request_args, build_base_accumulator, coerce_value_type, ensure_value_type,
    infer_value_type,
};
pub use accumulator::{merge_accumulator_state, retract_accumulator_state};
pub use registry::{create_window_accumulator, window_supports_tile_slide, AccumulatorType};
pub(crate) use registry::{get_accumulator_type, AggKind};
