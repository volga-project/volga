//! Shared answer strategies (rebuild from scratch / slide from durable acc).

mod rebuild;
mod slide;

pub(super) use rebuild::eval_rebuild;
pub(super) use slide::eval_slide_as_of;
