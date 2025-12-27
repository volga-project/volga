//! Backwards-compatible shim.
//!
//! New code should use `crate::runtime::operators::window::index::SortedBucketView`
//! and `crate::runtime::operators::window::index::sort_buckets`.

pub use crate::runtime::operators::window::index::{sort_buckets, SortedBucketView};
