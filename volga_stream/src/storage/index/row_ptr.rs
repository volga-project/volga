#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowPtr {
    /// Index into `SortedRangeView::segments()`.
    pub segment: usize,
    pub row: usize,
}

impl RowPtr {
    pub fn new(segment: usize, row: usize) -> Self {
        Self { segment, row }
    }
}

