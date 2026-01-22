use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::hash::Hash;

use ahash::RandomState;
use datafusion::scalar::ScalarValue;

use super::format::scalar_to_string;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TopKOrder {
    KeyDesc,
    MetricDesc,
}

pub(crate) trait TopKKey: Eq + Hash + Clone {
    fn cmp_key(&self, other: &Self) -> Ordering;
}

impl TopKKey for ScalarValue {
    fn cmp_key(&self, other: &Self) -> Ordering {
        cmp_scalar(self, other)
    }
}

#[derive(Debug, Clone)]
struct TopKEntry<K: TopKKey> {
    key: K,
    metric: ScalarValue,
    order: TopKOrder,
}

impl<K: TopKKey> PartialEq for TopKEntry<K> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.metric == other.metric && self.order == other.order
    }
}

impl<K: TopKKey> Eq for TopKEntry<K> {}

impl<K: TopKKey> PartialOrd for TopKEntry<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: TopKKey> Ord for TopKEntry<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.order {
            TopKOrder::KeyDesc => {
                let key_cmp = self.key.cmp_key(&other.key);
                if key_cmp != Ordering::Equal {
                    return key_cmp;
                }
                cmp_scalar(&self.metric, &other.metric)
            }
            TopKOrder::MetricDesc => {
                let metric_cmp = cmp_scalar(&self.metric, &other.metric);
                if metric_cmp != Ordering::Equal {
                    return metric_cmp;
                }
                self.key.cmp_key(&other.key)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct TopKMap<K: TopKKey> {
    order: TopKOrder,
    map: HashMap<K, ScalarValue, RandomState>,
    heap: BinaryHeap<TopKEntry<K>>,
}

impl<K: TopKKey> TopKMap<K> {
    pub(crate) fn new(order: TopKOrder) -> Self {
        Self {
            order,
            map: HashMap::with_hasher(RandomState::with_seeds(0, 0, 0, 0)),
            heap: BinaryHeap::new(),
        }
    }

    pub(crate) fn update_metric(&mut self, key: K, metric: ScalarValue) {
        if matches!(metric, ScalarValue::Null) {
            self.map.remove(&key);
            return;
        }
        self.map.insert(key.clone(), metric.clone());
        self.heap.push(TopKEntry {
            key,
            metric,
            order: self.order,
        });
    }

    pub(crate) fn remove(&mut self, key: &K) {
        self.map.remove(key);
    }

    pub(crate) fn top_n(&mut self, n: usize) -> Vec<(K, ScalarValue)> {
        if n == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(n);
        let mut kept: Vec<TopKEntry<K>> = Vec::with_capacity(n);
        while out.len() < n {
            let entry = match self.heap.pop() {
                Some(entry) => entry,
                None => break,
            };
            let current = self.map.get(&entry.key);
            if current == Some(&entry.metric) {
                out.push((entry.key.clone(), entry.metric.clone()));
                kept.push(entry);
            }
        }
        for entry in kept {
            self.heap.push(entry);
        }
        out
    }
}

fn cmp_scalar(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    match left.partial_cmp(right) {
        Some(ordering) => ordering,
        None => fallback_cmp_scalar(left, right),
    }
}

fn fallback_cmp_scalar(left: &ScalarValue, right: &ScalarValue) -> Ordering {
    let left_str = scalar_to_string(left).unwrap_or_default();
    let right_str = scalar_to_string(right).unwrap_or_default();
    left_str.cmp(&right_str)
}
