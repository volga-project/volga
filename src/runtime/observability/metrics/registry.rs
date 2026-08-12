//! In-process metrics registry: cumulative histograms + fanout init.

use metrics::atomics::AtomicU64;
use metrics::{
    Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_exporter_tcp::TcpBuilder;
use metrics_util::layers::FanoutBuilder;
use metrics_util::registry::{Registry, Storage};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::{Once, OnceLock};

use super::names::LATENCY_BUCKET_BOUNDARIES;
use super::names::LATENCY_HISTOGRAM_LEN;

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
pub(super) static METRICS_REGISTRY: OnceLock<Arc<Registry<Key, MetricsRegistryStorage>>> = OnceLock::new();
static INIT_METRICS: Once = Once::new();

pub fn init_metrics() {
    INIT_METRICS.call_once(|| {
        init_metrics_inner().expect("Failed to initialize metrics");
    });
}

pub fn prometheus_handle() -> Option<&'static PrometheusHandle> {
    PROMETHEUS_HANDLE.get()
}

/// Cumulative Prom-shaped latency hist (finite bounds + trailing +Inf).
#[derive(Debug)]
pub(crate) struct CumulativeHistogram {
    buckets: [AtomicU64; LATENCY_HISTOGRAM_LEN],
}

impl CumulativeHistogram {
    const fn new() -> Self {
        const ZERO: AtomicU64 = AtomicU64::new(0);
        Self { buckets: [ZERO; LATENCY_HISTOGRAM_LEN] }
    }

    pub(crate) fn snapshot_buckets(&self) -> Vec<u64> {
        self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).collect()
    }
}

impl HistogramFn for CumulativeHistogram {
    fn record(&self, value: f64) {
        let mut idx = LATENCY_BUCKET_BOUNDARIES.len();
        for (i, &boundary) in LATENCY_BUCKET_BOUNDARIES.iter().enumerate() {
            if value <= boundary {
                idx = i;
                break;
            }
        }
        for bucket in &self.buckets[idx..] {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
    }
}


/// `metrics_util` storage backend for Volga's in-process metrics registry.
#[derive(Debug)]
pub(super) struct MetricsRegistryStorage;

impl Storage<Key> for MetricsRegistryStorage {
    type Counter = Arc<AtomicU64>;
    type Gauge = Arc<AtomicU64>;
    type Histogram = Arc<CumulativeHistogram>;

    fn counter(&self, _: &Key) -> Self::Counter { Arc::new(AtomicU64::new(0)) }
    fn gauge(&self, _: &Key) -> Self::Gauge { Arc::new(AtomicU64::new(0)) }
    fn histogram(&self, _: &Key) -> Self::Histogram { Arc::new(CumulativeHistogram::new()) }
}

struct SnapshotRecorder {
    registry: Arc<Registry<Key, MetricsRegistryStorage>>,
}

impl Recorder for SnapshotRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.registry.get_or_create_counter(key, |c| Counter::from_arc(Arc::clone(c)))
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.registry.get_or_create_gauge(key, |g| Gauge::from_arc(Arc::clone(g)))
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.registry.get_or_create_histogram(key, |h| Histogram::from_arc(Arc::clone(h)))
    }
}

fn init_metrics_inner() -> Result<(), Box<dyn std::error::Error>> {
    assert!(!LATENCY_BUCKET_BOUNDARIES.contains(&f64::MAX), "LATENCY_BUCKET_BOUNDARIES should not contain f64::MAX");

    let prometheus_builder = PrometheusBuilder::new().set_buckets(&LATENCY_BUCKET_BOUNDARIES)?;
    let prometheus_recorder = prometheus_builder.build_recorder();
    let prometheus_handle = prometheus_recorder.handle();

    let registry = Arc::new(Registry::new(MetricsRegistryStorage));
    let snapshot_recorder = SnapshotRecorder { registry: Arc::clone(&registry) };

    let tcp_enabled = std::env::var("VOLGA_ENABLE_TCP_METRICS").ok().as_deref() == Some("1");
    let mut fanout_builder = FanoutBuilder::default()
        .add_recorder(prometheus_recorder)
        .add_recorder(snapshot_recorder);

    if tcp_enabled {
        if let Ok(tcp_recorder) = TcpBuilder::new()
            .listen_address("127.0.0.1:9999".parse::<std::net::SocketAddr>()?)
            .build()
        {
            fanout_builder = fanout_builder.add_recorder(tcp_recorder);
        } else {
            eprintln!("⚠️  VOLGA_ENABLE_TCP_METRICS=1 but failed to bind 127.0.0.1:9999; TCP metrics disabled");
        }
    }

    metrics::set_global_recorder(fanout_builder.build())?;
    PROMETHEUS_HANDLE.set(prometheus_handle).map_err(|_| "Prometheus handle already initialized")?;
    METRICS_REGISTRY.set(registry).map_err(|_| "Metrics registry already initialized")?;

    println!("✅ Volga metrics initialized with Prometheus + snapshot registry");
    if tcp_enabled {
        println!("🔍 TCP metrics streaming to 127.0.0.1:9999");
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use metrics::HistogramFn;

    use super::CumulativeHistogram;
    use crate::runtime::metrics::{
        HistogramMetrics, LATENCY_BUCKET_BOUNDARIES, LATENCY_HISTOGRAM_LEN,
    };

    #[test]
    fn cumulative_histogram_buckets_and_overflow() {
        let hist = CumulativeHistogram::new();
        hist.record(10.0);
        hist.record(10_000.0);
        let buckets = hist.snapshot_buckets();
        assert_eq!(buckets.len(), LATENCY_HISTOGRAM_LEN);
        assert_eq!(*buckets.last().unwrap(), 2);
        assert_eq!(buckets[LATENCY_BUCKET_BOUNDARIES.len() - 1], 1);

        let last_finite = *LATENCY_BUCKET_BOUNDARIES.last().unwrap();
        let mut all_overflow = vec![0u64; LATENCY_HISTOGRAM_LEN];
        *all_overflow.last_mut().unwrap() = 10;
        let stats = HistogramMetrics::new(all_overflow);
        assert_eq!(stats.p50, last_finite);
        assert_eq!(stats.p99, last_finite);
        assert_eq!(stats.avg, last_finite);
    }
}
