use serde::{Deserialize, Serialize};

pub type Timestamp = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TimeGranularity {
    Seconds(u32),
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Months(u32),
}

impl TimeGranularity {
    pub fn to_millis(&self) -> i64 {
        match self {
            TimeGranularity::Seconds(s) => *s as i64 * 1000,
            TimeGranularity::Minutes(m) => *m as i64 * 60 * 1000,
            TimeGranularity::Hours(h) => *h as i64 * 60 * 60 * 1000,
            TimeGranularity::Days(d) => *d as i64 * 24 * 60 * 60 * 1000,
            TimeGranularity::Months(m) => *m as i64 * 30 * 24 * 60 * 60 * 1000,
        }
    }

    pub fn is_multiple_of(&self, other: &TimeGranularity) -> bool {
        let self_millis = self.to_millis();
        let other_millis = other.to_millis();
        self_millis > other_millis && self_millis % other_millis == 0
    }

    pub fn start(&self, timestamp: Timestamp) -> Timestamp {
        let duration_millis = self.to_millis();
        (timestamp / duration_millis) * duration_millis
    }

    pub fn next_start(&self, timestamp: Timestamp) -> Timestamp {
        self.start(timestamp) + self.to_millis()
    }

    pub fn prev_start(&self, timestamp: Timestamp) -> Timestamp {
        self.start(timestamp) - self.to_millis()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TileConfig {
    pub granularities: Vec<TimeGranularity>,
}

impl TileConfig {
    pub fn new(mut granularities: Vec<TimeGranularity>) -> Result<Self, String> {
        granularities.sort();
        for i in 1..granularities.len() {
            if !granularities[i].is_multiple_of(&granularities[i - 1]) {
                return Err(format!(
                    "Granularity {:?} must be a multiple of {:?}",
                    granularities[i], granularities[i - 1]
                ));
            }
        }
        Ok(Self { granularities })
    }

    pub fn default_config() -> Self {
        Self::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
            TimeGranularity::Hours(1),
            TimeGranularity::Days(1),
        ])
        .expect("Default config should be valid")
    }

    pub fn min_granularity(&self) -> TimeGranularity {
        *self
            .granularities
            .first()
            .expect("TileConfig must have at least one granularity")
    }
}
