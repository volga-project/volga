use std::collections::BTreeSet;

use anyhow::Result;

use crate::common::KeyGroupRange;
use crate::runtime::operators::window::model::{
    Cursor, PartitionKey, WindowTrigger, WindowTriggerKind,
};
use crate::runtime::operators::window::store::backend::TriggerResume;

use super::schema::{align_down, kg_shard, TRIGGER_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;

/// Last raw clustering row plus the `(bucket, shard)` being scanned.
#[derive(Clone)]
struct Seek {
    bucket: i64,
    shard: i32,
    fire_ts: i64,
    fire_seq: i64,
    business_key: Vec<u8>,
    kind: i8,
    window_id: i64,
    attempt: i64,
    epoch: i64,
}

fn shards(range: KeyGroupRange, max_parallelism: usize) -> Vec<i32> {
    let mut out = BTreeSet::new();
    for kg in range.start..range.end {
        out.insert(kg_shard(kg, max_parallelism));
    }
    out.into_iter().collect()
}

fn first_partition(
    after: Option<Cursor>,
    through: Cursor,
    range: KeyGroupRange,
    max_parallelism: usize,
) -> Option<(i64, i32)> {
    let shards = shards(range, max_parallelism);
    let shard = *shards.first()?;
    let start = align_down(after.map(|c| c.ts).unwrap_or(0), TRIGGER_BUCKET_MS);
    let end = align_down(through.ts, TRIGGER_BUCKET_MS);
    (start <= end).then_some((start, shard))
}

fn next_partition(
    bucket: i64,
    shard: i32,
    through: Cursor,
    range: KeyGroupRange,
    max_parallelism: usize,
) -> Option<(i64, i32)> {
    let shards = shards(range, max_parallelism);
    let end = align_down(through.ts, TRIGGER_BUCKET_MS);
    if let Some(i) = shards.iter().position(|&s| s == shard) {
        if let Some(next) = shards.get(i + 1) {
            return Some((bucket, *next));
        }
    }
    let next_bucket = bucket + TRIGGER_BUCKET_MS;
    let first = *shards.first()?;
    (next_bucket <= end).then_some((next_bucket, first))
}

/// Clustering just before the first visible row at `after`.
fn min_seek(bucket: i64, shard: i32, after: Option<Cursor>) -> Seek {
    match after {
        None => Seek {
            bucket,
            shard,
            fire_ts: i64::MIN,
            fire_seq: i64::MIN,
            business_key: Vec::new(),
            kind: i8::MIN,
            window_id: i64::MIN,
            attempt: i64::MIN,
            epoch: i64::MIN,
        },
        Some(c) if c.seq_no == u64::MAX => Seek {
            bucket,
            shard,
            fire_ts: c.ts,
            fire_seq: i64::MAX,
            business_key: Vec::new(),
            kind: i8::MAX,
            window_id: i64::MAX,
            attempt: i64::MAX,
            epoch: i64::MAX,
        },
        Some(c) => Seek {
            bucket,
            shard,
            fire_ts: c.ts,
            fire_seq: c.seq_no as i64,
            business_key: Vec::new(),
            kind: i8::MIN,
            window_id: i64::MIN,
            attempt: i64::MIN,
            epoch: i64::MIN,
        },
    }
}

fn visible_trigger(
    client: &ScyllaWindowStoreClient,
    after: Option<Cursor>,
    through: Cursor,
    ts: i64,
    seq: i64,
    business_key: Vec<u8>,
    kind: i8,
    window_id: i64,
    key_group: i32,
    attempt: i64,
    epoch: i64,
) -> Option<WindowTrigger> {
    if !client.scope.key_group_range.contains(key_group as usize) {
        return None;
    }
    if !client.overlay_visible(key_group, attempt, epoch) {
        return None;
    }
    let fire_at = Cursor::new(ts, seq as u64);
    if after.map_or(false, |a| fire_at <= a) || fire_at > through {
        return None;
    }
    let kind = if kind == 0 {
        WindowTriggerKind::RowEmit
    } else {
        WindowTriggerKind::WindowEnd {
            window_id: window_id as usize,
        }
    };
    Some(WindowTrigger {
        fire_at,
        partition: PartitionKey {
            namespace: client.scope.namespace.bytes.clone(),
            business_key,
        },
        kind,
    })
}

pub(super) async fn load_triggers(
    client: &ScyllaWindowStoreClient,
    after: Option<Cursor>,
    through: Cursor,
    resume: Option<&TriggerResume>,
    limit: usize,
) -> Result<(Vec<WindowTrigger>, Option<TriggerResume>)> {
    client.inner.prepared().await?;
    let limit = limit.max(1);
    let range = client.scope.key_group_range;
    let max_p = client.scope.max_parallelism;
    let Some(start) = resume
        .and_then(TriggerResume::downcast::<Seek>)
        .map(|seek| seek.clone())
        .or_else(|| {
            first_partition(after, through, range, max_p)
                .map(|(bucket, shard)| min_seek(bucket, shard, after))
        })
    else {
        return Ok((Vec::new(), None));
    };

    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let result = session
        .execute_unpaged(
            &prepared.select_triggers,
            (
                client.scope.namespace.bytes.clone(),
                start.bucket,
                start.shard,
                start.fire_ts,
                start.fire_seq,
                start.business_key.clone(),
                start.kind,
                start.window_id,
                start.attempt,
                start.epoch,
                through.ts,
                limit as i32,
            ),
        )
        .await?;

    let mut selected = Vec::new();
    let mut raw_len = 0usize;
    let mut last_raw = None;
    for row in result
        .into_rows_result()?
        .rows::<(i64, i64, Vec<u8>, i8, i64, i32, i64, i64)>()?
    {
        let (ts, seq, business_key, kind, window_id, key_group, attempt, epoch) = row?;
        raw_len += 1;
        last_raw = Some(Seek {
            bucket: start.bucket,
            shard: start.shard,
            fire_ts: ts,
            fire_seq: seq,
            business_key: business_key.clone(),
            kind,
            window_id,
            attempt,
            epoch,
        });
        if let Some(trigger) = visible_trigger(
            client,
            after,
            through,
            ts,
            seq,
            business_key,
            kind,
            window_id,
            key_group,
            attempt,
            epoch,
        ) {
            selected.push(trigger);
        }
    }

    let next = if raw_len == limit {
        last_raw.map(TriggerResume::opaque)
    } else {
        next_partition(start.bucket, start.shard, through, range, max_p)
            .map(|(bucket, shard)| TriggerResume::opaque(min_seek(bucket, shard, after)))
    };
    Ok((selected, next))
}

#[cfg(test)]
mod partition_tests {
    use super::*;

    #[test]
    fn next_partition_walks_time_buckets() {
        let range = KeyGroupRange::full(1);
        let through = Cursor::new(TRIGGER_BUCKET_MS + 1, 0);
        let first = first_partition(None, through, range, 1).unwrap();
        assert_eq!(first, (0, 0));
        let second = next_partition(first.0, first.1, through, range, 1).unwrap();
        assert_eq!(second, (TRIGGER_BUCKET_MS, 0));
        assert!(next_partition(second.0, second.1, through, range, 1).is_none());
    }
}
