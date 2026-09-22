use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Result;
use futures::future::try_join_all;

use crate::common::KeyGroupRange;
use crate::runtime::operators::window::model::{
    Cursor, PartitionKey, WindowTrigger, WindowTriggerKind,
};

use super::schema::{align_down, kg_shard, TRIGGER_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;

fn shards(range: KeyGroupRange, max_parallelism: usize) -> Vec<i32> {
    let mut out = BTreeSet::new();
    for kg in range.start..range.end {
        out.insert(kg_shard(kg, max_parallelism));
    }
    out.into_iter().collect()
}

fn partitions(
    after: Option<Cursor>,
    through: Cursor,
    range: KeyGroupRange,
    max_parallelism: usize,
) -> Vec<(i64, i32)> {
    let shards = shards(range, max_parallelism);
    if shards.is_empty() {
        return Vec::new();
    }
    // Do not walk from unix 0 or to i64::MAX. First tick uses `through`'s
    // bucket; close (through == MAX) stays on the frontier bucket.
    let start = match after {
        Some(c) => align_down(c.ts, TRIGGER_BUCKET_MS),
        None if through.ts == i64::MAX => return Vec::new(),
        None => align_down(through.ts, TRIGGER_BUCKET_MS),
    };
    let end = if through.ts == i64::MAX {
        start
    } else {
        align_down(through.ts, TRIGGER_BUCKET_MS)
    };
    if start > end {
        return Vec::new();
    }
    let mut out = Vec::new();
    for bucket in super::schema::time_buckets(start, end, TRIGGER_BUCKET_MS) {
        for &shard in &shards {
            out.push((bucket, shard));
        }
    }
    out
}

fn min_seek(after: Option<Cursor>) -> (i64, i64) {
    match after {
        None => (i64::MIN, i64::MIN),
        Some(c) if c.seq_no == u64::MAX => (c.ts, i64::MAX),
        Some(c) => (c.ts, c.seq_no as i64),
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
) -> Result<Vec<WindowTrigger>> {
    let parts = partitions(
        after,
        through,
        client.scope.key_group_range,
        client.scope.max_parallelism,
    );
    if parts.is_empty() {
        return Ok(Vec::new());
    }
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let ns = client.scope.namespace.bytes.clone();
    let seek = min_seek(after);
    let futs = parts.into_iter().map(|(bucket, shard)| {
        let session = Arc::clone(&session);
        let select = prepared.select_triggers.clone();
        let ns = ns.clone();
        async move {
            let result = session
                .execute_unpaged(&select, (ns, bucket, shard, seek.0, seek.1, through.ts))
                .await?;
            Ok::<_, anyhow::Error>(result)
        }
    });

    let mut selected = Vec::new();
    for result in try_join_all(futs).await? {
        for row in result
            .into_rows_result()?
            .rows::<(i64, i64, Vec<u8>, i8, i64, i32, i64, i64)>()?
        {
            let (ts, seq, business_key, kind, window_id, key_group, attempt, epoch) = row?;
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
    }
    selected.sort();
    Ok(selected)
}

#[cfg(test)]
mod partition_tests {
    use super::*;

    #[test]
    fn due_bucket_walk_is_clamped() {
        let range = KeyGroupRange::full(1);
        let unix = 1_700_000_000_000i64;
        let unix_bucket = align_down(unix, TRIGGER_BUCKET_MS);

        let first = partitions(None, Cursor::new(unix, 0), range, 1);
        assert_ne!(unix_bucket, 0);
        assert_eq!(
            first,
            vec![(unix_bucket, 0)],
            "after None must not start at bucket 0 for a unix-ms through"
        );

        let close = partitions(
            Some(Cursor::new(5_000, u64::MAX)),
            Cursor::new(i64::MAX, u64::MAX),
            range,
            1,
        );
        assert_eq!(close.len(), 1, "through MAX must stay finite");
        assert_eq!(close, vec![(0, 0)]);

        assert!(
            partitions(None, Cursor::new(i64::MAX, u64::MAX), range, 1).is_empty(),
            "close without a frontier has no trigger bucket"
        );
        assert_eq!(
            partitions(
                Some(Cursor::new(0, u64::MAX)),
                Cursor::new(TRIGGER_BUCKET_MS + 1, 0),
                range,
                1,
            ),
            vec![(0, 0), (TRIGGER_BUCKET_MS, 0)],
        );
    }
}
