use std::collections::BTreeSet;

use anyhow::Result;
use scylla::response::query_result::QueryResult;

use crate::runtime::operators::window::model::{
    Cursor, PartitionKey, WindowTrigger, WindowTriggerKind,
};
use crate::runtime::operators::window::store::backend::TriggerResume;

use super::schema::{align_down, kg_shard, TRIGGER_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;

/// `(bucket_start, kg_shard)` partitions covering `(after, through]` for this task.
fn trigger_partitions(
    range: crate::common::KeyGroupRange,
    max_parallelism: usize,
    after: Option<Cursor>,
    through: Cursor,
) -> Vec<(i64, i32)> {
    let start_ts = after.map(|c| c.ts).unwrap_or(0);
    let mut bucket = align_down(start_ts, TRIGGER_BUCKET_MS);
    let end_bucket = align_down(through.ts, TRIGGER_BUCKET_MS);
    let mut parts = BTreeSet::new();
    while bucket <= end_bucket {
        for kg in range.start..range.end {
            parts.insert((bucket, kg_shard(kg, max_parallelism)));
        }
        bucket += TRIGGER_BUCKET_MS;
    }
    parts.into_iter().collect()
}

/// Last raw clustering row in the current trigger shard (seek key, not overlay).
#[derive(Clone)]
struct TriggerSeek {
    fire_ts: i64,
    fire_seq: i64,
    business_key: Vec<u8>,
    kind: i8,
    window_id: i64,
    attempt: Vec<u8>,
    epoch: i64,
}

async fn fetch_raw_page(
    client: &ScyllaWindowStoreClient,
    bucket: i64,
    shard: i32,
    start_ts: i64,
    through_ts: i64,
    seek: Option<&TriggerSeek>,
    limit: i32,
) -> Result<QueryResult> {
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let ns = client.scope.namespace.bytes.clone();
    let result = match seek {
        None => {
            session
                .execute_unpaged(
                    &prepared.select_triggers,
                    (ns, bucket, shard, start_ts, through_ts, limit),
                )
                .await?
        }
        Some(seek) => {
            session
                .execute_unpaged(
                    &prepared.select_triggers_after,
                    (
                        ns,
                        bucket,
                        shard,
                        seek.fire_ts,
                        seek.fire_seq,
                        seek.business_key.clone(),
                        seek.kind,
                        seek.window_id,
                        seek.attempt.clone(),
                        seek.epoch,
                        through_ts,
                        limit,
                    ),
                )
                .await?
        }
    };
    Ok(result)
}

async fn visible_trigger(
    client: &ScyllaWindowStoreClient,
    after: Option<Cursor>,
    through: Cursor,
    ts: i64,
    seq: i64,
    business_key: Vec<u8>,
    kind: i8,
    window_id: i64,
    key_group: i32,
    attempt: &[u8],
    epoch: i64,
) -> Result<Option<WindowTrigger>> {
    if !client.scope.key_group_range.contains(key_group as usize) {
        return Ok(None);
    }
    if !client.overlay_visible(attempt, epoch).await {
        return Ok(None);
    }
    let fire_at = Cursor::new(ts, seq as u64);
    if after.map_or(false, |a| fire_at <= a) || fire_at > through {
        return Ok(None);
    }
    let kind = if kind == 0 {
        WindowTriggerKind::RowEmit
    } else {
        WindowTriggerKind::WindowEnd {
            window_id: window_id as usize,
        }
    };
    Ok(Some(WindowTrigger {
        fire_at,
        partition: PartitionKey {
            namespace: client.scope.namespace.bytes.clone(),
            business_key,
        },
        kind,
    }))
}

fn seek_from_resume(resume: &TriggerResume) -> Option<TriggerSeek> {
    if resume.raw_attempt.is_empty() {
        return None;
    }
    let (kind, window_id) = match resume.last.kind {
        WindowTriggerKind::RowEmit => (0i8, 0i64),
        WindowTriggerKind::WindowEnd { window_id } => (1i8, window_id as i64),
    };
    Some(TriggerSeek {
        fire_ts: resume.last.fire_at.ts,
        fire_seq: resume.last.fire_at.seq_no as i64,
        business_key: resume.last.partition.business_key.clone(),
        kind,
        window_id,
        attempt: resume.raw_attempt.clone(),
        epoch: resume.raw_epoch,
    })
}

fn resume_from_seek(ns: &[u8], part_idx: usize, seek: &TriggerSeek) -> TriggerResume {
    let kind = if seek.kind == 0 {
        WindowTriggerKind::RowEmit
    } else {
        WindowTriggerKind::WindowEnd {
            window_id: seek.window_id as usize,
        }
    };
    TriggerResume {
        last: WindowTrigger {
            fire_at: Cursor::new(seek.fire_ts, seek.fire_seq as u64),
            partition: PartitionKey {
                namespace: ns.to_vec(),
                business_key: seek.business_key.clone(),
            },
            kind,
        },
        raw_attempt: seek.attempt.clone(),
        raw_epoch: seek.epoch,
        part_idx,
    }
}

fn resume_at_shard(ns: &[u8], after: Option<Cursor>, part_idx: usize) -> TriggerResume {
    TriggerResume {
        last: WindowTrigger {
            fire_at: after.unwrap_or(Cursor::new(0, 0)),
            partition: PartitionKey {
                namespace: ns.to_vec(),
                business_key: Vec::new(),
            },
            kind: WindowTriggerKind::RowEmit,
        },
        raw_attempt: Vec::new(),
        raw_epoch: 0,
        part_idx,
    }
}

fn resume_token(
    ns: &[u8],
    after: Option<Cursor>,
    part_idx: usize,
    part_count: usize,
    seek: Option<&TriggerSeek>,
) -> Option<TriggerResume> {
    if part_idx >= part_count {
        return None;
    }
    match seek {
        Some(seek) => Some(resume_from_seek(ns, part_idx, seek)),
        None => Some(resume_at_shard(ns, after, part_idx)),
    }
}

pub(super) async fn load_triggers(
    client: &ScyllaWindowStoreClient,
    after: Option<Cursor>,
    through: Cursor,
    resume: Option<&TriggerResume>,
    limit: usize,
) -> Result<(Vec<WindowTrigger>, Option<TriggerResume>)> {
    client.inner.prepared().await?;
    let parts = trigger_partitions(
        client.scope.key_group_range,
        client.scope.max_parallelism,
        after,
        through,
    );
    let (mut part_idx, mut seek) = match resume {
        Some(resume) => (resume.part_idx, seek_from_resume(resume)),
        None => (0, None),
    };
    let start_ts = after.map(|c| c.ts).unwrap_or(0);
    let limit = limit.max(1);
    let mut selected = Vec::new();

    while selected.len() < limit && part_idx < parts.len() {
        let remaining = (limit - selected.len()) as i32;
        let (bucket, shard) = parts[part_idx];
        let result = fetch_raw_page(
            client,
            bucket,
            shard,
            start_ts,
            through.ts,
            seek.as_ref(),
            remaining,
        )
        .await?;
        let mut raw_len = 0usize;
        let mut last_raw = None;
        for row in result
            .into_rows_result()?
            .rows::<(i64, i64, Vec<u8>, i8, i64, i32, Vec<u8>, i64)>()?
        {
            let (ts, seq, business_key, kind, window_id, key_group, attempt, epoch) = row?;
            raw_len += 1;
            last_raw = Some(TriggerSeek {
                fire_ts: ts,
                fire_seq: seq,
                business_key: business_key.clone(),
                kind,
                window_id,
                attempt: attempt.clone(),
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
                &attempt,
                epoch,
            )
            .await?
            {
                selected.push(trigger);
            }
        }
        if let Some(next) = last_raw {
            seek = Some(next);
        }
        if raw_len < remaining as usize {
            part_idx += 1;
            seek = None;
        } else if raw_len == 0 {
            break;
        }
    }

    if selected.is_empty() {
        return Ok((Vec::new(), None));
    }
    let next = if selected.len() == limit {
        resume_token(
            &client.scope.namespace.bytes,
            after,
            part_idx,
            parts.len(),
            seek.as_ref(),
        )
    } else {
        None
    };
    Ok((selected, next))
}

#[cfg(test)]
mod trigger_partition_tests {
    use super::*;
    use crate::common::KeyGroupRange;

    #[test]
    fn trigger_partitions_are_stable_and_deduped() {
        let parts = trigger_partitions(
            KeyGroupRange::full(1),
            1,
            None,
            Cursor::new(TRIGGER_BUCKET_MS + 1, 0),
        );
        assert_eq!(parts, vec![(0, 0), (TRIGGER_BUCKET_MS, 0)]);
    }
}
