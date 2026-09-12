use std::collections::{BTreeSet, VecDeque};

use anyhow::Result;

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

struct TriggerScan {
    client: ScyllaWindowStoreClient,
    after: Option<Cursor>,
    through: Cursor,
    start_ts: i64,
    parts: Vec<(i64, i32)>,
    part_idx: usize,
    seek: Option<TriggerSeek>,
    leftover: VecDeque<WindowTrigger>,
}

impl TriggerScan {
    async fn new(
        client: ScyllaWindowStoreClient,
        after: Option<Cursor>,
        through: Cursor,
        resume: Option<&TriggerResume>,
    ) -> Result<Self> {
        client.inner.prepared().await?;
        let parts = trigger_partitions(
            client.scope.key_group_range,
            client.scope.max_parallelism,
            after,
            through,
        );
        let (part_idx, seek) = match resume {
            Some(resume) => (resume.part_idx, seek_from_resume(resume)),
            None => (0, None),
        };
        Ok(Self {
            start_ts: after.map(|c| c.ts).unwrap_or(0),
            after,
            through,
            parts,
            part_idx,
            seek,
            leftover: VecDeque::new(),
            client,
        })
    }

    fn resume_token(&self) -> Option<TriggerResume> {
        if self.part_idx >= self.parts.len() && self.leftover.is_empty() {
            return None;
        }
        if let Some(seek) = &self.seek {
            return Some(resume_from_seek(
                &self.client.scope.namespace.bytes,
                self.part_idx,
                seek,
            ));
        }
        Some(TriggerResume {
            last: WindowTrigger {
                fire_at: self.after.unwrap_or(Cursor::new(0, 0)),
                partition: PartitionKey {
                    namespace: self.client.scope.namespace.bytes.clone(),
                    business_key: Vec::new(),
                },
                kind: WindowTriggerKind::RowEmit,
            },
            raw_attempt: Vec::new(),
            raw_epoch: 0,
            part_idx: self.part_idx,
        })
    }

    async fn take_visible(&mut self, limit: usize) -> Result<Vec<WindowTrigger>> {
        let limit = limit.max(1);
        let mut selected = Vec::new();
        while selected.len() < limit {
            if let Some(trigger) = self.leftover.pop_front() {
                selected.push(trigger);
                continue;
            }
            if !self.fetch_limit_page(limit).await? {
                break;
            }
        }
        Ok(selected)
    }

    async fn fetch_limit_page(&mut self, limit: usize) -> Result<bool> {
        let limit = limit as i32;
        loop {
            if self.part_idx >= self.parts.len() {
                return Ok(false);
            }
            let (bucket, shard) = self.parts[self.part_idx];
            let session = self.client.inner.session();
            let prepared = self.client.inner.prepared().await?;
            let ns = self.client.scope.namespace.bytes.clone();
            let result = match &self.seek {
                None => {
                    session
                        .execute_unpaged(
                            &prepared.select_triggers,
                            (ns, bucket, shard, self.start_ts, self.through.ts, limit),
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
                                self.through.ts,
                                limit,
                            ),
                        )
                        .await?
                }
            };
            let rows = result.into_rows_result()?;
            let mut raw_len = 0usize;
            let mut last_raw = None;
            for row in rows.rows::<(i64, i64, Vec<u8>, i8, i64, i32, Vec<u8>, i64)>()? {
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
                if let Some(trigger) = self.visible_trigger(
                    ts,
                    seq,
                    business_key,
                    kind,
                    window_id,
                    key_group,
                    &attempt,
                    epoch,
                ) {
                    self.leftover.push_back(trigger);
                }
            }
            if let Some(seek) = last_raw {
                self.seek = Some(seek);
            }
            let shard_done = raw_len < limit as usize;
            if shard_done {
                self.part_idx += 1;
                self.seek = None;
            }
            if !self.leftover.is_empty() {
                return Ok(true);
            }
            if !shard_done && raw_len == 0 {
                return Ok(false);
            }
        }
    }

    fn visible_trigger(
        &self,
        ts: i64,
        seq: i64,
        business_key: Vec<u8>,
        kind: i8,
        window_id: i64,
        key_group: i32,
        attempt: &[u8],
        epoch: i64,
    ) -> Option<WindowTrigger> {
        if !self.client.scope.key_group_range.contains(key_group as usize) {
            return None;
        }
        if !self.client.overlay_ok(attempt, epoch, None) {
            return None;
        }
        let fire_at = Cursor::new(ts, seq as u64);
        if self.after.map_or(false, |a| fire_at <= a) || fire_at > self.through {
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
                namespace: self.client.scope.namespace.bytes.clone(),
                business_key,
            },
            kind,
        })
    }
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

pub(super) async fn load_triggers(
    client: &ScyllaWindowStoreClient,
    after: Option<Cursor>,
    through: Cursor,
    resume: Option<&TriggerResume>,
    limit: usize,
) -> Result<(Vec<WindowTrigger>, Option<TriggerResume>)> {
    let mut scan = TriggerScan::new(client.clone(), after, through, resume).await?;
    let selected = scan.take_visible(limit).await?;
    if selected.is_empty() {
        return Ok((Vec::new(), None));
    }
    let next = if selected.len() == limit.max(1) {
        scan.resume_token()
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
