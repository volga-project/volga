//! Docker contract for the Scylla window store.
//!
//! Ignored by default (`src/tests/README.md`). `VOLGA_SCYLLA_CONTACT` reuses a
//! cluster; otherwise tests share one `scylladb/scylla:5.4` container. Each
//! `connect` allocates a fresh keyspace so a rerun does not see prior epochs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::api::spec::state::ScyllaConfig;
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun, TimeGranularity,
    WindowTiles, WindowTrigger, WindowTriggerKind,
};
use crate::runtime::operators::window::state::WindowOperatorState;
use crate::runtime::operators::window::store::backend::{
    ScyllaWindowStore, Version, WindowBackendSnapshot, WindowOperatorStore, WindowStoreTaskScope,
};
use crate::runtime::operators::window::store::data::cursors_from_batch;
use crate::runtime::state::OperatorStore;
use crate::test_utils::window_aggs as test_utils;
use arrow::array::RecordBatch;
use testcontainers::core::WaitFor;
use testcontainers::{clients, GenericImage, RunnableImage};

fn partition(ns: &StateNamespace) -> PartitionKey {
    PartitionKey {
        namespace: ns.bytes.clone(),
        business_key: 0u64.to_le_bytes().to_vec(),
    }
}

fn batch(cursors: &[(i64, u64)]) -> RecordBatch {
    let rows = cursors
        .iter()
        .map(|&(ts, seq_no)| (ts, ts as f64, "key", seq_no))
        .collect::<Vec<_>>();
    test_utils::batch(&rows)
}

fn raw_run(from: (i64, u64), to: (i64, u64)) -> RawRun {
    RawRun {
        from: Cursor::new(from.0, from.1),
        to: Cursor::new(to.0, to.1),
    }
}

fn raw_cursors(batches: &[RecordBatch]) -> Vec<Cursor> {
    batches
        .iter()
        .flat_map(|b| cursors_from_batch(b, 0).unwrap())
        .collect()
}

fn tiles(entries: &[(TimeGranularity, i64, usize)]) -> TileMap {
    entries
        .iter()
        .map(|&(granularity, start_ts, window_id)| {
            let mut value = WindowTiles::default();
            value.windows.insert(window_id, Default::default());
            ((granularity, start_ts), value)
        })
        .collect()
}

fn tile_keys(tiles: &TileMap) -> Vec<(TimeGranularity, i64)> {
    tiles.keys().copied().collect()
}

fn scope(ns: &StateNamespace, attempt: u64) -> WindowStoreTaskScope {
    let mut scope = WindowStoreTaskScope::for_test(ns.clone());
    scope.attempt = attempt;
    scope
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("ephemeral port").port()
}

fn contact() -> String {
    if let Ok(cp) = std::env::var("VOLGA_SCYLLA_CONTACT") {
        return cp;
    }
    static SHARED: OnceLock<String> = OnceLock::new();
    SHARED
        .get_or_init(|| {
            let docker = Box::leak(Box::new(clients::Cli::default()));
            let port = free_port();
            // One shard stays under the default fs.aio-max-nr (65536). Seastar
            // asks for ~50k slots per shard and refuses to boot past that.
            // Supervisord forwards Scylla's stdout onto the container's stderr.
            // The driver dials broadcast_rpc_address:native_transport_port, so
            // both must be the host port, not the container IP.
            let image = GenericImage::new("scylladb/scylla", "5.4").with_wait_for(
                WaitFor::message_on_stderr("Starting listening for CQL clients"),
            );
            let container = docker.run(
                RunnableImage::from((
                    image,
                    vec![
                        "--smp".to_string(),
                        "1".to_string(),
                        "--memory".to_string(),
                        "1G".to_string(),
                        "--overprovisioned".to_string(),
                        "1".to_string(),
                        "--broadcast-rpc-address".to_string(),
                        "127.0.0.1".to_string(),
                        "--native-transport-port".to_string(),
                        port.to_string(),
                    ],
                ))
                .with_mapped_port((port, port)),
            );
            std::mem::forget(container);
            format!("127.0.0.1:{port}")
        })
        .clone()
}

fn unique_keyspace(prefix: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

async fn connect(prefix: &str) -> ScyllaWindowStore {
    ScyllaWindowStore::connect(ScyllaConfig {
        contact_points: vec![contact()],
        keyspace: unique_keyspace(prefix),
        datacenter: None,
    })
    .await
    .expect("scylla connect via StateSessionHandle")
}

#[tokio::test]
#[ignore]
async fn window_scylla_store_commit_roundtrip() {
    let store = connect("volga_roundtrip").await;
    let ns = StateNamespace::new(b"op");
    let client = store.client(scope(&ns, 1));
    let partition = partition(&ns);
    let missing = PartitionKey {
        namespace: ns.bytes.clone(),
        business_key: 1u64.to_le_bytes().to_vec(),
    };
    let meta = KeyState {
        next_seq: 6,
        ..Default::default()
    };
    let stored_tiles = tiles(&[
        (TimeGranularity::Seconds(1), 0, 0),
        (TimeGranularity::Seconds(1), 1_000, 1),
        (TimeGranularity::Seconds(1), 2_000, 2),
        (TimeGranularity::Seconds(1), 3_000, 3),
        (TimeGranularity::Minutes(1), 1_000, 4),
    ]);
    let trigger = WindowTrigger {
        fire_at: Cursor::new(1_000, 1),
        partition: partition.clone(),
        kind: WindowTriggerKind::RowEmit,
    };
    client
        .commit_events(
            &partition,
            0,
            &batch(&[(30, 3), (10, 1), (20, 2), (20, 1), (40, 4), (50, 5)]),
            &stored_tiles,
            &meta,
            &[trigger.clone()],
        )
        .await
        .unwrap();

    assert_eq!(client.load_key_state(&partition).await.unwrap(), meta);
    assert_eq!(
        client.load_key_state(&missing).await.unwrap(),
        KeyState::default()
    );
    assert!(client.load_raw(&partition, &[]).await.unwrap().is_empty());
    assert!(client
        .load_raw(&missing, &[raw_run((0, 0), (100, 0))])
        .await
        .unwrap()
        .is_empty());
    assert!(client
        .load_triggers(None, Cursor::new(500, u64::MAX))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        raw_cursors(
            &client
                .load_raw(
                    &partition,
                    &[raw_run((10, 1), (30, 3)), raw_run((20, 1), (40, 4))],
                )
                .await
                .unwrap()
        ),
        vec![
            Cursor::new(10, 1),
            Cursor::new(20, 1),
            Cursor::new(20, 2),
            Cursor::new(30, 3),
        ]
    );
    assert_eq!(
        tile_keys(
            &client
                .load_tiles(
                    &partition,
                    &[TileRun {
                        granularity: TimeGranularity::Seconds(1),
                        start_ts: 1_000,
                        end_ts_exclusive: 3_000,
                    }],
                )
                .await
                .unwrap()
        ),
        vec![
            (TimeGranularity::Seconds(1), 1_000),
            (TimeGranularity::Seconds(1), 2_000),
        ]
    );
    assert_eq!(
        client
            .load_triggers(None, Cursor::new(2_000, u64::MAX))
            .await
            .unwrap(),
        vec![trigger]
    );
}

#[tokio::test]
#[ignore]
async fn window_scylla_store_reads_raw_across_minutes() {
    let store = connect("volga_raw_minutes").await;
    let ns = StateNamespace::new(b"op");
    let client = store.client(scope(&ns, 1));
    let partition = partition(&ns);
    client
        .commit_events(
            &partition,
            0,
            &batch(&[(10_000, 0), (70_000, 1)]),
            &TileMap::default(),
            &KeyState {
                next_seq: 2,
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();

    assert_eq!(
        raw_cursors(
            &client
                .load_raw(&partition, &[raw_run((0, 0), (80_000, 0))])
                .await
                .unwrap()
        ),
        vec![Cursor::new(10_000, 0), Cursor::new(70_000, 1)]
    );
}

#[tokio::test]
#[ignore]
async fn window_scylla_store_overlay_hides_other_attempt() {
    let store = connect("volga_overlay").await;
    let ns = StateNamespace::new(b"op");
    let writer = store.client(scope(&ns, 1));
    let other = store.client(scope(&ns, 2));
    let partition = partition(&ns);
    let trigger = WindowTrigger {
        fire_at: Cursor::new(1_000, 1),
        partition: partition.clone(),
        kind: WindowTriggerKind::RowEmit,
    };
    writer
        .commit_events(
            &partition,
            0,
            &batch(&[(1_000, 1)]),
            &tiles(&[(TimeGranularity::Seconds(1), 1_000, 1)]),
            &KeyState {
                next_seq: 2,
                ..Default::default()
            },
            &[trigger.clone()],
        )
        .await
        .unwrap();

    assert_eq!(writer.load_key_state(&partition).await.unwrap().next_seq, 2);
    assert_eq!(
        writer
            .load_triggers(None, Cursor::new(2_000, u64::MAX))
            .await
            .unwrap(),
        vec![trigger]
    );
    assert_eq!(
        other.load_key_state(&partition).await.unwrap(),
        KeyState::default()
    );
    assert!(other
        .load_raw(&partition, &[raw_run((0, 0), (2_000, 0))])
        .await
        .unwrap()
        .is_empty());
    assert!(other
        .load_tiles(
            &partition,
            &[TileRun {
                granularity: TimeGranularity::Seconds(1),
                start_ts: 0,
                end_ts_exclusive: 2_000,
            }],
        )
        .await
        .unwrap()
        .is_empty());
    assert!(other
        .load_triggers(None, Cursor::new(2_000, u64::MAX))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[ignore]
async fn window_scylla_store_restore_sees_checkpointed_prefix() {
    let store = connect("volga_restore").await;
    let ns = StateNamespace::new(b"op");
    let writer = store.client(scope(&ns, 1));
    let partition = partition(&ns);
    writer
        .commit_events(
            &partition,
            0,
            &batch(&[(1_000, 1)]),
            &tiles(&[(TimeGranularity::Seconds(1), 1_000, 1)]),
            &KeyState {
                next_seq: 2,
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    let snap = writer.checkpoint().await.unwrap();
    match &snap {
        WindowBackendSnapshot::Versioned {
            attempt,
            range: _,
            cuts,
        } => {
            assert_eq!(*attempt, 1);
            assert_eq!(cuts.len(), 1);
            assert!(cuts[0].allows(Version {
                attempt: 1,
                epoch: 0
            }));
        }
        WindowBackendSnapshot::InMemory { .. } => panic!("expected Versioned snapshot"),
    }

    let same_attempt = writer.restore(&snap).await.unwrap_err();
    assert!(
        same_attempt.to_string().contains("must be newer"),
        "{same_attempt}"
    );

    let successor = store.client(scope(&ns, 2));
    successor.restore(&snap).await.unwrap();
    assert_eq!(
        successor.load_key_state(&partition).await.unwrap().next_seq,
        2
    );

    successor
        .commit_events(
            &partition,
            0,
            &batch(&[(2_000, 2)]),
            &TileMap::new(),
            &KeyState {
                next_seq: 3,
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        successor.load_key_state(&partition).await.unwrap().next_seq,
        3
    );
    assert_eq!(writer.load_key_state(&partition).await.unwrap().next_seq, 2);

    let other = store.client(scope(&ns, 3));
    assert_eq!(
        other.load_key_state(&partition).await.unwrap(),
        KeyState::default()
    );
}

fn request_scope(ns: &StateNamespace, attempt: u64) -> WindowStoreTaskScope {
    let mut scope = scope(ns, attempt);
    scope.request_mode = true;
    scope
}

async fn meta_attempt_and_checkpoint(
    store: &ScyllaWindowStore,
    ns: &[u8],
    kg: i32,
) -> Option<(i64, i64)> {
    let cql = format!(
        "SELECT cur_attempt, checkpoint_id FROM {}.window_kg_meta WHERE namespace = ? AND key_group = ?",
        store.keyspace()
    );
    let result = store
        .session()
        .query_unpaged(cql, (ns.to_vec(), kg))
        .await
        .expect("select window_kg_meta");
    result
        .into_rows_result()
        .expect("meta rows")
        .maybe_first_row::<(Option<i64>, Option<i64>)>()
        .expect("decode meta")
        .map(|(attempt, checkpoint_id)| (attempt.unwrap_or(0), checkpoint_id.unwrap_or(0)))
}

#[tokio::test]
#[ignore]
async fn window_scylla_store_request_publish_heal_and_take() {
    let store = connect("volga_reqmeta").await;
    let ns = StateNamespace::new(b"op");
    let partition = partition(&ns);

    let writer = store.client(request_scope(&ns, 1));
    writer
        .commit_events(
            &partition,
            0,
            &batch(&[(1_000, 1)]),
            &TileMap::new(),
            &KeyState {
                next_seq: 2,
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    let snap = writer.checkpoint().await.unwrap();

    let healed = store.client(request_scope(&ns, 2));
    healed.restore(&snap).await.unwrap();
    healed
        .prepare_attempt(&snap, Some(1_000), Some(0), Some(4))
        .await
        .unwrap();
    assert_eq!(
        meta_attempt_and_checkpoint(&store, ns.bytes.as_slice(), 0).await,
        Some((2, 4)),
        "heal publishes the restored cut when the meta row is absent"
    );

    healed
        .commit_events(
            &partition,
            0,
            &batch(&[(2_000, 2)]),
            &TileMap::new(),
            &KeyState {
                next_seq: 3,
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    let snap2 = healed.checkpoint().await.unwrap();
    healed
        .on_checkpoint_complete(9, &snap2, Some(2_000), Some(0))
        .await
        .unwrap();
    assert_eq!(
        meta_attempt_and_checkpoint(&store, ns.bytes.as_slice(), 0).await,
        Some((2, 9)),
        "complete publishes the newer cut"
    );

    let taken = store.client(request_scope(&ns, 3));
    taken.restore(&snap2).await.unwrap();
    taken
        .prepare_attempt(&snap2, Some(2_000), Some(0), Some(9))
        .await
        .unwrap();
    assert_eq!(
        meta_attempt_and_checkpoint(&store, ns.bytes.as_slice(), 0).await,
        Some((3, 9)),
        "take_attempt moves cur_attempt and leaves the published cut"
    );
}

async fn query<R>(
    store: &ScyllaWindowStore,
    cql: &str,
    values: impl scylla::serialize::row::SerializeRow,
) -> Vec<R>
where
    R: for<'frame, 'meta> scylla::deserialize::row::DeserializeRow<'frame, 'meta>,
{
    let result = store
        .session()
        .query_unpaged(cql, values)
        .await
        .expect("query");
    result
        .into_rows_result()
        .expect("rows")
        .rows::<R>()
        .expect("decode")
        .map(|row| row.expect("row"))
        .collect()
}

fn sorted<T: Ord>(mut rows: Vec<T>) -> Vec<T> {
    rows.sort();
    rows
}

/// One maintain tick against Scylla: drop an expired minute, range-delete a
/// tile that ends at the floor, trim every cell to the three version slots,
/// and delete triggers at or below the committed watermark.
#[tokio::test]
#[ignore]
async fn window_scylla_store_maintain_gc() {
    let store = connect("volga_maintain").await;
    let ns = StateNamespace::new(b"op");
    let partition = partition(&ns);
    let writer = store.client(scope(&ns, 1));
    let live = (120_000_i64, 1_u64);
    let kept = vec![(1_i64, 2_i64), (1, 4), (1, 10)];

    for epoch in 0_u64..=10 {
        let mut cursors = vec![live];
        let mut tile_entries = vec![(TimeGranularity::Seconds(1), 119_001, 1)];
        let mut triggers = Vec::new();
        if epoch == 0 {
            cursors.push((1_000, 1));
            tile_entries.push((TimeGranularity::Seconds(1), 1_000, 1));
            triggers.push(WindowTrigger {
                fire_at: Cursor::new(120_000, 1),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            });
        }
        if epoch == 10 {
            triggers.push(WindowTrigger {
                fire_at: Cursor::new(180_000, 1),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            });
        }
        writer
            .commit_events(
                &partition,
                0,
                &batch(&cursors),
                &tiles(&tile_entries),
                &KeyState {
                    next_seq: epoch + 2,
                    ..Default::default()
                },
                &triggers,
            )
            .await
            .unwrap();
        if epoch == 2 || epoch == 4 {
            let snap = writer.checkpoint().await.unwrap();
            writer
                .on_checkpoint_complete(epoch, &snap, Some(120_000), Some(120_000))
                .await
                .unwrap();
        }
    }

    let task_state = WindowOperatorState::for_test(
        Arc::new(writer.clone()) as Arc<dyn WindowOperatorStore>,
        ns.clone(),
        Arc::from("maintain-task"),
        0,
        Arc::new(std::collections::BTreeMap::new()),
        0,
        0,
    )
    .with_tile_granularities(vec![TimeGranularity::Seconds(1).to_millis()]);
    task_state.seed_committed_watermark(120_000);
    writer.maintain(&ns, &task_state).await.unwrap();

    let key = partition.business_key.clone();
    let ns_bytes = ns.bytes.clone();
    let ks = store.keyspace();
    assert_eq!(
        sorted(
            query::<(i64,)>(
                &store,
                &format!(
                    "SELECT bucket_start FROM {ks}.window_kg_buckets WHERE namespace = ? AND key_group = ?"
                ),
                (ns_bytes.clone(), 0_i32),
            )
            .await,
        ),
        vec![(120_000,)],
        "expired minute leaves window_kg_buckets"
    );
    assert!(
        query::<(i64, i64)>(
            &store,
            &format!(
                "SELECT attempt, epoch FROM {ks}.window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ?"
            ),
            (ns_bytes.clone(), 0_i32, key.clone(), 0_i64),
        )
        .await
        .is_empty(),
        "raw partition below the floor is deleted"
    );
    assert_eq!(
        sorted(
            query::<(i64, i64)>(
                &store,
                &format!(
                    "SELECT attempt, epoch FROM {ks}.window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ?"
                ),
                (ns_bytes.clone(), 0_i32, key.clone(), 120_000_i64),
            )
            .await,
        ),
        kept,
        "live raw cell keeps this attempt, the cut, and the previous cut"
    );
    assert_eq!(
        sorted(
            query::<(i64, i64, i64)>(
                &store,
                &format!(
                    "SELECT tile_start, attempt, epoch FROM {ks}.window_tiles WHERE namespace = ? AND key_group = ? AND business_key = ? AND granularity_ms = ?"
                ),
                (ns_bytes.clone(), 0_i32, key.clone(), 1_000_i64),
            )
            .await,
        ),
        vec![
            (119_001, 1, 2),
            (119_001, 1, 4),
            (119_001, 1, 10),
        ],
        "tile ending at the floor is range-deleted; the overlap keeps three versions"
    );
    assert_eq!(
        sorted(
            query::<(i64, i64)>(
                &store,
                &format!(
                    "SELECT attempt, epoch FROM {ks}.window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?"
                ),
                (ns_bytes.clone(), 0_i32, key),
            )
            .await,
        ),
        vec![(1, 2), (1, 4), (1, 10)],
        "key state keeps the three version slots"
    );
    assert_eq!(
        query::<(i64,)>(
            &store,
            &format!(
                "SELECT fire_ts FROM {ks}.window_triggers WHERE namespace = ? AND kg_shard = ?"
            ),
            (ns_bytes, 0_i32),
        )
        .await,
        vec![(180_000,)],
        "triggers at or below the committed watermark are deleted"
    );

    assert!(writer
        .load_raw(&partition, &[raw_run((0, 0), (60_000, 0))])
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        raw_cursors(
            &writer
                .load_raw(&partition, &[raw_run((120_000, 0), (180_000, 0))])
                .await
                .unwrap()
        ),
        vec![Cursor::new(120_000, 1)]
    );
    assert_eq!(
        tile_keys(
            &writer
                .load_tiles(
                    &partition,
                    &[TileRun {
                        granularity: TimeGranularity::Seconds(1),
                        start_ts: 0,
                        end_ts_exclusive: 200_000,
                    }],
                )
                .await
                .unwrap()
        ),
        vec![(TimeGranularity::Seconds(1), 119_001)]
    );
    assert_eq!(
        writer
            .load_triggers(None, Cursor::new(200_000, u64::MAX))
            .await
            .unwrap(),
        vec![WindowTrigger {
            fire_at: Cursor::new(180_000, 1),
            partition: partition.clone(),
            kind: WindowTriggerKind::RowEmit,
        }]
    );
}
