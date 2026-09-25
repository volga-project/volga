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

fn cut_allows(snapshot: &WindowBackendSnapshot, attempt: u64, epoch: u64) -> bool {
    match snapshot {
        WindowBackendSnapshot::Versioned { cuts, .. } => cuts
            .iter()
            .any(|cut| cut.allows(Version { attempt, epoch })),
        WindowBackendSnapshot::InMemory { .. } => panic!("expected Versioned snapshot"),
    }
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

/// Maintain against Scylla: drop an expired minute, range-delete tiles that
/// end at or below the floor (including the `tile_keep_from` boundary), trim
/// tile and key-state versions to three versions, and delete triggers at or
/// below the committed watermark. A second tick keeps those rows.
#[tokio::test]
#[ignore]
async fn window_scylla_store_maintain_gc() {
    let store = connect("volga_maintain").await;
    let ns = StateNamespace::new(b"op");
    let partition = partition(&ns);
    let writer = store.client(scope(&ns, 1));
    let live = (120_000_i64, 1_u64);

    for epoch in 0_u64..=10 {
        let mut cursors = vec![live];
        let mut tile_entries = vec![
            (TimeGranularity::Seconds(1), 119_000, 1),
            (TimeGranularity::Seconds(1), 119_001, 1),
        ];
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

    let expired_only = PartitionKey {
        namespace: ns.bytes.clone(),
        business_key: 1u64.to_le_bytes().to_vec(),
    };
    for _ in 0..3 {
        writer
            .commit_events(
                &expired_only,
                0,
                &batch(&[(1_000, 1)]),
                &TileMap::new(),
                &KeyState {
                    next_seq: 1,
                    ..Default::default()
                },
                &[],
            )
            .await
            .unwrap();
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

    let key = partition.business_key.clone();
    let ns_bytes = ns.bytes.clone();
    let ks = store.keyspace();
    for _ in 0..2 {
        writer.maintain(&ns, &task_state).await.unwrap();
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
        (0_i64..=10).map(|epoch| (1, epoch)).collect::<Vec<_>>(),
        "a live minute keeps every cursor version"
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
        "119_000 is below tile_keep_from and is gone; 119_001 keeps three versions"
    );
        assert_eq!(
        sorted(
            query::<(i64, i64)>(
                &store,
                &format!(
                    "SELECT attempt, epoch FROM {ks}.window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?"
                ),
                (ns_bytes.clone(), 0_i32, key.clone()),
            )
            .await,
        ),
        vec![(1, 2), (1, 4), (1, 10)],
        "key state keeps the three versions"
    );
        assert_eq!(
        sorted(
            query::<(i64, i64)>(
                &store,
                &format!(
                    "SELECT attempt, epoch FROM {ks}.window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?"
                ),
                (ns_bytes.clone(), 0_i32, expired_only.business_key.clone()),
            )
            .await,
        ),
        vec![(1, 13)],
        "a key whose minutes all expired still drops versions outside the three versions"
    );
        assert_eq!(
            query::<(i64,)>(
                &store,
                &format!(
                    "SELECT fire_ts FROM {ks}.window_triggers WHERE namespace = ? AND kg_shard = ?"
                ),
                (ns_bytes.clone(), 0_i32),
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
            }],
        );
    }
}

async fn stored_versions(
    store: &ScyllaWindowStore,
    ns: &[u8],
    key: &[u8],
) -> (Vec<(i64, i64)>, Vec<(i64, i64, i64)>) {
    let ks = store.keyspace();
    let key_states = query::<(i64, i64)>(
        store,
        &format!(
            "SELECT attempt, epoch FROM {ks}.window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?"
        ),
        (ns.to_vec(), 0_i32, key.to_vec()),
    )
    .await;
    let raw = query::<(i64, i64, i64)>(
        store,
        &format!(
            "SELECT event_ts, attempt, epoch FROM {ks}.window_raw WHERE namespace = ? AND key_group = ? AND business_key = ? AND bucket_start = ?"
        ),
        (ns.to_vec(), 0_i32, key.to_vec(), 0_i64),
    )
    .await;
    (sorted(key_states), sorted(raw))
}

/// After restore, attempt 2 commits its own row and attempt 1 commits a later
/// epoch. One read keeps the new row and the checkpointed prefix, and drops
/// the zombie. Both writes stay stored.
#[tokio::test]
#[ignore]
async fn window_scylla_store_restored_reader_hides_zombie_write() {
    let store = connect("volga_zombie").await;
    let ns = StateNamespace::new(b"op");
    let partition = partition(&ns);
    let writer = store.client(scope(&ns, 1));
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
    assert!(cut_allows(&snap, 1, 0));
    assert!(!cut_allows(&snap, 1, 1));

    let reader = store.client(scope(&ns, 2));
    reader.restore(&snap).await.unwrap();
    assert_eq!(reader.load_key_state(&partition).await.unwrap().next_seq, 2);

    reader
        .commit_events(
            &partition,
            0,
            &batch(&[(3_000, 2)]),
            &TileMap::new(),
            &KeyState {
                next_seq: 3,
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    writer
        .commit_events(
            &partition,
            0,
            &batch(&[(2_000, 2)]),
            &TileMap::new(),
            &KeyState {
                next_seq: 9,
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();

    let span = [raw_run((0, 0), (4_000, 0))];
    assert_eq!(reader.load_key_state(&partition).await.unwrap().next_seq, 3);
    assert_eq!(
        raw_cursors(&reader.load_raw(&partition, &span).await.unwrap()),
        vec![Cursor::new(1_000, 1), Cursor::new(3_000, 2)]
    );
    assert_eq!(writer.load_key_state(&partition).await.unwrap().next_seq, 9);
    assert_eq!(
        raw_cursors(&writer.load_raw(&partition, &span).await.unwrap()),
        vec![Cursor::new(1_000, 1), Cursor::new(2_000, 2)]
    );
    assert_eq!(
        stored_versions(&store, ns.bytes.as_slice(), &partition.business_key).await,
        (
            vec![(1, 0), (1, 1), (2, 0)],
            vec![(1_000, 1, 0), (2_000, 1, 1), (3_000, 2, 0)]
        )
    );
}

/// Two snapshots exist. Restore uses the earlier one, and the later rows remain stored.
#[tokio::test]
#[ignore]
async fn window_scylla_store_restore_uses_earlier_cut() {
    let store = connect("volga_cut").await;
    let ns = StateNamespace::new(b"op");
    let partition = partition(&ns);
    let writer = store.client(scope(&ns, 1));
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
    let earlier = writer.checkpoint().await.unwrap();
    writer
        .commit_events(
            &partition,
            0,
            &batch(&[(2_000, 2)]),
            &TileMap::new(),
            &KeyState {
                next_seq: 9,
                ..Default::default()
            },
            &[],
        )
        .await
        .unwrap();
    let later = writer.checkpoint().await.unwrap();
    assert!(cut_allows(&earlier, 1, 0));
    assert!(!cut_allows(&earlier, 1, 1));
    assert!(cut_allows(&later, 1, 1));

    let successor = store.client(scope(&ns, 2));
    successor.restore(&earlier).await.unwrap();
    assert_eq!(
        successor.load_key_state(&partition).await.unwrap().next_seq,
        2
    );
    assert_eq!(
        raw_cursors(
            &successor
                .load_raw(&partition, &[raw_run((0, 0), (3_000, 0))])
                .await
                .unwrap()
        ),
        vec![Cursor::new(1_000, 1)]
    );
    assert_eq!(
        stored_versions(&store, ns.bytes.as_slice(), &partition.business_key).await,
        (vec![(1, 0), (1, 1)], vec![(1_000, 1, 0), (2_000, 1, 1)])
    );
}
