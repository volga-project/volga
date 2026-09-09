//! Docker / testcontainer contract for the Scylla window store.
//!
//! Ignored by default (`src/tests/README.md`). Point at an already-running
//! cluster with `VOLGA_SCYLLA_CONTACT=127.0.0.1:9042` (unique keyspace per
//! test). Otherwise each test starts `scylladb/scylla:5.4`. Later PRs add
//! checkpoint / overlay-restore / WRO cases to this file.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::spec::state::ScyllaConfig;
use crate::common::KeyGroupRange;
use crate::runtime::operators::window::model::{
    Cursor, KeyEvaluationState, KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun,
    TimeGranularity, WindowTiles, WindowTrigger, WindowTriggerKind,
};
use crate::runtime::operators::window::state::WindowOperatorState;
use crate::runtime::operators::window::store::backend::{
    stream_due, WindowOperatorStore, WindowStoreTaskScope, WriterId,
};
use crate::runtime::operators::window::store::data::cursors_from_batch;
use crate::runtime::state::OperatorStore;
use crate::test_utils::window_aggs as test_utils;
use arrow::array::RecordBatch;
use futures::TryStreamExt;
use testcontainers::{clients, Container, GenericImage};

use super::ScyllaWindowStore;

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

fn scope(ns: &StateNamespace, attempt: &[u8]) -> WindowStoreTaskScope {
    let mut scope = WindowStoreTaskScope::for_test(ns.clone());
    scope.attempt = attempt.to_vec();
    scope
}

fn contact<'a>(
    docker: &'a clients::Cli,
) -> (String, Option<Container<'a, GenericImage>>) {
    if let Ok(cp) = std::env::var("VOLGA_SCYLLA_CONTACT") {
        return (cp, None);
    }
    let container = docker.run(GenericImage::new("scylladb/scylla", "5.4"));
    let port = container.get_host_port_ipv4(9042);
    (format!("127.0.0.1:{port}"), Some(container))
}

async fn connect<'a>(
    docker: &'a clients::Cli,
    keyspace: &str,
) -> (Option<Container<'a, GenericImage>>, ScyllaWindowStore) {
    let (contact, container) = contact(docker);
    let store = ScyllaWindowStore::connect(ScyllaConfig {
        contact_points: vec![contact],
        keyspace: keyspace.to_string(),
        datacenter: None,
        serving_publish: Default::default(),
    })
    .await
    .expect("scylla connect via StateSessionHandle");
    (container, store)
}

fn live_scope(ns: &StateNamespace) -> WindowStoreTaskScope {
    WindowStoreTaskScope {
        namespace: ns.clone(),
        max_parallelism: 1,
        key_group_range: KeyGroupRange::full(1),
        writer_id: WriterId(b"writer".to_vec()),
        attempt: b"live".to_vec(),
    }
}

/// Full write → load_key_state / load_raw / stream_due loop.
#[tokio::test]
#[ignore]
async fn scylla_commit_load_and_stream_due() {
    let docker = clients::Cli::default();
    let (_container, store) = connect(&docker, "volga_loop").await;
    let ns = StateNamespace::new(b"op");
    let client = store.client(scope(&ns, b"a"));
    let partition = partition(&ns);
    let events = test_utils::batch(&[(1_000, 1_000.0, "key", 1)]);
    client
        .commit_events(
            &partition,
            0,
            &events,
            &Default::default(),
            &KeyState {
                next_seq: 2,
                ..Default::default()
            },
            &[WindowTrigger {
                fire_at: Cursor::new(1_000, 1),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            }],
        )
        .await
        .expect("commit");
    assert_eq!(client.load_key_state(&partition).await.unwrap().next_seq, 2);
    let loaded = client
        .load_raw(
            &partition,
            &[RawRun {
                from: Cursor::new(0, 0),
                to: Cursor::new(2_000, 0),
            }],
        )
        .await
        .unwrap();
    assert_eq!(loaded.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let mut due = stream_due(&client, None, Cursor::new(2_000, u64::MAX));
    let page = due.try_next().await.unwrap().unwrap();
    assert_eq!(page[0].triggers.len(), 1);
    assert!(due.try_next().await.unwrap().is_none());
}

#[tokio::test]
#[ignore]
async fn scylla_empty_loads_and_empty_runs() {
    let docker = clients::Cli::default();
    let (_container, store) = connect(&docker, "volga_empty").await;
    let ns = StateNamespace::new(b"op");
    let client = store.client(scope(&ns, b"a"));
    let partition = partition(&ns);
    let raw_runs = [raw_run((0, 0), (10, 0))];
    let tile_runs = [TileRun {
        granularity: TimeGranularity::Seconds(1),
        start_ts: 0,
        end_ts_exclusive: 10_000,
    }];

    assert_eq!(
        client.load_key_state(&partition).await.unwrap(),
        KeyState::default()
    );
    assert!(client
        .load_raw(&partition, &raw_runs)
        .await
        .unwrap()
        .is_empty());
    assert!(client
        .load_tiles(&partition, &tile_runs)
        .await
        .unwrap()
        .is_empty());
    assert!(client
        .load_raw(&partition, &[])
        .await
        .unwrap()
        .is_empty());
    assert!(client
        .load_tiles(&partition, &[])
        .await
        .unwrap()
        .is_empty());
    assert!(stream_due(&client, None, Cursor::new(2_000, u64::MAX))
        .try_next()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore]
async fn scylla_commit_stores_raw_tiles_and_key_state() {
    let docker = clients::Cli::default();
    let (_container, store) = connect(&docker, "volga_commit").await;
    let ns = StateNamespace::new(b"op");
    let client = store.client(scope(&ns, b"a"));
    let partition = partition(&ns);
    let meta = KeyState {
        next_seq: 3,
        ..Default::default()
    };
    let stored_tiles = tiles(&[(TimeGranularity::Seconds(1), 2_000, 3)]);
    client
        .commit_events(
            &partition,
            0,
            &batch(&[(2_000, 2), (1_000, 1)]),
            &stored_tiles,
            &meta,
            &[],
        )
        .await
        .unwrap();

    assert_eq!(client.load_key_state(&partition).await.unwrap(), meta);
    assert_eq!(
        raw_cursors(
            &client
                .load_raw(&partition, &[raw_run((0, 0), (3_000, 0))])
                .await
                .unwrap()
        ),
        vec![Cursor::new(1_000, 1), Cursor::new(2_000, 2)]
    );
    assert_eq!(
        tile_keys(
            &client
                .load_tiles(
                    &partition,
                    &[TileRun {
                        granularity: TimeGranularity::Seconds(1),
                        start_ts: 0,
                        end_ts_exclusive: 3_000,
                    }]
                )
                .await
                .unwrap()
        ),
        vec![(TimeGranularity::Seconds(1), 2_000)]
    );
}

#[tokio::test]
#[ignore]
async fn scylla_store_key_state_leaves_raw_and_tiles_unchanged() {
    let docker = clients::Cli::default();
    let (_container, store) = connect(&docker, "volga_keystate").await;
    let ns = StateNamespace::new(b"op");
    let client = store.client(scope(&ns, b"a"));
    let partition = partition(&ns);
    let stored_tiles = tiles(&[(TimeGranularity::Seconds(1), 1_000, 7)]);
    client
        .commit_events(
            &partition,
            0,
            &batch(&[(1_000, 4)]),
            &stored_tiles,
            &KeyState::default(),
            &[],
        )
        .await
        .unwrap();
    let updated_meta = KeyState {
        next_seq: 6,
        evaluation: Some(KeyEvaluationState {
            through: Cursor::new(1_000, 4),
            accumulators: BTreeMap::new(),
        }),
    };
    client
        .store_key_state(&partition, &updated_meta)
        .await
        .unwrap();

    assert_eq!(
        client.load_key_state(&partition).await.unwrap(),
        updated_meta
    );
    assert_eq!(
        raw_cursors(
            &client
                .load_raw(&partition, &[raw_run((0, 0), (2_000, 0))])
                .await
                .unwrap()
        ),
        vec![Cursor::new(1_000, 4)]
    );
    assert_eq!(
        tile_keys(
            &client
                .load_tiles(
                    &partition,
                    &[TileRun {
                        granularity: TimeGranularity::Seconds(1),
                        start_ts: 0,
                        end_ts_exclusive: 2_000,
                    }]
                )
                .await
                .unwrap()
        ),
        vec![(TimeGranularity::Seconds(1), 1_000)]
    );
}

#[tokio::test]
#[ignore]
async fn scylla_raw_ranges_are_half_open_and_ordered() {
    let docker = clients::Cli::default();
    let (_container, store) = connect(&docker, "volga_raw").await;
    let ns = StateNamespace::new(b"op");
    let client = store.client(scope(&ns, b"a"));
    let partition = partition(&ns);
    client
        .commit_events(
            &partition,
            0,
            &batch(&[(30, 3), (10, 1), (20, 2), (20, 1), (40, 4)]),
            &TileMap::new(),
            &KeyState::default(),
            &[],
        )
        .await
        .unwrap();
    client
        .commit_events(
            &partition,
            0,
            &batch(&[(50, 5)]),
            &TileMap::new(),
            &KeyState::default(),
            &[],
        )
        .await
        .unwrap();

    let loaded = client
        .load_raw(
            &partition,
            &[raw_run((10, 1), (30, 3)), raw_run((20, 1), (40, 4))],
        )
        .await
        .unwrap();
    assert_eq!(
        raw_cursors(&loaded),
        vec![
            Cursor::new(10, 1),
            Cursor::new(20, 1),
            Cursor::new(20, 2),
            Cursor::new(30, 3),
        ]
    );
}

#[tokio::test]
#[ignore]
async fn scylla_tile_ranges_are_half_open() {
    let docker = clients::Cli::default();
    let (_container, store) = connect(&docker, "volga_tiles").await;
    let ns = StateNamespace::new(b"op");
    let client = store.client(scope(&ns, b"a"));
    let partition = partition(&ns);
    let stored_tiles = tiles(&[
        (TimeGranularity::Seconds(1), 0, 0),
        (TimeGranularity::Seconds(1), 1_000, 1),
        (TimeGranularity::Seconds(1), 2_000, 2),
        (TimeGranularity::Seconds(1), 3_000, 3),
        (TimeGranularity::Minutes(1), 1_000, 4),
    ]);
    client
        .commit_events(
            &partition,
            0,
            &batch(&[]),
            &stored_tiles,
            &KeyState::default(),
            &[],
        )
        .await
        .unwrap();

    let loaded = client
        .load_tiles(
            &partition,
            &[TileRun {
                granularity: TimeGranularity::Seconds(1),
                start_ts: 1_000,
                end_ts_exclusive: 3_000,
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        tile_keys(&loaded),
        vec![
            (TimeGranularity::Seconds(1), 1_000),
            (TimeGranularity::Seconds(1), 2_000),
        ]
    );
}

#[tokio::test]
#[ignore]
async fn scylla_overlay_hides_other_attempt() {
    let docker = clients::Cli::default();
    let (_container, store) = connect(&docker, "volga_overlay").await;
    let ns = StateNamespace::new(b"op");
    let writer = store.client(scope(&ns, b"a"));
    let other = store.client(scope(&ns, b"b"));
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
            &[WindowTrigger {
                fire_at: Cursor::new(1_000, 1),
                partition: partition.clone(),
                kind: WindowTriggerKind::RowEmit,
            }],
        )
        .await
        .unwrap();

    assert_eq!(other.load_key_state(&partition).await.unwrap(), KeyState::default());
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
            }]
        )
        .await
        .unwrap()
        .is_empty());
    assert!(stream_due(&other, None, Cursor::new(2_000, u64::MAX))
        .try_next()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore]
async fn scylla_maintain_drops_unreachable_generations() {
    let docker = clients::Cli::default();
    let (_container, store) = connect(&docker, "volga_maintain").await;
    let ns = StateNamespace::new(b"op");
    let scope = live_scope(&ns);
    let client = store.client(scope.clone());
    let partition = partition(&ns);
    let events = test_utils::batch(&[(1_000, 1_000.0, "key", 1)]);
    client
        .commit_events(
            &partition,
            0,
            &events,
            &Default::default(),
            &KeyState {
                next_seq: 2,
                ..Default::default()
            },
            &[],
        )
        .await
        .expect("commit");

    let session = store.session();
    session
        .query_unpaged(
            "INSERT INTO window_key_states (namespace, key_group, business_key, attempt, epoch, key_state) VALUES (?, ?, ?, ?, ?, ?)",
            (
                ns.bytes.clone(),
                0i32,
                partition.business_key.clone(),
                b"dead".to_vec(),
                1i64,
                vec![1u8],
            ),
        )
        .await
        .unwrap();
    session
        .query_unpaged(
            "INSERT INTO window_raw (namespace, key_group, business_key, bucket_start, event_ts, seq_no, attempt, epoch, payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                ns.bytes.clone(),
                0i32,
                partition.business_key.clone(),
                0i64,
                1_000i64,
                99i64,
                b"dead".to_vec(),
                1i64,
                vec![1u8],
            ),
        )
        .await
        .unwrap();

    let task_state = WindowOperatorState::new(
        Arc::new(client.clone()) as Arc<dyn WindowOperatorStore>,
        Arc::from("t"),
        0,
        Arc::new(BTreeMap::new()),
        0,
        1_000_000,
        scope,
    );
    task_state
        .watermark_frontier
        .store(10_000, Ordering::Release);
    store.maintain(&ns, &task_state).await.unwrap();

    let attempts = session
        .query_unpaged(
            "SELECT attempt FROM window_key_states WHERE namespace = ? AND key_group = ? AND business_key = ?",
            (ns.bytes.clone(), 0i32, partition.business_key.clone()),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();
    let attempts: Vec<Vec<u8>> = attempts
        .rows::<(Vec<u8>,)>()
        .unwrap()
        .map(|row| row.unwrap().0)
        .collect();
    assert_eq!(attempts, vec![b"live".to_vec()]);
    assert_eq!(client.load_key_state(&partition).await.unwrap().next_seq, 2);
    let loaded = client
        .load_raw(
            &partition,
            &[RawRun {
                from: Cursor::new(0, 0),
                to: Cursor::new(2_000, 0),
            }],
        )
        .await
        .unwrap();
    assert_eq!(loaded.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}
