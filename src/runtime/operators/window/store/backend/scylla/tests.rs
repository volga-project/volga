//! Docker contract for the Scylla window store.
//!
//! Ignored by default (`src/tests/README.md`). `VOLGA_SCYLLA_CONTACT` reuses a
//! cluster; otherwise tests share one `scylladb/scylla:5.4` container. Unique
//! keyspace per test.

use std::sync::OnceLock;

use crate::api::spec::state::ScyllaConfig;
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun, TimeGranularity,
    WindowTiles, WindowTrigger, WindowTriggerKind,
};
use crate::runtime::operators::window::store::backend::{
    Version, WindowBackendSnapshot, WindowOperatorStore, WindowStoreTaskScope,
};
use crate::runtime::operators::window::store::data::cursors_from_batch;
use crate::test_utils::window_aggs as test_utils;
use arrow::array::RecordBatch;
use testcontainers::{clients, GenericImage};

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

fn scope(ns: &StateNamespace, attempt: u64) -> WindowStoreTaskScope {
    let mut scope = WindowStoreTaskScope::for_test(ns.clone());
    scope.attempt = attempt;
    scope
}

fn contact() -> String {
    if let Ok(cp) = std::env::var("VOLGA_SCYLLA_CONTACT") {
        return cp;
    }
    static SHARED: OnceLock<String> = OnceLock::new();
    SHARED
        .get_or_init(|| {
            let docker = Box::leak(Box::new(clients::Cli::default()));
            let container = docker.run(GenericImage::new("scylladb/scylla", "5.4"));
            let port = container.get_host_port_ipv4(9042);
            let contact = format!("127.0.0.1:{port}");
            std::mem::forget(container);
            contact
        })
        .clone()
}

async fn connect(keyspace: &str) -> ScyllaWindowStore {
    ScyllaWindowStore::connect(ScyllaConfig {
        contact_points: vec![contact()],
        keyspace: keyspace.to_string(),
        datacenter: None,
    })
    .await
    .expect("scylla connect via StateSessionHandle")
}

#[tokio::test]
#[ignore]
async fn scylla_commit_roundtrip() {
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
        (TimeGranularity::Seconds(1), 70_000, 5),
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
            &batch(&[
                (30, 3),
                (10, 1),
                (20, 2),
                (20, 1),
                (40, 4),
                (50, 5),
                (50_000, 0),
                (60_000, 0),
                (60_000, 1),
            ]),
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
        raw_cursors(
            &client
                .load_raw(&partition, &[raw_run((50_000, 0), (60_000, 2))])
                .await
                .unwrap()
        ),
        vec![
            Cursor::new(50_000, 0),
            Cursor::new(60_000, 0),
            Cursor::new(60_000, 1),
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
                        end_ts_exclusive: 120_000,
                    }],
                )
                .await
                .unwrap()
        ),
        vec![
            (TimeGranularity::Seconds(1), 1_000),
            (TimeGranularity::Seconds(1), 2_000),
            (TimeGranularity::Seconds(1), 3_000),
            (TimeGranularity::Seconds(1), 70_000),
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
async fn scylla_overlay_hides_other_attempt() {
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
async fn scylla_restore_sees_checkpointed_prefix() {
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
