//! gRPC remote handle over a process-local [`super::InMemWindowStore`].

use std::any::Any;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::common::grpc::stubs::window_store_service::window_store_service_server::WindowStoreService;
use crate::common::grpc::stubs::window_store_service::{
    BatchesReply, BindingMsg, BytesReply, CommitEventsReq, EmptyReply, LoadRawReq, LoadTilesReq,
    LoadWindowDataReq, MaintainReq, PartitionMsg, RestoreReq, StoreKeyStateReq, StreamDuePageReq,
    StreamDuePageReply, WindowDataReply,
};
use crate::common::grpc::window_store::{window_store_client, window_store_server};
use crate::common::grpc::{server_builder, spawn_with_shutdown, GrpcServeHandle};
use crate::common::KeyGroupRange;
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun, WindowTrigger,
};
use crate::runtime::operators::window::state::WindowOperatorState;
use crate::runtime::state::{OperatorStore, OperatorTaskState};
use crate::storage::in_memory_storage_grpc_server::InMemoryStorageServiceImpl;
use crate::common::grpc::storage::storage_server;

use super::codec::{decode_batch, decode_batches, decode_val, encode_batch, encode_batches, encode_val};
use super::{
    DueWindowWork, DueWorkStream, InMemWindowStore, WindowBackendSnapshot, WindowOperatorStore,
    WindowRequestStore, WindowStoreBinding, WriterId,
};

use crate::runtime::operators::window::store::data::WindowData;

fn status(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}

fn partition_from_msg(msg: &PartitionMsg) -> PartitionKey {
    PartitionKey {
        namespace: msg.namespace.clone(),
        business_key: msg.business_key.clone(),
    }
}

fn partition_to_msg(partition: &PartitionKey) -> PartitionMsg {
    PartitionMsg {
        namespace: partition.namespace.clone(),
        business_key: partition.business_key.clone(),
    }
}

fn binding_from_msg(msg: &BindingMsg) -> WindowStoreBinding {
    WindowStoreBinding {
        namespace: StateNamespace::new(&msg.namespace),
        max_parallelism: msg.max_parallelism as usize,
        owned: KeyGroupRange::new(msg.range_start as usize, msg.range_end as usize),
        writer_id: WriterId(msg.writer_id.clone()),
        attempt: msg.attempt.clone(),
    }
}

fn binding_to_msg(binding: &WindowStoreBinding) -> BindingMsg {
    BindingMsg {
        namespace: binding.namespace.bytes.clone(),
        max_parallelism: binding.max_parallelism as u64,
        range_start: binding.owned.start as u64,
        range_end: binding.owned.end as u64,
        writer_id: binding.writer_id.0.clone(),
        attempt: binding.attempt.clone(),
    }
}

#[derive(Debug, Clone)]
pub struct WindowStoreServiceImpl {
    store: InMemWindowStore,
}

impl WindowStoreServiceImpl {
    pub fn new() -> Self {
        Self {
            store: InMemWindowStore::new(),
        }
    }
}

impl Default for WindowStoreServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl WindowStoreService for WindowStoreServiceImpl {
    async fn load_key_state(
        &self,
        request: Request<PartitionMsg>,
    ) -> Result<Response<BytesReply>, Status> {
        let partition = partition_from_msg(&request.into_inner());
        let state = self
            .store
            .load_key_state(&partition)
            .await
            .map_err(status)?;
        Ok(Response::new(BytesReply {
            payload: encode_val(&state).map_err(status)?,
        }))
    }

    async fn load_raw(
        &self,
        request: Request<LoadRawReq>,
    ) -> Result<Response<BatchesReply>, Status> {
        let req = request.into_inner();
        let partition = partition_from_msg(req.partition.as_ref().ok_or_else(|| {
            Status::invalid_argument("load_raw missing partition")
        })?);
        let runs: Vec<RawRun> = decode_val(&req.raw_runs).map_err(status)?;
        let batches = self
            .store
            .load_raw(&partition, &runs)
            .await
            .map_err(status)?;
        Ok(Response::new(BatchesReply {
            batches: encode_batches(&batches).map_err(status)?,
        }))
    }

    async fn load_tiles(
        &self,
        request: Request<LoadTilesReq>,
    ) -> Result<Response<BytesReply>, Status> {
        let req = request.into_inner();
        let partition = partition_from_msg(req.partition.as_ref().ok_or_else(|| {
            Status::invalid_argument("load_tiles missing partition")
        })?);
        let runs: Vec<TileRun> = decode_val(&req.tile_runs).map_err(status)?;
        let tiles = self
            .store
            .load_tiles(&partition, &runs)
            .await
            .map_err(status)?;
        Ok(Response::new(BytesReply {
            payload: encode_val(&tiles).map_err(status)?,
        }))
    }

    async fn commit_events(
        &self,
        request: Request<CommitEventsReq>,
    ) -> Result<Response<EmptyReply>, Status> {
        let req = request.into_inner();
        let partition = partition_from_msg(req.partition.as_ref().ok_or_else(|| {
            Status::invalid_argument("commit_events missing partition")
        })?);
        let events = decode_batch(&req.events).map_err(status)?;
        let tiles: TileMap = decode_val(&req.tiles).map_err(status)?;
        let key_state: KeyState = decode_val(&req.key_state).map_err(status)?;
        let triggers: Vec<WindowTrigger> = decode_val(&req.triggers).map_err(status)?;
        self.store
            .commit_events(
                &partition,
                req.ts_column_index as usize,
                &events,
                &tiles,
                &key_state,
                &triggers,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(EmptyReply {}))
    }

    async fn stream_due_page(
        &self,
        request: Request<StreamDuePageReq>,
    ) -> Result<Response<StreamDuePageReply>, Status> {
        let req = request.into_inner();
        let binding = binding_from_msg(req.binding.as_ref().ok_or_else(|| {
            Status::invalid_argument("stream_due missing binding")
        })?);
        let after = if req.after.is_empty() {
            None
        } else {
            Some(decode_val::<Cursor>(&req.after).map_err(status)?)
        };
        let through = Cursor::new(req.through_ts, req.through_seq);
        let resume_after = if req.resume_after.is_empty() {
            None
        } else {
            Some(decode_val::<WindowTrigger>(&req.resume_after).map_err(status)?)
        };
        let client = self.store.bind_assignment(binding);
        let mut stream = client.stream_due(after, through);
        // Skip pages until we pass resume_after (client owns paging; one page per RPC).
        use futures::TryStreamExt;
        let mut skipped = resume_after.is_some();
        while let Some(work) = stream.try_next().await.map_err(|e| status(e))? {
            let last = work
                .iter()
                .flat_map(|w| w.triggers.iter())
                .max()
                .cloned();
            if skipped {
                if let (Some(resume), Some(last)) = (resume_after.as_ref(), last.as_ref()) {
                    if last <= resume {
                        continue;
                    }
                }
                skipped = false;
            }
            return Ok(Response::new(StreamDuePageReply {
                work: encode_val(&work).map_err(status)?,
                resume_after: last
                    .as_ref()
                    .map(encode_val)
                    .transpose()
                    .map_err(status)?
                    .unwrap_or_default(),
                done: false,
            }));
        }
        Ok(Response::new(StreamDuePageReply {
            work: Vec::new(),
            resume_after: Vec::new(),
            done: true,
        }))
    }

    async fn store_key_state(
        &self,
        request: Request<StoreKeyStateReq>,
    ) -> Result<Response<EmptyReply>, Status> {
        let req = request.into_inner();
        let partition = partition_from_msg(req.partition.as_ref().ok_or_else(|| {
            Status::invalid_argument("store_key_state missing partition")
        })?);
        let state: KeyState = decode_val(&req.key_state).map_err(status)?;
        self.store
            .store_key_state(&partition, &state)
            .await
            .map_err(status)?;
        Ok(Response::new(EmptyReply {}))
    }

    async fn checkpoint(
        &self,
        request: Request<BindingMsg>,
    ) -> Result<Response<BytesReply>, Status> {
        let binding = binding_from_msg(&request.into_inner());
        let snapshot = self
            .store
            .bind_assignment(binding)
            .checkpoint()
            .await
            .map_err(status)?;
        Ok(Response::new(BytesReply {
            payload: encode_val(&snapshot).map_err(status)?,
        }))
    }

    async fn restore(&self, request: Request<RestoreReq>) -> Result<Response<EmptyReply>, Status> {
        let req = request.into_inner();
        let binding = binding_from_msg(req.binding.as_ref().ok_or_else(|| {
            Status::invalid_argument("restore missing binding")
        })?);
        let snapshot: WindowBackendSnapshot = decode_val(&req.snapshot).map_err(status)?;
        self.store
            .bind_assignment(binding)
            .restore(&snapshot)
            .await
            .map_err(status)?;
        Ok(Response::new(EmptyReply {}))
    }

    async fn load_window_data(
        &self,
        request: Request<LoadWindowDataReq>,
    ) -> Result<Response<WindowDataReply>, Status> {
        let req = request.into_inner();
        let partition = partition_from_msg(req.partition.as_ref().ok_or_else(|| {
            Status::invalid_argument("load_window_data missing partition")
        })?);
        let raw_runs: Vec<RawRun> = decode_val(&req.raw_runs).map_err(status)?;
        let tile_runs: Vec<TileRun> = decode_val(&req.tile_runs).map_err(status)?;
        let data = WindowRequestStore::load_window_data(&self.store, &partition, &raw_runs, &tile_runs)
            .await
            .map_err(status)?;
        Ok(Response::new(WindowDataReply {
            raw_batches: encode_batches(data.raw_batches()).map_err(status)?,
            tiles: encode_val(data.tile_map()).map_err(status)?,
        }))
    }

    async fn maintain(&self, request: Request<MaintainReq>) -> Result<Response<EmptyReply>, Status> {
        let req = request.into_inner();
        let ns = StateNamespace::new(&req.namespace);
        self.store
            .maintain_cutoff(
                &ns,
                KeyGroupRange::new(req.range_start as usize, req.range_end as usize),
                req.max_parallelism as usize,
                req.watermark,
                req.floor,
                &req.task_id,
            )
            .map_err(status)?;
        Ok(Response::new(EmptyReply {}))
    }
}

/// Combined sink + window-store gRPC server for cluster tests.
pub struct InMemoryGrpcStateServer {
    serve: Option<GrpcServeHandle>,
}

impl InMemoryGrpcStateServer {
    pub fn new() -> Self {
        Self { serve: None }
    }

    pub async fn start(&mut self, addr: &str) -> Result<()> {
        let addr = addr.parse()?;
        let sink = storage_server(InMemoryStorageServiceImpl::new());
        let window = window_store_server(WindowStoreServiceImpl::new());
        self.serve = Some(spawn_with_shutdown(
            addr,
            server_builder().add_service(sink).add_service(window),
        ));
        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(mut serve) = self.serve.take() {
            serve.stop().await;
        }
    }
}

impl Drop for InMemoryGrpcStateServer {
    fn drop(&mut self) {
        if let Some(mut serve) = self.serve.take() {
            serve.abort();
        }
    }
}

#[derive(Debug, Clone)]
pub struct GrpcWindowStore {
    endpoint: String,
    client: Arc<Mutex<Option<crate::common::grpc::stubs::window_store_service::window_store_service_client::WindowStoreServiceClient<tonic::transport::Channel>>>>,
    binding: Option<WindowStoreBinding>,
}

impl GrpcWindowStore {
    pub fn connect(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            client: Arc::new(Mutex::new(None)),
            binding: None,
        }
    }

    pub fn bind_assignment(&self, binding: WindowStoreBinding) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            binding: Some(binding),
        }
    }

    fn binding(&self) -> Result<&WindowStoreBinding> {
        self.binding
            .as_ref()
            .ok_or_else(|| anyhow!("gRPC window store client is not bound"))
    }

    async fn client(
        &self,
    ) -> Result<
        crate::common::grpc::stubs::window_store_service::window_store_service_client::WindowStoreServiceClient<
            tonic::transport::Channel,
        >,
    > {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            *guard = Some(window_store_client(&self.endpoint).await?);
        }
        Ok(guard.as_ref().expect("client").clone())
    }
}

#[async_trait]
impl WindowOperatorStore for GrpcWindowStore {
    async fn load_key_state(&self, partition: &PartitionKey) -> Result<KeyState> {
        let mut client = self.client().await?;
        let reply = client
            .load_key_state(partition_to_msg(partition))
            .await?
            .into_inner();
        decode_val(&reply.payload)
    }

    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>> {
        let mut client = self.client().await?;
        let reply = client
            .load_raw(LoadRawReq {
                partition: Some(partition_to_msg(partition)),
                raw_runs: encode_val(&runs.to_vec())?,
            })
            .await?
            .into_inner();
        decode_batches(&reply.batches)
    }

    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap> {
        let mut client = self.client().await?;
        let reply = client
            .load_tiles(LoadTilesReq {
                partition: Some(partition_to_msg(partition)),
                tile_runs: encode_val(&runs.to_vec())?,
            })
            .await?
            .into_inner();
        decode_val(&reply.payload)
    }

    async fn commit_events(
        &self,
        partition: &PartitionKey,
        ts_column_index: usize,
        events: &RecordBatch,
        tiles: &TileMap,
        meta: &KeyState,
        triggers: &[WindowTrigger],
    ) -> Result<()> {
        let mut client = self.client().await?;
        client
            .commit_events(CommitEventsReq {
                partition: Some(partition_to_msg(partition)),
                ts_column_index: ts_column_index as u64,
                events: encode_batch(events)?,
                tiles: encode_val(tiles)?,
                key_state: encode_val(meta)?,
                triggers: encode_val(&triggers.to_vec())?,
            })
            .await?;
        Ok(())
    }

    fn stream_due<'a>(&'a self, after: Option<Cursor>, through: Cursor) -> DueWorkStream<'a> {
        let store = self.clone();
        Box::pin(futures::stream::try_unfold(
            (store, after, None::<Vec<u8>>),
            move |(store, after, resume)| {
                async move {
                    let binding = store.binding()?.clone();
                    let mut client = store.client().await?;
                    let reply = client
                        .stream_due_page(StreamDuePageReq {
                            binding: Some(binding_to_msg(&binding)),
                            after: after
                                .map(|c| encode_val(&c))
                                .transpose()?
                                .unwrap_or_default(),
                            through_ts: through.ts,
                            through_seq: through.seq_no,
                            resume_after: resume.unwrap_or_default(),
                        })
                        .await?
                        .into_inner();
                    if reply.done {
                        return Ok(None);
                    }
                    let work: Vec<DueWindowWork> = decode_val(&reply.work)?;
                    Ok(Some((
                        work,
                        (store, after, Some(reply.resume_after)),
                    )))
                }
            },
        ))
    }

    async fn store_key_state(&self, partition: &PartitionKey, state: &KeyState) -> Result<()> {
        let mut client = self.client().await?;
        client
            .store_key_state(StoreKeyStateReq {
                partition: Some(partition_to_msg(partition)),
                key_state: encode_val(state)?,
            })
            .await?;
        Ok(())
    }

    async fn checkpoint(&self) -> Result<WindowBackendSnapshot> {
        let mut client = self.client().await?;
        let reply = client
            .checkpoint(binding_to_msg(self.binding()?))
            .await?
            .into_inner();
        decode_val(&reply.payload)
    }

    async fn restore(&self, snapshot: &WindowBackendSnapshot) -> Result<()> {
        let mut client = self.client().await?;
        client
            .restore(RestoreReq {
                binding: Some(binding_to_msg(self.binding()?)),
                snapshot: encode_val(snapshot)?,
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl WindowRequestStore for GrpcWindowStore {
    async fn load_window_data(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
    ) -> Result<WindowData> {
        let mut client = self.client().await?;
        let reply = client
            .load_window_data(LoadWindowDataReq {
                partition: Some(partition_to_msg(partition)),
                raw_runs: encode_val(&raw_runs.to_vec())?,
                tile_runs: encode_val(&tile_runs.to_vec())?,
            })
            .await?
            .into_inner();
        Ok(WindowData::new(
            decode_batches(&reply.raw_batches)?,
            decode_val(&reply.tiles)?,
        ))
    }
}

#[async_trait]
impl OperatorStore for GrpcWindowStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn maintain(&self, ns: &StateNamespace, state: &dyn OperatorTaskState) -> Result<()> {
        let Some(wo) = state.as_any().downcast_ref::<WindowOperatorState>() else {
            return Ok(());
        };
        let Some((watermark, floor)) = wo.retention_cutoff() else {
            return Ok(());
        };
        let owned = state.key_group_range();
        let mut client = self.client().await?;
        client
            .maintain(MaintainReq {
                namespace: ns.bytes.clone(),
                range_start: owned.start as u64,
                range_end: owned.end as u64,
                max_parallelism: state.max_parallelism() as u64,
                watermark,
                floor,
                task_id: state.task_id().to_string(),
            })
            .await?;
        Ok(())
    }
}

/// Stream due on the server by consuming the full client stream is inefficient.
/// The page RPC above re-walks from the start each time; acceptable for InMemoryGrpc tests.
const _: () = ();
