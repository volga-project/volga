//! Request-worker pool: configure/start HTTP replicas outside the streaming attempt.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use kameo::actor::ActorRef;
use kameo::message::{Context, Message};
use kameo::Actor;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::common::failure::FailureEvent;
use crate::orchestrator::orchestrator::{WorkerNode, WorkerRole};
use crate::orchestrator::task_assignment::TaskWorkerMapping;
use crate::runtime::consts::{
    runtime_consts, MASTER_DISCOVERY_TIMEOUT, MASTER_REPLACEMENT_TIMEOUT,
};

use super::attempt::session::{
    map_session_error, Configure, ShutdownWorker, StartHeartbeat, StartWorker, WorkerSession,
};
use super::events::LifecycleEvent;
use super::state::{MasterState, PipelineContext};
use super::worker_client::WorkerClient;

#[derive(Actor)]
pub(super) struct RequestPool {
    state: Arc<MasterState>,
    pipeline: Arc<PipelineContext>,
    sessions: HashMap<String, ActorRef<WorkerSession>>,
    failure_tx: mpsc::Sender<FailureEvent>,
    failure_rx: Option<mpsc::Receiver<FailureEvent>>,
    supervisor: Option<JoinHandle<()>>,
}

pub(super) struct Start;
pub(super) struct Shutdown;
struct FailureMsg(FailureEvent);

impl RequestPool {
    pub(super) fn spawn(
        state: Arc<MasterState>,
        pipeline: Arc<PipelineContext>,
    ) -> ActorRef<Self> {
        let (failure_tx, failure_rx) = mpsc::channel(256);
        kameo::spawn(Self {
            state,
            pipeline,
            sessions: HashMap::new(),
            failure_tx,
            failure_rx: Some(failure_rx),
            supervisor: None,
        })
    }

    fn bind_address(node: &WorkerNode) -> String {
        format!("{}:{}", node.worker_ip, node.transport_port)
    }

    async fn start(&mut self, self_ref: ActorRef<Self>) -> Result<(), String> {
        let expected = self.pipeline.expected_request_workers;
        if expected == 0 {
            return Ok(());
        }
        if self.pipeline.request_graph.is_none() {
            return Err(
                "expected request workers but compiled spec has no request graph".to_string(),
            );
        }
        let timeout = runtime_consts().duration(MASTER_DISCOVERY_TIMEOUT);
        let nodes = self
            .state
            .wait_for_ready_workers(expected, timeout, WorkerRole::Request)
            .await
            .map_err(|error| error.to_string())?;
        self.connect_and_start(&nodes).await?;
        self.start_supervisor(self_ref);
        let worker_ids: Vec<String> = nodes.keys().cloned().collect();
        self.state
            .record_lifecycle_event(LifecycleEvent::RequestWorkersStarted { worker_ids })
            .await;
        Ok(())
    }

    async fn connect_and_start(
        &mut self,
        nodes: &HashMap<String, WorkerNode>,
    ) -> Result<(), String> {
        for (worker_id, node) in nodes {
            if self.sessions.contains_key(worker_id) {
                continue;
            }
            let addr = format!("{}:{}", node.worker_ip, node.worker_port);
            let client = WorkerClient::open(worker_id, addr, 0)
                .await
                .map_err(|error| format!("{worker_id}: {error}"))?;
            let session = WorkerSession::spawn(worker_id.clone(), client);
            let bind = Self::bind_address(node);
            session
                .ask(Configure {
                    pipeline_id: self.pipeline.pipeline_id.clone(),
                    spec: self.pipeline.spec.clone(),
                    vertex_ids: Vec::new(),
                    mapping: TaskWorkerMapping::new(),
                    task_restore_data: Vec::new(),
                    restoring: false,
                    role: WorkerRole::Request,
                    request_bind_address: Some(bind),
                })
                .await
                .map_err(map_session_error)
                .map_err(|error| format!("{worker_id}: {error}"))?;
            session
                .ask(StartWorker)
                .await
                .map_err(map_session_error)
                .map_err(|error| format!("{worker_id}: {error}"))?;
            let _ = session
                .ask(StartHeartbeat {
                    failure_tx: self.failure_tx.clone(),
                })
                .await;
            self.sessions.insert(worker_id.clone(), session);
        }
        Ok(())
    }

    fn start_supervisor(&mut self, self_ref: ActorRef<Self>) {
        let Some(mut failure_rx) = self.failure_rx.take() else {
            return;
        };
        self.supervisor = Some(tokio::spawn(async move {
            while let Some(event) = failure_rx.recv().await {
                let _ = self_ref.tell(FailureMsg(event)).await;
            }
        }));
    }

    async fn replace_worker(&mut self, worker_id: String) {
        if let Some(session) = self.sessions.remove(&worker_id) {
            let _ = session.stop_gracefully().await;
        }
        println!("[MASTER] Replacing request worker {worker_id}");
        if let Err(error) = self.state.request_replacement(&[worker_id.clone()]).await {
            println!("[MASTER] request worker replacement failed: {error}");
            return;
        }
        let timeout = runtime_consts().duration(MASTER_REPLACEMENT_TIMEOUT);
        let nodes = match self
            .state
            .wait_for_ready_workers(
                self.pipeline.expected_request_workers,
                timeout,
                WorkerRole::Request,
            )
            .await
        {
            Ok(nodes) => nodes,
            Err(error) => {
                println!("[MASTER] waiting for replacement request worker failed: {error}");
                return;
            }
        };
        if let Err(error) = self.connect_and_start(&nodes).await {
            println!("[MASTER] starting replacement request worker failed: {error}");
            return;
        }
        self.state
            .record_lifecycle_event(LifecycleEvent::RequestWorkerReplaced { worker_id })
            .await;
    }

    async fn shutdown(&mut self) {
        if let Some(handle) = self.supervisor.take() {
            handle.abort();
        }
        let sessions: Vec<_> = self.sessions.drain().collect();
        let futures = sessions.into_iter().map(|(worker_id, session)| async move {
            let result = session.ask(ShutdownWorker).await;
            if let Err(error) = result {
                println!("[MASTER] request worker {worker_id} shutdown: {error:?}");
            }
            let _ = session.stop_gracefully().await;
        });
        futures::future::join_all(futures).await;
    }
}

impl Message<Start> for RequestPool {
    type Reply = Result<(), String>;

    async fn handle(&mut self, _msg: Start, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.start(ctx.actor_ref()).await
    }
}

impl Message<Shutdown> for RequestPool {
    type Reply = ();

    async fn handle(
        &mut self,
        _msg: Shutdown,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.shutdown().await;
    }
}

impl Message<FailureMsg> for RequestPool {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: FailureMsg,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut replace = HashSet::new();
        replace.insert(msg.0.worker_id);
        for worker_id in replace {
            self.replace_worker(worker_id).await;
        }
    }
}

impl Drop for RequestPool {
    fn drop(&mut self) {
        if let Some(handle) = self.supervisor.take() {
            handle.abort();
        }
    }
}
