use kameo::message::{Context, Message};

use crate::runtime::observability::snapshot_types::WorkerSnapshot;

use super::config::WorkerIdentity;
use super::messages::{
    Close, CloseTasks, Configure, GetIdentity, GetState, Reset, RunTasks, RunTestLifecycle,
    Shutdown, Start, StopSources, TriggerBarrier,
};
use super::Worker;

impl Message<Configure> for Worker {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: Configure,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.is_running() {
            return Err("Worker is already running; reset before reconfigure".to_string());
        }
        self.reset_async().await;
        self.configure(msg.0);
        Ok(())
    }
}

impl Message<Start> for Worker {
    type Reply = Result<(), String>;

    async fn handle(&mut self, msg: Start, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.require_attempt(msg.execution_attempt_id)?;
        self.start().await
    }
}

impl Message<RunTasks> for Worker {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: RunTasks,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.require_attempt(msg.execution_attempt_id)?;
        self.signal_tasks_run().await
    }
}

impl Message<CloseTasks> for Worker {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: CloseTasks,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.require_attempt(msg.execution_attempt_id)?;
        self.signal_tasks_close().await
    }
}

impl Message<GetState> for Worker {
    type Reply = Result<WorkerSnapshot, String>;

    async fn handle(
        &mut self,
        msg: GetState,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.require_attempt(msg.execution_attempt_id)?;
        Ok(self.get_state().await)
    }
}

impl Message<GetIdentity> for Worker {
    type Reply = Result<WorkerIdentity, String>;

    async fn handle(
        &mut self,
        _msg: GetIdentity,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.identity())
    }
}

impl Message<StopSources> for Worker {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        msg: StopSources,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.require_attempt(msg.execution_attempt_id)?;
        Ok(self.stop_sources())
    }
}

impl Message<TriggerBarrier> for Worker {
    type Reply = Result<bool, String>;

    async fn handle(
        &mut self,
        msg: TriggerBarrier,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.require_attempt(msg.execution_attempt_id)?;
        Ok(self.trigger_checkpoint_barrier(msg.checkpoint_id).await)
    }
}

impl Message<Reset> for Worker {
    type Reply = ();

    async fn handle(&mut self, _msg: Reset, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.reset_async().await;
    }
}

impl Message<Shutdown> for Worker {
    type Reply = Result<(), String>;

    async fn handle(
        &mut self,
        msg: Shutdown,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.require_attempt(msg.execution_attempt_id)?;
        self.close_async().await;
        Ok(())
    }
}

impl Message<Close> for Worker {
    type Reply = ();

    async fn handle(&mut self, _msg: Close, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.close_async().await;
    }
}

impl Message<RunTestLifecycle> for Worker {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: RunTestLifecycle,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.run_test_lifecycle(msg.state_updates).await;
    }
}
