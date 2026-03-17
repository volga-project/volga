use anyhow::Result;
use kameo::Actor;
use kameo::message::Context;
use crate::runtime::observability::TaskSnapshot;
use crate::runtime::stream_task::StreamTask;

#[derive(Debug, Clone)]
pub enum StreamTaskMessage {
    Start,
    Close,
    GetState,
    Run,
    TriggerCheckpoint(u64),
}

#[derive(Actor)]
pub struct StreamTaskActor {
    task: StreamTask,
}

impl StreamTaskActor {
    pub fn new(task: StreamTask) -> Self {
        Self { task }
    }
}

impl kameo::message::Message<StreamTaskMessage> for StreamTaskActor {
    type Reply = Result<TaskSnapshot>;

    async fn handle(&mut self, msg: StreamTaskMessage, _ctx: &mut Context<StreamTaskActor, Result<TaskSnapshot>>) -> Self::Reply {
        match msg {
            StreamTaskMessage::Start => {
                self.task.start().await;
                Ok(self.task.get_state().await)
            }
            StreamTaskMessage::Close => {
                self.task.signal_to_close();
                Ok(self.task.get_state().await)
            }
            StreamTaskMessage::GetState => {
                Ok(self.task.get_state().await)
            }
            StreamTaskMessage::Run => {
                self.task.signal_to_run();
                Ok(self.task.get_state().await)
            }
            StreamTaskMessage::TriggerCheckpoint(checkpoint_id) => {
                self.task.signal_trigger_checkpoint(checkpoint_id);
                Ok(self.task.get_state().await)
            }
        }
    }
}