use anyhow::Result;
use kameo::Actor;
use kameo::message::Context;
use crate::runtime::stream_task::{StreamTask, StreamTaskState};

#[derive(Debug)]
pub enum StreamTaskMessage {
    Run,
    Close,
    GetState
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
    type Reply = Result<StreamTaskState>;

    async fn handle(&mut self, msg: StreamTaskMessage, _ctx: &mut Context<StreamTaskActor, Result<StreamTaskState>>) -> Self::Reply {
        match msg {
            StreamTaskMessage::Run => {
                self.task.run().await;
                Ok(self.task.get_state())
            }
            StreamTaskMessage::Close => {
                self.task.close_and_wait().await;
                Ok(self.task.get_state())
            }
            StreamTaskMessage::GetState => {
                Ok(self.task.get_state())
            }
        }
    }
}