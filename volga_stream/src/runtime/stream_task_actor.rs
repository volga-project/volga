use anyhow::Result;
use kameo::Actor;
use kameo::message::Context;
use crate::runtime::stream_task::{StreamTask, StreamTaskState};

#[derive(Debug)]
pub enum StreamTaskMessage {
    Start,
    Close,
    GetState,
    Run
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
        }
    }
}