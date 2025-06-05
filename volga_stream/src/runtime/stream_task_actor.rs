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
                Ok(StreamTaskState {
                    vertex_id: self.task.vertex_id().to_string(),
                    status: self.task.get_status(),
                })
            }
            StreamTaskMessage::Close => {
                self.task.close_and_wait().await;
                Ok(StreamTaskState {
                    vertex_id: self.task.vertex_id().to_string(),
                    status: self.task.get_status(),
                })
            }
            StreamTaskMessage::GetState => {
                Ok(StreamTaskState {
                    vertex_id: self.task.vertex_id().to_string(),
                    status: self.task.get_status(),
                })
            }
        }
    }
}