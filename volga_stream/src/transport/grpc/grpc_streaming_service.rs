use tonic::{Request, Response, Status};
use tokio::sync::mpsc;
use crate::common::message::Message;

pub mod message_stream {
    tonic::include_proto!("message_stream");
}

use message_stream::{
    message_stream_service_server::MessageStreamService, GrpcMessage, EmptyResponse,
};

use crate::transport::grpc::grpc_streaming_service::message_stream::message_stream_service_client::MessageStreamServiceClient;

/// Server implementation of the MessageStreamService
#[derive(Default)]
pub struct MessageStreamServiceImpl {
    // Channel to send received messages to the application
    tx: Option<mpsc::Sender<(Message, String)>>,
}

impl MessageStreamServiceImpl {
    pub fn new(tx: mpsc::Sender<(Message, String)>) -> Self {
        Self {
            tx: Some(tx),
        }
    }
}

#[tonic::async_trait]
impl MessageStreamService for MessageStreamServiceImpl {
    async fn stream_messages(
        &self,
        request: Request<tonic::Streaming<GrpcMessage>>,
    ) -> Result<Response<EmptyResponse>, Status> {
        let mut stream = request.into_inner();
        let tx = self.tx.as_ref().unwrap().clone();

        // Process incoming messages
        while let Some(message) = stream.message().await? {
            let message_data = message.message_data;
            let channel_id = message.channel_id;
            
            // Deserialize the message
            let deserialized_message = Message::from_bytes(&message_data);
            
            // Send to application via channel
            if let Err(e) = tx.send((deserialized_message, channel_id)).await {
                eprintln!("[SERVER] Failed to send message to application: {}", e);
                return Err(Status::internal("Failed to process message"));
            }
        }

        Ok(Response::new(EmptyResponse {}))
    }
}

/// Client for streaming messages to a server
pub struct MessageStreamClient {
    client: MessageStreamServiceClient<tonic::transport::Channel>,
}

impl MessageStreamClient {
    pub async fn connect(addr: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client = MessageStreamServiceClient::connect(addr).await?;
        Ok(Self { client })
    }

    pub async fn stream_messages(
        &mut self,
        rx: mpsc::Receiver<(Message, String)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stream = tokio_stream::StreamExt::map(
            tokio_stream::wrappers::ReceiverStream::new(rx),
            |(message, channel_id)| {
                // Serialize the message
                let message_data = message.to_bytes();
                GrpcMessage {
                    message_data,
                    channel_id,
                }
            }
        );

        let request = Request::new(stream);
        let _response = self.client.stream_messages(request).await?;
        
        Ok(())
    }
}
