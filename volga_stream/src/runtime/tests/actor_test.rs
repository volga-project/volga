// use anyhow::Result;
// use kameo::actor::{ActorRef, ActorSystem};
// use crate::runtime::actors::{StreamTaskActor, StreamTaskMessage, TransportBackendActor, TransportBackendMessage};
// use crate::runtime::task::StreamTask;
// use crate::runtime::execution_graph::{ExecutionGraph, OperatorConfig};
// use crate::runtime::runtime_context::RuntimeContext;
// use crate::transport::channel::Channel;
// use crate::transport::transport_client::TransportClient;
// use crate::runtime::partition::PartitionType;
// use tokio::time::{sleep, Duration};

// #[tokio::test]
// async fn test_actor_based_execution() -> Result<()> {
//     // Create actor system
//     let system = ActorSystem::new();
    
//     // Create transport backend actor
//     let backend_actor = system.spawn(TransportBackendActor::new());
    
//     // Create source task
//     let source_task = StreamTask::new(
//         "source".to_string(),
//         OperatorConfig::SourceConfig(Default::default()),
//         RuntimeContext::new(),
//     )?;
//     let source_actor = system.spawn(StreamTaskActor::new(source_task));
    
//     // Create sink task
//     let sink_task = StreamTask::new(
//         "sink".to_string(),
//         OperatorConfig::SinkConfig(Default::default()),
//         RuntimeContext::new(),
//     )?;
//     let sink_actor = system.spawn(StreamTaskActor::new(sink_task));
    
//     // Register clients with transport backend
//     backend_actor.send(TransportBackendMessage::RegisterClient {
//         vertex_id: "source".to_string(),
//         client: source_actor.get_task().transport_client(),
//     }).await?;
    
//     backend_actor.send(TransportBackendMessage::RegisterClient {
//         vertex_id: "sink".to_string(),
//         client: sink_actor.get_task().transport_client(),
//     }).await?;
    
//     // Create and register channel
//     let channel = Channel::Local {
//         channel_id: "source_to_sink".to_string(),
//     };
    
//     backend_actor.send(TransportBackendMessage::RegisterChannel {
//         vertex_id: "source".to_string(),
//         channel: channel.clone(),
//         is_input: false,
//     }).await?;
    
//     backend_actor.send(TransportBackendMessage::RegisterChannel {
//         vertex_id: "sink".to_string(),
//         channel: channel.clone(),
//         is_input: true,
//     }).await?;
    
//     // Create collector in source task
//     source_actor.send(StreamTaskMessage::CreateCollector {
//         channel_id: "source_to_sink".to_string(),
//         partition_type: PartitionType::Forward,
//         target_operator_id: "sink".to_string(),
//     }).await?;
    
//     // Start transport backend
//     backend_actor.send(TransportBackendMessage::Start).await?;
    
//     // Open and run tasks
//     source_actor.send(StreamTaskMessage::Open).await?;
//     sink_actor.send(StreamTaskMessage::Open).await?;
    
//     source_actor.send(StreamTaskMessage::Run).await?;
//     sink_actor.send(StreamTaskMessage::Run).await?;
    
//     // Let the tasks run for a while
//     sleep(Duration::from_secs(5)).await;
    
//     // Close tasks and transport backend
//     source_actor.send(StreamTaskMessage::Close).await?;
//     sink_actor.send(StreamTaskMessage::Close).await?;
//     backend_actor.send(TransportBackendMessage::Close).await?;
    
//     Ok(())
// } 