use crate::transport::transport::{DataReader, DataWriter, TransportConfig, DataReaderConfig, DataWriterConfig, Transport};
use crate::transport::implementations::dummy_transport::{DummyDataReader, DummyDataWriter};
use crate::transport::implementations::network_transport::{NetworkDataReader, NetworkDataWriter};
use std::sync::Arc;

pub fn create_transport(config: TransportConfig) -> Transport {
    let reader = match &config.reader_config {
        DataReaderConfig::Dummy => Some(Box::new(DummyDataReader::new()) as Box<dyn DataReader>),
        DataReaderConfig::Network { output_queue_capacity_bytes, response_batch_period_ms } => {
            Some(Box::new(NetworkDataReader::new(
                config.id.clone(),
                config.name.clone(),
                config.job_name.clone(),
                config.channels.clone(),
                DataReaderConfig::Network {
                    output_queue_capacity_bytes: *output_queue_capacity_bytes,
                    response_batch_period_ms: *response_batch_period_ms,
                }
            )) as Box<dyn DataReader>)
        }
    };

    let writer = match &config.writer_config {
        DataWriterConfig::Dummy => Some(Box::new(DummyDataWriter::new()) as Box<dyn DataWriter>),
        DataWriterConfig::Network { in_flight_timeout_s, max_capacity_bytes_per_channel } => {
            Some(Box::new(NetworkDataWriter::new(
                config.id.clone(),
                config.name.clone(),
                config.job_name.clone(),
                config.channels.clone(),
                DataWriterConfig::Network {
                    in_flight_timeout_s: *in_flight_timeout_s,
                    max_capacity_bytes_per_channel: *max_capacity_bytes_per_channel,
                }
            )) as Box<dyn DataWriter>)
        }
    };

    Transport { reader, writer }
} 