use super::{data_reader::DataReaderConfig, data_writer::DataWriterConfig, io_loop::ZmqConfig, remote_transfer_handler::TransferConfig};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct NetworkConfig {
    pub data_reader: DataReaderConfig,
    pub data_writer: DataWriterConfig,
    pub transfer: TransferConfig,
    pub zmq: Option<ZmqConfig>,
}

impl NetworkConfig {

    pub fn new(yaml_path: &str) -> NetworkConfig {
        let file = std::fs::File::open(yaml_path).unwrap();
        let network_config: NetworkConfig = serde_yaml::from_reader(file).unwrap();
        network_config
    }
}