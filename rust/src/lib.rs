use pyo3::prelude::*;
pub mod network;
pub mod newtork_v2;
use network::{data_reader::DataReaderConfig, data_writer::DataWriterConfig, io_loop::ZmqConfig, py_interface::*, remote_transfer_handler::TransferConfig};

#[pymodule]
fn volga_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyLocalChannel>()?;
    m.add_class::<PyRemoteChannel>()?;
    m.add_class::<PyDataReader>()?;
    m.add_class::<PyDataWriter>()?;
    m.add_class::<PyTransferReceiver>()?;
    m.add_class::<PyTransferSender>()?;
    m.add_class::<PyIOLoop>()?;
    m.add_class::<DataReaderConfig>()?;
    m.add_class::<DataWriterConfig>()?;
    m.add_class::<TransferConfig>()?;
    m.add_class::<ZmqConfig>()?;
    Ok(())
}

