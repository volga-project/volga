use std::{borrow::{Borrow, BorrowMut}, hash::Hash, sync::{Arc, RwLock}};

use pyo3::{pyclass, pymethods, types::PyBytes, IntoPy, Py, PyResult, PyTryFrom, Python};
use rmp::decode::bytes;

use super::{channel::Channel, data_reader::{self, DataReader}, data_writer::DataWriter, io_loop::IOLoop};

pub trait ToRustChannel {
    fn to_rust_channel(&self) -> Channel;
}

#[derive(Clone)]
#[pyclass(extends=PyChannel, name="LocalChannel")]
pub struct PyLocalChannel {
    #[pyo3(get, set)]
    channel_id: String,
    #[pyo3(get, set)]
    ipc_addr: String
}

#[pymethods]
impl PyLocalChannel {

    #[new]
    pub fn new(channel_id: String, ipc_addr: String) -> (Self, PyChannel) {
        (PyLocalChannel{channel_id: channel_id.clone(), ipc_addr: ipc_addr.clone()}, PyChannel{channel_id: channel_id.clone()})
    }
}

impl ToRustChannel for PyLocalChannel {
    
    fn to_rust_channel(&self) -> Channel {
        Channel::Local {channel_id: self.channel_id.clone(), ipc_addr: self.ipc_addr.clone()}
    }
}

#[derive(Clone)]
#[pyclass(extends=PyChannel, name="RemoteChannel")]
pub struct PyRemoteChannel {
    #[pyo3(get, set)]
    channel_id: String,
    #[pyo3(get, set)]
    source_local_ipc_addr: String,
    #[pyo3(get, set)]
    source_node_ip: String,
    #[pyo3(get, set)]
    source_node_id: String,
    #[pyo3(get, set)]
    target_local_ipc_addr: String,
    #[pyo3(get, set)]
    target_node_ip: String,
    #[pyo3(get, set)]
    target_node_id: String,
    #[pyo3(get, set)]
    port: i32,
}

impl ToRustChannel for PyRemoteChannel {
    
    fn to_rust_channel(&self) -> Channel {
        Channel::Remote {
            channel_id: self.channel_id.clone(), 
            source_local_ipc_addr: self.source_local_ipc_addr.clone(), 
            source_node_ip: self.source_node_ip.clone(), 
            source_node_id: self.source_node_id.clone(), 
            target_local_ipc_addr: self.target_local_ipc_addr.clone(), 
            target_node_ip: self.target_node_ip.clone(), 
            target_node_id: self.target_node_id.clone(), 
            port: self.port.clone() 
        }
    }
}

#[pymethods]
impl PyRemoteChannel {

    #[new]
    pub fn new(
        channel_id: String,
        source_local_ipc_addr: String,
        source_node_ip: String,
        source_node_id: String,
        target_local_ipc_addr: String,
        target_node_ip: String,
        target_node_id: String,
        port: i32,
    ) -> (Self, PyChannel) {
        (PyRemoteChannel{
            channel_id: channel_id.clone(), 
            source_local_ipc_addr: source_local_ipc_addr.clone(), 
            source_node_ip: source_node_ip.clone(), 
            source_node_id: source_node_id.clone(),
            target_local_ipc_addr: target_local_ipc_addr.clone(), 
            target_node_ip: target_node_ip.clone(), 
            target_node_id: target_node_id.clone(), 
            port: port.clone()
        }, 
        PyChannel{channel_id: channel_id.clone()})
    }
}

#[derive(Clone)]
#[pyclass(name="Channel", subclass)]
pub struct PyChannel {
    #[pyo3(get, set)]
    channel_id: String,
}

#[pymethods]
impl PyChannel {

    #[new]
    pub fn new(channel_id: String) -> PyChannel {
        PyChannel{channel_id}
    }
}

// impl ToRustChannel for PyChannel {
//     fn to_rust_channel(&self) -> Channel {
//         // TODO
//     }
// }


#[pyclass(name="RustDataReader")]
pub struct PyDataReader {
    data_reader: Arc<DataReader>
}


#[pymethods]
impl PyDataReader {

    #[new]
    pub fn new(name: String, channels: Vec<PyLocalChannel>) -> PyDataReader { // TODO we need to pass generic PyChannel
        let mut rust_channels = Vec::new();
        for ch in channels {
            rust_channels.push(ch.to_rust_channel());
        };
        let data_reader = DataReader::new(name, rust_channels);
        PyDataReader{data_reader: Arc::new(data_reader)}
    }

    pub fn start(&self) {
        self.data_reader.start();
    }

    pub fn close(&self) {
        self.data_reader.close();
    }

    pub fn read_bytes(&self, py: Python) -> Option<Py<PyBytes>>{
        let bytes = self.data_reader.read_bytes();
        if !bytes.is_none() {
            let bytes = bytes.unwrap();
            let pb = PyBytes::new(py, bytes.as_slice());
            Some(pb.into())
        } else {
            None
        }
    }
}


#[pyclass(name="RustDataWriter")]
pub struct PyDataWriter {
    data_writer: Arc<DataWriter>
}

#[pymethods]
impl PyDataWriter {

    #[new]
    pub fn new(name: String, channels: Vec<PyLocalChannel>) -> PyDataWriter { // TODO we need to pass generic PyChannel
        let mut rust_channels = Vec::new();
        for ch in channels {
            rust_channels.push(ch.to_rust_channel());
        };
        let data_writer = DataWriter::new(name, rust_channels);
        PyDataWriter{data_writer: Arc::new(data_writer)}
    }

    pub fn start(&self) {
        self.data_writer.start();
    }

    pub fn close(&self) {
        self.data_writer.close();
    }

    pub fn write_bytes(&self, channel_id: String, b: &PyBytes, timeout_ms: i32, retry_step_micros: u64) -> Option<u128> {
        let bytes = b.as_bytes().to_vec();
        self.data_writer.write_bytes(&channel_id, Box::new(bytes), timeout_ms, retry_step_micros)
    }
}

#[pyclass(name="RustIOLoop")]
pub struct PyIOLoop {
    io_loop: IOLoop,
}

#[pymethods]
impl PyIOLoop {

    #[new]
    pub fn new() -> PyIOLoop {
        PyIOLoop{
            io_loop: IOLoop::new(),
        }
    }

    pub fn register_data_writer(&self, dw: &PyDataWriter) {
        self.io_loop.register_handler(dw.data_writer.clone());
    }

    pub fn register_data_reader(&self, dr: &PyDataReader) {
        self.io_loop.register_handler(dr.data_reader.clone());
    }

    pub fn start(&self, num_io_threads: usize) {
        self.io_loop.start_io_threads(num_io_threads)
    }

    pub fn close(&self) {
        self.io_loop.close()
    }

}