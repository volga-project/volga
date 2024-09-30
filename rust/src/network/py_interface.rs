use std::{any::Any, borrow::{Borrow, BorrowMut}, hash::Hash, sync::{Arc, RwLock}};

use pyo3::{pyclass, pymethods, types::{PyBytes, PyTuple}, IntoPy, Py, PyAny, PyResult, PyTryFrom, Python};

use super::{channel::Channel, data_reader::{self, DataReader, DataReaderConfig}, data_writer::{DataWriter, DataWriterConfig}, io_loop::{Direction, IOHandler, IOLoop, ZmqConfig}, remote_transfer_handler::{RemoteTransferHandler, TransferConfig}};

pub trait ToRustChannel {
    fn to_rust_channel(&self) -> Channel;
}

#[derive(Clone)]
#[pyclass(name="RustLocalChannel")]
pub struct PyLocalChannel {
    #[pyo3(get, set)]
    channel_id: String,
    #[pyo3(get, set)]
    ipc_addr: String
}

#[pymethods]
impl PyLocalChannel {

    #[new]
    pub fn new(channel_id: String, ipc_addr: String) -> Self {
        PyLocalChannel{channel_id: channel_id.clone(), ipc_addr: ipc_addr.clone()}
    }
}

impl ToRustChannel for PyLocalChannel {
    
    fn to_rust_channel(&self) -> Channel {
        Channel::Local {channel_id: self.channel_id.clone(), ipc_addr: self.ipc_addr.clone()}
    }
}

#[derive(Clone)]
#[pyclass(name="RustRemoteChannel")]
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
    ) -> Self {
        PyRemoteChannel{
            channel_id: channel_id.clone(), 
            source_local_ipc_addr: source_local_ipc_addr.clone(), 
            source_node_ip: source_node_ip.clone(), 
            source_node_id: source_node_id.clone(),
            target_local_ipc_addr: target_local_ipc_addr.clone(), 
            target_node_ip: target_node_ip.clone(), 
            target_node_id: target_node_id.clone(), 
            port: port.clone()
        }
    }
}


#[pyclass(name="RustDataReader")]
pub struct PyDataReader {
    data_reader: Arc<DataReader>
}


#[pymethods]
impl PyDataReader {

    #[new]
    pub fn new(name: String, job_name: String, config: &DataReaderConfig, channels: Vec<&PyAny>) -> PyDataReader {
        let mut rust_channels = Vec::new();
        for ch in channels {
            let ext: Result<PyLocalChannel, pyo3::PyErr> = ch.extract();
            if ext.is_ok() {
                rust_channels.push(ext.unwrap().to_rust_channel());
            } else {
                let ext: Result<PyRemoteChannel, pyo3::PyErr> = ch.extract();
                rust_channels.push(ext.unwrap().to_rust_channel());
            }
        };
        let data_reader = DataReader::new(name, job_name, config.clone(), rust_channels);
        PyDataReader{data_reader: Arc::new(data_reader)}
    }

    pub fn start(&self) {
        (self.data_reader.clone() as Arc<dyn IOHandler>).start();
    }

    pub fn close(&self) {
        (self.data_reader.clone() as Arc<dyn IOHandler>).close();
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
    pub fn new(name: String, job_name: String, config: &DataWriterConfig, channels: Vec<&PyAny>) -> PyDataWriter {
        let mut rust_channels = Vec::new();
        for ch in channels {
            let ext: Result<PyLocalChannel, pyo3::PyErr> = ch.extract();
            if ext.is_ok() {
                rust_channels.push(ext.unwrap().to_rust_channel());
            } else {
                let ext: Result<PyRemoteChannel, pyo3::PyErr> = ch.extract();
                rust_channels.push(ext.unwrap().to_rust_channel());
            }
        };
        let data_writer = DataWriter::new(name, job_name, config.clone(), rust_channels);
        PyDataWriter{data_writer: Arc::new(data_writer)}
    }

    pub fn start(&self) {
        self.data_writer.start();
    }

    pub fn close(&self) {
        self.data_writer.close();
    }

    pub fn write_bytes(&self, channel_id: String, b: &PyBytes, _noop1: bool, timeout_ms: u128, _noop2: u128) -> Option<u128> {
        let bytes = b.as_bytes().to_vec();
        self.data_writer.write_bytes(&channel_id, Box::new(bytes), timeout_ms)
    }
}


#[pyclass(name="RustTransferSender")]
pub struct PyTransferSender {
    transfer_sender: Arc<RemoteTransferHandler>
}


#[pymethods]
impl PyTransferSender {
    #[new]
    pub fn new(name: String, job_name: String, config: &TransferConfig, channels: Vec<PyRemoteChannel>) -> PyTransferSender {
        let mut rust_channels = Vec::new();
        for ch in channels {
            rust_channels.push(ch.to_rust_channel());
        };
        let transfer_sender = RemoteTransferHandler::new(name, job_name, rust_channels, config.clone(), Direction::Sender);
        PyTransferSender{transfer_sender: Arc::new(transfer_sender)}
    }

    pub fn start(&self) {
        self.transfer_sender.start();
    }

    pub fn close(&self) {
        self.transfer_sender.close();
    }
}

#[pyclass(name="RustTransferReceiver")]
pub struct PyTransferReceiver {
    transfer_receiver: Arc<RemoteTransferHandler>
}


#[pymethods]
impl PyTransferReceiver {
    #[new]
    pub fn new(name: String, job_name: String, config: &TransferConfig, channels: Vec<PyRemoteChannel>) -> PyTransferReceiver {
        let mut rust_channels = Vec::new();
        for ch in channels {
            rust_channels.push(ch.to_rust_channel());
        };
        let transfer_receiver = RemoteTransferHandler::new(name, job_name, rust_channels, config.clone(), Direction::Receiver);
        PyTransferReceiver{transfer_receiver: Arc::new(transfer_receiver)}
    }

    pub fn start(&self) {
        self.transfer_receiver.start();
    }

    pub fn close(&self) {
        self.transfer_receiver.close();
    }
}

#[pyclass(name="RustIOLoop")]
pub struct PyIOLoop {
    io_loop: IOLoop,
}

#[pymethods]
impl PyIOLoop {

    #[new]
    pub fn new(name: String, zmq_config: Option<ZmqConfig>) -> PyIOLoop {
        PyIOLoop{
            io_loop: IOLoop::new(name, zmq_config),
        }
    }

    pub fn register_data_writer(&self, dw: &PyDataWriter) {
        self.io_loop.register_handler(dw.data_writer.clone());
    }

    pub fn register_data_reader(&self, dr: &PyDataReader) {
        self.io_loop.register_handler(dr.data_reader.clone());
    }

    pub fn register_transfer_sender(&self, ts: &PyTransferSender) {
        self.io_loop.register_handler(ts.transfer_sender.clone());
    }

    pub fn register_transfer_receiver(&self, tr: &PyTransferReceiver) {
        self.io_loop.register_handler(tr.transfer_receiver.clone());
    }

    pub fn connect(&self, num_io_threads: usize, timeout_ms: u128) -> Option<String> {
        self.io_loop.connect(num_io_threads, timeout_ms)
    }

    pub fn start(&self) {
        self.io_loop.start()
    }

    pub fn close(&self) {
        self.io_loop.close()
    }
}