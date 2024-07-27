use pyo3::{pyclass, pymethods, types::PyBytes, Python};
use rmp::decode::bytes;

use super::{channel::Channel, data_reader::{self, DataReader}};

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
struct PyDataReader {
    data_reader: DataReader
}


#[pymethods]
impl PyDataReader {
    #[new]
    pub fn new(name: String, channels: Vec<PyLocalChannel>) -> PyDataReader { // TODO we need to pass generic PyChannel
        let mut rust_channels = Vec::new();
        for ch in channels {
            rust_channels.push(ch.to_rust_channel());
        };
        let data_reader = DataReader::new(name.clone(), rust_channels);
        PyDataReader{data_reader}
    }

    pub fn start(&mut self) {
        self.data_reader.start()
    }

    pub fn close(&mut self) {
        self.data_reader.close()
    }

    // pub fn read_bytes(&self, py: Python) -> Option<PyBytes> {
    //     let bytes = self.data_reader.read_bytes();
    //     if !bytes.is_none() {
    //         Ok(PyBytes::new(py, bytes.unwrap().as_ptr()).into())
    //     } else {
    //         None
    //     }
    // }

}


#[pyclass(name="RustDataWriter")]
struct PyDataWriter {

}

#[pymethods]
impl PyDataWriter {

}

#[pyclass(name="RustIOLoop")]
struct PyIOLoop {

}

#[pymethods]
impl PyIOLoop {

}