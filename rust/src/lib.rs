use pyo3::prelude::*;
pub mod network;
use network::py_interface::*;

#[pymodule]
fn volga_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyLocalChannel>()?;
    m.add_class::<PyRemoteChannel>()?;
    m.add_class::<PyDataReader>()?;
    m.add_class::<PyDataWriter>()?;
    m.add_class::<PyTransferReceiver>()?;
    m.add_class::<PyTransferSender>()?;
    m.add_class::<PyIOLoop>()?;
    Ok(())
}

