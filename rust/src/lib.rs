use pyo3::prelude::*;
pub mod network;
use network::py_interface::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}


#[pymodule]
fn volga_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_class::<PyChannel>()?;
    m.add_class::<PyLocalChannel>()?;
    m.add_class::<PyRemoteChannel>()?;
    Ok(())
}

