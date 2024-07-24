use pyo3::prelude::*;
pub mod network;
use network::socket_meta::ipc_path_from_addr;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn test_function() -> PyResult<String> {
    let s = ipc_path_from_addr(&String::from("test"));
    Ok(s)
}

// / A Python module implemented in Rust.
#[pymodule]
fn volga_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

