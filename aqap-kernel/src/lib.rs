// aqap-kernel/src/lib.rs
use pyo3::prelude::*;

mod types;
mod error;
mod wire;
mod crypto;
mod schema;
mod router;

/// AQAP v3 Protocol Kernel — Python module
#[pymodule]
fn aqap_kernel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.1.0")?;
    m.add("MAGIC", types::MAGIC)?;
    m.add("HEADER_SIZE", types::HEADER_SIZE)?;

    // Wire format
    m.add_class::<wire::WireHeader>()?;
    m.add_function(wrap_pyfunction!(wire::wire_header_encode, m)?)?;
    m.add_function(wrap_pyfunction!(wire::wire_header_decode, m)?)?;

    // Wire message
    m.add_class::<wire::WireMessage>()?;
    m.add_function(wrap_pyfunction!(wire::wire_message_encode, m)?)?;
    m.add_function(wrap_pyfunction!(wire::wire_message_decode, m)?)?;

    Ok(())
}
