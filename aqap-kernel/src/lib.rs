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
    Ok(())
}
