// aqap-kernel/src/crypto.rs
use pyo3::prelude::*;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::hmac;
use ring::hkdf::{KeyType, Salt, HKDF_SHA256};
use ring::rand::{SecureRandom, SystemRandom};

use crate::error::{ErrorCode, KernelError};

// ── Helper: arbitrary-length OKM for ring 0.17 HKDF ──

/// A length wrapper implementing ring::hkdf::KeyType for arbitrary output sizes.
struct HkdfLen(usize);

impl KeyType for HkdfLen {
    fn len(&self) -> usize {
        self.0
    }
}

// ── Core crypto primitives (non-PyO3) ──

/// HKDF key derivation
/// Derives a key of `len` bytes from the master key using the given salt and info.
pub fn derive_key(master: &[u8], salt_bytes: &[u8], info: &[u8], len: usize) -> Vec<u8> {
    let salt = Salt::new(HKDF_SHA256, salt_bytes);
    let prk = salt.extract(master);
    let info_slice = [info];
    let okm = prk
        .expand(&info_slice, HkdfLen(len))
        .expect("HKDF expand should not fail for reasonable lengths");
    let mut out = vec![0u8; len];
    okm.fill(&mut out)
        .expect("HKDF fill should not fail for reasonable lengths");
    out
}

/// AES-256-GCM encrypt
/// Returns: nonce(12B) + ciphertext+tag
pub fn encrypt(key: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, KernelError> {
    let unbound =
        UnboundKey::new(&AES_256_GCM, key).map_err(|e| KernelError::Security {
            code: ErrorCode::DecryptFailed,
            detail: format!("invalid key: {}", e),
        })?;
    let key = LessSafeKey::new(unbound);

    let rng = SystemRandom::new();
    let mut nonce_bytes = [0u8; 12];
    rng.fill(&mut nonce_bytes).map_err(|e| KernelError::Security {
        code: ErrorCode::DecryptFailed,
        detail: format!("RNG error: {}", e),
    })?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);

    let mut in_out = plaintext.to_vec();
    key.seal_in_place_append_tag(nonce, Aad::from(aad), &mut in_out)
        .map_err(|e| KernelError::Security {
            code: ErrorCode::DecryptFailed,
            detail: format!("encrypt: {}", e),
        })?;

    // Prepend nonce
    let mut result = Vec::with_capacity(12 + in_out.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&in_out);
    Ok(result)
}

/// AES-256-GCM decrypt
/// Expects: nonce(12B) + ciphertext+tag
pub fn decrypt(key: &[u8], data: &[u8], aad: &[u8]) -> Result<Vec<u8>, KernelError> {
    if data.len() < 28 {
        // 12 nonce + 16 tag minimum
        return Err(KernelError::Security {
            code: ErrorCode::DecryptFailed,
            detail: "ciphertext too short".into(),
        });
    }

    let unbound =
        UnboundKey::new(&AES_256_GCM, key).map_err(|e| KernelError::Security {
            code: ErrorCode::DecryptFailed,
            detail: format!("invalid key: {}", e),
        })?;
    let key = LessSafeKey::new(unbound);

    let nonce = Nonce::assume_unique_for_key(data[..12].try_into().unwrap());
    let mut in_out = data[12..].to_vec();

    let plaintext = key.open_in_place(nonce, Aad::from(aad), &mut in_out)
        .map_err(|e| KernelError::Security {
            code: ErrorCode::DecryptFailed,
            detail: format!("decrypt: {}", e),
        })?;

    Ok(plaintext.to_vec())
}

/// HMAC-SHA256 sign
pub fn sign(key: &[u8], data: &[u8]) -> Vec<u8> {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&s_key, data);
    tag.as_ref().to_vec()
}

/// HMAC-SHA256 verify (constant-time)
pub fn verify(key: &[u8], data: &[u8], sig: &[u8]) -> bool {
    let s_key = hmac::Key::new(hmac::HMAC_SHA256, key);
    hmac::verify(&s_key, data, sig).is_ok()
}

// ── PyO3 exported functions ──
// These wrap the core primitives, converting KernelError → PyValueError.

#[pyfunction]
pub fn hkdf_derive(master: Vec<u8>, salt: Vec<u8>, info: Vec<u8>, len: usize) -> Vec<u8> {
    derive_key(&master, &salt, &info, len)
}

#[pyfunction]
pub fn encrypt_payload(key: Vec<u8>, plaintext: Vec<u8>, aad: Vec<u8>) -> PyResult<Vec<u8>> {
    encrypt(&key, &plaintext, &aad)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))
}

#[pyfunction]
pub fn decrypt_payload(key: Vec<u8>, data: Vec<u8>, aad: Vec<u8>) -> PyResult<Vec<u8>> {
    decrypt(&key, &data, &aad)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))
}

#[pyfunction]
pub fn sign_envelope(key: Vec<u8>, data: Vec<u8>) -> Vec<u8> {
    sign(&key, &data)
}

#[pyfunction]
pub fn verify_envelope(key: Vec<u8>, data: Vec<u8>, sig: Vec<u8>) -> bool {
    verify(&key, &data, &sig)
}

// ── SecurityContext ──

/// SecurityContext holds a key hierarchy derived from a master key.
///
/// On `load()`, it derives three 32-byte keys via HKDF:
///   - encrypt_key: AES-256-GCM payload encryption
///   - sign_key: HMAC-SHA256 envelope signing
///   - route_key: HMAC-SHA256 route/topic signing
#[pyclass]
pub struct SecurityContext {
    master_key: Vec<u8>,
    encrypt_key: Option<Vec<u8>>,
    sign_key: Option<Vec<u8>>,
    route_key: Option<Vec<u8>>,
}

#[pymethods]
impl SecurityContext {
    #[new]
    fn new(master_key: Vec<u8>) -> Self {
        SecurityContext {
            master_key,
            encrypt_key: None,
            sign_key: None,
            route_key: None,
        }
    }

    /// Derive the three operational keys from the master key.
    /// Must be called before encrypt/decrypt/sign/verify.
    fn load(&mut self) -> PyResult<()> {
        self.encrypt_key = Some(derive_key(
            &self.master_key,
            b"aqap-v3-payload",
            b"encrypt",
            32,
        ));
        self.sign_key = Some(derive_key(
            &self.master_key,
            b"aqap-v3-envelope",
            b"sign",
            32,
        ));
        self.route_key = Some(derive_key(
            &self.master_key,
            b"aqap-v3-route",
            b"route",
            32,
        ));
        Ok(())
    }

    /// Encrypt plaintext with AAD. Returns nonce(12B) + ciphertext+tag.
    fn encrypt(&self, plaintext: Vec<u8>, aad: &str) -> PyResult<Vec<u8>> {
        let key = self.encrypt_key.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("SecurityContext not loaded")
        })?;
        encrypt(key, &plaintext, aad.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))
    }

    /// Decrypt ciphertext (nonce(12B) + ciphertext+tag) with AAD.
    fn decrypt(&self, data: Vec<u8>, aad: &str) -> PyResult<Vec<u8>> {
        let key = self.encrypt_key.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("SecurityContext not loaded")
        })?;
        decrypt(key, &data, aad.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e)))
    }

    /// Sign data with HMAC-SHA256. Returns 32-byte signature.
    fn sign(&self, data: Vec<u8>) -> PyResult<Vec<u8>> {
        let key = self.sign_key.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("SecurityContext not loaded")
        })?;
        Ok(sign(key, &data))
    }

    /// Verify signature with constant-time comparison.
    fn verify(&self, data: Vec<u8>, sig: Vec<u8>) -> PyResult<bool> {
        let key = self.sign_key.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("SecurityContext not loaded")
        })?;
        Ok(verify(key, &data, &sig))
    }

    /// Whether the context has been loaded with derived keys.
    #[getter]
    fn is_loaded(&self) -> bool {
        self.encrypt_key.is_some()
    }

    /// Sign a topic string with the route key for topic tampering prevention.
    fn sign_route(&self, topic: &str) -> PyResult<Vec<u8>> {
        let key = self.route_key.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("SecurityContext not loaded")
        })?;
        Ok(sign(key, topic.as_bytes()))
    }

    /// Verify a route signature.
    fn verify_route(&self, topic: &str, sig: Vec<u8>) -> PyResult<bool> {
        let key = self.route_key.as_ref().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("SecurityContext not loaded")
        })?;
        Ok(verify(key, topic.as_bytes(), &sig))
    }
}
