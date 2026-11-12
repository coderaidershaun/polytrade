//! Polymarket signing: L1 proves wallet control (enabling API credential minting), L2 authenticates
//! private HTTP requests, and Order authorizes trades on chain. Three independent signatures, each
//! over different payloads and key material.

pub mod address;
pub mod amount;
pub mod eip712;
pub mod hex;
pub mod key;
pub mod l1;
pub mod l2;
pub mod order;

const ADDRESS_HEADER: &str = "POLY_ADDRESS";
const SIGNATURE_HEADER: &str = "POLY_SIGNATURE";
const TIMESTAMP_HEADER: &str = "POLY_TIMESTAMP";
