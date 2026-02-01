//! CORK Protocol Buffer generated code.
//!
//! This crate contains the generated Rust code from the CORK gRPC protocol definitions.

pub mod cork {
    pub mod v1 {
        tonic::include_proto!("cork.v1");
    }
}

// Re-export commonly used types at the crate root for convenience
pub use cork::v1::*;
