//! gRPC API handlers for CorkCore service.
//!
//! This module contains the implementation of the CorkCore gRPC service
//! as defined in `proto/cork/v1/core.proto`.

pub mod core_service;

pub use core_service::CorkCoreService;
