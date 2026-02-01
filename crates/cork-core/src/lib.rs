//! CORK Core - gRPC server and execution engine (Kernel).
//!
//! This crate provides the main CORK server implementation including:
//! - gRPC API handlers for CorkCore service
//! - Run/Stage/Node state machine
//! - GraphPatch validation and application
//! - Scheduler (LIST_HEURISTIC)
//! - Worker client for tool invocation
//!
//! # Implementation Status
//!
//! This is the skeleton implementation. Full implementation will be added in subsequent tasks.

pub mod api;
