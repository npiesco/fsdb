//! Security & Access Control Module
//!
//! Provides enterprise-grade security features:
//! - User authentication with bcrypt password hashing
//! - Role-based access control (RBAC)
//! - Audit logging
//! - Permission enforcement

pub mod auth;
pub mod rbac;
pub mod audit;

pub use auth::{User, UserStore, Credentials, AuthContext};
pub use rbac::{Role, Permission, RoleManager};
pub use audit::{AuditLog, AuditEntry, AuditLogger};

