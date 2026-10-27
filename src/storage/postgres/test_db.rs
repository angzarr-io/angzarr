//! A fresh, migrated Postgres database per test.
//!
//! The tests run against the server named by `ANGZARR_TEST_POSTGRES_URL`
//! (`just test-postgres` starts a throwaway `postgres:16` container and sets
//! it). Each call creates a new database on that server, so tests never see
//! each other's rows. Without the variable the tests fail rather than pass
//! vacuously.

use sqlx::postgres::PgPool;
use sqlx::Connection;

const URL_VAR: &str = "ANGZARR_TEST_POSTGRES_URL";

/// The server URL with its path replaced by `database`.
fn with_database(url: &str, database: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let server = match base.rfind('/') {
        Some(i) if i > "postgres://".len() => &base[..i],
        _ => base,
    };
    match query {
        Some(q) => format!("{server}/{database}?{q}"),
        None => format!("{server}/{database}"),
    }
}

/// Create an empty database and return its URL.
pub(crate) async fn fresh_database() -> String {
    let url = std::env::var(URL_VAR).unwrap_or_else(|_| {
        panic!("{URL_VAR} is not set: run these tests with `just test-postgres`")
    });
    let name = format!("t_{}", uuid::Uuid::new_v4().simple());
    let mut admin = sqlx::PgConnection::connect(&url).await.unwrap();
    sqlx::query(&format!("CREATE DATABASE {name}"))
        .execute(&mut admin)
        .await
        .unwrap();
    with_database(&url, &name)
}

/// A pool on a fresh database with the baseline schema migrated.
pub(crate) async fn fresh_pool() -> PgPool {
    super::postgres_pool(&fresh_database().await).await.unwrap()
}
