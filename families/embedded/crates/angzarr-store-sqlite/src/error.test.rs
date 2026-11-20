use angzarr_ports::{Arg, StoreError};

use super::{causes, damaged, unavailable, Damage, SqlError};
use angzarr_ports::angzarr_diagnostic::code;

/// An error's `Display` is its code, then its args (` name=value` each),
/// never English of angzarr's (decision 14.37): text is rendered only
/// where it is shown, from the catalog. A third party's text (the
/// driver's) is carried verbatim as `ext_err_txt`, quoted where it would
/// not parse back (holding whitespace, `=`, `"` or `\`, or empty), its
/// `"` and `\` escaped.
#[test]
fn an_error_displays_its_code_and_args_never_english() {
    assert_eq!(
        SqlError::BackupUncreatable {
            path: "/p/x".to_string(),
            detail: "Permission denied".to_string()
        }
        .to_string(),
        concat!(
            code!("backup-uncreatable"),
            " path=/p/x ext_err_txt=\"Permission denied\""
        )
    );
    let unavailable = SqlError::Unavailable {
        errno: None,
        detail: "disk I/O error".to_string(),
    }
    .to_string();
    assert_eq!(
        unavailable,
        concat!(code!("driver-failed"), " ext_err_txt=\"disk I/O error\"")
    );
    assert!(
        !unavailable.contains("the database failed"),
        "{unavailable}"
    );
    assert_eq!(
        SqlError::Undecodable {
            detail: "bad \"utf8\" at C:\\db".to_string()
        }
        .to_string(),
        concat!(
            code!("undecodable"),
            " ext_err_txt=\"bad \\\"utf8\\\" at C:\\\\db\""
        )
    );
    assert_eq!(
        SqlError::Undecodable {
            detail: String::new()
        }
        .to_string(),
        concat!(code!("undecodable"), " ext_err_txt=\"\"")
    );
    assert_eq!(
        SqlError::Undecodable {
            detail: "k=v".to_string()
        }
        .to_string(),
        concat!(code!("undecodable"), " ext_err_txt=\"k=v\"")
    );
    assert_eq!(
        SqlError::Undecodable {
            detail: "plain".to_string()
        }
        .to_string(),
        concat!(code!("undecodable"), " ext_err_txt=plain")
    );
}

#[test]
fn damage_shows_the_first_problem_and_counts_the_rest() {
    let damage = |problems: &[&str]| Damage {
        problems: problems.iter().map(|p| p.to_string()).collect(),
    };
    let one = damage(&["row 1 missing from index i"]);
    assert_eq!(
        one.to_string(),
        concat!(
            code!("damaged"),
            " first=\"row 1 missing from index i\" more=0"
        )
    );
    assert_eq!(one.code(), code!("damaged"));
    let args = one.args();
    assert_eq!(args.len(), 2);
    assert_eq!(
        args[0],
        ("first", Arg::Str("row 1 missing from index i".to_string()))
    );
    assert_eq!(args[1], ("more", Arg::Uint(0)));

    let two = damage(&["a", "b"]);
    assert_eq!(
        two.to_string(),
        concat!(code!("damaged"), " first=a more=1")
    );
    assert_eq!(two.args()[1], ("more", Arg::Uint(1)));
    let three = damage(&["a", "b", "c"]);
    assert_eq!(
        three.to_string(),
        concat!(code!("damaged"), " first=a more=2")
    );
    assert_eq!(three.problems(), ["a", "b", "c"]);
}

/// A failure reaches the ports as angzarr-ports' `StoreError`, this crate's
/// code its `Cause`; SQLite's primary result code is its `errno` and its
/// text, verbatim, its `ext_err_txt`, never wrapped in English of
/// angzarr's (the owner's ruling, 2026-10-29).
#[test]
fn a_failure_reaches_the_ports_with_the_drivers_code_and_text() {
    let locked = SqlError::Unavailable {
        errno: Some(5),
        detail: "database is locked".to_string(),
    };
    let StoreError::Unavailable(cause) = StoreError::Unavailable(locked.cause()) else {
        panic!("unavailable");
    };
    assert_eq!(cause.code, code!("unavailable"));
    assert_eq!(
        cause.args,
        [
            ("errno", Arg::Uint(5)),
            ("ext_err_txt", Arg::Str("database is locked".to_string())),
        ]
    );
    assert_eq!(
        locked.to_string(),
        concat!(
            code!("unavailable"),
            " errno=5 ext_err_txt=\"database is locked\""
        )
    );
    let StoreError::Unavailable(cause) = unavailable("pool timed out") else {
        panic!("unavailable");
    };
    assert_eq!(
        cause.code,
        code!("driver-failed"),
        "the driver failed before SQLite answered: a key of its own, whose \
         message names no errno"
    );
    assert_eq!(
        cause.args,
        [("ext_err_txt", Arg::Str("pool timed out".to_string()))],
        "no errno where the driver gave none"
    );
    let StoreError::Unavailable(cause) = damaged(26, "file is not a database".to_string()) else {
        panic!("unavailable");
    };
    assert_eq!(cause.code, code!("damaged-file"));
    assert_eq!(
        cause.args,
        [
            ("errno", Arg::Uint(26)),
            (
                "ext_err_txt",
                Arg::Str("file is not a database".to_string())
            ),
        ]
    );
}

#[test]
fn every_cause_has_this_crates_code_once() {
    let codes: Vec<&str> = causes().iter().map(|c| c.code).collect();
    assert_eq!(
        codes,
        [
            code!("unavailable"),
            code!("driver-failed"),
            code!("undecodable"),
            code!("backup-uncreatable"),
            code!("damaged-file"),
            code!("no-data-version"),
            code!("bad-root"),
            code!("bad-dead-letter-place"),
        ]
    );
    assert_eq!(
        SqlError::BadRoot { length: 3 }.cause().args,
        [("length", Arg::Uint(3))],
        "a cause carries its args (decision 14.37)"
    );
}

/// SQLite's own error, as a cause: its primary result code (the low byte
/// of the extended code the driver reports) as `errno`, its text verbatim
/// as `ext_err_txt`, whether the driver's error comes alone or inside the
/// migrator's.
#[tokio::test]
async fn sqlites_error_carries_its_primary_code_and_its_text() {
    use sqlx::{ConnectOptions, Executor};
    use std::str::FromStr;
    let mut conn = sqlx::sqlite::SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .connect()
        .await
        .unwrap();
    conn.execute("CREATE TABLE t (x INTEGER UNIQUE); INSERT INTO t VALUES (1);")
        .await
        .unwrap();
    let insert = "INSERT INTO t VALUES (1)";
    let error = sqlx::query(insert)
        .execute(&mut conn)
        .await
        .expect_err("a second 1 violates UNIQUE");
    let text = error
        .as_database_error()
        .expect("SQLite's")
        .message()
        .to_string();
    // SQLITE_CONSTRAINT_UNIQUE is 2067; its primary code, SQLITE_CONSTRAINT, 19.
    let expected = vec![
        ("errno", Arg::Uint(19)),
        ("ext_err_txt", Arg::Str(text.clone())),
    ];
    let StoreError::Unavailable(cause) = super::unavailable(error) else {
        panic!("unavailable");
    };
    assert_eq!(cause.args, expected);
    let again = sqlx::query(insert)
        .execute(&mut conn)
        .await
        .expect_err("still a second 1");
    let migrating = sqlx::migrate::MigrateError::Execute(again);
    let StoreError::Unavailable(cause) = super::unavailable(migrating) else {
        panic!("unavailable");
    };
    assert_eq!(cause.args, expected, "the migrator's, the driver's within");
}
