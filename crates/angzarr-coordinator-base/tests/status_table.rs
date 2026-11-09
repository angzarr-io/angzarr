//! `Code` and `Coded::spec` against the router's status table, kept as data
//! (`tests/data/router_status_table.txt`): the router is not a dependency,
//! so its table is copied there and this holds the duplicate to it.

use angzarr_coordinator_base::{Code, Coded};
use angzarr_proto::v1::AngzarrReason;

const TABLE: &str = include_str!("data/router_status_table.txt");

fn rows(kind: &str) -> Vec<Vec<&'static str>> {
    TABLE
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| line.split_whitespace().collect::<Vec<_>>())
        .filter(|row| row.first() == Some(&kind))
        .map(|row| row[1..].to_vec())
        .collect()
}

/// Every code the router has is a `Code` of the same name and number, and
/// `Code` has no code the router lacks except the additions the table marks
/// (`addition <NAME> <wire number>`): UNAUTHENTICATED, which the gate answers
/// a bad or revoked credential with and the router's table does not have.
#[test]
fn the_codes_are_the_routers() {
    let router = rows("code");
    let additions = rows("addition");
    assert_eq!(additions.len(), 1, "UNAUTHENTICATED is the one addition");
    assert_eq!(router.len() + additions.len(), Code::ALL.len());
    for row in &additions {
        assert!(
            !router.iter().any(|r| r[0] == row[0]),
            "{} is the router's, not an addition",
            row[0]
        );
        let number: i32 = row[1].parse().expect("a number");
        let code = Code::from_number(number).unwrap_or_else(|| panic!("no Code {number}"));
        assert_eq!(code.name(), row[0]);
    }
    for row in &router {
        let number: i32 = row[1].parse().expect("a number");
        let code = Code::from_number(number).unwrap_or_else(|| panic!("no Code {number}"));
        assert_eq!(code.name(), row[0]);
    }
    for code in Code::ALL {
        assert!(
            router
                .iter()
                .chain(&additions)
                .any(|row| row[0] == code.name()),
            "{} is in neither the router's table nor the additions",
            code.name()
        );
    }
}

/// Every key the router answers is answered here under the same status.
#[test]
fn the_keys_are_answered_under_the_routers_statuses() {
    let router = rows("key");
    assert!(router.len() >= 13, "the table has its rows");
    for row in router {
        let reason = AngzarrReason::from_str_name(row[0])
            .unwrap_or_else(|| panic!("{} is no live core key", row[0]));
        let coded = Coded::spec(reason, []);
        assert_eq!(coded.key, row[0]);
        assert_eq!(coded.code.name(), row[1], "{}", row[0]);
    }
}
