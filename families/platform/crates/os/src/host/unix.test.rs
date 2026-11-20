//! What a detached spawn's child does before its exec, run here in the
//! test's own process: the descriptor limit read, and descriptors made
//! close-on-exec one by one (where `close_range` is not had).

use std::os::fd::AsRawFd;

use super::{cloexec_below, descriptor_limit, DESCRIPTOR_CAP};

fn soft_limit() -> libc::rlimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: a plain call filling `limit`.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    limit
}

fn set_soft_limit(limit: &libc::rlimit) {
    // SAFETY: a plain call reading `limit`.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, limit) }, 0);
}

/// The limit is the process's soft open-file limit when that is below the
/// cap. Each test runs in a process of its own (nextest), so the lowered
/// limit is this one's alone, and it is put back.
#[test]
fn test_the_descriptor_limit_is_the_soft_limit_below_the_cap() {
    assert_eq!(DESCRIPTOR_CAP, 65536);
    let was = soft_limit();
    let low = libc::rlimit {
        rlim_cur: 512.min(was.rlim_max),
        rlim_max: was.rlim_max,
    };
    set_soft_limit(&low);
    let read = descriptor_limit();
    set_soft_limit(&was);
    assert_eq!(i64::from(read), i64::try_from(low.rlim_cur).unwrap());
}

fn cloexec(fd: libc::c_int) -> bool {
    // SAFETY: a plain call on a descriptor number.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(flags >= 0, "descriptor {fd} is open");
    flags & libc::FD_CLOEXEC != 0
}

fn clear_cloexec(fd: libc::c_int) {
    // SAFETY: a plain call on a descriptor this test owns.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_SETFD, 0) }, 0);
}

/// Each descriptor below the limit is made close-on-exec, from 3 up; the
/// limit itself, and what is above it, are left as they are.
#[test]
fn test_descriptors_below_the_limit_are_made_close_on_exec_and_no_others() {
    let (reader, writer) = std::io::pipe().unwrap();
    let (low, high) = {
        let (a, b) = (reader.as_raw_fd(), writer.as_raw_fd());
        (a.min(b), a.max(b))
    };
    assert!(low >= 3);
    clear_cloexec(low);
    clear_cloexec(high);
    cloexec_below(high);
    assert!(cloexec(low), "below the limit");
    assert!(!cloexec(high), "the limit itself is left");
    cloexec_below(high + 1);
    assert!(cloexec(high), "below the raised limit");
}
