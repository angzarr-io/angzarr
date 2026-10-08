//! A CASCADE fan-out calls the same saga/PM endpoints for every command; the
//! cache must hand back one shared channel per endpoint instead of dialing a
//! new connection per call.

use super::*;

#[tokio::test]
async fn test_same_url_reuses_one_channel() {
    let cache = ChannelCache::new();
    assert!(cache.is_empty());
    cache.channel("http://127.0.0.1:1").unwrap();
    cache.channel("http://127.0.0.1:1").unwrap();
    assert_eq!(cache.len(), 1);
    cache.channel("http://127.0.0.1:2").unwrap();
    assert_eq!(cache.len(), 2);
    assert!(!cache.is_empty());
}

#[tokio::test]
async fn test_invalid_url_is_invalid_argument_and_not_cached() {
    let cache = ChannelCache::new();
    let err = cache.channel("not a url\n").unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(cache.is_empty());
}
