use super::*;
use crate::messages::FARM_OK_CODES;

#[test]
fn test_is_benign_connectivity_notice() {
    // Logging-policy invariant: only ConnectivityStatus::Ok (data-farm-OK
    // confirmations) is benign → info. Broken/Inactive/Connecting stay at warn.
    for code in FARM_OK_CODES {
        let notice = Notice::synthesized(code, "farm OK".into());
        assert!(is_benign_connectivity_notice(&notice), "code {code} should be benign");
    }

    // Not benign: broken codes (Broken), inactive/connecting codes (still warn),
    // the range boundaries, and a code outside WARNING_CODE_RANGE entirely.
    for code in [
        2100, 2103, // Market data farm connection is broken
        2105, // HMDS data farm connection is broken
        2157, // Sec-def data farm connection is broken
        2107, 2108, // inactive but available on demand — not benign
        2119, // connecting — not benign
        2169, 200, // outside / boundary
    ] {
        let notice = Notice::synthesized(code, "not benign".into());
        assert!(!is_benign_connectivity_notice(&notice), "code {code} should not be benign");
    }
}

#[test]
fn test_log_unrouted_notice_traverses_all_severities() {
    // Smoke test: the project has no log-capture harness, so we can't assert the
    // emitted level. Drive each branch of log_unrouted_notice to confirm the
    // benign (info), warning (warn), and error paths are reachable and panic-free.
    log_unrouted_notice(&Notice::synthesized(FARM_OK_CODES[0], "farm OK".into()));
    log_unrouted_notice(&Notice::synthesized(2103, "farm broken".into()));
    log_unrouted_notice(&Notice::synthesized(200, "no security definition".into()));
}

#[test]
fn test_fibonacci_backoff() {
    let mut backoff = FibonacciBackoff::new(10);

    assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    assert_eq!(backoff.next_delay(), Duration::from_secs(2));
    assert_eq!(backoff.next_delay(), Duration::from_secs(3));
    assert_eq!(backoff.next_delay(), Duration::from_secs(5));
    assert_eq!(backoff.next_delay(), Duration::from_secs(8));
    assert_eq!(backoff.next_delay(), Duration::from_secs(10)); // capped at max
    assert_eq!(backoff.next_delay(), Duration::from_secs(10)); // stays at max
}

#[test]
fn error_route_owners_reject_cross_namespace_collisions() {
    let owners = ErrorRouteOwners::default();
    let request = owners.claim_request(42).unwrap().commit();
    assert_eq!(owners.origin(42), Some(IdOrigin::Request));
    assert!(matches!(owners.claim_order(42), Err(Error::InvalidArgument(_))));

    owners.release_request(request);
    owners.claim_order(42).unwrap().commit();
    assert_eq!(owners.origin(42), Some(IdOrigin::Order));
    assert!(matches!(owners.claim_request(42), Err(Error::InvalidArgument(_))));
}

#[test]
fn stale_request_cleanup_cannot_release_a_new_generation() {
    let owners = ErrorRouteOwners::default();
    let old = owners.claim_request(42).unwrap().commit();
    owners.release_request(old);
    let _new = owners.claim_request(42).unwrap().commit();

    owners.release_request(old);
    assert_eq!(owners.origin(42), Some(IdOrigin::Request));
}

#[test]
fn error_route_owner_claims_roll_back_only_when_no_write_committed() {
    let owners = ErrorRouteOwners::default();
    drop(owners.claim_order(42).unwrap());
    assert_eq!(owners.origin(42), None);

    let first = owners.claim_order(42).unwrap();
    let second = owners.claim_order(42).unwrap();
    first.commit();
    drop(second);
    assert_eq!(owners.origin(42), Some(IdOrigin::Order));
}

#[test]
fn terminal_order_ownership_has_a_grace_period_then_expires() {
    let owners = ErrorRouteOwners::default();
    owners.claim_order(42).unwrap().commit();
    owners.mark_order_terminal(42);
    assert_eq!(owners.origin(42), Some(IdOrigin::Order));

    owners.mark_order_terminal_with_grace(42, Duration::ZERO);
    assert_eq!(owners.origin(42), None);
    owners.claim_request(42).unwrap().commit();
    assert_eq!(owners.origin(42), Some(IdOrigin::Request));
}

#[test]
fn expiry_heap_prunes_terminal_orders_without_revisiting_their_ids() {
    let owners = ErrorRouteOwners::default();
    for id in 1..=100 {
        owners.claim_order(id).unwrap().commit();
        owners.mark_order_terminal_with_grace(id, Duration::ZERO);
    }
    owners.claim_request(1_000).unwrap().commit();

    let state = owners.state.lock().unwrap();
    assert_eq!(state.entries.len(), 1);
    assert!(state.entries.contains_key(&1_000));
    assert!(state.expirations.is_empty());
}

#[test]
fn failed_order_refresh_restores_expiry_while_successful_refresh_clears_it() {
    let owners = ErrorRouteOwners::default();
    owners.claim_order(42).unwrap().commit();
    owners.mark_order_terminal_with_grace(42, Duration::from_millis(10));
    let failed_refresh = owners.claim_order(42).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    drop(failed_refresh);
    assert_eq!(owners.origin(42), None);

    owners.claim_order(42).unwrap().commit();
    owners.mark_order_terminal_with_grace(42, Duration::from_millis(10));
    owners.claim_order(42).unwrap().commit();
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(owners.origin(42), Some(IdOrigin::Order));
}

#[test]
fn terminal_status_during_pending_order_write_survives_commit() {
    let owners = ErrorRouteOwners::default();
    let initial_write = owners.claim_order(42).unwrap();
    owners.mark_order_terminal_with_grace(42, Duration::ZERO);
    initial_write.commit();
    assert_eq!(owners.origin(42), None);

    owners.claim_order(99).unwrap().commit();
    let refresh = owners.claim_order(99).unwrap();
    owners.mark_order_terminal_with_grace(99, Duration::from_millis(10));
    refresh.commit();

    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(owners.origin(99), None);
}
