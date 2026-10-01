//! Subscription target types for event filtering.
//!
//! Used internally for subscription matching in sagas and process managers.

/// A subscription target: domain + event types to filter.
///
/// Used to declare which events a component subscribes to.
///
/// # Example
///
/// ```
/// use angzarr::descriptor::Target;
///
/// // Subscribe to all events from "order" domain
/// let all_orders = Target::domain("order");
///
/// // Subscribe to specific event types
/// let specific = Target::new("order", vec!["OrderCreated", "OrderShipped"]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Target {
    /// Domain to subscribe to.
    pub domain: String,
    /// Event types to filter. Empty means all events from domain.
    pub types: Vec<String>,
}

impl Target {
    /// Create a target for a domain with specific event types.
    pub fn new(domain: impl Into<String>, types: Vec<impl Into<String>>) -> Self {
        Self {
            domain: domain.into(),
            types: types.into_iter().map(Into::into).collect(),
        }
    }

    /// Create a target for all events from a domain.
    pub fn domain(domain: impl Into<String>) -> Self {
        Self {
            domain: domain.into(),
            types: Vec::new(),
        }
    }

    /// Check if an event type matches this target.
    ///
    /// Returns true if:
    /// - types is empty (matches all), OR
    /// - any configured type matches `event_type` on a token boundary
    ///
    /// Matching rules per configured type `t`:
    /// - If `t` contains `.`, it is a fully-qualified name: it must equal the
    ///   event's fully-qualified name, whatever resolver prefix
    ///   (`type.googleapis.com/`, `/`, none) either side carries.
    /// - Otherwise, take the last `.`/`/`-delimited token of `event_type`
    ///   and require equality with `t`. Short names like `"OrderCreated"`
    ///   match `"type.googleapis.com/example.OrderCreated"` but NOT
    ///   `"type.googleapis.com/example.MyOrderCreated"` — preventing the
    ///   silent fan-out a bare `ends_with` produced.
    pub fn matches_type(&self, event_type: &str) -> bool {
        if self.types.is_empty() {
            return true;
        }
        self.types.iter().any(|t| matches_type_token(event_type, t))
    }
}

/// Token-boundary match between a configured subscription type and an
/// event's `type_url`. See [`Target::matches_type`] for the rules.
fn matches_type_token(event_type: &str, subscription_type: &str) -> bool {
    use crate::proto_ext::type_url::fqn;
    if subscription_type.contains('.') {
        fqn(event_type) == fqn(subscription_type)
    } else {
        let last_token = event_type.rsplit(['.', '/']).next().unwrap_or(event_type);
        last_token == subscription_type
    }
}

/// Parse subscriptions from environment variable.
///
/// Format: `domain1:Type1,Type2;domain2:Type3` or `domain1;domain2` (all types).
/// Whitespace around domains and types is ignored.
///
/// # Example
///
/// ```
/// use angzarr::descriptor::parse_subscriptions;
///
/// // Parse specific types
/// let subs = parse_subscriptions("order:OrderCreated,OrderShipped;inventory:StockReserved");
/// assert_eq!(subs.len(), 2);
/// assert_eq!(subs[0].domain, "order");
/// assert_eq!(subs[0].types, vec!["OrderCreated", "OrderShipped"]);
///
/// // Parse all events from domain
/// let subs = parse_subscriptions("order;inventory");
/// assert_eq!(subs.len(), 2);
/// assert!(subs[0].types.is_empty());
/// ```
pub fn parse_subscriptions(env_value: &str) -> Vec<Target> {
    if env_value.is_empty() {
        return Vec::new();
    }

    env_value
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|part| {
            if let Some((domain, types_str)) = part.split_once(':') {
                let types: Vec<String> = types_str
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
                Target::new(domain.trim(), types)
            } else {
                Target::domain(part)
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "descriptor.test.rs"]
mod tests;
