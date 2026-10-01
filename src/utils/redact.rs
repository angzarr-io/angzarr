//! Credential redaction for connection strings written to logs.

/// `uri` with the password in its userinfo replaced by `***`.
///
/// `scheme://user:secret@host/db` becomes `scheme://user:***@host/db`;
/// a URI without a password (or without userinfo) is returned unchanged.
/// Query parameters are not inspected, so credentials passed that way must
/// not be logged at all.
pub fn redact_uri(uri: &str) -> String {
    let Some(scheme_end) = uri.find("://") else {
        return uri.to_string();
    };
    let authority_start = scheme_end + 3;
    let authority_end = uri[authority_start..]
        .find(['/', '?', '#'])
        .map_or(uri.len(), |i| authority_start + i);
    let authority = &uri[authority_start..authority_end];
    let Some(at) = authority.rfind('@') else {
        return uri.to_string();
    };
    let userinfo = &authority[..at];
    let Some(colon) = userinfo.find(':') else {
        return uri.to_string();
    };
    format!(
        "{}{}:***{}",
        &uri[..authority_start],
        &userinfo[..colon],
        &uri[authority_start + at..]
    )
}

#[cfg(test)]
#[path = "redact.test.rs"]
mod tests;
