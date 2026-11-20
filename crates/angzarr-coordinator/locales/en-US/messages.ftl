# Messages for angzarr-coordinator's errors. Each id is an error's code
# without the crate's name, which the build prefixes
# (angzarr_diagnostic::build); the variables are its args.
# The crate exports this as angzarr_coordinator::EN_US.

hidden = a request for "{ $domain }" from { $arrival } was answered as an unknown domain: its reach is { $reach }
unimplemented = this endpoint does not implement { $operation }
unauthenticated = the credential is missing, stale or revoked
framework-only = { $operation } may be called by a framework component only
denied-not-exposed = the request for "{ $domain }" from { $arrival } was refused: its reach is { $reach }
denied-type-not-allowed = the principal may not send "{ $type_url }"
denied-body-kind-not-allowed = the principal may not send a body of kind { $kind }
denied-not-owner = the principal does not own a stream the request names in "{ $domain }"
denied-selection-not-allowed = the principal may not use the read operation { $operation }
